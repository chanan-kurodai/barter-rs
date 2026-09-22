use crate::{
    Identifier, MarketStream,
    error::DataError,
    event::MarketEvent,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        reconnect,
        reconnect::stream::{
            ReconnectingStream, ReconnectionBackoffPolicy, init_reconnecting_stream,
        },
    },
    subscription::{Subscription, SubscriptionKind, display_subscriptions_without_exchange},
};
use barter_instrument::exchange::ExchangeId;
use derive_more::Constructor;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display, sync::{Arc, OnceLock}, time::Duration};
use tracing::info;

/// Default [`ReconnectionBackoffPolicy`] for a [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub const STREAM_RECONNECTION_POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
    backoff_ms_initial: 125,
    backoff_multiplier: 2,
    backoff_ms_max: 60000,
};

/// Idle bound between consecutive application items on BOOK streams, measured
/// and signed off in docs/validation/2026-07-02-stream-idle-gap-measurement.md
/// (hairpin-main repo). Rule: max(300, ceil(3 x largest merged-connection
/// organic gap)) = max(300, ceil(3 x 325.2s)) = 976. The merged-connection gap
/// is the quantity this timer actually observes: one timer per (venue, kind)
/// connection, re-armed by ANY instrument's item on that connection.
pub const BOOK_IDLE_MAX_SECS: u64 = 976;

/// Idle bound keyed by subscription kind. Books must never be silent for
/// minutes; trades are legitimately sparse on thin venues (and some venues
/// close idle connections themselves), so trades are DISABLED unless an
/// [`IdleResetPolicy`] enables them for a specific exchange. Unknown kinds
/// default to disabled (safe). Matches BOTH the SubscriptionKind::as_str()
/// forms and the SubKind enum-variant names defensively — a future refactor
/// to SubKind-generic calls must not silently disable the timeout.
fn idle_timeout_for_kind(kind: &str) -> Option<Duration> {
    match kind {
        "l1" | "l2" | "OrderBooksL1" | "OrderBooksL2" => {
            Some(Duration::from_secs(BOOK_IDLE_MAX_SECS))
        }
        _ => None,
    }
}

/// Is this kind string the public-trades stream (either spelling)?
fn is_trades_kind(kind: &str) -> bool {
    matches!(kind, "public_trades" | "PublicTrades" | "trades")
}

/// Idle bound for one (exchange, kind) stream: books keep [`BOOK_IDLE_MAX_SECS`];
/// trades use the process-wide [`IdleResetPolicy`] override for that exchange, if set.
///
/// **What the timer measures.** The gap between consecutive ITEMS yielded by the
/// `MarketStream` after decoding — valid market events and decode-error items alike.
/// WebSocket control frames (ping/pong) and raw bytes that produce no item do NOT
/// re-arm it. A thin market can therefore trip it on natural silence: an idle reset is
/// an intervention, not a confirmed outage.
pub fn idle_timeout_for(exchange: ExchangeId, kind: &str) -> Option<Duration> {
    if is_trades_kind(kind) {
        return idle_reset_policy()
            .and_then(|p| p.trades_idle_max.get(&exchange).copied());
    }
    idle_timeout_for_kind(kind)
}

/// Phase of a reconnect intervention, reported through [`IdleResetPolicy::notify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectPhase {
    /// The idle timer elapsed: the inner stream was ENDED to force a physical
    /// re-initialisation. Records an intervention, not a confirmed outage.
    IdleResetAttempted { idle_secs: u64 },
    /// The (re-)initialisation closure succeeded; `attempt` is 0 for the first
    /// connect of the process and increments per re-initialisation.
    ReconnectSucceeded { attempt: usize },
    /// A re-initialisation attempt failed; the next attempt waits `backoff_ms`.
    ReconnectFailed { attempt: usize, backoff_ms: u64 },
}

/// One reconnect-phase notice for a (exchange, kind) stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectNotice {
    pub exchange: ExchangeId,
    /// The stream's kind string as carried by its [`StreamKey`] (`None` for general streams).
    pub kind: Option<&'static str>,
    pub phase: ReconnectPhase,
}

/// Callback receiving [`ReconnectNotice`]s. Runs inside the stream combinators, so it
/// must be cheap and non-blocking (send on a channel, bump a counter).
pub type ReconnectNotify = Arc<dyn Fn(ReconnectNotice) + Send + Sync>;

/// Process-wide idle-reset policy, set once by the application before streams are
/// initialised (see [`set_idle_reset_policy`]). Absent policy == legacy behaviour:
/// book streams reset after [`BOOK_IDLE_MAX_SECS`], trades never reset.
#[derive(Clone, Default)]
pub struct IdleResetPolicy {
    /// Per-exchange idle bound for the public-trades stream. Exchanges not listed keep
    /// trades resets DISABLED.
    pub trades_idle_max: HashMap<ExchangeId, Duration>,
    /// Optional observer for every reconnect phase (all kinds, all exchanges).
    pub notify: Option<ReconnectNotify>,
}

impl std::fmt::Debug for IdleResetPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdleResetPolicy")
            .field("trades_idle_max", &self.trades_idle_max)
            .field("notify", &self.notify.is_some())
            .finish()
    }
}

static IDLE_RESET_POLICY: OnceLock<IdleResetPolicy> = OnceLock::new();

/// Install the process-wide [`IdleResetPolicy`]. Returns `Err(policy)` if one was
/// already installed (the first one wins; streams already running are unaffected).
pub fn set_idle_reset_policy(policy: IdleResetPolicy) -> Result<(), IdleResetPolicy> {
    IDLE_RESET_POLICY.set(policy)
}

/// The installed policy, if any.
pub fn idle_reset_policy() -> Option<&'static IdleResetPolicy> {
    IDLE_RESET_POLICY.get()
}

/// Convenient type alias for a [`MarketEvent`] [`Result`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamResult<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, Result<MarketEvent<InstrumentKey, Kind>, DataError>>;

/// Convenient type alias for a [`MarketEvent`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamEvent<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, MarketEvent<InstrumentKey, Kind>>;

/// Initialises a [`reconnecting`](`ReconnectingStream`) [`MarketStream`] using a collection of
/// [`Subscription`]s.
///
/// The provided [`ReconnectionBackoffPolicy`] dictates how the exponential backoff scales
/// between reconnections.
pub async fn init_market_stream<Exchange, Instrument, Kind>(
    policy: ReconnectionBackoffPolicy,
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
) -> Result<impl Stream<Item = MarketStreamResult<Instrument::Key, Kind::Event>>, DataError>
where
    Exchange: StreamSelector<Instrument, Kind>,
    Instrument: InstrumentData + Display,
    Kind: SubscriptionKind + Display,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    // Determine ExchangeId associated with these Subscriptions
    let exchange = Exchange::ID;

    // Determine StreamKey for use in logging
    let stream_key = subscriptions
        .first()
        .map(|sub| StreamKey::new("market_stream", exchange, Some(sub.kind.as_str())))
        .ok_or(DataError::SubscriptionsEmpty)?;

    // Kind string is the SubscriptionKind::as_str() of the CONCRETE marker type
    // ("l1"/"l2"/"public_trades") — the dynamic builder constructs concrete-kind
    // subscriptions in every match arm.
    let idle_max = subscriptions
        .first()
        .and_then(|sub| idle_timeout_for(exchange, sub.kind.as_str()));
    let notify = idle_reset_policy().and_then(|p| p.notify.clone());

    info!(
        %exchange,
        subscriptions = %display_subscriptions_without_exchange(&subscriptions),
        ?policy,
        ?stream_key,
        ?idle_max,
        "MarketStream with auto reconnect initialising"
    );

    Ok(init_reconnecting_stream(move || {
        let subscriptions = subscriptions.clone();
        async move { Exchange::Stream::init::<Exchange::SnapFetcher>(&subscriptions).await }
    })
    .await?
    .with_reconnect_backoff_notify(policy, stream_key, notify.clone())
    .with_idle_timeout_notify(idle_max, stream_key, notify)
    .with_termination_on_error(|error| error.is_terminal(), stream_key)
    .with_reconnection_events(exchange))
}

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct StreamKey<Kind = &'static str> {
    pub stream: &'static str,
    pub exchange: ExchangeId,
    pub kind: Option<Kind>,
}

impl StreamKey {
    pub fn new_general(stream: &'static str, exchange: ExchangeId) -> Self {
        Self::new(stream, exchange, None)
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            None => write!(f, "{}-{}", self.stream, self.exchange),
            Some(kind) => write!(f, "{}-{}-{}", self.stream, self.exchange, kind),
        }
    }
}

#[cfg(test)]
mod idle_timeout_tests {
    use super::*;
    use crate::subscription::{
        SubscriptionKind,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    };

    #[test]
    fn kind_mapping_pinned() {
        // Marker-coupled assertions: survive any change to the string values.
        assert!(idle_timeout_for_kind(OrderBooksL1.as_str()).is_some());
        assert!(idle_timeout_for_kind(OrderBooksL2.as_str()).is_some());
        assert!(idle_timeout_for_kind(PublicTrades.as_str()).is_none());
        // Raw-string forms: guard both as_str() values and enum-variant names,
        // so a future refactor to SubKind-generic calls cannot silently
        // disable the timeout.
        assert!(idle_timeout_for_kind("l1").is_some());
        assert!(idle_timeout_for_kind("l2").is_some());
        assert!(idle_timeout_for_kind("OrderBooksL1").is_some());
        assert!(idle_timeout_for_kind("OrderBooksL2").is_some());
        assert!(idle_timeout_for_kind("public_trades").is_none());
        assert!(idle_timeout_for_kind("PublicTrades").is_none());
    }
}

#[cfg(test)]
mod idle_reset_policy_tests {
    use super::*;

    /// One test owns the process-wide OnceLock: before installation trades are disabled
    /// on every exchange (legacy); after installation only the listed exchange's trades
    /// stream gets a bound, books keep theirs, and a second installation is refused.
    #[test]
    fn trades_bound_is_per_exchange_and_books_are_unchanged() {
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "public_trades"), None);
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "l2"), Some(Duration::from_secs(BOOK_IDLE_MAX_SECS)));

        let mut trades_idle_max = HashMap::new();
        trades_idle_max.insert(ExchangeId::BinanceTh, Duration::from_secs(600));
        assert!(set_idle_reset_policy(IdleResetPolicy { trades_idle_max, notify: None }).is_ok());

        for spelling in ["public_trades", "PublicTrades", "trades"] {
            assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, spelling), Some(Duration::from_secs(600)), "{spelling}");
        }
        assert_eq!(idle_timeout_for(ExchangeId::Bitkub, "public_trades"), None, "not listed: still disabled");
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "l2"), Some(Duration::from_secs(BOOK_IDLE_MAX_SECS)));
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "l1"), Some(Duration::from_secs(BOOK_IDLE_MAX_SECS)));
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "liquidations"), None);

        assert!(set_idle_reset_policy(IdleResetPolicy::default()).is_err(), "first policy wins");
        assert_eq!(idle_timeout_for(ExchangeId::BinanceTh, "public_trades"), Some(Duration::from_secs(600)));
    }
}
