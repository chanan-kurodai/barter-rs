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
use std::{fmt::Display, time::Duration};
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
/// close idle connections themselves), so trades are DISABLED. Unknown kinds
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
        .and_then(|sub| idle_timeout_for_kind(sub.kind.as_str()));

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
    .with_reconnect_backoff(policy, stream_key)
    .with_idle_timeout(idle_max, stream_key)
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
