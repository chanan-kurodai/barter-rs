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

/// Idle bounds for TRADES streams, venue-keyed because organic trade-gap
/// profiles differ by ~20x across venues. Measured over the clean days
/// 2026-07-08..14 and signed off in
/// docs/validation/2026-07-20-trades-idle-gap-measurement.md (hairpin-main
/// repo). Rule per venue: ceil(3 x largest merged-connection organic gap).
/// Motivated by the 2026-07-15 incident: Bitkub trades muted on both
/// collectors for ~4.8 days with no timeout to end the silent inner stream.
pub const BITKUB_TRADES_IDLE_MAX_SECS: u64 = 1028; // ceil(3 x 342.341s)
pub const BINANCE_TH_TRADES_IDLE_MAX_SECS: u64 = 2482; // ceil(3 x 827.205s)
pub const ORBIX_TRADES_IDLE_MAX_SECS: u64 = 21245; // ceil(3 x 7081.563s)

/// Idle bound keyed by (exchange, subscription kind). Books use one measured
/// constant everywhere; trades use venue-keyed constants, and are DISABLED on
/// venues without a measurement artifact (an unmeasured threshold could end
/// healthy sparse streams). Unknown kinds default to disabled (safe). Matches
/// BOTH the SubscriptionKind::as_str() forms and the SubKind enum-variant
/// names defensively — a future refactor to SubKind-generic calls must not
/// silently disable the timeout.
fn idle_timeout_for(exchange: ExchangeId, kind: &str) -> Option<Duration> {
    match kind {
        "l1" | "l2" | "OrderBooksL1" | "OrderBooksL2" => {
            Some(Duration::from_secs(BOOK_IDLE_MAX_SECS))
        }
        "public_trades" | "PublicTrades" => match exchange {
            ExchangeId::Bitkub => Some(Duration::from_secs(BITKUB_TRADES_IDLE_MAX_SECS)),
            ExchangeId::BinanceTh => Some(Duration::from_secs(BINANCE_TH_TRADES_IDLE_MAX_SECS)),
            ExchangeId::Orbix => Some(Duration::from_secs(ORBIX_TRADES_IDLE_MAX_SECS)),
            _ => None,
        },
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
        .and_then(|sub| idle_timeout_for(exchange, sub.kind.as_str()));

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
    fn book_mapping_pinned_for_every_exchange() {
        // Books are venue-independent: same measured constant everywhere.
        for exchange in [
            ExchangeId::BinanceTh,
            ExchangeId::Bitkub,
            ExchangeId::Orbix,
            ExchangeId::BinanceSpot,
        ] {
            // Marker-coupled assertions: survive any change to the string values.
            assert_eq!(
                idle_timeout_for(exchange, OrderBooksL1.as_str()),
                Some(Duration::from_secs(BOOK_IDLE_MAX_SECS))
            );
            assert_eq!(
                idle_timeout_for(exchange, OrderBooksL2.as_str()),
                Some(Duration::from_secs(BOOK_IDLE_MAX_SECS))
            );
            // Raw-string forms: guard both as_str() values and enum-variant
            // names, so a future refactor to SubKind-generic calls cannot
            // silently disable the timeout.
            assert!(idle_timeout_for(exchange, "l1").is_some());
            assert!(idle_timeout_for(exchange, "l2").is_some());
            assert!(idle_timeout_for(exchange, "OrderBooksL1").is_some());
            assert!(idle_timeout_for(exchange, "OrderBooksL2").is_some());
        }
    }

    #[test]
    fn trades_mapping_venue_aware() {
        // Trades thresholds are venue-keyed: organic trade-gap profiles differ
        // by ~20x across venues (see the 2026-07-20 measurement artifact).
        let cases = [
            (ExchangeId::Bitkub, BITKUB_TRADES_IDLE_MAX_SECS),
            (ExchangeId::BinanceTh, BINANCE_TH_TRADES_IDLE_MAX_SECS),
            (ExchangeId::Orbix, ORBIX_TRADES_IDLE_MAX_SECS),
        ];
        for (exchange, secs) in cases {
            assert_eq!(
                idle_timeout_for(exchange, PublicTrades.as_str()),
                Some(Duration::from_secs(secs))
            );
            // Enum-variant name form guarded like the books.
            assert_eq!(
                idle_timeout_for(exchange, "PublicTrades"),
                Some(Duration::from_secs(secs))
            );
        }
    }

    #[test]
    fn trades_disabled_for_unmeasured_exchanges() {
        // No measurement artifact for a venue => no trades timeout (fail-safe:
        // an unmeasured threshold could end healthy sparse streams).
        assert!(idle_timeout_for(ExchangeId::BinanceSpot, PublicTrades.as_str()).is_none());
        assert!(idle_timeout_for(ExchangeId::BinanceSpot, "PublicTrades").is_none());
    }

    #[test]
    fn unknown_kind_disabled() {
        assert!(idle_timeout_for(ExchangeId::Bitkub, "liquidations").is_none());
        assert!(idle_timeout_for(ExchangeId::Bitkub, "").is_none());
    }
}
