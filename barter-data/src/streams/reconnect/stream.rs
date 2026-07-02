use crate::streams::{consumer::StreamKey, reconnect::Event};
use barter_integration::channel::Tx;
use derive_more::Constructor;
use futures::Stream;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::{convert, fmt::Debug, future, future::Future, time::Duration};
use tracing::{error, info, warn};

/// Utilities for handling a continually reconnecting [`Stream`] initialised via the
/// [`init_reconnecting_stream`] function.
pub trait ReconnectingStream
where
    Self: Stream + Sized,
{
    /// Add an exponential backoff policy to an initialised [`ReconnectingStream`] using the
    /// provided [`ReconnectionBackoffPolicy`].
    fn with_reconnect_backoff<St, InitError>(
        self,
        policy: ReconnectionBackoffPolicy,
        stream_key: StreamKey,
    ) -> impl Stream<Item = St>
    where
        Self: Stream<Item = Result<St, InitError>>,
        St: Stream,
        InitError: Debug,
    {
        self.enumerate()
            .scan(
                ReconnectionState::from(policy),
                move |state, (attempt, result)| match result {
                    Ok(stream) => {
                        info!(attempt, ?stream_key, "successfully initialised Stream");
                        state.reset_backoff();
                        futures::future::Either::Left(future::ready(Some(Ok(stream))))
                    }
                    Err(error) => {
                        warn!(
                            attempt,
                            ?stream_key,
                            ?error,
                            "failed to re-initialise Stream"
                        );
                        let sleep_fut = state.generate_sleep_future();
                        state.multiply_backoff();
                        futures::future::Either::Right(Box::pin(async move {
                            sleep_fut.await;
                            Some(Err(error))
                        }))
                    }
                },
            )
            .filter_map(|result| future::ready(result.ok()))
    }

    /// Ends the current inner [`Stream`] if no item arrives within `idle_max`,
    /// which causes the outer reconnecting stream to initialize the next
    /// connection. Closes the half-dead-connection hole (2026-07-02 incident):
    /// a venue that keeps the socket open but stops sending data produces no
    /// error item, so `with_termination_on_error` alone never advances.
    /// `None` disables the timer (e.g. legitimately-sparse trade streams).
    ///
    /// The timer bounds the gap between CONSECUTIVE items (tokio-stream's
    /// `timeout` re-arms on every item). `map_while` both unwraps the
    /// `Result<Item, Elapsed>` shape introduced by `timeout` (preserving the
    /// original item type downstream) and actually ENDS the stream on the
    /// first `Elapsed` — `timeout` alone would keep yielding later items.
    fn with_idle_timeout<St>(
        self,
        idle_max: Option<Duration>,
        stream_key: StreamKey,
    ) -> impl Stream<Item = impl Stream<Item = St::Item>>
    where
        Self: Stream<Item = St>,
        St: Stream,
    {
        self.map(move |stream| match idle_max {
            None => futures::future::Either::Left(stream),
            Some(d) => futures::future::Either::Right(tokio_stream::StreamExt::map_while(
                tokio_stream::StreamExt::timeout(stream, d),
                move |item| match item {
                    Ok(inner_item) => Some(inner_item),
                    Err(elapsed) => {
                        warn!(
                            ?stream_key,
                            idle_secs = d.as_secs(),
                            ?elapsed,
                            "no application data within idle_max — ending stream to force reconnect"
                        );
                        None
                    }
                },
            )),
        })
    }

    /// Terminates the inner [`Stream`] if the encountered error is determined to be unrecoverable
    /// by the provided closure. This will cause the [`ReconnectingStream`] to re-initialise the
    /// inner [`Stream`].
    fn with_termination_on_error<St, T, E, FnIsTerminal>(
        self,
        is_terminal: FnIsTerminal,
        stream_key: StreamKey,
    ) -> impl Stream<Item = impl Stream<Item = Result<T, E>>>
    where
        Self: Stream<Item = St>,
        St: Stream<Item = Result<T, E>>,
        FnIsTerminal: Fn(&E) -> bool + Copy,
    {
        self.map(move |stream| {
            tokio_stream::StreamExt::map_while(stream, {
                move |result| match result {
                    Ok(item) => Some(Ok(item)),
                    Err(error) if is_terminal(&error) => {
                        error!(
                            ?stream_key,
                            "MarketStream encountered terminal error that requires reconnecting"
                        );
                        None
                    }
                    Err(error) => Some(Err(error)),
                }
            })
        })
    }

    /// Maps every [`ReconnectingStream`] `Stream::Item` into an [`reconnect::Event::Item`](Event),
    /// and chain a [`reconnect::Event::Reconnecting`](Event)
    fn with_reconnection_events<St, Origin>(
        self,
        origin: Origin,
    ) -> impl Stream<Item = Event<Origin, St::Item>>
    where
        Self: Stream<Item = St>,
        St: Stream,
        Origin: Clone + 'static,
    {
        self.map(move |stream| {
            stream
                .map(Event::Item)
                .chain(futures::stream::once(future::ready(Event::Reconnecting(
                    origin.clone(),
                ))))
        })
        .flatten()
    }

    /// Handles all encountered errors with the provided closure before filtering them out,
    /// returning a [`Stream`] of the Ok values. Useful for logging recoverable errors before
    /// continuing.
    fn with_error_handler<FnOnErr, Origin, T, E>(
        self,
        op: FnOnErr,
    ) -> impl Stream<Item = Event<Origin, T>>
    where
        Self: Stream<Item = Event<Origin, Result<T, E>>>,
        FnOnErr: Fn(E) + 'static,
    {
        self.filter_map(move |event| {
            std::future::ready(match event {
                Event::Reconnecting(origin) => Some(Event::Reconnecting(origin)),
                Event::Item(Ok(item)) => Some(Event::Item(item)),
                Event::Item(Err(error)) => {
                    op(error);
                    None
                }
            })
        })
    }

    /// Future for forwarding items in [`Self`] to the provided channel [`Tx`].
    fn forward_to<Transmitter>(self, tx: Transmitter) -> impl Future<Output = ()> + Send
    where
        Self: Stream + Sized + Send,
        Self::Item: Into<Transmitter::Item>,
        Transmitter: Tx + Send + 'static,
    {
        tokio_stream::StreamExt::map_while(self, move |event| tx.send(event.into()).ok()).collect()
    }
}

impl<T> ReconnectingStream for T where T: Stream {}

/// Initialise a [`ReconnectingStream`] using the provided initialisation closure.
pub async fn init_reconnecting_stream<FnInit, St, FnInitError, FnInitFut>(
    init_stream: FnInit,
) -> Result<impl Stream<Item = Result<St, FnInitError>>, FnInitError>
where
    FnInit: Fn() -> FnInitFut,
    FnInitFut: Future<Output = Result<St, FnInitError>>,
{
    let initial = init_stream().await?;
    let reconnections = futures::stream::repeat_with(init_stream).then(convert::identity);

    Ok(futures::stream::once(future::ready(Ok(initial))).chain(reconnections))
}

/// Reconnection backoff policy for a [`ReconnectingStream::with_reconnect_backoff`].
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize, Constructor,
)]
pub struct ReconnectionBackoffPolicy {
    /// Initial backoff millisecond duration after the first `Stream` disconnection.
    ///
    /// This value then scales with the `backoff_multiplier` in the case of repeated failed
    /// `Stream` reconnection attempts.
    pub backoff_ms_initial: u64,

    /// Scaling factor for the backoff duration in the case of repeated `Stream` reconnection
    /// attempts.
    pub backoff_multiplier: u8,

    /// Maximum possible backoff duration between reconnection attempts.
    pub backoff_ms_max: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize)]
struct ReconnectionState {
    policy: ReconnectionBackoffPolicy,
    backoff_ms_current: u64,
}

impl From<ReconnectionBackoffPolicy> for ReconnectionState {
    fn from(policy: ReconnectionBackoffPolicy) -> Self {
        Self {
            backoff_ms_current: policy.backoff_ms_initial,
            policy,
        }
    }
}

impl ReconnectionState {
    fn reset_backoff(&mut self) {
        self.backoff_ms_current = self.policy.backoff_ms_initial;
    }

    fn multiply_backoff(&mut self) {
        let next = self.backoff_ms_current * self.policy.backoff_multiplier as u64;
        let next_capped = std::cmp::min(next, self.policy.backoff_ms_max);
        self.backoff_ms_current = next_capped;
    }

    fn generate_sleep_future(&self) -> tokio::time::Sleep {
        let sleep_duration = std::time::Duration::from_millis(self.backoff_ms_current);
        tokio::time::sleep(sleep_duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::exchange::ExchangeId;
    use futures_util::StreamExt as FuturesStreamExt;
    use std::time::Duration;

    fn key() -> StreamKey {
        StreamKey::new("test_stream", ExchangeId::BinanceSpot, Some("l2"))
    }

    /// A stream that yields the given items immediately, then stays silent forever.
    fn items_then_silence<T: Clone + 'static>(
        items: Vec<T>,
    ) -> impl Stream<Item = T> {
        futures::stream::iter(items).chain(futures::stream::pending())
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_ends_a_silent_inner_stream() {
        // outer stream of ONE inner stream that never yields
        let outer = futures::stream::iter(vec![futures::stream::pending::<u32>()]);
        let collected: Vec<u32> = outer
            .with_idle_timeout(Some(Duration::from_secs(300)), key())
            .flatten()
            .collect()
            .await;
        // the inner stream ended (idle timeout), yielding nothing — and the
        // test itself terminating proves the end (a pending stream would hang;
        // the paused clock auto-advances past the deadline)
        assert!(collected.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_preserves_items_and_type() {
        let outer = futures::stream::iter(vec![items_then_silence(vec![1u32, 2, 3])]);
        let collected: Vec<u32> = outer
            .with_idle_timeout(Some(Duration::from_secs(300)), key())
            .flatten()
            .collect()
            .await;
        assert_eq!(collected, vec![1, 2, 3]); // items pass through; silence then ends it
    }

    #[tokio::test(start_paused = true)]
    async fn idle_timeout_none_passes_through_and_completes() {
        // None => no timer at all; a FINITE inner stream completes normally
        let outer = futures::stream::iter(vec![futures::stream::iter(vec![7u32, 8])]);
        let collected: Vec<u32> = outer
            .with_idle_timeout(None, key())
            .flatten()
            .collect()
            .await;
        assert_eq!(collected, vec![7, 8]);
    }

    #[tokio::test(start_paused = true)]
    async fn ended_inner_stream_produces_reconnecting_marker_downstream() {
        // The composed behavior the whole fix exists for: the idle timeout ends
        // the current inner stream, which causes with_reconnection_events to
        // emit the Reconnecting marker (same as any inner-stream end).
        let outer = futures::stream::iter(vec![futures::stream::pending::<u32>()]);
        let events: Vec<Event<ExchangeId, u32>> = outer
            .with_idle_timeout(Some(Duration::from_secs(300)), key())
            .with_reconnection_events(ExchangeId::BinanceSpot)
            .collect()
            .await;
        assert!(matches!(events.as_slice(), [Event::Reconnecting(ExchangeId::BinanceSpot)]));
    }
}
