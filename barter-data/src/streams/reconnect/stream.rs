use crate::streams::{
    consumer::{ReconnectNotice, ReconnectNotify, ReconnectPhase, StreamKey},
    reconnect::Event,
};
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
        self.with_reconnect_backoff_notify(policy, stream_key, None)
    }

    /// [`with_reconnect_backoff`](Self::with_reconnect_backoff) that also reports every
    /// (re-)initialisation outcome to `notify`: [`ReconnectPhase::ReconnectSucceeded`]
    /// (attempt 0 = the first connect) or [`ReconnectPhase::ReconnectFailed`] with the
    /// backoff the next attempt will wait. Backoff stays bounded by the policy: a
    /// stream that keeps failing to initialise never enters a tight loop.
    fn with_reconnect_backoff_notify<St, InitError>(
        self,
        policy: ReconnectionBackoffPolicy,
        stream_key: StreamKey,
        notify: Option<ReconnectNotify>,
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
                        if let Some(n) = &notify {
                            n(ReconnectNotice {
                                exchange: stream_key.exchange,
                                kind: stream_key.kind,
                                phase: ReconnectPhase::ReconnectSucceeded { attempt },
                            });
                        }
                        futures::future::Either::Left(future::ready(Some(Ok(stream))))
                    }
                    Err(error) => {
                        warn!(
                            attempt,
                            ?stream_key,
                            ?error,
                            "failed to re-initialise Stream"
                        );
                        let backoff_ms = state.backoff_ms_current;
                        let sleep_fut = state.generate_sleep_future();
                        state.multiply_backoff();
                        if let Some(n) = &notify {
                            n(ReconnectNotice {
                                exchange: stream_key.exchange,
                                kind: stream_key.kind,
                                phase: ReconnectPhase::ReconnectFailed { attempt, backoff_ms },
                            });
                        }
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
    /// The first window arms when the connection stream is yielded, so the
    /// timeout also bounds time-to-first-item after a successful connect —
    /// a connection that comes up muted is caught too.
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
        self.with_idle_timeout_notify(idle_max, stream_key, None)
    }

    /// [`with_idle_timeout`](Self::with_idle_timeout) that also reports
    /// [`ReconnectPhase::IdleResetAttempted`] to `notify` at the moment the silent inner
    /// stream is ended. Ending the inner stream DROPS it (its WebSocket with it) once the
    /// outer stream advances, so no consumer of the old connection survives the reset.
    fn with_idle_timeout_notify<St>(
        self,
        idle_max: Option<Duration>,
        stream_key: StreamKey,
        notify: Option<ReconnectNotify>,
    ) -> impl Stream<Item = impl Stream<Item = St::Item>>
    where
        Self: Stream<Item = St>,
        St: Stream,
    {
        self.map(move |stream| match idle_max {
            None => futures::future::Either::Left(stream),
            Some(d) => {
                let notify = notify.clone();
                futures::future::Either::Right(tokio_stream::StreamExt::map_while(
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
                            if let Some(n) = &notify {
                                n(ReconnectNotice {
                                    exchange: stream_key.exchange,
                                    kind: stream_key.kind,
                                    phase: ReconnectPhase::IdleResetAttempted { idle_secs: d.as_secs() },
                                });
                            }
                            None
                        }
                    },
                ))
            }
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
    use std::time::Duration;

    fn key() -> StreamKey {
        StreamKey::new("test_stream", ExchangeId::BinanceSpot, Some("l2"))
    }

    /// A stream that yields the given items immediately, then stays silent forever.
    fn items_then_silence<T>(items: Vec<T>) -> impl Stream<Item = T> {
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
    async fn idle_timeout_none_never_arms_a_timer() {
        // None + a silent inner stream: even an enormous clock advance must
        // not end the stream — proves the None arm truly has no timer.
        let outer = futures::stream::iter(vec![futures::stream::pending::<u32>()]);
        let stream = outer.with_idle_timeout(None, key()).flatten();
        futures::pin_mut!(stream);
        // poll once first: if a timer existed, this would arm it
        assert!(futures::poll!(stream.next()).is_pending());
        tokio::time::advance(Duration::from_secs(100_000)).await;
        assert!(futures::poll!(stream.next()).is_pending());
    }

    #[tokio::test(start_paused = true)]
    async fn next_connection_delivers_after_idle_end() {
        // The full recovery story: the first connection idles out, the outer
        // stream advances to the next connection, and its data flows.
        let outer = futures::stream::iter(vec![
            items_then_silence(vec![]),     // first connection: muted, idles out
            items_then_silence(vec![9u32]), // next connection: delivers
        ]);
        let collected: Vec<u32> = outer
            .with_idle_timeout(Some(Duration::from_secs(300)), key())
            .flatten()
            .collect()
            .await;
        assert_eq!(collected, vec![9]);
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
        assert!(matches!(
            events.as_slice(),
            [Event::Reconnecting(ExchangeId::BinanceSpot)]
        ));
    }
}

/// Physical idle reset for a trades-style stream (2026-09-14 collector-a incident: a
/// BinanceTH trades socket went silent without a server close and was never re-opened).
/// These tests drive the same combinator chain `init_market_stream` builds, with a
/// paused clock, and observe (a) re-initialisation, (b) the old inner stream being dropped,
/// (c) bounded backoff on failed re-initialisation, (d) re-arming on traffic,
/// (e) independence between a trades stream and a book stream, and (f) the notice sequence.
#[cfg(test)]
mod idle_reset_tests {
    use super::*;
    use crate::streams::consumer::{ReconnectNotice, ReconnectPhase, STREAM_RECONNECTION_POLICY};
    use barter_instrument::exchange::ExchangeId;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use tokio::sync::mpsc;

    const IDLE: Duration = Duration::from_secs(600);

    fn trades_key() -> StreamKey {
        StreamKey::new("market_stream", ExchangeId::BinanceTh, Some("public_trades"))
    }
    fn book_key() -> StreamKey {
        StreamKey::new("market_stream", ExchangeId::BinanceTh, Some("l2"))
    }

    /// An inner "connection": a channel-fed stream whose Drop is observable.
    struct Conn {
        rx: mpsc::UnboundedReceiver<u32>,
        dropped: Arc<AtomicBool>,
    }
    impl Drop for Conn {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }
    impl Stream for Conn {
        type Item = u32;
        fn poll_next(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<u32>> {
            self.rx.poll_recv(cx)
        }
    }

    struct Recorder {
        notices: Arc<Mutex<Vec<(ReconnectNotice, Duration)>>>,
        t0: tokio::time::Instant,
    }
    impl Recorder {
        fn new() -> (Self, ReconnectNotify) {
            let notices = Arc::new(Mutex::new(Vec::new()));
            let t0 = tokio::time::Instant::now();
            let n2 = notices.clone();
            let f: ReconnectNotify = Arc::new(move |n| {
                n2.lock().unwrap().push((n, tokio::time::Instant::now() - t0));
            });
            (Self { notices, t0 }, f)
        }
        fn phases(&self) -> Vec<ReconnectPhase> {
            self.notices.lock().unwrap().iter().map(|(n, _)| n.phase).collect()
        }
    }

    /// Build the production chain over a sequence of prepared connections.
    async fn chain(
        conns: Vec<Result<Conn, &'static str>>,
        key: StreamKey,
        idle: Option<Duration>,
        notify: Option<ReconnectNotify>,
        init_count: Arc<AtomicUsize>,
    ) -> impl Stream<Item = Event<ExchangeId, u32>> {
        let conns = Arc::new(Mutex::new(conns.into_iter()));
        let init = move || {
            let conns = conns.clone();
            let init_count = init_count.clone();
            async move {
                init_count.fetch_add(1, Ordering::SeqCst);
                conns.lock().unwrap().next().unwrap_or(Err("exhausted"))
            }
        };
        init_reconnecting_stream(init)
            .await
            .expect("first connect succeeds in these tests")
            .with_reconnect_backoff_notify(STREAM_RECONNECTION_POLICY, key, notify.clone())
            .with_idle_timeout_notify(idle, key, notify)
            .with_reconnection_events(key.exchange)
    }

    fn conn() -> (Conn, mpsc::UnboundedSender<u32>, Arc<AtomicBool>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let dropped = Arc::new(AtomicBool::new(false));
        (Conn { rx, dropped: dropped.clone() }, tx, dropped)
    }

    #[tokio::test(start_paused = true)]
    async fn silent_trades_stream_is_closed_and_reinitialised() {
        let (c1, _tx1, _) = conn();
        let (c2, tx2, _) = conn();
        let inits = Arc::new(AtomicUsize::new(0));
        let (rec, notify) = Recorder::new();
        let s = chain(vec![Ok(c1), Ok(c2)], trades_key(), Some(IDLE), Some(notify), inits.clone()).await;
        futures::pin_mut!(s);
        // c1 never speaks; the paused clock auto-advances to the idle deadline
        let first = s.next().await.unwrap();
        // the Reconnecting marker surfaces when the silent connection is ENDED; the next
        // connection is initialised on the following poll
        assert!(matches!(first, Event::Reconnecting(ExchangeId::BinanceTh)));
        tx2.send(9).unwrap();
        assert!(matches!(s.next().await.unwrap(), Event::Item(9)));
        assert_eq!(inits.load(Ordering::SeqCst), 2, "a second connection was initialised");
        let reset_at = rec.notices.lock().unwrap()[1].1;
        assert_eq!(reset_at, IDLE, "reset fired exactly at the idle bound");
    }

    #[tokio::test(start_paused = true)]
    async fn old_connection_is_dropped_and_its_late_items_never_surface() {
        let (c1, tx1, dropped1) = conn();
        let (c2, tx2, _) = conn();
        let s = chain(vec![Ok(c1), Ok(c2)], trades_key(), Some(IDLE), None, Arc::new(AtomicUsize::new(0))).await;
        futures::pin_mut!(s);
        assert!(matches!(s.next().await.unwrap(), Event::Reconnecting(_)));
        assert!(dropped1.load(Ordering::SeqCst), "old connection dropped after the reset");
        // a message on the OLD connection after the reset must not reappear
        assert!(tx1.send(1).is_err(), "old receiver is gone: sender sees closed");
        tx2.send(2).unwrap();
        assert!(matches!(s.next().await.unwrap(), Event::Item(2)));
    }

    #[tokio::test(start_paused = true)]
    async fn failed_reinitialisation_backs_off_and_is_bounded() {
        let (c1, _tx1, _) = conn();
        let (c2, tx2, _) = conn();
        let (rec, notify) = Recorder::new();
        let inits = Arc::new(AtomicUsize::new(0));
        let s = chain(
            vec![Ok(c1), Err("down"), Err("down"), Err("down"), Ok(c2)],
            trades_key(), Some(IDLE), Some(notify), inits.clone(),
        ).await;
        futures::pin_mut!(s);
        assert!(matches!(s.next().await.unwrap(), Event::Reconnecting(_)));
        tx2.send(5).unwrap();
        assert!(matches!(s.next().await.unwrap(), Event::Item(5)));
        let phases = rec.phases();
        assert_eq!(
            phases,
            vec![
                ReconnectPhase::ReconnectSucceeded { attempt: 0 },
                ReconnectPhase::IdleResetAttempted { idle_secs: 600 },
                ReconnectPhase::ReconnectFailed { attempt: 1, backoff_ms: 125 },
                ReconnectPhase::ReconnectFailed { attempt: 2, backoff_ms: 250 },
                ReconnectPhase::ReconnectFailed { attempt: 3, backoff_ms: 500 },
                ReconnectPhase::ReconnectSucceeded { attempt: 4 },
            ]
        );
        // the retries took the backoff time, not a tight loop: 125+250+500 ms after the reset
        let t = &rec.notices.lock().unwrap();
        let reset = t[1].1;
        let ok = t[5].1;
        assert_eq!(ok - reset, Duration::from_millis(875));
        assert_eq!(inits.load(Ordering::SeqCst), 5);
    }

    #[tokio::test(start_paused = true)]
    async fn backoff_is_capped_at_policy_max() {
        let (c1, _tx1, _) = conn();
        let mut conns: Vec<Result<Conn, &'static str>> = vec![Ok(c1)];
        conns.extend(std::iter::repeat_with(|| Err("down")).take(12));
        let (c2, tx2, _) = conn();
        conns.push(Ok(c2));
        let (rec, notify) = Recorder::new();
        let s = chain(conns, trades_key(), Some(IDLE), Some(notify), Arc::new(AtomicUsize::new(0))).await;
        futures::pin_mut!(s);
        assert!(matches!(s.next().await.unwrap(), Event::Reconnecting(_)));
        tx2.send(1).unwrap();
        assert!(matches!(s.next().await.unwrap(), Event::Item(1)));
        let backoffs: Vec<u64> = rec.phases().into_iter().filter_map(|p| match p {
            ReconnectPhase::ReconnectFailed { backoff_ms, .. } => Some(backoff_ms), _ => None }).collect();
        assert_eq!(backoffs[..4], [125, 250, 500, 1000]);
        assert_eq!(*backoffs.last().unwrap(), 60_000, "capped at backoff_ms_max");
        assert!(backoffs.windows(2).all(|w| w[1] >= w[0]));
    }

    #[tokio::test(start_paused = true)]
    async fn traffic_within_the_bound_rearms_the_timer() {
        let (c1, tx1, dropped1) = conn();
        let inits = Arc::new(AtomicUsize::new(0));
        let s = chain(vec![Ok(c1)], trades_key(), Some(IDLE), None, inits.clone()).await;
        futures::pin_mut!(s);
        for i in 0..5u32 {
            tokio::time::advance(IDLE - Duration::from_secs(1)).await; // 599 s of silence, then a trade
            tx1.send(i).unwrap();
            assert!(matches!(s.next().await.unwrap(), Event::Item(x) if x == i));
        }
        assert_eq!(inits.load(Ordering::SeqCst), 1, "no reset across ~50 minutes of sparse traffic");
        assert!(!dropped1.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn resetting_trades_leaves_the_book_stream_untouched() {
        let (t1, _ttx1, t_dropped) = conn();
        let (t2, ttx2, _) = conn();
        let (b1, btx1, b_dropped) = conn();
        let t_inits = Arc::new(AtomicUsize::new(0));
        let b_inits = Arc::new(AtomicUsize::new(0));
        let trades = chain(vec![Ok(t1), Ok(t2)], trades_key(), Some(IDLE), None, t_inits.clone()).await;
        let book = chain(vec![Ok(b1)], book_key(), Some(Duration::from_secs(976)), None, b_inits.clone()).await;
        futures::pin_mut!(trades);
        futures::pin_mut!(book);
        // the book keeps talking every 100 s while trades stay silent past 600 s
        for i in 0..7u32 {
            tokio::time::advance(Duration::from_secs(100)).await;
            btx1.send(i).unwrap();
            assert!(matches!(book.next().await.unwrap(), Event::Item(x) if x == i));
        }
        assert!(matches!(trades.next().await.unwrap(), Event::Reconnecting(_)));
        ttx2.send(42).unwrap();
        assert!(matches!(trades.next().await.unwrap(), Event::Item(42)));
        assert_eq!(t_inits.load(Ordering::SeqCst), 2);
        assert_eq!(b_inits.load(Ordering::SeqCst), 1, "book stream never re-initialised");
        assert!(t_dropped.load(Ordering::SeqCst) && !b_dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn notices_distinguish_reset_attempted_reconnected_and_first_data() {
        let (c1, _tx1, _) = conn();
        let (c2, tx2, _) = conn();
        let (rec, notify) = Recorder::new();
        let s = chain(vec![Ok(c1), Ok(c2)], trades_key(), Some(IDLE), Some(notify), Arc::new(AtomicUsize::new(0))).await;
        futures::pin_mut!(s);
        let ev1 = s.next().await.unwrap();
        // 1. reset attempted: reported when the silent connection is ended, and the
        //    Reconnecting marker surfaces at that moment
        assert!(matches!(ev1, Event::Reconnecting(_)));
        assert_eq!(rec.phases(), vec![
            ReconnectPhase::ReconnectSucceeded { attempt: 0 },
            ReconnectPhase::IdleResetAttempted { idle_secs: 600 },
        ]);
        // 2. reconnect succeeded: reported when the next connection initialises, which
        //    happens on the next poll — before any data
        tx2.send(7).unwrap();
        let ev2 = s.next().await.unwrap();
        assert_eq!(rec.phases().last().copied(), Some(ReconnectPhase::ReconnectSucceeded { attempt: 1 }));
        // 3. first data after the reset is a distinct, later event
        assert!(matches!(ev2, Event::Item(7)), "first data after the reset");
        let n = &rec.notices.lock().unwrap();
        assert_eq!(n[1].0.kind, Some("public_trades"));
        assert_eq!(n[1].0.exchange, ExchangeId::BinanceTh);
    }
}
