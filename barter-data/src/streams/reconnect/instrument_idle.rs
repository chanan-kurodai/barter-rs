//! Per-instrument application-data deadlines for a multiplexed connection group.
use crate::{
    event::MarketEvent,
    streams::consumer::{ReconnectNotice, ReconnectNotify, ReconnectPhase, StreamKey},
};
use futures::Stream;
use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::{Instant, Sleep};

pub(super) struct InstrumentIdle<St, Key> {
    stream: Option<Pin<Box<St>>>,
    deadlines: Vec<(Key, Instant)>,
    timer: Pin<Box<Sleep>>,
    idle: Duration,
    key: StreamKey,
    episode: u64,
    notify: Option<ReconnectNotify>,
}

// Only the boxed stream and Sleep are structurally pinned.
impl<St, Key> Unpin for InstrumentIdle<St, Key> {}

impl<St, Key: Eq> InstrumentIdle<St, Key> {
    pub(super) fn new(
        stream: St,
        keys: Vec<Key>,
        idle: Duration,
        key: StreamKey,
        episode: u64,
        notify: Option<ReconnectNotify>,
    ) -> Self {
        let deadline = Instant::now() + idle;
        let mut deadlines = Vec::new();
        for key in keys {
            if !deadlines.iter().any(|(existing, _)| existing == &key) {
                deadlines.push((key, deadline));
            }
        }
        Self {
            stream: Some(Box::pin(stream)),
            deadlines,
            timer: Box::pin(tokio::time::sleep_until(deadline)),
            idle,
            key,
            episode,
            notify,
        }
    }
}

impl<St, Key, Kind, Error> Stream for InstrumentIdle<St, Key>
where
    St: Stream<Item = Result<MarketEvent<Key, Kind>, Error>>,
    Key: Eq + Debug,
{
    type Item = St::Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.stream.is_none() {
            return Poll::Ready(None);
        }
        // Poll the deadline first: a perpetually ready sibling must not starve it.
        if this.timer.as_mut().poll(cx).is_ready() {
            let expired = this
                .deadlines
                .iter()
                .find(|(_, deadline)| *deadline <= Instant::now());
            tracing::warn!(stream_key = ?this.key, instrument = ?expired.map(|(key, _)| key), idle_secs = this.idle.as_secs(),
                "instrument application-data deadline reached — ending group to force reconnect");
            this.stream.take();
            if let Some(notify) = &this.notify {
                notify(ReconnectNotice {
                    exchange: this.key.exchange,
                    kind: this.key.kind,
                    phase: ReconnectPhase::IdleResetAttempted {
                        idle_secs: this.idle.as_secs(),
                    },
                    episode: this.episode,
                });
            }
            return Poll::Ready(None);
        }
        match this.stream.as_mut().unwrap().as_mut().poll_next(cx) {
            Poll::Ready(Some(item)) => {
                // Raw frames, decoding errors and other instruments cannot refresh this key.
                if let Ok(event) = &item {
                    if let Some((_, deadline)) = this
                        .deadlines
                        .iter_mut()
                        .find(|(key, _)| key == &event.instrument)
                    {
                        *deadline = Instant::now() + this.idle;
                        let earliest = this
                            .deadlines
                            .iter()
                            .map(|(_, deadline)| *deadline)
                            .min()
                            .unwrap();
                        this.timer.as_mut().reset(earliest);
                    }
                }
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) => {
                this.stream.take();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
