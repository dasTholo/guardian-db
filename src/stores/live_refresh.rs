//! The live index task's loop: drain an iroh-docs event stream and refresh
//! the in-memory index for the events that ask for it — without ever making
//! the stream wait on a refresh.
//!
//! WHY THE DRAIN AND THE REFRESH ARE TWO HALVES. iroh-docs' sync actor hands
//! every replica event to every subscriber by AWAITING that subscriber's
//! bounded channel (`SUBSCRIBE_CHANNEL_CAP`, 256), inside the insert that
//! raised it. A refresh starts with `get_many`, a request to that same
//! actor. The loop this replaces awaited the refresh between two reads of
//! the stream, so a sync that delivered more than 256 entries while a
//! refresh was pending filled the channel: the actor waited on the loop, the
//! loop waited on the actor, and every later docs call of that node — every
//! read the daemon's tick makes — hung for good. The `NZ` rehearsal hit it
//! with ~250 revisions plus their images (2026-10-19); `tests/live_index_burst.rs`
//! holds it down with 600 rows.
//!
//! Here the drain only records that a refresh is wanted, and the refresh runs
//! beside it in the same task, so the stream is read while a refresh waits on
//! the actor.
//!
//! COALESCED, which the old loop was not: it ran one FULL refresh per
//! `InsertRemote`, O(N) each, O(N²) over a burst of N rows. Every wanted
//! event that arrives while a refresh runs folds into ONE refresh after it —
//! never zero: an event is always followed by a refresh that STARTED after
//! it, so the index still converges on the document's latest state.

use futures::{Stream, StreamExt};
use std::future::Future;
use tokio::sync::mpsc;

/// Drain `events`; for each one `wants_refresh` selects, make sure a
/// `refresh` starts after it. Returns once the stream has ended and the
/// refresh it left pending has run.
pub(crate) async fn refresh_on_events<S, P, F, Fut>(events: S, wants_refresh: P, mut refresh: F)
where
    S: Stream + Unpin,
    P: Fn(&S::Item) -> bool,
    F: FnMut() -> Fut,
    Fut: Future<Output = ()>,
{
    // Capacity 1 is the coalescing: a token in the channel is "a refresh is
    // pending and has not started yet".
    let (pending_tx, mut pending_rx) = mpsc::channel::<()>(1);
    let drain = async move {
        let mut events = events;
        while let Some(event) = events.next().await {
            if wants_refresh(&event) {
                // `Full` is a refresh already pending, which starts after
                // this event anyway — nothing is lost by dropping the token.
                let _ = pending_tx.try_send(());
            }
        }
        // `pending_tx` drops here: the refresh half ends after what is pending.
    };
    let refreshes = async move {
        while pending_rx.recv().await.is_some() {
            refresh().await;
        }
    };
    tokio::join!(drain, refreshes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::SinkExt;
    use futures::channel::mpsc as fmpsc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::{Notify, oneshot};

    /// The deadlock's shape: the event source is a SMALL bounded channel and
    /// the first refresh cannot finish until the source has delivered far
    /// more than that channel holds. The old loop hung right here.
    #[tokio::test]
    async fn the_stream_keeps_draining_while_a_refresh_waits() {
        let (mut tx, rx) = fmpsc::channel::<u32>(4);
        let started = Arc::new(Notify::new());
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Mutex::new(Some(release_rx)));
        let count = Arc::new(AtomicUsize::new(0));

        let run = tokio::spawn({
            let (started, release, count) = (started.clone(), release.clone(), count.clone());
            refresh_on_events(rx, |_| true, move || {
                let (started, release, count) = (started.clone(), release.clone(), count.clone());
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    if let Some(gate) = release.lock().await.take() {
                        gate.await.ok();
                    }
                }
            })
        });

        tx.send(0).await.unwrap();
        started.notified().await;
        // 100 events into a channel of 4 while the first refresh stands still.
        tokio::time::timeout(Duration::from_secs(10), async {
            for i in 1..=100 {
                tx.send(i).await.unwrap();
            }
        })
        .await
        .expect("the events are drained while the refresh waits");
        release_tx.send(()).unwrap();
        drop(tx);

        tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("the loop ends with its stream")
            .unwrap();
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "the first refresh, and ONE for the 100 events that arrived during it"
        );
    }

    #[tokio::test]
    async fn an_event_after_a_refresh_gets_its_own_refresh() {
        let (mut tx, rx) = fmpsc::channel::<u32>(4);
        let count = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(Notify::new());
        let run = tokio::spawn({
            let (count, done) = (count.clone(), done.clone());
            refresh_on_events(rx, |_| true, move || {
                let (count, done) = (count.clone(), done.clone());
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    done.notify_one();
                }
            })
        });
        for i in 0..3 {
            tx.send(i).await.unwrap();
            done.notified().await;
        }
        drop(tx);
        run.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 3, "never folded into a refresh that ran before it");
    }

    #[tokio::test]
    async fn events_the_predicate_declines_refresh_nothing() {
        let events = futures::stream::iter([1u32, 3, 5]);
        let count = AtomicUsize::new(0);
        refresh_on_events(events, |n| n % 2 == 0, || {
            count.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
