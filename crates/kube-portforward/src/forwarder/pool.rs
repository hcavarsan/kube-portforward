use std::sync::Arc;
use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};

use arc_swap::ArcSwap;
use quanta::Instant;
use tokio::sync::RwLock as TokioRwLock;

use super::Forwarder;
use crate::pod_watch::ReadyPod;
use crate::session::Session;

pub(super) struct PooledSession {
    pub(super) session: Arc<Session>,
    pub(super) created_at: Instant,
}

pub(super) struct SessionPool {
    pub(super) entries: Vec<PooledSession>,
    pub(super) target: Option<ReadyPod>,
    pub(super) prefetch_in_flight: bool,
    /// Number of sessions currently being opened. Exposed as `Arc<AtomicUsize>`
    /// so [`OpeningSlot`] can decrement it on drop without needing the
    /// `RwLock`, providing cancel-safety for in-flight session opens.
    pub(super) opening_count: Arc<AtomicUsize>,
    /// Lock-free snapshot of live sessions for fast-path reads.
    /// Updated atomically after every mutation to `entries` via
    /// [`refresh_snapshot`]. Readers (`find_reusable_session`,
    /// `try_reuse_session`) load this without acquiring any lock.
    pub(super) snapshot: Arc<ArcSwap<Vec<Arc<Session>>>>,
}

impl SessionPool {
    pub(super) fn new() -> Self {
        Self {
            entries: Vec::new(),
            target: None,
            prefetch_in_flight: false,
            opening_count: Arc::new(AtomicUsize::new(0)),
            snapshot: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }

    /// Rebuild the lock-free snapshot from current entries.
    /// Must be called after every mutation to `entries` so concurrent
    /// readers see a consistent view.
    pub(super) fn refresh_snapshot(&self) {
        let sessions: Vec<Arc<Session>> = self
            .entries
            .iter()
            .map(|e| Arc::clone(&e.session))
            .collect();
        self.snapshot.store(Arc::new(sessions));
    }

    pub(super) fn has_in_flight_open(&self) -> bool {
        self.opening_count.load(Ordering::Relaxed) > 0 || self.prefetch_in_flight
    }
}

/// RAII slot reservation for an in-flight session open.
///
/// Incrementing `opening_count` is paired with decrement-on-drop so callers
/// that get cancelled mid-await (e.g. dropped future, timeout) cannot leak
/// slots. The slot is held for the duration of the open try; on drop
/// (success OR cancellation OR error) the counter is decremented.
#[must_use = "OpeningSlot decrements opening_count on drop; bind to a variable"]
pub(super) struct OpeningSlot {
    counter: Arc<AtomicUsize>,
}

impl OpeningSlot {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for OpeningSlot {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Drain all sessions from the pool under a write lock, then cancel each
/// session's cancellation token outside the lock.
pub(super) async fn drain_and_cancel_all(sessions: &TokioRwLock<SessionPool>) {
    let drained: Vec<PooledSession> = {
        let mut pool = sessions.write().await;
        pool.target = None;
        let drained = std::mem::take(&mut pool.entries);
        pool.refresh_snapshot();
        drained
    };
    for pooled in drained {
        pooled.session.cancellation_token().cancel();
    }
}

pub(super) async fn install_if_current(
    sessions: &TokioRwLock<SessionPool>, opened_for: &ReadyPod, session: Arc<Session>,
) -> Result<Arc<Session>, Arc<Session>> {
    let mut pool = sessions.write().await;
    if pool.target.as_ref() == Some(opened_for) {
        pool.entries.push(PooledSession {
            session: Arc::clone(&session),
            created_at: Instant::now(),
        });
        pool.refresh_snapshot();
        Ok(session)
    } else {
        Err(session)
    }
}

impl Forwarder {
    pub(super) async fn retire_dead_sessions(&self) {
        let retired: Vec<_> = {
            let mut pool = self.sessions.write().await;
            let retired: Vec<_> = pool
                .entries
                .extract_if(.., |entry| {
                    entry.session.cancellation_token().is_cancelled() || entry.session.is_drained()
                })
                .collect();
            if !retired.is_empty() {
                pool.refresh_snapshot();
            }
            retired
        };
        for pooled in retired {
            pooled.session.cancellation_token().cancel();
        }
    }

    /// scan the snapshot (lock-free atomic load).
    /// The hot queue serves as a recency hint via `refresh_snapshot`, but
    /// reads use the snapshot directly to avoid pop-then-push-back races
    /// that can drop valid sessions when the queue is full.
    pub(super) fn find_reusable_session(&self) -> Option<Arc<Session>> {
        let snap = self.session_snap.load();
        for session in snap.iter() {
            if !session.cancellation_token().is_cancelled() && !session.is_full() {
                return Some(Arc::clone(session));
            }
        }
        None
    }

    pub(super) async fn try_reuse_session(
        &self, target_port: u16, ready: &ReadyPod,
    ) -> Option<Arc<Session>> {
        let snap = self.session_snap.load();
        for session in snap.iter() {
            if !session.cancellation_token().is_cancelled() && !session.is_full() {
                let chosen = Arc::clone(session);
                // release the snapshot guard before awaiting.
                drop(snap);
                self.maybe_prefetch(&chosen, target_port, ready).await;
                return Some(chosen);
            }
        }
        None
    }

    /// Reserve a slot for a new session
    pub(super) async fn reserve_new_slot(&self) -> Result<OpeningSlot, crate::error::Error> {
        let pool = self.sessions.write().await;
        let projected = pool.entries.len() + pool.opening_count.load(Ordering::Relaxed);
        if projected >= self.config.max_sessions {
            return Err(crate::error::Error::CapacityExhausted {
                in_use: projected,
                capacity: self.config.max_sessions,
            });
        }
        Ok(OpeningSlot::new(Arc::clone(&pool.opening_count)))
    }

    pub(super) fn next_call_id(&self) -> u64 {
        self.call_counter.fetch_add(1, Ordering::Relaxed)
    }
}

#[cfg(test)]
pub(super) async fn fake_session(port: u16) -> Arc<Session> {
    use tokio::io::{
        AsyncReadExt,
        AsyncWriteExt,
    };

    let (client, mut server) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        loop {
            match server.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    if server.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    let (ws_write, ws_read) = spdy_mux::split_raw_spdy(client);
    let mux = spdy_mux::Session::with_config(
        vec![(ws_write, ws_read)],
        tokio_util::sync::CancellationToken::new(),
        spdy_mux::MuxConfig::default(),
    )
    .await
    .expect("loopback echo peer completes the initial PING handshake");
    Arc::new(Session::from_spdy(
        mux,
        crate::subprotocol::Subprotocol::LegacySpdy,
        port,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready(name: &str, uid: &str) -> ReadyPod {
        ReadyPod::new(name.into(), Some(uid.into()))
    }

    #[tokio::test]
    async fn gate_only_blocks_on_genuine_in_flight_work() {
        let mut pool = SessionPool::new();
        assert!(!pool.has_in_flight_open());

        let session = fake_session(8080).await;
        pool.entries.push(PooledSession {
            session,
            created_at: Instant::now(),
        });
        pool.refresh_snapshot();
        assert!(!pool.has_in_flight_open());

        let slot = OpeningSlot::new(Arc::clone(&pool.opening_count));
        assert!(pool.has_in_flight_open());
        drop(slot);
        assert!(!pool.has_in_flight_open());

        pool.prefetch_in_flight = true;
        assert!(pool.has_in_flight_open());
    }

    #[tokio::test]
    async fn install_rejects_a_session_opened_for_a_stale_target() {
        let sessions = TokioRwLock::new(SessionPool::new());
        let pod_a = ready("a", "uid-a");
        let pod_b = ready("b", "uid-b");
        {
            let mut pool = sessions.write().await;
            pool.target = Some(pod_b.clone());
        }

        let session_a = fake_session(8080).await;
        let result = install_if_current(&sessions, &pod_a, Arc::clone(&session_a)).await;
        assert!(
            result.is_err(),
            "a session opened for a target the pool already moved away from must not be installed"
        );
        assert!(sessions.read().await.entries.is_empty());

        let session_b = fake_session(8080).await;
        let result = install_if_current(&sessions, &pod_b, Arc::clone(&session_b)).await;
        assert!(
            result.is_ok(),
            "a session opened for the still-current target must install"
        );
        drain_and_cancel_all(&sessions).await;
        assert!(session_b.cancellation_token().is_cancelled());
        assert!(!session_a.cancellation_token().is_cancelled());
        session_a.cancellation_token().cancel();
    }
}
