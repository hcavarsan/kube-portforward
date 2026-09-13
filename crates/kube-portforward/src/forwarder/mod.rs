//! Long-lived port-forward orchestrator.
//!
//! [`Forwarder`] sits above [`Session`] and owns:
//!
//! - a [`PodWatcher`] tracking the currently ready pod,
//! - a bounded pool of [`Session`]s drained-on-pod-change,
//! - prune and prefetch background tasks.
//!
//! Callers get a [`Stream`] from [`Forwarder::connect`] without thinking
//! about pod identity, capacity, or recreation after pod rollover.

mod builder;
mod lifecycle;
mod pool;
mod prefetch;

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
pub use builder::ForwarderBuilder;
use pool::{
    SessionPool,
    drain_and_cancel_all,
};
use tokio::sync::{
    Mutex as TokioMutex,
    RwLock as TokioRwLock,
    Semaphore,
    broadcast,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::ReadyPod;
use crate::client::Client;
use crate::error::Error;
use crate::pod_watch::{
    PodChange,
    PodWatcher,
};
use crate::recovery::RecoveryCallback;
use crate::session::Session;
use crate::stream::Stream;

const DEFAULT_MAX_SESSIONS: usize = 128;
const DEFAULT_PRUNE_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_PRUNE_IDLE_AGE: Duration = Duration::from_secs(60);
const DEFAULT_PREFETCH_THRESHOLD: f32 = 0.60;
const READY_POD_WAIT: Duration = Duration::from_secs(5);
const CONNECTION_SLOT_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTION_SLOT_PERMITS: usize = 50;

#[derive(Clone, Copy)]
struct ForwarderConfig {
    max_sessions: usize,
    prune_interval: Duration,
    prune_idle_age: Duration,
    prefetch_threshold: f32,
}

impl Default for ForwarderConfig {
    fn default() -> Self {
        Self {
            max_sessions: DEFAULT_MAX_SESSIONS,
            prune_interval: DEFAULT_PRUNE_INTERVAL,
            prune_idle_age: DEFAULT_PRUNE_IDLE_AGE,
            prefetch_threshold: DEFAULT_PREFETCH_THRESHOLD,
        }
    }
}

/// Long-lived port-forward orchestrator. Construct via [`Forwarder::builder`].
pub struct Forwarder {
    pf_client: Arc<Client>,
    namespace: Arc<str>,
    pod_watcher: Arc<PodWatcher>,
    sessions: Arc<TokioRwLock<SessionPool>>,
    /// Lock-free snapshot of live sessions, shared with
    /// `SessionPool::snapshot`. Updated atomically by `refresh_snapshot`
    /// after every pool mutation. Reads (`find_reusable_session`,
    /// `try_reuse_session`) load this without acquiring any lock.
    session_snap: Arc<ArcSwap<Vec<Arc<Session>>>>,
    config: ForwarderConfig,
    cancel: CancellationToken,
    session_cancel: CancellationToken,
    recovery_callback: RecoveryCallback,
    portforward_semaphore: Arc<Semaphore>,
    background_tasks: Arc<TokioMutex<JoinSet<()>>>,
    call_counter: std::sync::atomic::AtomicU64,
    /// Notified when a new session finishes opening. Concurrent callers
    /// waiting for a session wake up and try to reuse the newly created one
    /// instead of each opening their own.
    session_ready: Arc<tokio::sync::Notify>,
}

impl Forwarder {
    /// Acquire a stream to the target pod's `target_port`. Waits up to 5s
    /// for a ready pod on first call. Opens new sessions on demand and
    /// retires drained ones.
    pub async fn connect(&self, target_port: u16) -> Result<Stream, Error> {
        self.connect_on_pod(target_port)
            .await
            .map(|(stream, _)| stream)
    }

    /// Same as [`connect`](Self::connect), reporting the pod the stream
    /// reaches.
    ///
    /// A named target port resolves to a number in one pod's spec, and a
    /// rollout can map the same name to a different number. A caller that
    /// resolved the port itself needs the pod identity to tell whether the
    /// number it used still applies. The identity carries the UID as well as
    /// the name, because a StatefulSet replacement reuses the name.
    pub async fn connect_on_pod(&self, target_port: u16) -> Result<(Stream, ReadyPod), Error> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(Error::Cancelled),
            result = async {
                if target_port == 0 {
                    return Err(Error::Configuration("target port must be greater than zero".into()));
                }
                for _ in 0..self.config.max_sessions {
                    let (session, pod) = self.ensure_session_for_pod(target_port).await?;
                    match session.connect().await {
                        Ok(stream) => return Ok((stream, pod)),
                        Err(Error::CapacityExhausted { .. }) => {}
                        Err(err) => return Err(err),
                    }
                }
                Err(Error::CapacityExhausted {
                    in_use: 0,
                    capacity: self.config.max_sessions,
                })
            } => result,
        }
    }

    /// Cancellation token tripped by [`Forwarder::shutdown`].
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub fn ready_pod(&self) -> Option<String> {
        self.pod_watcher.ready_pod().map(|p| p.name)
    }

    /// Ready pod with its UID, which a name alone cannot distinguish after a
    /// StatefulSet replaces a pod under the same name.
    pub fn ready_pod_identity(&self) -> Option<ReadyPod> {
        self.pod_watcher.ready_pod()
    }

    pub async fn wait_for_ready_pod(&self, timeout: Duration) -> Option<String> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => None,
            ready = self.pod_watcher.wait_for_ready_pod(timeout) => ready.map(|pod| pod.name),
        }
    }

    pub fn has_running_pods(&self) -> bool {
        self.pod_watcher.has_running_pods()
    }

    pub fn subscribe_pod_changes(&self) -> broadcast::Receiver<PodChange> {
        self.pod_watcher.subscribe()
    }

    /// Cancel background tasks, drain all sessions. Idempotent; callable
    /// through an `Arc<Forwarder>` shared with other owners.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.pod_watcher.shutdown();
        self.cancel.cancel();
        self.session_cancel.cancel();
        drain_and_cancel_all(&self.sessions).await;
        let mut set = self.background_tasks.lock().await;
        set.abort_all();
        while let Some(join_result) = set.join_next().await {
            match join_result {
                Err(e) if e.is_panic() => {
                    tracing::warn!("background task panicked during shutdown: {e}");
                }
                Err(e) if !e.is_cancelled() => {
                    tracing::warn!("background task failed during shutdown: {e}");
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::net::TcpListener;

    use super::*;
    use crate::pod_watch::PodSelector;

    async fn waiting_forwarder() -> (Arc<Forwarder>, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url: http::Uri = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let client = kube::Client::try_from(kube::Config::new(url.clone())).unwrap();
        let forwarder = Forwarder::builder(client, url, "default")
            .pod_selector(PodSelector::Name("waiting".into()))
            .build()
            .await
            .unwrap();
        (Arc::new(forwarder), listener)
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_connections_and_prevents_reopening() {
        let (forwarder, _listener) = waiting_forwarder().await;
        let pending = forwarder.connect(8080);
        tokio::pin!(pending);
        assert!(futures::poll!(&mut pending).is_pending());

        let other_owner = Arc::clone(&forwarder);
        tokio::time::timeout(Duration::from_secs(1), other_owner.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), &mut pending)
                .await
                .unwrap(),
            Err(Error::Cancelled)
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), forwarder.connect(8080))
                .await
                .unwrap(),
            Err(Error::Cancelled)
        ));
        tokio::time::timeout(Duration::from_secs(1), forwarder.shutdown())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn zero_target_port_fails_before_waiting_for_pods() {
        let (forwarder, _listener) = waiting_forwarder().await;
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), forwarder.connect(0))
                .await
                .unwrap(),
            Err(Error::Configuration(_))
        ));
        forwarder.shutdown().await.unwrap();
    }
}
