use std::sync::Arc;
use std::time::Duration;

use quanta::Instant;
use tokio::sync::RwLock as TokioRwLock;
use tracing::debug;

use super::pool::{
    SessionPool,
    install_if_current,
};
use super::{
    CONNECTION_SLOT_TIMEOUT,
    Forwarder,
    READY_POD_WAIT,
};
use crate::error::Error;
use crate::pod_watch::PodChange;
use crate::recovery::RecoverySignal;
use crate::session::Session;

impl Forwarder {
    pub(super) async fn ensure_session(&self, target_port: u16) -> Result<Arc<Session>, Error> {
        let call_id = self.next_call_id();
        let t_total = Instant::now();
        let mut retry_after_stale_open = true;

        loop {
            let t0 = Instant::now();
            let ready = self
                .pod_watcher
                .wait_for_ready_pod(READY_POD_WAIT)
                .await
                .ok_or_else(|| Error::Configuration("no ready pod available".into()))?;
            tracing::info!(
                call_id,
                elapsed_ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
                "ensure_session: ready_pod resolved"
            );

            // fast path: check under read lock.
            let needs_drain = {
                let pool = self.sessions.read().await;
                pool.target.as_ref() != Some(&ready)
            };
            if needs_drain {
                let drained: Vec<_> = {
                    let mut pool = self.sessions.write().await;
                    // re-check under write lock (another task may have already
                    // drained).
                    if pool.target.as_ref() == Some(&ready) {
                        Vec::new()
                    } else {
                        pool.target = Some(ready.clone());
                        let drained = std::mem::take(&mut pool.entries);
                        pool.refresh_snapshot();
                        drained
                    }
                };
                for pooled in drained {
                    pooled.session.cancellation_token().cancel();
                }
            }

            // reuse an existing non-full session.
            if let Some(s) = self.try_reuse_session(target_port, &ready).await {
                return Ok(s);
            }

            self.retire_dead_sessions().await;

            if let Some(s) = self.find_reusable_session() {
                self.maybe_prefetch(&s, target_port, &ready).await;
                return Ok(s);
            }

            // if another caller is already opening (or prefetching) a
            // session, wait for it instead of opening a duplicate
            //
            // `Notified` future must be created while holding
            // the pool lock (before `drop(pool)`). If created after the lock
            // is dropped, `notify_waiters()` can fire in the gap between
            // `drop(pool)` and `notified()`, causing the notification to be
            // missed and the caller to wait the full timeout.
            {
                let pool = self.sessions.read().await;
                if pool.has_in_flight_open() {
                    // register the waiter before releasing the lock. Creating
                    // the Notified future alone is not enough, registration
                    // happens on first poll, not on creation. enable() forces
                    // registration so any notify_waiters() that fires between
                    // drop(pool) and the .await is captured.
                    let notified = self.session_ready.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(pool);
                    let _ = tokio::time::timeout(Duration::from_secs(5), notified).await;
                    // newly created session should be available now.
                    if let Some(s) = self.try_reuse_session(target_port, &ready).await {
                        return Ok(s);
                    }
                    // failed or was full.
                }
            }

            // decrements opening_count on drop, cancel safe.
            let _slot = self.reserve_new_slot().await?;

            let permit = tokio::time::timeout(
                CONNECTION_SLOT_TIMEOUT,
                self.portforward_semaphore.acquire(),
            )
            .await;
            let _permit = match permit {
                Ok(Ok(p)) => p,
                Ok(Err(_)) => {
                    return Err(Error::Network("connection slot semaphore closed".into()));
                }
                Err(_) => {
                    return Err(Error::Network(
                        "timed out waiting for connection slot".into(),
                    ));
                }
            };

            let t_open = Instant::now();
            let open_result = self.open_session(&ready.name, target_port).await;
            tracing::info!(
                call_id,
                elapsed_ms = u64::try_from(t_open.elapsed().as_millis()).unwrap_or(u64::MAX),
                outcome = if open_result.is_ok() { "ok" } else { "err" },
                "ensure_session: open_session done"
            );

            let session = Arc::new(open_result?);
            match install_if_current(&self.sessions, &ready, session).await {
                Ok(session) => {
                    // wake all callers waiting at the coalescing gate so they
                    // can reuse this session instead of opening more.
                    self.session_ready.notify_waiters();

                    self.maybe_prefetch(&session, target_port, &ready).await;
                    tracing::info!(
                        call_id,
                        elapsed_ms =
                            u64::try_from(t_total.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "ensure_session: total"
                    );
                    return Ok(session);
                }
                Err(stale) => {
                    stale.cancellation_token().cancel();
                    if retry_after_stale_open {
                        retry_after_stale_open = false;
                        continue;
                    }
                    return Err(Error::Configuration(
                        "target pod changed while opening a session".into(),
                    ));
                }
            }
        }
    }

    pub(super) async fn open_session(&self, pod_name: &str, port: u16) -> Result<Session, Error> {
        let cb = Arc::clone(&self.recovery_callback);
        self.pf_client
            .session(&*self.namespace, pod_name, port)
            .cancellation_token(self.session_cancel.child_token())
            .on_recovery(move |signal: RecoverySignal| (cb)(signal))
            .open()
            .await
    }

    pub(super) async fn spawn_pod_change_reactor(&self) {
        let mut rx = self.pod_watcher.subscribe();
        let sessions = Arc::clone(&self.sessions);
        let cancel = self.cancel.clone();
        let recovery_cb = Arc::clone(&self.recovery_callback);
        self.background_tasks.lock().await.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    ev = rx.recv() => match ev {
                        Ok(PodChange::Died(name)) => {
                            if handle_pod_died(&sessions, &name).await {
                                debug!("forwarder: pod {} died, draining sessions", name);
                                (recovery_cb)(RecoverySignal::ServerClose);
                            }
                        }
                        Ok(PodChange::Ready(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    }
                }
            }
        });
    }
}

async fn handle_pod_died(sessions: &TokioRwLock<SessionPool>, dead_pod_name: &str) -> bool {
    let drained = {
        let mut pool = sessions.write().await;
        if pool
            .target
            .as_ref()
            .is_none_or(|target| target.name != dead_pod_name)
        {
            return false;
        }
        pool.target = None;
        let drained = std::mem::take(&mut pool.entries);
        pool.refresh_snapshot();
        drained
    };
    for pooled in drained {
        pooled.session.cancellation_token().cancel();
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::forwarder::pool::{
        PooledSession,
        fake_session,
    };
    use crate::pod_watch::ReadyPod;

    fn ready(name: &str, uid: &str) -> ReadyPod {
        ReadyPod::new(name.into(), Some(uid.into()))
    }

    #[tokio::test]
    async fn died_event_for_a_non_target_pod_does_not_drain_the_pool() {
        let sessions = TokioRwLock::new(SessionPool::new());
        let pod_b = ready("b", "uid-b");
        let session_b = fake_session(8080).await;
        {
            let mut pool = sessions.write().await;
            pool.target = Some(pod_b);
            pool.entries.push(PooledSession {
                session: Arc::clone(&session_b),
                created_at: Instant::now(),
            });
            pool.refresh_snapshot();
        }

        let drained = handle_pod_died(&sessions, "a").await;
        assert!(
            !drained,
            "an old pod's Died event must not match an unrelated target"
        );
        assert!(!session_b.cancellation_token().is_cancelled());
        assert_eq!(sessions.read().await.entries.len(), 1);

        let drained = handle_pod_died(&sessions, "b").await;
        assert!(drained, "Died for the actual target must drain the pool");
        assert!(session_b.cancellation_token().is_cancelled());
        assert!(sessions.read().await.entries.is_empty());
    }
}
