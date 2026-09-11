use std::sync::Arc;
use std::time::Duration;

use quanta::Instant;
use tokio::sync::RwLock as TokioRwLock;
use tracing::debug;

use super::Forwarder;
use super::pool::{
    PooledSession,
    SessionPool,
};
use crate::pod_watch::ReadyPod;
use crate::recovery::RecoverySignal;
use crate::session::Session;

impl Forwarder {
    pub(super) async fn maybe_prefetch(
        &self, session: &Arc<Session>, target_port: u16, ready: &ReadyPod,
    ) {
        // replenish spare streams if below low watermark (non-blocking
        // best-effort). `replenish_spare_streams` internally guards
        // against concurrent runs via a CAS, so spawning multiple tasks
        // is safe but wasteful.
        if session.needs_replenish() && !session.is_full() {
            let session_clone = Arc::clone(session);
            self.background_tasks.lock().await.spawn(async move {
                session_clone.replenish_spare_streams().await;
            });
        }

        // use operating capacity (scheduling cap) for prefetch threshold,
        // not the hard cap. At pool=6 × operating_max=64 this triggers
        // at ~230 active pairs (0.60 × 384).
        let capacity = session.operating_capacity();
        if capacity == 0 {
            return;
        }
        let in_use = session.in_use();
        // `capacity` and `in_use` are bounded by `operating_max * pool_size`,
        // well under 2^24 in every realistic deployment, so the f32 ratio
        // comparison is precise. Going through integers here would just
        // recompute the same threshold less readably.
        #[allow(clippy::cast_precision_loss)]
        if (in_use as f32) < (capacity as f32) * self.config.prefetch_threshold {
            return;
        }

        {
            let mut pool = self.sessions.write().await;
            if pool.prefetch_in_flight
                || pool.entries.len() >= self.config.max_sessions
                || pool.target.as_ref() != Some(ready)
            {
                return;
            }
            pool.prefetch_in_flight = true;
        }

        let pf_client = Arc::clone(&self.pf_client);
        let sessions = Arc::clone(&self.sessions);
        let namespace = Arc::clone(&self.namespace);
        let session_cancel = self.session_cancel.clone();
        let config = self.config;
        let recovery_cb = Arc::clone(&self.recovery_callback);
        let session_ready = Arc::clone(&self.session_ready);
        let target = ready.clone();

        self.background_tasks.lock().await.spawn(async move {
            debug!("forwarder: prefetching session for pod {}", target.name);
            let cb = Arc::clone(&recovery_cb);
            let open_result = pf_client
                .session(&*namespace, &target.name, target_port)
                .cancellation_token(session_cancel.child_token())
                .on_recovery(move |signal: RecoverySignal| (cb)(signal))
                .open()
                .await;

            let mut pool = sessions.write().await;
            pool.prefetch_in_flight = false;
            match open_result {
                Ok(new) => {
                    let ok = pool.entries.len() < config.max_sessions
                        && pool.target.as_ref() == Some(&target);
                    if ok {
                        pool.entries.push(PooledSession {
                            session: Arc::new(new),
                            created_at: Instant::now(),
                        });
                        pool.refresh_snapshot();
                        drop(pool);
                        session_ready.notify_waiters();
                    } else {
                        drop(pool);
                        new.cancellation_token().cancel();
                    }
                }
                Err(e) => debug!("forwarder: prefetch failed: {}", e),
            }
        });
    }

    pub(super) async fn spawn_prune(&self) {
        let sessions = Arc::clone(&self.sessions);
        let cancel = self.cancel.clone();
        let interval_dur = self.config.prune_interval;
        let idle_age = self.config.prune_idle_age;
        self.background_tasks.lock().await.spawn(async move {
            let mut interval = tokio::time::interval(interval_dur);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                tokio::select! {
                    biased;
                    () = cancel.cancelled() => break,
                    _ = interval.tick() => {
                        prune_once(&sessions, idle_age).await;
                    }
                }
            }
        });
    }
}

async fn prune_once(sessions: &Arc<TokioRwLock<SessionPool>>, idle_age: Duration) {
    let dropped: Vec<_> = {
        let mut pool = sessions.write().await;
        let limit = pool.entries.len().saturating_sub(1);
        let dropped: Vec<_> = pool
            .entries
            .extract_if(.., |entry| {
                entry.session.cancellation_token().is_cancelled()
                    || (entry.session.in_use() == 0 && entry.created_at.elapsed() > idle_age)
            })
            .take(limit)
            .collect();
        if !dropped.is_empty() {
            pool.refresh_snapshot();
        }
        dropped
    };
    for pooled in dropped {
        pooled.session.cancellation_token().cancel();
    }
}
