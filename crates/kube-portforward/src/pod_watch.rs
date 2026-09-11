//! Generic Kubernetes pod watcher.
//!
//! Tracks pod readiness in a namespace using `kube_runtime::reflector` and
//! resolves a [`PodSelector`] (label expression or pod name) to a currently
//! ready pod. Lifecycle events are broadcast on a [`tokio::sync::broadcast`]
//! channel as [`PodChange`].

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    Api,
    ResourceExt,
};
use kube_runtime::{
    WatchStreamExt,
    reflector::{
        self,
        ReflectHandle,
        Store,
    },
    watcher::{
        self,
        Config as WatcherConfig,
    },
};
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{
    debug,
    error,
};

use crate::error::Error;

/// Pod selection strategy.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PodSelector {
    /// Match a single pod by exact name.
    Name(String),
    /// Match pods by a Kubernetes label selector expression
    /// (e.g. `app=nginx,tier=frontend`).
    Labels { selector: String },
}

/// Policy selecting which pods are considered targetable.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum PodReadiness {
    /// Pod phase is `Running`, its `Ready` condition is `True`, and it
    /// is not terminating (no `deletionTimestamp`).
    #[default]
    Ready,
    /// Pod phase is `Running` and the pod is not terminating (no
    /// `deletionTimestamp`), regardless of its `Ready` condition. Useful to
    /// detect a pod mid-rollout before its readiness probe passes.
    Running,
}

/// Pod lifecycle change sent to subscribers.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum PodChange {
    /// A pod matching the selector became ready (or replaced a previously
    /// ready pod). Has the new pod name.
    Ready(String),
    /// The previously ready pod was deleted. Has the dead pod name.
    Died(String),
}

/// Snapshot of the currently ready pod.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReadyPod {
    pub name: String,
    pub uid: Option<String>,
}

impl ReadyPod {
    /// Create a new `ReadyPod` snapshot.
    pub const fn new(name: String, uid: Option<String>) -> Self {
        Self { name, uid }
    }
}

/// Watches pods in a namespace and tracks the currently ready pod matching
/// a [`PodSelector`]. Owns a background reflector task that's aborted on
/// [`PodWatcher::shutdown`].
pub struct PodWatcher {
    store: Store<Pod>,
    _subscriber: ReflectHandle<Pod>,
    latest_ready: Arc<ArcSwapOption<ReadyPod>>,
    change_tx: broadcast::Sender<PodChange>,
    selector: PodSelector,
    reflector_task: JoinHandle<()>,
    subscriber_task: JoinHandle<()>,
    cancel: CancellationToken,
    readiness: PodReadiness,
}

impl Drop for PodWatcher {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.reflector_task.abort();
        self.subscriber_task.abort();
    }
}

impl PodWatcher {
    /// Start a watcher against `namespace` selecting pods by `selector`.
    pub fn new(
        client: kube::Client, namespace: &str, selector: PodSelector, readiness: PodReadiness,
    ) -> impl Future<Output = Result<Self, Error>> {
        futures::future::lazy(move |_| {
            let label_expr = match &selector {
                PodSelector::Labels { selector } => selector.clone(),
                PodSelector::Name(_) => String::new(),
            };

            let (store, writer) = reflector::store_shared(256);
            let subscriber = writer.subscribe().ok_or_else(|| {
                Error::Configuration("failed to create pod reflector subscriber".into())
            })?;

            let cancel = CancellationToken::new();
            let latest_ready: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
            let (change_tx, _) = broadcast::channel(16);

            let pods_api: Api<Pod> = Api::namespaced(client, namespace);
            let watcher_config = if label_expr.is_empty() {
                WatcherConfig::default()
            } else {
                WatcherConfig::default().labels(&label_expr)
            };

            let reflector_cancel = cancel.clone();
            let reflector_latest = Arc::clone(&latest_ready);
            let reflector_change_tx = change_tx.clone();
            let reflector_task = tokio::spawn(async move {
                let stream = watcher::watcher(pods_api, watcher_config)
                    .default_backoff()
                    .modify(|pod| {
                        pod.managed_fields_mut().clear();
                        pod.annotations_mut().clear();
                        if let Some(status) = &mut pod.status {
                            status.container_statuses = None;
                            status.init_container_statuses = None;
                            status.ephemeral_container_statuses = None;
                        }
                    })
                    .reflect_shared(writer);

                let mut stream = std::pin::pin!(stream);
                loop {
                    tokio::select! {
                        biased;
                        () = reflector_cancel.cancelled() => break,
                        next = stream.next() => match next {
                            Some(Ok(watcher::Event::Delete(pod))) => {
                                handle_deleted_pod(
                                    &reflector_latest,
                                    &pod.name_any(),
                                    pod.metadata.uid.as_deref(),
                                    &reflector_change_tx,
                                );
                            }
                            Some(Ok(_)) => {}
                            Some(Err(e)) => {
                                error!("pod reflector error: {}", e);
                                tokio::select! {
                                    biased;
                                    () = reflector_cancel.cancelled() => break,
                                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                                }
                            }
                            None => break,
                        },
                    }
                }
            });

            let subscriber_cancel = cancel.clone();
            let subscriber_latest = Arc::clone(&latest_ready);
            let subscriber_change_tx = change_tx.clone();
            let subscriber_selector = selector.clone();
            let subscriber_readiness = readiness;
            let subscriber_handle = subscriber.clone();
            let subscriber_task = tokio::spawn(async move {
                let mut stream = std::pin::pin!(subscriber_handle);
                loop {
                    tokio::select! {
                        biased;
                        () = subscriber_cancel.cancelled() => break,
                        next = stream.next() => match next {
                            Some(pod) => {
                                update_latest(
                                    &subscriber_latest,
                                    &pod,
                                    &subscriber_selector,
                                    subscriber_readiness,
                                    &subscriber_change_tx,
                                );
                            }
                            None => break,
                        },
                    }
                }
            });

            Ok(Self {
                store,
                _subscriber: subscriber,
                latest_ready,
                change_tx,
                selector,
                reflector_task,
                subscriber_task,
                cancel,
                readiness,
            })
        })
    }

    /// Returns the currently ready pod, if any. Single scan of the store.
    pub fn ready_pod(&self) -> Option<ReadyPod> {
        let cached = self.latest_ready.load_full();
        let mut first_ready: Option<ReadyPod> = None;

        for pod in self.store.state() {
            if !is_pod_selected(&pod, &self.selector, self.readiness) {
                continue;
            }
            if let Some(c) = &cached {
                if pod.name_any() == c.name && pod.metadata.uid.as_deref() == c.uid.as_deref() {
                    return Some((**c).clone());
                }
            }
            // track first ready pod as fallback
            if first_ready.is_none() {
                first_ready = Some(ReadyPod {
                    name: pod.name_any(),
                    uid: pod.metadata.uid.clone(),
                });
            }
        }

        match first_ready {
            Some(ready) => {
                self.latest_ready.store(Some(Arc::new(ready.clone())));
                Some(ready)
            }
            None => {
                self.latest_ready.store(None);
                None
            }
        }
    }

    /// Wait until a ready pod shows up or `timeout` elapses.
    /// Wakes on `PodChange` events instead of polling.
    pub async fn wait_for_ready_pod(&self, timeout: Duration) -> Option<ReadyPod> {
        let mut rx = self.subscribe();
        if self.cancel.is_cancelled() {
            return None;
        }
        if let Some(pod) = self.ready_pod() {
            return Some(pod);
        }

        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => return None,
                () = &mut deadline => return None,
                ev = rx.recv() => {
                    match ev {
                        Ok(PodChange::Ready(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {
                            if let Some(pod) = self.ready_pod() {
                                return Some(pod);
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => return None,
                        _ => {}
                    }
                }
            }
        }
    }

    /// Subscribe to pod lifecycle events. Each new subscriber only sees
    /// events sent after the subscription.
    pub fn subscribe(&self) -> broadcast::Receiver<PodChange> {
        self.change_tx.subscribe()
    }

    /// Cancel background tasks. Idempotent.
    pub fn shutdown(&self) {
        self.cancel.cancel();
    }

    /// Whether a selected pod is currently in the `Running` phase and not
    /// terminating, regardless of the watcher's configured
    /// [`PodReadiness`] policy. Useful to detect a pending rollout (new
    /// pod up but not yet passing its readiness probe) even when the
    /// watcher itself is configured with [`PodReadiness::Ready`].
    pub fn has_running_pods(&self) -> bool {
        self.store
            .state()
            .into_iter()
            .any(|pod| is_pod_selected(&pod, &self.selector, PodReadiness::Running))
    }

    pub(crate) fn contains_identity(&self, name: &str, uid: Option<&str>) -> bool {
        self.store
            .state()
            .into_iter()
            .any(|pod| pod.name_any() == name && pod.metadata.uid.as_deref() == uid)
    }
}

fn update_latest(
    latest: &Arc<ArcSwapOption<ReadyPod>>, pod: &Pod, selector: &PodSelector,
    readiness: PodReadiness, change_tx: &broadcast::Sender<PodChange>,
) {
    if !is_pod_selected(pod, selector, readiness) {
        return;
    }

    let name = pod.name_any();
    let uid = pod.metadata.uid.clone();

    let prev = latest.load();
    let changed = match prev.as_deref() {
        Some(cur) if cur.name == name => cur.uid != uid,
        Some(_) => false,
        None => true,
    };

    if changed {
        let ready = Arc::new(ReadyPod {
            name: name.clone(),
            uid,
        });
        latest.store(Some(ready));
        debug!("pod_watch: ready pod changed to {}", name);
        let _ = change_tx.send(PodChange::Ready(name));
    }
}

fn handle_deleted_pod(
    latest: &Arc<ArcSwapOption<ReadyPod>>, name: &str, uid: Option<&str>,
    change_tx: &broadcast::Sender<PodChange>,
) {
    let prev = latest.rcu(|cur| {
        if cur
            .as_deref()
            .is_some_and(|c| c.name == name && c.uid.as_deref() == uid)
        {
            None
        } else {
            cur.clone()
        }
    });
    if prev
        .as_deref()
        .is_some_and(|c| c.name == name && c.uid.as_deref() == uid)
    {
        let _ = change_tx.send(PodChange::Died(name.to_string()));
    }
}

fn matches_selector(pod: &Pod, selector: &PodSelector) -> bool {
    match selector {
        PodSelector::Name(name) => pod.name_any() == *name,
        PodSelector::Labels { .. } => true,
    }
}

/// Whether `pod` matches `selector` and satisfies `readiness`.
fn is_pod_selected(pod: &Pod, selector: &PodSelector, readiness: PodReadiness) -> bool {
    if !matches_selector(pod, selector) {
        return false;
    }
    let Some(status) = pod.status.as_ref() else {
        return false;
    };
    if status.phase.as_deref() != Some("Running") {
        return false;
    }
    match readiness {
        PodReadiness::Running => pod.metadata.deletion_timestamp.is_none(),
        PodReadiness::Ready => {
            pod.metadata.deletion_timestamp.is_none()
                && status
                    .conditions
                    .as_ref()
                    .map(|cs| cs.iter().any(|c| c.type_ == "Ready" && c.status == "True"))
                    .unwrap_or(false)
        }
    }
}

#[cfg(test)]
fn test_pod_with_readiness(name: &str, uid: &str, ready: bool) -> Pod {
    use k8s_openapi::api::core::v1::{
        PodCondition,
        PodStatus,
    };
    use kube::api::ObjectMeta;

    Pod {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            uid: Some(uid.to_string()),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            conditions: Some(vec![PodCondition {
                type_: "Ready".into(),
                status: if ready { "True" } else { "False" }.into(),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[cfg(test)]
pub(crate) fn ready_test_pod(name: &str, uid: &str) -> Pod {
    test_pod_with_readiness(name, uid, true)
}

#[cfg(test)]
pub(crate) fn unready_test_pod(name: &str, uid: &str) -> Pod {
    test_pod_with_readiness(name, uid, false)
}

#[cfg(test)]
impl PodWatcher {
    pub(crate) fn for_test(
        selector: PodSelector, readiness: PodReadiness,
    ) -> (Self, reflector::store::Writer<Pod>) {
        let (store, writer) = reflector::store_shared(16);
        let subscriber = writer
            .subscribe()
            .expect("a writer created via store_shared supports subscribe");
        let (change_tx, _) = broadcast::channel(16);
        let watcher = Self {
            store,
            _subscriber: subscriber,
            latest_ready: Arc::new(ArcSwapOption::const_empty()),
            change_tx,
            selector,
            reflector_task: tokio::spawn(async {}),
            subscriber_task: tokio::spawn(async {}),
            cancel: CancellationToken::new(),
            readiness,
        };
        (watcher, writer)
    }

    pub(crate) fn test_apply(&self, writer: &mut reflector::store::Writer<Pod>, pod: &Pod) {
        writer.apply_watcher_event(&watcher::Event::Apply(pod.clone()));
        update_latest(
            &self.latest_ready,
            pod,
            &self.selector,
            self.readiness,
            &self.change_tx,
        );
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::core::v1::{
        PodCondition,
        PodStatus,
    };
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
    use k8s_openapi::jiff::Timestamp;
    use kube::api::ObjectMeta;

    use super::*;

    fn mk_pod(name: &str, ready: bool, running: bool) -> Pod {
        mk_pod_full(name, None, ready, running, false)
    }

    fn mk_pod_full(
        name: &str, uid: Option<&str>, ready: bool, running: bool, terminating: bool,
    ) -> Pod {
        Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                uid: uid.map(str::to_string),
                deletion_timestamp: terminating.then(|| Time(Timestamp::now())),
                ..Default::default()
            },
            status: Some(PodStatus {
                phase: Some(if running { "Running" } else { "Pending" }.to_string()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".into(),
                    status: if ready { "True" } else { "False" }.into(),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn ready_running_pod_is_ready_for_labels_selector() {
        let pod = mk_pod("p1", true, true);
        assert!(is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: "app=x".into()
            },
            PodReadiness::Ready,
        ));
    }

    #[test]
    fn not_running_pod_is_not_ready() {
        let pod = mk_pod("p1", true, false);
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Ready,
        ));
    }

    #[test]
    fn name_selector_filters_by_name() {
        let pod = mk_pod("p1", true, true);
        assert!(is_pod_selected(
            &pod,
            &PodSelector::Name("p1".into()),
            PodReadiness::Ready
        ));
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Name("p2".into()),
            PodReadiness::Ready
        ));
    }

    #[test]
    fn ready_condition_must_be_true() {
        let pod = mk_pod("p1", false, true);
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Ready,
        ));
    }

    #[test]
    fn running_policy_accepts_running_pod_without_ready_condition() {
        let pod = mk_pod("p1", false, true);
        assert!(is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Running,
        ));
    }

    #[test]
    fn running_policy_rejects_non_running_pod() {
        let pod = mk_pod("p1", true, false);
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Running,
        ));
    }

    #[test]
    fn running_policy_rejects_terminating_pod() {
        let pod = mk_pod_full("p1", None, true, true, true);
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Running,
        ));
    }

    #[test]
    fn update_latest_detects_uid_replacement_of_same_named_pod() {
        let latest: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
        let (change_tx, mut change_rx) = broadcast::channel(4);
        let selector = PodSelector::Labels {
            selector: String::new(),
        };

        let first = mk_pod_full("p1", Some("uid-a"), true, true, false);
        update_latest(&latest, &first, &selector, PodReadiness::Ready, &change_tx);
        let cached = latest.load_full().expect("first pod cached");
        assert_eq!(cached.uid.as_deref(), Some("uid-a"));
        assert!(matches!(change_rx.try_recv(), Ok(PodChange::Ready(name)) if name == "p1"));

        // same name, new UID (e.g. a StatefulSet pod recreated during a
        // rollout): must be treated as a change, not silently ignored.
        let replaced = mk_pod_full("p1", Some("uid-b"), true, true, false);
        update_latest(
            &latest,
            &replaced,
            &selector,
            PodReadiness::Ready,
            &change_tx,
        );
        let cached = latest.load_full().expect("replacement pod cached");
        assert_eq!(cached.uid.as_deref(), Some("uid-b"));
        assert!(matches!(change_rx.try_recv(), Ok(PodChange::Ready(name)) if name == "p1"));
    }

    #[test]
    fn update_latest_is_a_noop_for_unchanged_pod() {
        let latest: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
        let (change_tx, mut change_rx) = broadcast::channel(4);
        let selector = PodSelector::Labels {
            selector: String::new(),
        };

        let pod = mk_pod_full("p1", Some("uid-a"), true, true, false);
        update_latest(&latest, &pod, &selector, PodReadiness::Ready, &change_tx);
        assert!(change_rx.try_recv().is_ok());

        update_latest(&latest, &pod, &selector, PodReadiness::Ready, &change_tx);
        assert!(
            change_rx.try_recv().is_err(),
            "no change event for an unchanged pod"
        );
    }

    #[test]
    fn ready_policy_rejects_terminating_pod() {
        let pod = mk_pod_full("p1", None, true, true, true);
        assert!(!is_pod_selected(
            &pod,
            &PodSelector::Labels {
                selector: String::new()
            },
            PodReadiness::Ready,
        ));
    }

    #[test]
    fn update_latest_keeps_current_target_when_a_different_pod_becomes_ready() {
        let latest: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
        let (change_tx, mut change_rx) = broadcast::channel(4);
        let selector = PodSelector::Labels {
            selector: String::new(),
        };

        let p1 = mk_pod_full("p1", Some("uid-a"), true, true, false);
        update_latest(&latest, &p1, &selector, PodReadiness::Ready, &change_tx);
        assert!(matches!(change_rx.try_recv(), Ok(PodChange::Ready(name)) if name == "p1"));

        let p2 = mk_pod_full("p2", Some("uid-b"), true, true, false);
        update_latest(&latest, &p2, &selector, PodReadiness::Ready, &change_tx);
        let cached = latest.load_full().expect("cache still populated");
        assert_eq!(cached.name, "p1");
        assert!(
            change_rx.try_recv().is_err(),
            "no Ready broadcast for a pod that didn't take over the target"
        );
    }

    #[tokio::test]
    async fn ready_pod_falls_back_once_the_cached_target_stops_being_selectable() {
        let (watcher, mut writer) = PodWatcher::for_test(
            PodSelector::Labels {
                selector: String::new(),
            },
            PodReadiness::Ready,
        );

        let p1 = mk_pod_full("p1", Some("uid-a"), true, true, false);
        watcher.test_apply(&mut writer, &p1);
        let p2 = mk_pod_full("p2", Some("uid-b"), true, true, false);
        watcher.test_apply(&mut writer, &p2);

        assert_eq!(watcher.ready_pod().expect("a ready pod").name, "p1");
        assert_eq!(watcher.ready_pod().expect("a ready pod").name, "p1");

        let p1_terminating = mk_pod_full("p1", Some("uid-a"), true, true, true);
        watcher.test_apply(&mut writer, &p1_terminating);
        assert_eq!(watcher.ready_pod().expect("a ready pod").name, "p2");
    }

    #[test]
    fn died_only_fires_for_the_currently_cached_pod() {
        let latest: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
        latest.store(Some(Arc::new(ReadyPod::new(
            "p1".into(),
            Some("uid-a".into()),
        ))));
        let (change_tx, mut change_rx) = broadcast::channel(4);

        handle_deleted_pod(&latest, "old-pod", Some("uid-x"), &change_tx);
        assert!(change_rx.try_recv().is_err());
        assert_eq!(latest.load_full().expect("cache untouched").name, "p1");

        handle_deleted_pod(&latest, "p1", Some("uid-a"), &change_tx);
        assert!(latest.load_full().is_none());
        assert!(matches!(change_rx.try_recv(), Ok(PodChange::Died(name)) if name == "p1"));
    }

    #[test]
    fn handle_deleted_pod_ignores_a_stale_delete_for_a_replaced_uid() {
        let latest: Arc<ArcSwapOption<ReadyPod>> = Arc::new(ArcSwapOption::const_empty());
        latest.store(Some(Arc::new(ReadyPod::new(
            "p1".into(),
            Some("uid-b".into()),
        ))));
        let (change_tx, mut change_rx) = broadcast::channel(4);

        handle_deleted_pod(&latest, "p1", Some("uid-a"), &change_tx);
        assert!(change_rx.try_recv().is_err());
        assert_eq!(
            latest.load_full().expect("cache untouched").uid.as_deref(),
            Some("uid-b")
        );

        handle_deleted_pod(&latest, "p1", Some("uid-b"), &change_tx);
        assert!(latest.load_full().is_none());
        assert!(matches!(change_rx.try_recv(), Ok(PodChange::Died(name)) if name == "p1"));
    }
}
