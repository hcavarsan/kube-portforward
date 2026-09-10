use std::sync::Arc;

use spdy_mux::{
    split_fastws,
    split_raw_spdy,
};
use tokio_util::sync::CancellationToken;

use crate::connect::upgrade_spdy_with_fallback;
use crate::error::Error;
use crate::recovery::RecoveryCallback;
use crate::session::Session;
use crate::subprotocol::Subprotocol;

/// Pool size for the  multiplexer
const DEFAULT_SPDY_POOL_SIZE: usize = 6;

/// entry point that bundles a `kube::Client` with its cluster URL.
#[derive(Clone)]
pub struct Client {
    kube: kube::Client,
    cluster_url: http::Uri,
}

impl Client {
    pub const fn new(kube_client: kube::Client, cluster_url: http::Uri) -> Self {
        Self {
            kube: kube_client,
            cluster_url,
        }
    }

    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn session(
        &self, namespace: impl Into<String>, pod: impl Into<String>, port: u16,
    ) -> SessionBuilder<'_> {
        SessionBuilder {
            client: self,
            namespace: namespace.into(),
            pod: pod.into(),
            port,
            cancel: None,
            recovery_callback: None,
            spdy_pool_size: DEFAULT_SPDY_POOL_SIZE,
        }
    }

    pub const fn kube_client(&self) -> &kube::Client {
        &self.kube
    }

    pub const fn cluster_url(&self) -> &http::Uri {
        &self.cluster_url
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    kube: Option<kube::Client>,
    cluster_url: Option<http::Uri>,
}

impl ClientBuilder {
    pub fn kube_client(mut self, c: kube::Client) -> Self {
        self.kube = Some(c);
        self
    }

    pub fn cluster_url(mut self, u: http::Uri) -> Self {
        self.cluster_url = Some(u);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        let kube = self
            .kube
            .ok_or_else(|| Error::Configuration("kube_client is required".into()))?;
        let cluster_url = self
            .cluster_url
            .ok_or_else(|| Error::Configuration("cluster_url is required".into()))?;
        Ok(Client::new(kube, cluster_url))
    }
}

/// Builder for opening a session
pub struct SessionBuilder<'c> {
    client: &'c Client,
    namespace: String,
    pod: String,
    port: u16,
    cancel: Option<CancellationToken>,
    recovery_callback: Option<RecoveryCallback>,
    spdy_pool_size: usize,
}

impl SessionBuilder<'_> {
    pub fn cancellation_token(mut self, t: CancellationToken) -> Self {
        self.cancel = Some(t);
        self
    }

    /// Number of parallel upgraded connections in the SPDY pool. Each one
    /// gets its own reader/writer task pair
    pub fn spdy_pool_size(mut self, n: usize) -> Self {
        self.spdy_pool_size = n.max(1);
        self
    }

    pub fn on_recovery<F>(mut self, cb: F) -> Self
    where
        F: Fn(crate::recovery::RecoverySignal) + Send + Sync + 'static,
    {
        self.recovery_callback = Some(Arc::new(cb));
        self
    }

    pub async fn open(self) -> Result<Session, Error> {
        let cancel = self.cancel.unwrap_or_default();
        let recovery_callback: RecoveryCallback = self
            .recovery_callback
            .unwrap_or_else(|| Arc::new(|_signal| {}));

        open_spdy_session(
            self.client,
            &self.namespace,
            &self.pod,
            self.port,
            self.spdy_pool_size,
            cancel,
            recovery_callback,
        )
        .await
    }
}

/// Open a SPDY session, probe with the first upgrade, then fill the rest
/// of the pool in parallel using whatever transport the probe picked.
async fn open_spdy_session(
    client: &Client, namespace: &str, pod: &str, port: u16, pool_size: usize,
    cancel: CancellationToken, recovery_callback: RecoveryCallback,
) -> Result<Session, Error> {
    let first = upgrade_spdy_with_fallback(
        client.kube_client(),
        client.cluster_url(),
        namespace,
        pod,
        &recovery_callback,
    )
    .await?;
    let chosen_protocol = first.protocol;
    let first_upgraded = first.upgraded;

    tracing::info!(
        pod = %pod,
        pool_size,
        protocol = %chosen_protocol,
        "SPDY tunnel: probe succeeded"
    );

    let extra_upgrades = if pool_size > 1 {
        let t_parallel = std::time::Instant::now();
        let mut join_set = tokio::task::JoinSet::new();
        for i in 1..pool_size {
            let kube = client.kube_client().clone();
            let url = client.cluster_url().clone();
            let ns = namespace.to_owned();
            let pod_name = pod.to_owned();
            join_set.spawn(async move {
                let result = match chosen_protocol {
                    Subprotocol::Spdy31Tunnel => {
                        crate::connect::upgrade_spdy_tunnel(&kube, &url, &ns, &pod_name).await
                    }
                    Subprotocol::LegacySpdy => {
                        crate::connect::upgrade_legacy_spdy(&kube, &url, &ns, &pod_name).await
                    }
                };
                (i, result)
            });
        }
        let mut succeeded = Vec::with_capacity(pool_size - 1);
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok((_, Ok(upgraded))) => succeeded.push(upgraded.upgraded),
                Ok((i, Err(e))) => {
                    tracing::debug!("SPDY pool: connection {i}/{pool_size} failed: {e}");
                }
                Err(e) => {
                    tracing::debug!("SPDY pool: connection task panicked: {e}");
                }
            }
        }
        tracing::info!(
            pool_opened = succeeded.len() + 1,
            pool_target = pool_size,
            elapsed_ms = u64::try_from(t_parallel.elapsed().as_millis()).unwrap_or(u64::MAX),
            "SPDY pool: parallel connections opened"
        );
        succeeded
    } else {
        Vec::new()
    };

    let config = spdy_mux::MuxConfig {
        pool_size: extra_upgrades.len() + 1,
        ..Default::default()
    };
    let all_upgrades = std::iter::once(first_upgraded).chain(extra_upgrades);
    let t_pool = std::time::Instant::now();
    let spdy_session = match chosen_protocol {
        Subprotocol::Spdy31Tunnel => {
            let pairs: Vec<_> = all_upgrades.map(split_fastws).collect();
            spdy_mux::Session::with_config(pairs, cancel.clone(), config).await
        }
        Subprotocol::LegacySpdy => {
            let pairs: Vec<_> = all_upgrades.map(split_raw_spdy).collect();
            spdy_mux::Session::with_config(pairs, cancel.clone(), config).await
        }
    }
    .map_err(Error::from)?;

    tracing::info!(
        pod = %pod,
        pool_healthy = spdy_session.capacity() > 0,
        pool_init_ms = u64::try_from(t_pool.elapsed().as_millis()).unwrap_or(u64::MAX),
        protocol = %chosen_protocol,
        "SPDY session ready"
    );
    Ok(Session::from_spdy(spdy_session, chosen_protocol, port))
}
