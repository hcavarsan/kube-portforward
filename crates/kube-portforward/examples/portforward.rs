//! Deploys an nginx pod, drives a `Forwarder` against it through three
//! phases:
//!   1. three `GET /` over one upgrade
//!   2. force-delete the pod, recreate it, watch the Forwarder retarget; and
//!      validates another GET succeeds
//!   3. cancel the token, see graceful drain
//!
//! Run:
//! ```text
//! cargo run -p kube-portforward --example portforward
//! ```

#[path = "common.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use kube_portforward::{
    Forwarder,
    PodSelector,
    RecoverySignal,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};
use tokio_util::sync::CancellationToken;

const NAMESPACE: &str = "default";
const POD: &str = "kfpf-example";
const PORT: u16 = 80;
const CONCURRENT_STREAMS: usize = 3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,kube_portforward=debug,spdy_mux=debug".into()),
        )
        .init();

    let config = kube::Config::infer().await?;
    let cluster_url = config.cluster_url.clone();
    let kube_client = kube::Client::try_from(config)?;

    common::ensure_nginx_pod(&kube_client, NAMESPACE, POD).await?;
    let run_result = run(&kube_client, cluster_url).await;
    common::delete_pod(&kube_client, NAMESPACE, POD).await;
    run_result
}

async fn run(kube_client: &kube::Client, cluster_url: http::Uri) -> anyhow::Result<()> {
    println!("* kube-portforward Forwarder");
    println!("* cluster {cluster_url}");
    println!("* namespace={NAMESPACE} selector=app={POD} port={PORT}");
    println!("* pool=2  capacity={CONCURRENT_STREAMS}  keepalive=5s/15s  grace=3s");

    let cancel = CancellationToken::new();
    let forwarder = Arc::new(
        Forwarder::builder(kube_client.clone(), cluster_url, NAMESPACE)
            .pod_selector(PodSelector::Labels {
                selector: format!("app={POD}"),
            })
            .max_sessions(2)
            .session_capacity(CONCURRENT_STREAMS)
            .keepalive(Duration::from_secs(5), Duration::from_secs(15))
            .shutdown_grace(Duration::from_secs(3))
            .prune(Duration::from_secs(30), Duration::from_secs(120))
            .prefetch_threshold(0.75)
            .cancellation_token(cancel.clone())
            .on_recovery(|sig: RecoverySignal| println!("* recovery {sig:?}"))
            .build()
            .await?,
    );
    wait_for_ready_pod(&forwarder, Duration::from_secs(30)).await?;
    println!("* target pod {:?}", forwarder.ready_pod());

    println!();
    println!("* phase 1: {CONCURRENT_STREAMS} multiplexed GETs over one upgrade");
    fan_out(&forwarder).await?;

    println!();
    println!("* phase 2: simulating pod restart");
    common::force_delete_and_wait(kube_client, NAMESPACE, POD).await?;
    // Pod is gone. Forwarder loses its session;
    common::ensure_nginx_pod(kube_client, NAMESPACE, POD).await?;
    // PodWatcher sees the new pod and the Forwarder retargets automatically.
    wait_for_ready_pod(&forwarder, Duration::from_secs(30)).await?;
    println!(
        "* target pod {:?}  (auto-retargeted)",
        forwarder.ready_pod()
    );
    fan_out(&forwarder).await?;

    // graceful shutdown
    println!();
    println!("* phase 3: cancel + graceful drain");
    cancel.cancel();
    cancel.cancelled().await;
    Arc::try_unwrap(forwarder)
        .ok()
        .expect("forwarder still shared")
        .shutdown()
        .await?;
    println!("* shutdown complete");
    Ok(())
}

/// Fire parallel GETs through one Forwarder.
async fn fan_out(forwarder: &Arc<Forwarder>) -> anyhow::Result<()> {
    let mut tasks = Vec::with_capacity(CONCURRENT_STREAMS);
    for i in 0..CONCURRENT_STREAMS {
        let fwd = Arc::clone(forwarder);
        tasks.push(tokio::spawn(async move {
            let mut stream = fwd.connect(PORT).await?;
            let req = format!("GET /?stream={i} HTTP/1.0\r\nHost: localhost\r\n\r\n");
            stream.write_all(req.as_bytes()).await?;
            // HTTP server closes after the response, so read_to_end
            // gives the full payload in a deterministic byte count.
            let mut buf = Vec::with_capacity(4096);
            stream.read_to_end(&mut buf).await?;
            let status = std::str::from_utf8(&buf[..buf.len().min(64)])
                .ok()
                .and_then(|s| s.lines().next())
                .unwrap_or("<binary>")
                .to_string();
            Ok::<_, anyhow::Error>((buf.len(), status))
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        match t.await? {
            Ok((n, status)) => {
                println!("> GET /?stream={i} HTTP/1.0");
                println!("< {status}  [{n} bytes]");
            }
            Err(e) => println!("* stream {i} error: {e}"),
        }
    }
    Ok(())
}

/// Poll `forwarder.ready_pod()` until Some or timeout. The PodWatcher
/// updates this when a matching pod becomes Ready again.
async fn wait_for_ready_pod(forwarder: &Forwarder, timeout: Duration) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if forwarder.ready_pod().is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    anyhow::bail!("forwarder did not retarget within {timeout:?}")
}
