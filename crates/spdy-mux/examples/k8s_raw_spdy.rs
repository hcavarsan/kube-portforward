//! `spdy-mux` over the legacy `Upgrade: SPDY/3.1` HTTP/1.1 path.
//!
//! Self-contained: deploys an `nginx:alpine` pod, performs the upgrade
//! via `kube::Client`, multiplexes a `GET /` through a kubelet-shaped
//! stream pair, deletes the pod.
//!
//! Run:
//! ```text
//! cargo run -p spdy-mux --example k8s_raw_spdy
//! ```

#[path = "common.rs"]
mod common;

use http::{
    Method,
    Request,
    header,
};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use kube::client::Body;
use spdy_mux::{
    MuxConfig,
    Session,
    split_raw_spdy,
};
use tokio_util::sync::CancellationToken;

const LEGACY_SPDY_UPGRADE: &str = "SPDY/3.1";
const PORTFORWARD_STREAM_PROTOCOL: &str = "portforward.k8s.io";

const NAMESPACE: &str = "default";
const POD: &str = "spdy-mux-example-raw";
const PORT: u16 = 80;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,spdy_mux=debug".into()),
        )
        .init();

    let config = kube::Config::infer().await?;
    let cluster_url = config.cluster_url.clone();
    let kube_client = kube::Client::try_from(config)?;

    common::ensure_nginx_pod(&kube_client, NAMESPACE, POD).await?;
    let result = run(&kube_client, &cluster_url).await;
    common::delete_pod(&kube_client, NAMESPACE, POD).await;
    result
}

async fn run(kube_client: &kube::Client, cluster_url: &http::Uri) -> anyhow::Result<()> {
    println!("* legacy SPDY/3.1 upgrade over HTTP/1.1");
    println!("* cluster {cluster_url}");

    let uri: http::Uri = format!(
        "{}/api/v1/namespaces/{NAMESPACE}/pods/{POD}/portforward",
        cluster_url.to_string().trim_end_matches('/')
    )
    .parse()?;
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, LEGACY_SPDY_UPGRADE)
        .header("X-Stream-Protocol-Version", PORTFORWARD_STREAM_PROTOCOL)
        .body(Body::from(Vec::new()))?;
    let res = common::send_with_trace(kube_client, req).await?;
    if res.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        anyhow::bail!("expected 101, got {}", res.status());
    }
    let upgraded: Upgraded = hyper::upgrade::on(res).await?;
    println!("* upgraded to raw SPDY/3.1");

    let (writer, reader) = split_raw_spdy(TokioIo::new(upgraded));
    let session = Session::with_config(
        vec![(writer, reader)],
        CancellationToken::new(),
        MuxConfig::default(),
    )
    .await?;
    println!("* SPDY session ready");

    common::nginx_get(&session, PORT).await?;
    session.close().await?;
    println!("* session closed");
    Ok(())
}
