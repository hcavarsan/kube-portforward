//! `spdy-mux` over the modern WebSocket-tunnelled SPDY path
//! (KEP-4006, `SPDY/3.1+portforward.k8s.io` subprotocol).
//!
//! Self-contained: deploys an `nginx:alpine` pod, performs the
//! WebSocket upgrade via `kube::Client`, multiplexes a `GET /` through
//! a kubelet-shaped stream pair, deletes the pod.
//!
//! Requires a Kubernetes 1.31+ apiserver with `PortForwardWebsockets`
//! enabled (default since 1.31).
//!
//! Run:
//! ```text
//! cargo run -p spdy-mux --example k8s_websocket_over_spdy
//! ```

#[path = "common.rs"]
mod common;

use base64::Engine;
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
    split_fastws,
};
use tokio_util::sync::CancellationToken;

const SPDY_SUBPROTOCOL: &str = "SPDY/3.1+portforward.k8s.io";

const NAMESPACE: &str = "default";
const POD: &str = "spdy-mux-example-ws";
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
    println!("* WebSocket-tunnelled SPDY/3.1 upgrade (KEP-4006)");
    println!("* cluster {cluster_url}");

    let uri: http::Uri = format!(
        "{}/api/v1/namespaces/{NAMESPACE}/pods/{POD}/portforward",
        cluster_url.to_string().trim_end_matches('/')
    )
    .parse()?;
    let key = ws_key();
    let req = Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header(header::SEC_WEBSOCKET_VERSION, "13")
        .header(header::SEC_WEBSOCKET_KEY, key)
        .header(header::SEC_WEBSOCKET_PROTOCOL, SPDY_SUBPROTOCOL)
        .body(Body::from(Vec::new()))?;
    let res = common::send_with_trace(kube_client, req).await?;
    if res.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        anyhow::bail!("expected 101, got {}", res.status());
    }
    let negotiated = res
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if negotiated != SPDY_SUBPROTOCOL {
        anyhow::bail!("server picked subprotocol {negotiated:?}, wanted {SPDY_SUBPROTOCOL:?}");
    }
    println!("* WebSocket open, subprotocol={negotiated}");
    let upgraded: Upgraded = hyper::upgrade::on(res).await?;

    let (writer, reader) = split_fastws(TokioIo::new(upgraded));
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

/// `Sec-WebSocket-Key`: 16 random bytes, base64-encoded.
fn ws_key() -> String {
    let bytes: [u8; 16] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}
