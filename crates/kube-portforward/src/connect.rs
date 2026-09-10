//! HTTP upgrade  for SPDY/3.1 port-forwarding.
//!
//! Provides two upgrade paths, both of which deliver SPDY/3.1 frames to the
//! apiserver:
//!
//! - [`upgrade_spdy_tunnel`] — SPDY tunnelled inside a WebSocket, advertised as
//!   `Sec-WebSocket-Protocol: SPDY/3.1+portforward.k8s.io`. Works against
//!   apiservers that implement KEP-4006 (Kubernetes 1.30+).
//! - [`upgrade_legacy_spdy`] — raw SPDY/3.1 over HTTP/1.1 upgrade, the original
//!   `kubectl port-forward` wire protocol. Works against older apiservers and
//!   clusters with `PortForwardWebsockets` disabled.
//!
//! [`upgrade_spdy_with_fallback`] orchestrates fallback: it
//! tries the WebSocket-tunnelled path first and, on a non-network failure

use http::{
    Method,
    Request,
    Uri,
    header,
};
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use kube::client::Body;

use crate::error::Error;
use crate::recovery::{
    RecoveryCallback,
    RecoverySignal,
};
use crate::subprotocol::Subprotocol;

const SPDY_SUBPROTOCOL: &str = "SPDY/3.1+portforward.k8s.io";
const LEGACY_SPDY_UPGRADE: &str = "SPDY/3.1";
const LEGACY_STREAM_PROTOCOL: &str = "portforward.k8s.io";

/// One upgraded transport ready to carry SPDY frames.
pub(crate) struct SpdyUpgraded {
    pub upgraded: TokioIo<Upgraded>,
    pub protocol: Subprotocol,
}

fn name_is_valid(s: &str) -> bool {
    !s.is_empty()
        && s.is_ascii()
        && s.bytes()
            .all(|b| !matches!(b, b'/' | b'?' | b'#') && !b.is_ascii_control())
}

fn portforward_uri(cluster_url: &Uri, namespace: &str, pod: &str) -> Result<Uri, Error> {
    if !name_is_valid(namespace) || !name_is_valid(pod) {
        return Err(Error::Configuration(
            "invalid namespace or pod name: has a forbidden character or non-ASCII".into(),
        ));
    }
    let scheme = cluster_url
        .scheme()
        .ok_or_else(|| Error::Configuration("cluster_url has no scheme".into()))?;
    let authority = cluster_url
        .authority()
        .ok_or_else(|| Error::Configuration("cluster_url has no authority".into()))?;
    let path = format!("/api/v1/namespaces/{namespace}/pods/{pod}/portforward");
    format!("{scheme}://{authority}{path}")
        .parse()
        .map_err(|e: http::uri::InvalidUri| {
            Error::Configuration(format!("invalid port-forward URI: {e}"))
        })
}

/// Build a WebSocket-tunnelled SPDY upgrade request.
fn build_spdy_tunnel_request(
    cluster_url: &Uri, namespace: &str, pod: &str,
) -> Result<Request<Vec<u8>>, Error> {
    let uri = portforward_uri(cluster_url, namespace, pod)?;
    Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(header::SEC_WEBSOCKET_PROTOCOL, SPDY_SUBPROTOCOL)
        .body(Vec::new())
        .map_err(|e: http::Error| {
            Error::Configuration(format!("failed to build port-forward request: {e}"))
        })
}

/// Build a raw SPDY/3.1 upgrade request (no WebSocket envelope).
fn build_legacy_spdy_request(
    cluster_url: &Uri, namespace: &str, pod: &str,
) -> Result<Request<Vec<u8>>, Error> {
    let uri = portforward_uri(cluster_url, namespace, pod)?;
    Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, LEGACY_SPDY_UPGRADE)
        .header("X-Stream-Protocol-Version", LEGACY_STREAM_PROTOCOL)
        .body(Vec::new())
        .map_err(|e: http::Error| {
            Error::Configuration(format!("failed to build port-forward request: {e}"))
        })
}

/// Send a WebSocket upgrade request, and return the raw upgraded connection.
async fn perform_ws_upgrade(
    kube_client: &kube::Client, request: Request<Vec<u8>>,
) -> Result<TokioIo<Upgraded>, Error> {
    let (mut parts, body) = request.into_parts();
    parts
        .headers
        .insert(header::CONNECTION, "Upgrade".parse().unwrap());
    parts
        .headers
        .insert(header::UPGRADE, "websocket".parse().unwrap());
    parts
        .headers
        .insert(header::SEC_WEBSOCKET_VERSION, "13".parse().unwrap());
    let key = generate_ws_key();
    parts
        .headers
        .insert(header::SEC_WEBSOCKET_KEY, key.parse().unwrap());

    let res = kube_client
        .send(Request::from_parts(parts, Body::from(body)))
        .await
        .map_err(Error::Kube)?;

    if res.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        let status_code = res.status().as_u16();
        return Err(Error::UpgradeFailed {
            status: Some(status_code),
            message: format!("SPDY-over-WebSocket upgrade: expected 101, got {status_code}"),
        });
    }

    let negotiated = res
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if negotiated != SPDY_SUBPROTOCOL {
        return Err(Error::ProtocolViolation {
            context: "SPDY-over-WebSocket subprotocol negotiation",
            detail: format!(
                "server picked an unexpected subprotocol: {negotiated:?} (wanted {SPDY_SUBPROTOCOL:?})"
            ),
        });
    }

    let upgraded = hyper::upgrade::on(res)
        .await
        .map_err(|e| Error::Network(format!("failed to complete HTTP upgrade: {e}")))?;
    Ok(TokioIo::new(upgraded))
}

/// Generate a `Sec-WebSocket-Key` header value .
fn generate_ws_key() -> String {
    use base64::Engine;
    let bytes: [u8; 16] = rand::random();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Send a raw  upgrade request to `SPDY/3.1` and return the raw upgraded
/// connection.
async fn perform_legacy_spdy_upgrade(
    kube_client: &kube::Client, request: Request<Vec<u8>>,
) -> Result<TokioIo<Upgraded>, Error> {
    let (parts, body) = request.into_parts();
    let res = kube_client
        .send(Request::from_parts(parts, Body::from(body)))
        .await
        .map_err(Error::Kube)?;

    if res.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        let status_code = res.status().as_u16();
        return Err(Error::UpgradeFailed {
            status: Some(status_code),
            message: format!("legacy SPDY upgrade: expected 101, got {status_code}"),
        });
    }

    let upgrade_hdr = res
        .headers()
        .get(header::UPGRADE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !upgrade_hdr.eq_ignore_ascii_case(LEGACY_SPDY_UPGRADE) {
        return Err(Error::ProtocolViolation {
            context: "legacy SPDY upgrade",
            detail: format!(
                "server sent back an unexpected Upgrade header: {upgrade_hdr:?} (wanted {LEGACY_SPDY_UPGRADE:?})"
            ),
        });
    }
    let stream_protocol = res
        .headers()
        .get("X-Stream-Protocol-Version")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if stream_protocol != LEGACY_STREAM_PROTOCOL {
        return Err(Error::ProtocolViolation {
            context: "legacy SPDY upgrade",
            detail: format!(
                "server sent back an unexpected X-Stream-Protocol-Version: {stream_protocol:?} \
                 (wanted {LEGACY_STREAM_PROTOCOL:?})"
            ),
        });
    }

    let upgraded = hyper::upgrade::on(res)
        .await
        .map_err(|e| Error::Network(format!("failed to complete HTTP upgrade: {e}")))?;
    Ok(TokioIo::new(upgraded))
}

/// Try a SPDY over WebSocket upgrade.
pub(crate) async fn upgrade_spdy_tunnel(
    kube_client: &kube::Client, cluster_url: &Uri, namespace: &str, pod: &str,
) -> Result<SpdyUpgraded, Error> {
    let request = build_spdy_tunnel_request(cluster_url, namespace, pod)?;
    tracing::debug!(
        uri = %request.uri(),
        sec_websocket_protocol = SPDY_SUBPROTOCOL,
        "upgrade_spdy_tunnel: sending WebSocket upgrade request"
    );
    let t = std::time::Instant::now();
    let upgraded = perform_ws_upgrade(kube_client, request).await?;
    tracing::debug!(
        pod = %pod,
        elapsed_ms = u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX),
        "upgrade_spdy_tunnel: upgrade complete"
    );
    Ok(SpdyUpgraded {
        upgraded,
        protocol: Subprotocol::Spdy31Tunnel,
    })
}

/// Try a legacy SPDY upgrade, without websocket.
pub(crate) async fn upgrade_legacy_spdy(
    kube_client: &kube::Client, cluster_url: &Uri, namespace: &str, pod: &str,
) -> Result<SpdyUpgraded, Error> {
    let request = build_legacy_spdy_request(cluster_url, namespace, pod)?;
    tracing::debug!(
        uri = %request.uri(),
        upgrade = LEGACY_SPDY_UPGRADE,
        "upgrade_legacy_spdy: sending raw HTTP upgrade request"
    );
    let t = std::time::Instant::now();
    let upgraded = perform_legacy_spdy_upgrade(kube_client, request).await?;
    tracing::debug!(
        pod = %pod,
        elapsed_ms = u64::try_from(t.elapsed().as_millis()).unwrap_or(u64::MAX),
        "upgrade_legacy_spdy: upgrade complete"
    );
    Ok(SpdyUpgraded {
        upgraded,
        protocol: Subprotocol::LegacySpdy,
    })
}

/// Whether a failed first try should trigger the legacy fallback.
const fn should_fallback(err: &Error) -> bool {
    matches!(
        err,
        Error::UpgradeFailed { .. } | Error::ProtocolViolation { .. }
    )
}

/// Try the WebSocket-tunnelled SPDY path; on a rejected upgrade or
/// subprotocol mismatch, fall back to raw SPDY/3.1.
pub(crate) async fn upgrade_spdy_with_fallback(
    kube_client: &kube::Client, cluster_url: &Uri, namespace: &str, pod: &str,
    recovery_callback: &RecoveryCallback,
) -> Result<SpdyUpgraded, Error> {
    match upgrade_spdy_tunnel(kube_client, cluster_url, namespace, pod).await {
        Ok(up) => Ok(up),
        Err(ws_err) if should_fallback(&ws_err) => {
            tracing::info!(
                pod = %pod,
                error = %ws_err,
                "SPDY-over-WebSocket rejected, falling back to legacy SPDY upgrade"
            );
            match upgrade_legacy_spdy(kube_client, cluster_url, namespace, pod).await {
                Ok(up) => Ok(up),
                Err(fallback_err) => {
                    let status = if let Error::UpgradeFailed { status, .. } = &fallback_err {
                        *status
                    } else if let Error::UpgradeFailed { status, .. } = &ws_err {
                        *status
                    } else {
                        None
                    };
                    recovery_callback(RecoverySignal::UpgradeFailed {
                        status,
                        message: format!(
                            "websocket upgrade rejected ({ws_err}); legacy fallback also failed: \
                             {fallback_err}"
                        ),
                    });
                    Err(fallback_err)
                }
            }
        }
        Err(e) => {
            if matches!(&e, Error::Kube(_) | Error::Network(_)) {
                recovery_callback(RecoverySignal::UpgradeFailed {
                    status: None,
                    message: e.to_string(),
                });
            }
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;

    use http::{
        Response,
        StatusCode,
    };
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Clone, Copy)]
    enum FallbackScenario {
        SuccessfulFallback,
        FailedFallback,
    }

    async fn handle_upgrade_request(
        scenario: FallbackScenario, mut req: Request<Incoming>,
    ) -> Result<Response<String>, Infallible> {
        let upgrade_hdr = req
            .headers()
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_ascii_lowercase();

        if upgrade_hdr == "websocket" {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(String::new())
                .unwrap());
        }

        if upgrade_hdr == "spdy/3.1" {
            return Ok(match scenario {
                FallbackScenario::SuccessfulFallback => {
                    let on_upgrade = hyper::upgrade::on(&mut req);
                    tokio::spawn(async move {
                        if let Ok(upgraded) = on_upgrade.await {
                            let mut io = TokioIo::new(upgraded);
                            let mut buf = [0u8; 1024];
                            while matches!(io.read(&mut buf).await, Ok(n) if n > 0) {}
                        }
                    });
                    Response::builder()
                        .status(StatusCode::SWITCHING_PROTOCOLS)
                        .header(header::UPGRADE, LEGACY_SPDY_UPGRADE)
                        .header("X-Stream-Protocol-Version", LEGACY_STREAM_PROTOCOL)
                        .body(String::new())
                        .unwrap()
                }
                FallbackScenario::FailedFallback => Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .body(String::new())
                    .unwrap(),
            });
        }

        Ok(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(String::new())
            .unwrap())
    }

    async fn spawn_fake_apiserver(
        scenario: FallbackScenario,
    ) -> (Uri, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback listener");
        let addr = listener.local_addr().expect("listener has a local addr");
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let svc = service_fn(move |req| handle_upgrade_request(scenario, req));
                    let _ = http1::Builder::new()
                        .serve_connection(io, svc)
                        .with_upgrades()
                        .await;
                });
            }
        });
        let uri: Uri = format!("http://{addr}")
            .parse()
            .expect("loopback address is a valid URI");
        (uri, handle)
    }

    fn fake_kube_client(cluster_url: &Uri) -> kube::Client {
        kube::Client::try_from(kube::Config::new(cluster_url.clone()))
            .expect("build a kube::Client against a local, TLS-free apiserver")
    }

    fn recording_callback() -> (
        RecoveryCallback,
        Arc<crossbeam_queue::SegQueue<RecoverySignal>>,
    ) {
        let calls = Arc::new(crossbeam_queue::SegQueue::new());
        let sink = Arc::clone(&calls);
        let callback: RecoveryCallback = Arc::new(move |signal| {
            sink.push(signal);
        });
        (callback, calls)
    }

    #[tokio::test]
    async fn successful_fallback_reports_no_upgrade_failed() {
        let (cluster_url, server) =
            spawn_fake_apiserver(FallbackScenario::SuccessfulFallback).await;
        let kube_client = fake_kube_client(&cluster_url);
        let (callback, calls) = recording_callback();

        let result = upgrade_spdy_with_fallback(
            &kube_client,
            &cluster_url,
            "default",
            "demo-pod",
            &callback,
        )
        .await;
        server.abort();

        match result {
            Ok(upgraded) => assert!(matches!(upgraded.protocol, Subprotocol::LegacySpdy)),
            Err(e) => panic!("expected the legacy fallback to succeed, got: {e}"),
        }
        assert!(
            calls.is_empty(),
            "a successful fallback must not emit a recovery signal"
        );
    }

    #[tokio::test]
    async fn failed_fallback_reports_terminal_upgrade_failure_once() {
        let (cluster_url, server) = spawn_fake_apiserver(FallbackScenario::FailedFallback).await;
        let kube_client = fake_kube_client(&cluster_url);
        let (callback, calls) = recording_callback();

        let result = upgrade_spdy_with_fallback(
            &kube_client,
            &cluster_url,
            "default",
            "demo-pod",
            &callback,
        )
        .await;
        server.abort();

        match result {
            Ok(_) => panic!("expected both upgrade attempts to fail"),
            Err(Error::UpgradeFailed { status, .. }) => {
                assert_eq!(status, Some(502));
            }
            Err(e) => panic!("expected a terminal UpgradeFailed, got: {e}"),
        }

        assert_eq!(
            calls.len(),
            1,
            "a terminal failure must emit exactly one recovery signal"
        );
        match calls.pop() {
            Some(RecoverySignal::UpgradeFailed { status, .. }) => {
                assert_eq!(status, Some(502));
            }
            other => panic!("unexpected recovery signal: {other:?}"),
        }
    }
}
