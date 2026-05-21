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
        Err(e) if should_fallback(&e) => {
            tracing::info!(
                pod = %pod,
                error = %e,
                "SPDY-over-WebSocket rejected, falling back to legacy SPDY upgrade"
            );
            let status = if let Error::UpgradeFailed { status, .. } = &e {
                *status
            } else {
                None
            };
            recovery_callback(RecoverySignal::UpgradeFailed {
                status,
                message: e.to_string(),
            });
            upgrade_legacy_spdy(kube_client, cluster_url, namespace, pod).await
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
