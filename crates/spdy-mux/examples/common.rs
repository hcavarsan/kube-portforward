//! Shared helpers: deploy/teardown a throwaway nginx pod, pretty-print
//! HTTP upgrades, drive an HTTP request through a SPDY stream.
//! Included via `#[path = "common.rs"] mod common;`.

#![allow(dead_code, unreachable_pub)]

use std::time::Duration;

use anyhow::{
    Context,
    Result,
    bail,
};
use http::{
    HeaderMap,
    Method,
    Response,
    StatusCode,
    Uri,
};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api,
    DeleteParams,
    PostParams,
};
use kube::client::Body;
use kube::runtime::wait::{
    Condition,
    await_condition,
};
use serde_json::json;
use spdy_mux::{
    Session,
    Stream,
};
use tokio::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

/// Create `namespace/name` running `nginx:alpine`, wait up to 90s for Ready.
pub async fn ensure_nginx_pod(
    kube_client: &kube::Client, namespace: &str, name: &str,
) -> Result<String> {
    let pods: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);

    if pods.get_opt(name).await?.is_none() {
        let manifest: Pod = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": name,
                "labels": { "app": name, "managed-by": "spdy-mux-example" },
            },
            "spec": {
                "containers": [{
                    "name": "nginx",
                    "image": "nginx:alpine",
                    "ports": [{ "containerPort": 80 }],
                }],
                "terminationGracePeriodSeconds": 1,
            }
        }))?;
        pods.create(&PostParams::default(), &manifest)
            .await
            .with_context(|| format!("create pod {namespace}/{name}"))?;
        println!("* created pod {namespace}/{name}, waiting for Ready");
    } else {
        println!("* pod {namespace}/{name} exists, reusing");
    }

    let ready = await_condition(pods.clone(), name, is_pod_ready());
    match tokio::time::timeout(Duration::from_secs(90), ready).await {
        Ok(Ok(_)) => {
            println!("* pod {namespace}/{name} Ready");
            Ok(name.to_string())
        }
        Ok(Err(e)) => bail!("waiting for pod ready: {e}"),
        Err(_) => bail!("pod {namespace}/{name} did not become Ready within 90s"),
    }
}

/// Best-effort delete. Errors logged, not propagated.
pub async fn delete_pod(kube_client: &kube::Client, namespace: &str, name: &str) {
    let pods: Api<Pod> = Api::namespaced(kube_client.clone(), namespace);
    match pods.delete(name, &DeleteParams::default()).await {
        Ok(_) => println!("* deleted pod {namespace}/{name}"),
        Err(e) => eprintln!("* delete {namespace}/{name} failed: {e}"),
    }
}

/// Send `req` via `kube::Client`, printing request + response in
/// curl `-v` shape. Returns the response.
pub async fn send_with_trace(
    kube_client: &kube::Client, req: http::Request<Body>,
) -> Result<Response<Body>> {
    print_request(req.method(), req.uri(), req.headers());
    let res = kube_client.send(req).await?;
    print_response(res.status(), res.headers());
    Ok(res)
}

fn print_request(method: &Method, uri: &Uri, headers: &HeaderMap) {
    println!("> {method} {uri}");
    for (name, value) in headers {
        println!("> {name}: {}", value.to_str().unwrap_or("<binary>"));
    }
    println!(">");
}

fn print_response(status: StatusCode, headers: &HeaderMap) {
    println!("< HTTP/1.1 {status}");
    for (name, value) in headers {
        println!("< {name}: {}", value.to_str().unwrap_or("<binary>"));
    }
    println!("<");
}

/// Open a kubelet-shaped paired stream on `session` for `port` and run
/// `GET /` against it. Prints the SPDY stream headers and the raw HTTP
/// bytes flowing through.
pub async fn nginx_get(session: &Session, port: u16) -> Result<()> {
    let port_s = port.to_string();
    let mk = |kind: &str| {
        vec![
            ("streamtype".into(), kind.to_string()),
            ("port".into(), port_s.clone()),
            ("requestid".into(), "0".into()),
        ]
    };
    let error_headers = mk("error");
    let data_headers = mk("data");

    println!("> SYN_STREAM error");
    for (k, v) in &error_headers {
        println!(">   {k}: {v}");
    }
    println!("> SYN_STREAM data");
    for (k, v) in &data_headers {
        println!(">   {k}: {v}");
    }

    let mut stream: Stream = session.open_stream_pair(error_headers, data_headers).await?;
    println!("* paired stream open");

    let req = b"GET / HTTP/1.0\r\nHost: localhost\r\n\r\n";
    stream.write_all(req).await?;
    for line in std::str::from_utf8(req).unwrap_or("").lines() {
        println!("> {line}");
    }
    println!(">");

    // HTTP/1.0: read until the server half-closes for the full response.
    let mut buf = Vec::with_capacity(8192);
    stream.read_to_end(&mut buf).await?;
    let head = String::from_utf8_lossy(&buf[..buf.len().min(400)]);
    for line in head.lines() {
        println!("< {line}");
    }
    println!("* read {} bytes from pod", buf.len());
    Ok(())
}

fn is_pod_ready() -> impl Condition<Pod> {
    |obj: Option<&Pod>| {
        obj.and_then(|p| p.status.as_ref())
            .and_then(|s| s.conditions.as_ref())
            .map(|conds| {
                conds
                    .iter()
                    .any(|c| c.type_ == "Ready" && c.status == "True")
            })
            .unwrap_or(false)
    }
}
