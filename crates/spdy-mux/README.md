# spdy-mux

SPDY/3.1 stream multiplexer in Rust.

## Why

Rust does not have a working SPDY/3.1 multiplexer. Every implementation I found was either unmaintained, only the byte-channel framing that runs inside a SPDY stream, or a stub pointing at the Go reference code with a `todo!()`.

So I wrote one. It speaks SPDY/3.1 framing with the standard zlib dictionary, runs a small pool of parallel transports with power-of-two-choices load balancing, and handles per-stream and session-level flow control plus PING keepalive.

It knows nothing about Kubernetes. You supply the headers and the codec sends them verbatim.

## Who needs it

You want this if you do Kubernetes port-forward, or if you talk to a CRI runtime like containerd or CRI-O. SPDY is no longer in browsers or in the proxy ecosystem, and Kubernetes itself is migrating off it (KEP-4006: WebSocket-tunneled streaming, Beta since 1.31, kubelet leg Beta in 1.36).

If you pick a streaming protocol from scratch in 2026, use HTTP/2 or QUIC. SPDY is here because Kubernetes still uses it, and that migration will take years.

## The shape

Every SPDY/3.1 peer I have hit on the wire uses the same pattern: open two streams together, one for data and one for errors, with the error stream half-closed at open time. The API enforces this. You call `open_stream_pair(error_headers, data_headers)` and you get back a `Stream` that wraps the two together.

If your peer wants single streams or a different pair convention, this crate will not fit. You can build a different session type on top of the codec layer (`codec.rs`, `dictionary.rs`, `transport.rs`), which is pure SPDY/3.1 framing, but the multiplexer API is opinionated.

## Tradeoffs

No community. Nobody else is fuzz-testing Rust SPDY code. The Go reference implementation still receives security fixes in 2026 for things like header accounting and frame-length enforcement, and those bug classes apply to any SPDY/3.1 implementation. I track upstream commits.

Lazy open is built in. The codec does not put SYN_STREAM on the wire until you write the first byte. The kubelet dials the upstream eagerly on SYN_STREAM, and a fast-closing target server will close the idle TCP before you ever use it. Lazy open avoids that race.

Zlib header compression carries the CRIME attack surface. SPDY/3.1 was the original target of CVE-2012-4930. Inside a Kubernetes API server connection over TLS the risk is low, but the bug class is real.

## Quick start

```rust,no_run
use spdy_mux::{MuxConfig, Session, split_raw_spdy};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

# async fn run<S>(upgraded: S) -> Result<(), Box<dyn std::error::Error>>
# where S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static {
let (writer, reader) = split_raw_spdy(upgraded);
let cancel = CancellationToken::new();

let session =
    Session::with_config(vec![(writer, reader)], cancel, MuxConfig::default()).await?;

// You supply the headers. The codec sends them verbatim.
let error_headers = vec![
    ("streamtype".into(), "error".into()),
    ("port".into(), "8080".into()),
    ("requestid".into(), "0".into()),
];
let data_headers = vec![
    ("streamtype".into(), "data".into()),
    ("port".into(), "8080".into()),
    ("requestid".into(), "0".into()),
];

let mut stream = session.open_stream_pair(error_headers, data_headers).await?;
stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await?;
# Ok(()) }
```

For a pool of parallel transports, pass them all to `with_config`:

```rust,ignore
let pairs: Vec<_> = upgrades.into_iter().map(split_raw_spdy).collect();
let session = Session::with_config(pairs, cancel, MuxConfig::default()).await?;
```

If a pool member dies, the session evicts it and keeps serving from the remaining transports.

## Architecture

```
Session::open_stream_pair(error_headers, data_headers)
  |
  v
Pool of MuxHandles (P2C routing on inflight x rtt estimate)
  |
  v
MuxHandle: 1 reader + 5 worker tasks + 1 writer + 1 supervisor
  |
  v
WsFrameReader / WsFrameWriter (transport adapter)
fastwebsockets, or raw AsyncRead + AsyncWrite
```

Each connection runs one reader task, five frame workers partitioned by `stream_id % 5`, one writer task, and a supervisor that cancels the session if any task exits unexpectedly. The codec is shared across all tasks on a connection.

## The longer story

This exists for the same reason `kube-portforward` exists one layer up. I was building a port-forward desktop tool. It fell over under wrk and vegeta load. The fix was multiplexing, and the Rust ecosystem had nothing to multiplex with.

I read the Go reference implementation and traced kubectl wire bytes against a real cluster, which was the only way to get to a working codec. Most of it followed the SPDY/3.1 spec directly. The hard parts were wire-order quirks that the spec does not mention but the kubelet enforces anyway. The `writer.rs` comments call those out where I found them.

## Examples

Both examples are self-contained. Each one deploys an `nginx:alpine` pod, runs a port-forward through `spdy-mux`, then deletes the pod. You only need a reachable cluster via `KUBECONFIG` (or `~/.kube/config`).

```
cargo run -p spdy-mux --example k8s_raw_spdy
cargo run -p spdy-mux --example k8s_websocket_over_spdy
```

- `k8s_raw_spdy`: legacy `Upgrade: SPDY/3.1` over HTTP/1.1, the original kubectl wire format. Uses `split_raw_spdy`.
- `k8s_websocket_over_spdy`: WebSocket-tunnelled SPDY (KEP-4006, `SPDY/3.1+portforward.k8s.io`, default since Kubernetes 1.31). Uses `split_fastws`.

If you want pool management, pod watching, recovery callbacks, graceful drain, and automatic fallback between the two paths, use `kube-portforward`, which sits on top of this crate.

## License

GPL-3.0.
