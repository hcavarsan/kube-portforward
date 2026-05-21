# kube-portforward

Kubernetes port-forward over multiplexed SPDY/3.1 with WebSocket transport,
falling back to the legacy `Upgrade: SPDY/3.1` path.

This workspace contains two crates:

| Crate | crates.io | docs.rs | What it is |
|---|---|---|---|
| [`spdy-mux`](crates/spdy-mux/README.md) | [![Crates.io](https://img.shields.io/crates/v/spdy-mux.svg)](https://crates.io/crates/spdy-mux) | [![Docs.rs](https://img.shields.io/docsrs/spdy-mux)](https://docs.rs/spdy-mux) | SPDY/3.1 stream multiplexer over WebSocket or raw transports. Kubernetes-agnostic. |
| [`kube-portforward`](crates/kube-portforward/README.md) | [![Crates.io](https://img.shields.io/crates/v/kube-portforward.svg)](https://crates.io/crates/kube-portforward) | [![Docs.rs](https://img.shields.io/docsrs/kube-portforward)](https://docs.rs/kube-portforward) | Kubernetes port-forward built on `spdy-mux`. Pool, fallback, watcher, drain. |

## Install

```sh
cargo add kube-portforward
# or, lower-level:
cargo add spdy-mux
```

## License

GPL-3.0-only. See [LICENSE](LICENSE).
