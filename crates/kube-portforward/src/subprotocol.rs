/// Protocol negotiated for a port-forward session.
///
/// Both variants speak the SPDY/3.1 frame format on the wire. They only
/// differ in how frames reach the apiserver: tunnelled inside WebSocket
/// binary messages, or sent straight over a raw HTTP/1.1 upgrade.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Subprotocol {
    /// `SPDY/3.1+portforward.k8s.io` — SPDY frames tunnelled inside
    /// WebSocket binary messages. Negotiated via `Sec-WebSocket-Protocol`
    Spdy31Tunnel,
    /// Legacy SPDY/3.1 over a raw HTTP upgrade ( without WebSocket).
    /// Falls back here when the apiserver rejects the upgrade
    LegacySpdy,
}

impl std::fmt::Display for Subprotocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spdy31Tunnel => f.write_str("SPDY/3.1+ws"),
            Self::LegacySpdy => f.write_str("SPDY/3.1"),
        }
    }
}
