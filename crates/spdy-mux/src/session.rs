use std::sync::atomic::{
    AtomicU64,
    AtomicUsize,
    Ordering,
};
use std::time::Instant;

use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::mux::{
    MuxConfig,
    MuxHandle,
};
use crate::stream::Stream;
use crate::transport::{
    WsFrameReader,
    WsFrameWriter,
};

/// Per-handle load metrics for P2C routing.
/// Cost = (inflight_streams + 1) * rtt_estimate_ns. Lower cost = preferred
/// handle.
///
/// RTT timestamps are tracked per-call via the [`RttSample`] guard, not
/// stored in shared state. This avoids the race where two concurrent opens
/// would clobber each other's start timestamps.
struct HandleMetrics {
    /// Exponentially weighted RTT estimate in nanoseconds.
    rtt_ns: AtomicU64,
}

impl HandleMetrics {
    const fn new() -> Self {
        Self {
            // seed with 1ms to avoid zero-cost bias before first measurement.
            rtt_ns: AtomicU64::new(1_000_000),
        }
    }

    /// Begin an RTT measurement. The returned guard records the sample
    /// when you call `complete()`. If dropped without calling `complete()`,
    /// no measurement is recorded — that's intentional for early-return
    /// paths (closed handle, capacity exhausted).
    fn start_sample(&self) -> RttSample<'_> {
        RttSample {
            metrics: self,
            start: Instant::now(),
        }
    }

    /// Update the RTT estimate using Peak-EWMA: adopt new peaks immediately,
    /// decay toward measurements below the peak.
    ///
    /// Uses a compare-and-swap loop so concurrent updates from racing opens
    /// don't lose samples. Failure to CAS just retries; the cost is bounded
    /// by the number of concurrent opens on one handle (typically 1-2).
    fn record_rtt(&self, elapsed_ns: u64) {
        let mut prev = self.rtt_ns.load(Ordering::Relaxed);
        loop {
            let next = if elapsed_ns > prev {
                // new peak: adopt immediately for fast spike adaptation.
                elapsed_ns
            } else {
                // decay toward current measurement: new = prev*0.9 +
                // elapsed*0.1
                (prev / 10) * 9 + elapsed_ns / 10
            };
            match self.rtt_ns.compare_exchange_weak(
                prev,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => prev = actual,
            }
        }
    }

    /// P2C cost metric: (inflight + 1) × rtt_estimate.
    fn cost(&self, inflight: usize) -> u64 {
        let rtt = self.rtt_ns.load(Ordering::Relaxed);
        rtt.saturating_mul((inflight as u64).saturating_add(1))
    }
}

/// Per-call RTT measurement. Calling [`complete`] records the elapsed
/// time. Dropping without completing is intentional (early-return paths).
struct RttSample<'a> {
    metrics: &'a HandleMetrics,
    start: Instant,
}

impl RttSample<'_> {
    fn complete(self) {
        let elapsed_ns = self.start.elapsed().as_nanos();
        // cap at u64::MAX (won't happen in practice but defensive).
        let elapsed_ns = u64::try_from(elapsed_ns).unwrap_or(u64::MAX);
        self.metrics.record_rtt(elapsed_ns);
    }
}

/// SPDY/3.1 session: one or more transport connections carrying paired
/// streams to a SPDY peer.
///
/// When `pool_size > 1`, each transport gets its own reader/writer task
/// pair and streams are distributed via power-of-two-choices across the
/// pool for parallel writes at high concurrency. Pool size 1 keeps the
/// original single-connection behaviour.
///
/// # Transport break contract
///
/// When a transport closes or errors, every stream on that handle
/// receives `BrokenPipe`. The session doesn't reconnect. Layers above
/// (typically a forwarder) open a fresh session on transport failure.
pub struct Session {
    pool: Vec<MuxHandle>,
    metrics: Vec<HandleMetrics>,
    next: AtomicUsize,
    cancel: CancellationToken,
}

impl Session {
    /// Create a session with explicit configuration from pre-split WebSocket
    /// transport pairs.
    ///
    /// Each `(writer, reader)` pair gets its own `MuxHandle` with independent
    /// reader/writer tasks. All handshakes and initial PING roundtrips
    /// complete before this method returns. Streams are then distributed
    /// round-robin across the pool.
    ///
    /// # Graceful degradation
    ///
    /// If some connections fail their initial PING but at least one succeeds,
    /// the session proceeds with the healthy subset. Only returns an error
    /// when ALL connections fail (or the input is empty).
    pub async fn with_config<W, R>(
        connections: Vec<(W, R)>, cancel: CancellationToken, config: MuxConfig,
    ) -> Result<Self, Error>
    where
        W: WsFrameWriter + 'static,
        R: WsFrameReader + 'static,
    {
        if connections.is_empty() {
            return Err(Error::MuxClosed);
        }
        let total = connections.len();
        let mut pool = Vec::with_capacity(total);
        let mut last_error = None;
        for (i, (writer, reader)) in connections.into_iter().enumerate() {
            match MuxHandle::spawn(writer, reader, cancel.child_token(), config.clone()).await {
                Ok(mux) => pool.push(mux),
                Err(e) => {
                    tracing::warn!(
                        index = i,
                        total,
                        error = %e,
                        "SPDY pool: connection {}/{} failed initial PING, skipping",
                        i + 1,
                        total,
                    );
                    last_error = Some(e);
                }
            }
        }
        if pool.is_empty() {
            // all connections failed: propagate the last error.
            return Err(last_error.unwrap_or(Error::MuxClosed));
        }
        if pool.len() < total {
            tracing::info!(
                healthy = pool.len(),
                total,
                "SPDY pool: proceeding with {}/{} connections",
                pool.len(),
                total,
            );
        }
        let metrics = (0..pool.len()).map(|_| HandleMetrics::new()).collect();
        Ok(Self {
            pool,
            metrics,
            next: AtomicUsize::new(0),
            cancel,
        })
    }

    /// Open a paired stream using power-of-two-choices with Peak-EWMA
    /// load estimation.
    ///
    /// Picks two random live handles, compares their cost
    /// (inflight × RTT estimate), and opens on the cheaper one. Falls back
    /// to a round-robin scan when both picks are at capacity or closed.
    ///
    /// `error_headers` and `data_headers` are passed verbatim to the codec
    /// as the SYN_STREAM header block for the respective stream. The
    /// session doesn't interpret them.
    pub fn open_stream_pair(
        &self, error_headers: Vec<(String, String)>, data_headers: Vec<(String, String)>,
    ) -> impl Future<Output = Result<Stream, Error>> {
        futures::future::lazy(move |_| {
            let pool_size = self.pool.len();

            if pool_size >= 2 {
                let (a, b) = self.pick_two(pool_size);
                let preferred = if self.handle_cost(a) <= self.handle_cost(b) {
                    [a, b]
                } else {
                    [b, a]
                };
                for &idx in &preferred {
                    if let Some(stream) =
                        self.try_open(idx, error_headers.clone(), data_headers.clone())?
                    {
                        return Ok(stream);
                    }
                }
            }

            for round in 0..pool_size {
                let idx = self.next.fetch_add(1, Ordering::Relaxed) % pool_size;
                if let Some(stream) =
                    self.try_open(idx, error_headers.clone(), data_headers.clone())?
                {
                    return Ok(stream);
                }
                tracing::debug!(
                    handle = idx,
                    round,
                    "SPDY session: handle unavailable, trying next"
                );
            }

            Err(Error::CapacityExhausted {
                in_use: self.in_use(),
                limit: self.capacity() as u32,
            })
        })
    }

    /// Try to open a stream on the given handle index.
    /// Returns Ok(Some(stream)) on success, Ok(None) if handle is closed or
    /// at capacity, Err on fatal errors.
    ///
    /// RTT is measured per-call via [`RttSample`], only recorded on success
    /// to avoid contaminating the load estimate with capacity-rejection
    /// latency (which is fast and unrepresentative of actual stream-open
    /// cost).
    fn try_open(
        &self, idx: usize, error_headers: Vec<(String, String)>,
        data_headers: Vec<(String, String)>,
    ) -> Result<Option<Stream>, Error> {
        let mux = &self.pool[idx];
        if mux.is_closed() {
            return Ok(None);
        }
        let sample = self.metrics[idx].start_sample();
        match mux.open_stream_pair(error_headers, data_headers) {
            Ok(stream) => {
                sample.complete();
                tracing::debug!(
                    handle = idx,
                    active = mux.active_pairs(),
                    cost = self.handle_cost(idx),
                    "SPDY session: stream opened via P2C"
                );
                Ok(Some(stream))
            }
            Err(Error::CapacityExhausted { .. }) => {
                // sample dropped without complete(): no spurious RTT record.
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// P2C cost for a handle: inflight × rtt_estimate.
    /// Closed handles get u64::MAX cost (never selected).
    fn handle_cost(&self, idx: usize) -> u64 {
        let mux = &self.pool[idx];
        if mux.is_closed() {
            return u64::MAX;
        }
        self.metrics[idx].cost(mux.active_pairs())
    }

    /// Pick two distinct random indices using xorshift on the atomic counter.
    /// Cheap and good enough for load balancing (no rand dependency needed).
    fn pick_two(&self, pool_size: usize) -> (usize, usize) {
        // use fetch_add as a cheap entropy source.
        let seed = self.next.fetch_add(1, Ordering::Relaxed) as u64;
        let a = (seed % pool_size as u64) as usize;
        // LCG multiplier from Knuth's MMIX (also used by PCG family).
        let b = ((seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1)) % pool_size as u64)
            as usize;
        if a == b {
            (a, (a + 1) % pool_size)
        } else {
            (a, b)
        }
    }

    /// Total capacity across all live pool members (hard cap).
    pub fn capacity(&self) -> usize {
        self.pool
            .iter()
            .filter(|m| !m.is_closed())
            .map(|m| m.max_concurrent() as usize)
            .sum()
    }

    /// Total operating capacity across all live pool members (scheduling cap).
    pub fn operating_capacity(&self) -> usize {
        self.pool
            .iter()
            .filter(|m| !m.is_closed())
            .map(MuxHandle::operating_capacity)
            .sum()
    }

    pub fn in_use(&self) -> usize {
        self.pool.iter().map(MuxHandle::active_pairs).sum()
    }

    pub fn available(&self) -> usize {
        self.capacity().saturating_sub(self.in_use())
    }

    pub fn is_full(&self) -> bool {
        self.pool
            .iter()
            .all(|m| m.is_closed() || m.active_pairs() >= m.max_concurrent() as usize)
    }

    /// Returns true when all underlying WebSockets have closed.
    pub fn is_drained(&self) -> bool {
        self.pool.iter().all(MuxHandle::is_closed)
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Close the SPDY session by cancelling the mux tasks.
    pub fn close(self) -> impl Future<Output = Result<(), Error>> {
        futures::future::lazy(move |_| {
            self.cancel.cancel();
            drop(self);
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::{
        AsyncReadExt,
        AsyncWriteExt,
        DuplexStream,
        ReadHalf,
        WriteHalf,
    };
    use tokio::sync::oneshot;

    use super::*;
    use crate::codec::SpdyCodec;
    use crate::transport::{
        RawSpdyReader,
        RawSpdyWriter,
        split_raw_spdy,
    };

    const TEST_DUPLEX_BUF: usize = 64 * 1024;

    type ClientConn = (
        RawSpdyWriter<WriteHalf<DuplexStream>>,
        RawSpdyReader<ReadHalf<DuplexStream>>,
    );

    fn healthy_connection() -> (ClientConn, oneshot::Sender<()>) {
        let (client_io, peer_io) = tokio::io::duplex(TEST_DUPLEX_BUF);
        let (kill_tx, mut kill_rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let (mut peer_read, mut peer_write) = tokio::io::split(peer_io);
            let codec = SpdyCodec::with_max_frame_size(1024 * 1024);
            let ping = codec.encode_ping(1);
            if peer_write.write_all(&ping).await.is_err() {
                return;
            }
            if peer_write.flush().await.is_err() {
                return;
            }
            let mut buf = [0u8; 4096];
            loop {
                tokio::select! {
                    _ = &mut kill_rx => break,
                    res = peer_read.read(&mut buf) => {
                        match res {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {}
                        }
                    }
                }
            }
        });
        (split_raw_spdy(client_io), kill_tx)
    }

    fn dead_connection() -> ClientConn {
        let (client_io, peer_io) = tokio::io::duplex(TEST_DUPLEX_BUF);
        drop(peer_io);
        split_raw_spdy(client_io)
    }

    async fn wait_until(mut condition: impl FnMut() -> bool, timeout: Duration) -> bool {
        let start = tokio::time::Instant::now();
        loop {
            if condition() {
                return true;
            }
            if start.elapsed() >= timeout {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn dead_peer_does_not_cancel_healthy_siblings() {
        let parent = CancellationToken::new();
        let (conn_a, kill_a) = healthy_connection();
        let (conn_b, _keep_b) = healthy_connection();

        let session =
            Session::with_config(vec![conn_a, conn_b], parent.clone(), MuxConfig::default())
                .await
                .expect("both peers should complete the initial PING handshake");
        assert_eq!(session.pool.len(), 2);
        assert!(!session.pool[0].is_closed());
        assert!(!session.pool[1].is_closed());

        let _ = kill_a.send(());

        assert!(wait_until(|| session.pool[0].is_closed(), Duration::from_secs(2)).await);
        assert!(!session.pool[1].is_closed());

        for _ in 0..4 {
            session
                .open_stream_pair(Vec::new(), Vec::new())
                .await
                .expect("the healthy handle should still accept streams");
        }
    }

    #[tokio::test]
    async fn parent_cancellation_closes_all_children() {
        let parent = CancellationToken::new();
        let (conn_a, _keep_a) = healthy_connection();
        let (conn_b, _keep_b) = healthy_connection();

        let session =
            Session::with_config(vec![conn_a, conn_b], parent.clone(), MuxConfig::default())
                .await
                .expect("both peers should complete the initial PING handshake");
        assert!(!session.is_drained());

        parent.cancel();

        assert!(wait_until(|| session.is_drained(), Duration::from_secs(2)).await);
    }

    #[tokio::test]
    async fn partial_initialization_retains_healthy_connections() {
        let parent = CancellationToken::new();
        let (healthy, _keep_alive) = healthy_connection();
        let dead = dead_connection();

        let session =
            Session::with_config(vec![healthy, dead], parent.clone(), MuxConfig::default())
                .await
                .expect("the healthy connection should survive partial initialization");

        assert_eq!(session.pool.len(), 1);
        assert!(!session.is_drained());

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!session.pool[0].is_closed());

        session
            .open_stream_pair(Vec::new(), Vec::new())
            .await
            .expect("the surviving handle should still accept streams");
    }
}
