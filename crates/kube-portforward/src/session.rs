use std::sync::atomic::{
    AtomicBool,
    AtomicU32,
    Ordering,
};

use crossbeam_queue::ArrayQueue;
use tokio_util::sync::CancellationToken;

use crate::error::Error;
use crate::stream::Stream;
use crate::subprotocol::Subprotocol;

const SPARE_STREAM_CAP: usize = 16;

/// When spare count drops to or below this threshold, the background
/// replenisher refills
const SPARE_STREAM_LOW_WATERMARK: usize = 8;

/// One header  carried on a SPDY SYN_STREAM frame.
type SpdyHeader = (String, String);

/// The error stream and data stream header lists for one paired
/// `portforward.k8s.io` connection.
type PortforwardHeaderPair = (Vec<SpdyHeader>, Vec<SpdyHeader>);

/// One port forward session that multiplexes many concurrent
/// local connections over a pool of upgraded connections to the apiserver.
pub struct Session {
    inner: spdy_mux::Session,
    protocol: Subprotocol,
    /// Target pod port. The kubelet expects this in the SYN_STREAM
    /// `port` header for every paired stream we open.
    port: u16,
    /// request id counter, kubelet uses this header to
    /// pair the data and error streams of one logical TCP connection.
    next_request_id: AtomicU32,
    /// Pre-opened spare streams for instant connect(). Background task
    /// replenishes when count drops to or below `SPARE_STREAM_LOW_WATERMARK`.
    spare_streams: ArrayQueue<Stream>,
    /// Guard against concurrent replenishment. Set by `replenish_spare_streams`
    /// on entry, cleared on exit.
    replenishing: AtomicBool,
}

impl Session {
    pub(crate) fn from_spdy(session: spdy_mux::Session, protocol: Subprotocol, port: u16) -> Self {
        Self {
            spare_streams: ArrayQueue::new(SPARE_STREAM_CAP),
            replenishing: AtomicBool::new(false),
            inner: session,
            protocol,
            port,
            next_request_id: AtomicU32::new(0),
        }
    }

    /// Build the K8s `portforward.k8s.io v1`  headers for one
    /// stream-pair connection, header names are lowercase
    fn portforward_headers(&self) -> PortforwardHeaderPair {
        let request_id = self
            .next_request_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string();
        let port = self.port.to_string();
        let error_headers = vec![
            ("streamtype".to_string(), "error".to_string()),
            ("port".to_string(), port.clone()),
            ("requestid".to_string(), request_id.clone()),
        ];
        let data_headers = vec![
            ("streamtype".to_string(), "data".to_string()),
            ("port".to_string(), port),
            ("requestid".to_string(), request_id),
        ];
        (error_headers, data_headers)
    }

    /// Grab the next stream and return a bidirectional [`Stream`].
    pub async fn connect(&self) -> Result<Stream, Error> {
        while let Some(stream) = self.spare_streams.pop() {
            if !stream.is_read_closed() {
                return Ok(stream);
            }
            tracing::debug!("spare stream stale (remote closed while idle), discarding");
        }
        self.open_new_stream().await
    }

    async fn open_new_stream(&self) -> Result<Stream, Error> {
        let (error_headers, data_headers) = self.portforward_headers();
        self.inner
            .open_stream_pair(error_headers, data_headers)
            .await
            .map(Stream::from_spdy)
            .map_err(Error::from)
    }

    /// Pre open spare streams up to `SPARE_STREAM_CAP`.
    pub async fn replenish_spare_streams(&self) {
        if self
            .replenishing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let _guard = ReplenishGuard(&self.replenishing);

        while self.spare_streams.len() < SPARE_STREAM_CAP {
            if self.is_full() || self.cancellation_token().is_cancelled() {
                break;
            }
            match self.open_new_stream().await {
                Ok(stream) => {
                    if self.spare_streams.push(stream).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }

    pub fn spare_count(&self) -> usize {
        self.spare_streams.len()
    }

    pub fn needs_replenish(&self) -> bool {
        self.spare_count() <= SPARE_STREAM_LOW_WATERMARK
    }

    pub const fn protocol(&self) -> Subprotocol {
        self.protocol
    }

    /// Max concurrent streams this session can hold.
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    pub fn operating_capacity(&self) -> usize {
        self.inner.operating_capacity()
    }

    pub fn in_use(&self) -> usize {
        self.inner.in_use()
    }

    pub fn available(&self) -> usize {
        self.inner.available()
    }

    pub fn is_full(&self) -> bool {
        self.inner.is_full()
    }

    pub fn is_drained(&self) -> bool {
        self.inner.is_drained()
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.inner.cancellation_token()
    }

    /// Gracefully close the session.
    pub async fn close(self) -> Result<(), Error> {
        self.inner.close().await.map_err(Error::from)
    }
}

/// guard that clears the `replenishing` flag on drop.
struct ReplenishGuard<'a>(&'a AtomicBool);

impl Drop for ReplenishGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::io;
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

    type LoopbackConn = (
        spdy_mux::RawSpdyWriter<WriteHalf<DuplexStream>>,
        spdy_mux::RawSpdyReader<ReadHalf<DuplexStream>>,
    );

    fn loopback_conn(kill_rx: Option<oneshot::Receiver<()>>) -> LoopbackConn {
        let (client, mut server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let echo = async {
                loop {
                    let mut header = [0; 8];
                    if server.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    let length = u32::from_be_bytes([0, header[5], header[6], header[7]]) as usize;
                    let mut payload = vec![0; length];
                    if server.read_exact(&mut payload).await.is_err() {
                        return;
                    }
                    let control = header[0] & 0x80 != 0;
                    let ping = control && u16::from_be_bytes([header[2], header[3]]) == 6;
                    if (ping || (!control && !payload.is_empty()))
                        && (server.write_all(&header).await.is_err()
                            || server.write_all(&payload).await.is_err())
                    {
                        return;
                    }
                }
            };
            if let Some(kill) = kill_rx {
                tokio::select! {
                    _ = kill => {}
                    () = echo => {}
                }
            } else {
                echo.await;
            }
        });
        spdy_mux::split_raw_spdy(client)
    }

    #[tokio::test]
    async fn connect_skips_a_cached_spare_bound_to_a_dead_mux() {
        let (kill_tx, kill_rx) = oneshot::channel();
        let mux = spdy_mux::Session::with_config(
            vec![loopback_conn(Some(kill_rx)), loopback_conn(None)],
            CancellationToken::new(),
            spdy_mux::MuxConfig::default(),
        )
        .await
        .unwrap();
        let session = Session::from_spdy(mux, Subprotocol::LegacySpdy, 8080);
        let capacity = session.inner.capacity();
        for _ in 0..2 {
            let spare = session.open_new_stream().await.unwrap();
            assert!(session.spare_streams.push(spare).is_ok());
        }
        kill_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while session.inner.capacity() == capacity {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for _ in 0..2 {
            let mut stream = session.connect().await.unwrap();
            let payload = b"hello-through-the-healthy-transport";
            let received = tokio::time::timeout(Duration::from_secs(5), async {
                stream.write_all(payload).await?;
                let mut received = vec![0; payload.len()];
                stream.read_exact(&mut received).await?;
                Ok::<_, io::Error>(received)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(received, payload);
        }
    }
}
