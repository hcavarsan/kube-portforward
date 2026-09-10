use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool,
    Ordering,
};
use std::task::{
    Context,
    Poll,
};

use bytes::{
    Buf,
    Bytes,
    BytesMut,
};
use tokio::io::{
    AsyncBufRead,
    AsyncRead,
    AsyncWrite,
    ReadBuf,
};
use tokio::sync::mpsc;
use tokio_util::sync::PollSender;

use crate::mux::{
    MuxCommand,
    MuxHandle,
    SendWindow,
    StreamRegistration,
};

/// Bidirectional SPDY/3.1 stream pair: a writable "data" stream plus an
/// "error" stream half-closed at open time. The shape suits any peer that
/// uses paired streams (Kubernetes port-forward is one such peer; the
/// multiplexer treats the headers as opaque).
///
/// Streams are **lazily opened on the wire**: `MuxHandle::open_stream_pair`
/// reserves a session slot and creates the per-stream channels, but no
/// SPDY `SYN_STREAM` frame is sent until the consumer actually writes or
/// reads. This avoids the idle-upstream-close race for peers that dial
/// an upstream connection eagerly on `SYN_STREAM` while preserving the
/// pre-opened spare-stream throughput optimization.
///
/// Implements `AsyncRead + AsyncWrite` on the data half. The error half is
/// available via `split()`.
pub struct Stream {
    state: StreamState,
}

enum StreamState {
    /// Pair reserved, channels created, but no SPDY stream IDs allocated
    /// and no `SYN_STREAM` on the wire yet. Transitions to `Opened` on the
    /// first non-empty `poll_write`, or the first real `poll_read`.
    Unopened {
        error_headers: Vec<(String, String)>,
        data_headers: Vec<(String, String)>,
        mux: MuxHandle,
        data_rx: mpsc::Receiver<Bytes>,
        error_rx: mpsc::Receiver<Bytes>,
        /// Sender handed to the data worker at realize time. `Option` so it
        /// can be moved out without re-creating the channel.
        pending_data_tx: Option<mpsc::Sender<Bytes>>,
        pending_error_tx: Option<mpsc::Sender<Bytes>>,
        max_frame_size: u32,
        read_buf: Option<Bytes>,
        read_eof: bool,
        /// Guard that releases `active_pairs` on drop. Always present in
        /// the Unopened state.
        release_guard: Option<PairReleaseGuard>,
    },
    Opened {
        data_id: u32,
        data_rx: mpsc::Receiver<Bytes>,
        error_rx: mpsc::Receiver<Bytes>,
        write_tx: PollSender<MuxCommand>,
        send_window: Arc<SendWindow>,
        max_frame_size: u32,
        read_buf: Option<Bytes>,
        read_eof: bool,
        graceful_shutdown: Arc<AtomicBool>,
        guard: StreamGuard,
    },
    /// Terminal state used while moving out of `Unopened` during realize.
    /// Shouldn't be seen by a user.
    Transitioning,
}

/// Guards the session's `active_pairs` counter for unopened streams.
/// Once the stream realizes, the counter is owned by `StreamGuard` instead
/// and this guard is disarmed so drop becomes a no-op.
struct PairReleaseGuard {
    mux: MuxHandle,
    armed: bool,
}

impl PairReleaseGuard {
    const fn new(mux: MuxHandle) -> Self {
        Self { mux, armed: true }
    }

    /// Disarm without releasing. Use when ownership of the pair counter
    /// transfers to a `StreamGuard`.
    const fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for PairReleaseGuard {
    fn drop(&mut self) {
        if self.armed {
            self.mux.release_pair();
        }
    }
}

/// Open-stream guard owning the IDs, drop permits, and graceful-shutdown
/// flag. Replaces the previous `StreamGuard` and carries the same RST /
/// worker-close contract.
struct StreamGuard {
    data_id: u32,
    error_id: u32,
    mux: MuxHandle,
    ctrl_permit_error: Option<mpsc::OwnedPermit<MuxCommand>>,
    ctrl_permit_data: Option<mpsc::OwnedPermit<MuxCommand>>,
    close_reg_permit_error: Option<mpsc::OwnedPermit<StreamRegistration>>,
    close_reg_permit_data: Option<mpsc::OwnedPermit<StreamRegistration>>,
    /// Set to true when `poll_shutdown()` sends DATA+FIN (graceful half-close).
    /// When true, `Drop` skips RST_STREAM for the data stream. The peer
    /// already knows we're done writing and will close its end naturally.
    /// This mirrors TCP semantics: shutdown(SHUT_WR) + close() sends FIN,
    /// not RST.
    graceful_shutdown: Arc<AtomicBool>,
}

/// RST_STREAM status code for CANCEL.
const RST_STATUS_CANCEL: u32 = 5;

impl Drop for StreamGuard {
    fn drop(&mut self) {
        // guaranteed path: use pre-reserved permits for infallible delivery.
        // OwnedPermit::send() is synchronous, so no async is needed in Drop.
        let graceful = self.graceful_shutdown.load(Ordering::Acquire);

        // the error stream is already half-closed at open time: the
        // `OpenStreamPair` writer command emitted an empty
        // DATA+FIN on `error_id` right after the two SYN_STREAM frames
        // (matching kubectl's `errorStream.Close()` behavior). Sending
        // RST_STREAM here would be wrong — we never use the error stream
        // for writes after open, and the peer interprets RST_STREAM as
        // an abnormal termination. Drop the permit unused.
        //
        // data stream: skip RST if poll_shutdown() already sent DATA+FIN
        // (graceful half-close).
        let _ = self.ctrl_permit_error.take();
        if !graceful && let Some(permit) = self.ctrl_permit_data.take() {
            permit.send(MuxCommand::CloseStream {
                stream_id: self.data_id,
                status: RST_STATUS_CANCEL,
            });
        }

        // 2. notify workers via close-reg channels (stream entry cleanup and
        //    send-window poisoning).
        if let Some(permit) = self.close_reg_permit_error.take() {
            permit.send(StreamRegistration::Close {
                stream_id: self.error_id,
            });
        }
        if let Some(permit) = self.close_reg_permit_data.take() {
            permit.send(StreamRegistration::Close {
                stream_id: self.data_id,
            });
        }

        // 3. release the session slot.
        self.mux.release_pair();
    }
}

/// Runtime handles needed to construct a lazily-opened SPDY stream.
pub(crate) struct UnopenedStreamParts {
    pub error_headers: Vec<(String, String)>,
    pub data_headers: Vec<(String, String)>,
    pub mux: MuxHandle,
    pub data_rx: mpsc::Receiver<Bytes>,
    pub error_rx: mpsc::Receiver<Bytes>,
    pub pending_data_tx: mpsc::Sender<Bytes>,
    pub pending_error_tx: mpsc::Sender<Bytes>,
    pub max_frame_size: u32,
}

/// Result of a successful realize call: the wire-visible bits a stream
/// needs to switch into `Opened` state.
pub(crate) struct OpenedStreamParts {
    pub data_id: u32,
    pub error_id: u32,
    pub send_window: Arc<SendWindow>,
    pub ctrl_permit_error: mpsc::OwnedPermit<MuxCommand>,
    pub ctrl_permit_data: mpsc::OwnedPermit<MuxCommand>,
    pub close_reg_permit_error: mpsc::OwnedPermit<StreamRegistration>,
    pub close_reg_permit_data: mpsc::OwnedPermit<StreamRegistration>,
}

impl Stream {
    pub(crate) fn new_unopened(parts: UnopenedStreamParts) -> Self {
        let UnopenedStreamParts {
            error_headers,
            data_headers,
            mux,
            data_rx,
            error_rx,
            pending_data_tx,
            pending_error_tx,
            max_frame_size,
        } = parts;
        let release_guard = PairReleaseGuard::new(mux.clone());
        Self {
            state: StreamState::Unopened {
                error_headers,
                data_headers,
                mux,
                data_rx,
                error_rx,
                pending_data_tx: Some(pending_data_tx),
                pending_error_tx: Some(pending_error_tx),
                max_frame_size,
                read_buf: None,
                read_eof: false,
                release_guard: Some(release_guard),
            },
        }
    }

    /// Returns true if the remote has already closed this stream's read
    /// side (FIN or RST received while idle). Used by spare-stream checkout
    /// to discard stale pre-opened streams.
    ///
    /// Unopened streams are never stale: no `SYN_STREAM` was sent yet, so
    /// the apiserver hasn't created a backing pod TCP connection.
    pub fn is_read_closed(&self) -> bool {
        match &self.state {
            StreamState::Unopened {
                read_eof, data_rx, ..
            } => *read_eof || data_rx.is_closed(),
            StreamState::Opened {
                read_eof, data_rx, ..
            } => *read_eof || data_rx.is_closed(),
            StreamState::Transitioning => false,
        }
    }

    /// Split into data half (AsyncRead + AsyncWrite) and error half
    /// (AsyncRead).
    ///
    /// Splitting an unopened stream is supported: both halves share the
    /// same underlying open state, and whichever side reads or writes
    /// first drives the (synchronous) realize call.
    pub fn split(self) -> (DataStream, ErrorStream) {
        match self.state {
            StreamState::Unopened {
                error_headers,
                data_headers,
                mux,
                data_rx,
                error_rx,
                pending_data_tx,
                pending_error_tx,
                max_frame_size,
                read_buf,
                read_eof,
                release_guard,
                ..
            } => {
                let shared = Arc::new(parking_lot::Mutex::new(SharedSplitState::Unopened(
                    UnopenedShared {
                        error_headers,
                        data_headers,
                        mux,
                        pending_data_tx,
                        pending_error_tx,
                        release_guard,
                    },
                )));
                (
                    DataStream {
                        data_rx,
                        max_frame_size,
                        read_buf,
                        read_eof,
                        shared: Arc::clone(&shared),
                    },
                    ErrorStream {
                        error_rx,
                        error_buf: None,
                        error_eof: false,
                        shared,
                    },
                )
            }
            StreamState::Opened {
                data_id,
                data_rx,
                error_rx,
                write_tx,
                send_window,
                max_frame_size,
                read_buf,
                read_eof,
                graceful_shutdown,
                guard,
            } => {
                let opened = OpenedShared {
                    data_id,
                    write_tx,
                    send_window,
                    graceful_shutdown,
                    guard,
                };
                let shared = Arc::new(parking_lot::Mutex::new(SharedSplitState::Opened(opened)));
                (
                    DataStream {
                        data_rx,
                        max_frame_size,
                        read_buf,
                        read_eof,
                        shared: Arc::clone(&shared),
                    },
                    ErrorStream {
                        error_rx,
                        error_buf: None,
                        error_eof: false,
                        shared,
                    },
                )
            }
            StreamState::Transitioning => {
                unreachable!("split() called on transitioning stream")
            }
        }
    }
}

impl Unpin for Stream {}

/// Shared `poll_read` logic for channel-backed streams.
fn poll_read_channel(
    rx: &mut mpsc::Receiver<Bytes>, read_buf: &mut Option<Bytes>, read_eof: &mut bool,
    cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
) -> Poll<io::Result<()>> {
    if *read_eof {
        return Poll::Ready(Ok(()));
    }

    // drain buffered data first
    if let Some(ref mut remaining) = *read_buf {
        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        if to_copy >= remaining.len() {
            *read_buf = None;
        } else {
            *remaining = remaining.slice(to_copy..);
        }
        return Poll::Ready(Ok(()));
    }

    // poll channel for more data
    match rx.poll_recv(cx) {
        Poll::Ready(Some(data)) => {
            let to_copy = data.len().min(buf.remaining());
            buf.put_slice(&data[..to_copy]);
            if to_copy < data.len() {
                *read_buf = Some(data.slice(to_copy..));
            }
            Poll::Ready(Ok(()))
        }
        Poll::Ready(None) => {
            *read_eof = true;
            Poll::Ready(Ok(()))
        }
        Poll::Pending => Poll::Pending,
    }
}

/// Shared `consume` logic for `AsyncBufRead`.
fn consume_channel_buf(read_buf: &mut Option<Bytes>, amt: usize) {
    if let Some(ref mut bytes) = *read_buf {
        let consumed = amt.min(bytes.len());
        bytes.advance(consumed);
        if bytes.is_empty() {
            *read_buf = None;
        }
    }
}

/// Shared `poll_fill_buf` logic for channel-backed streams.
fn poll_fill_buf_channel<'a>(
    rx: &'a mut mpsc::Receiver<Bytes>, read_buf: &'a mut Option<Bytes>, read_eof: &'a mut bool,
    cx: &mut Context<'_>,
) -> Poll<io::Result<&'a [u8]>> {
    loop {
        if read_buf.as_ref().is_some_and(|b| !b.is_empty()) {
            return Poll::Ready(Ok(read_buf.as_deref().unwrap()));
        }
        if read_buf.is_some() {
            *read_buf = None;
        }
        if *read_eof {
            return Poll::Ready(Ok(&[]));
        }
        match rx.poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {
                *read_eof = true;
                return Poll::Ready(Ok(&[]));
            }
            Poll::Ready(Some(b)) => {
                *read_buf = Some(b);
            }
        }
    }
}

/// Send DATA+FIN on a fully opened stream and mark the guard graceful so
/// `Drop` skips RST_STREAM for the data half.
fn poll_shutdown_opened(
    graceful_shutdown: &AtomicBool, write_tx: &mut PollSender<MuxCommand>, data_id: u32,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    if graceful_shutdown.load(Ordering::Acquire) {
        return Poll::Ready(Ok(()));
    }
    match write_tx.poll_reserve(cx) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Err(_)) => return Poll::Ready(Err(broken_pipe())),
        Poll::Ready(Ok(())) => {}
    }
    if write_tx
        .send_item(MuxCommand::SendData {
            stream_id: data_id,
            payload: Bytes::new(),
            fin: true,
        })
        .is_err()
    {
        return Poll::Ready(Err(broken_pipe()));
    }
    graceful_shutdown.store(true, Ordering::Release);
    Poll::Ready(Ok(()))
}

fn broken_pipe() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "mux closed")
}

/// Build a complete SPDY DATA frame in a single allocation and send it as a
/// pre-encoded raw frame, enforcing per-stream send window flow control.
///
/// Session-level send window isn't enforced on purpose. The peer (kubelet
/// apiserver) never sends session-level WINDOW_UPDATE with stream_id=0, so
/// enforcing it would deadlock once the initial window drains. Per-stream
/// windows still provide proper backpressure.
///
/// Clamps write size to max_frame_size - 8 (the 8-byte SPDY DATA header).
///
/// Ordering invariant (prevents window leak on Pending):
///   1. `poll_reserve` cmd_tx permit (may return Pending; no side effects)
///   2. Read send window, compute n = min(buf.len(), stream_window,
///      max_payload)
///   3. If n == 0: register waker, return Pending
///   4. `stream_window.consume(n)`: debit committed
///   5. `send_item(frame)`: infallible after successful reserve
///   6. return Ready(Ok(n))
fn poll_write_via_sender(
    write_tx: &mut PollSender<MuxCommand>, stream_id: u32, send_window: &SendWindow,
    max_frame_size: u32, cx: &mut Context<'_>, buf: &[u8],
) -> Poll<io::Result<usize>> {
    // early check: stream was closed (window poisoned by reader)
    if send_window.is_closed() {
        return Poll::Ready(Err(broken_pipe()));
    }

    // acquire cmd_tx permit. No side effects on Pending.
    match write_tx.poll_reserve(cx) {
        Poll::Ready(Ok(())) => {}
        Poll::Ready(Err(_)) => return Poll::Ready(Err(broken_pipe())),
        Poll::Pending => return Poll::Pending,
    }

    // maximum DATA payload is max_frame_size - 8 (8-byte SPDY DATA header).
    let max_payload = (max_frame_size as usize).saturating_sub(8);
    let max_payload = if max_payload == 0 {
        buf.len()
    } else {
        max_payload
    };

    // compute write size, clamped to per-stream window AND max_frame_size.
    let stream_avail = send_window.available().max(0) as usize;
    let mut n = buf.len().min(stream_avail).min(max_payload);

    if n == 0 {
        // per-stream window exhausted. Register waker.
        send_window.register_waker(cx.waker());

        // re-check for poisoning
        if send_window.is_closed() {
            return Poll::Ready(Err(broken_pipe()));
        }
        // re-check window after registering waker (lost wake guard)
        let stream_avail = send_window.available().max(0) as usize;
        n = buf.len().min(stream_avail).min(max_payload);
        if n == 0 {
            return Poll::Pending;
        }
    }

    // debit per-stream window via CAS.
    if !send_window.consume(n) {
        return Poll::Ready(Err(broken_pipe()));
    }

    // build DATA frame and send via the reserved permit.
    let write_buf = &buf[..n];
    let mut frame = BytesMut::with_capacity(8 + n);
    frame.extend_from_slice(&(stream_id & 0x7FFF_FFFF).to_be_bytes());
    let flags_len = (n as u32) & 0x00FF_FFFF;
    frame.extend_from_slice(&flags_len.to_be_bytes());
    frame.extend_from_slice(write_buf);

    let cmd = MuxCommand::SendRawFrame {
        frame: frame.freeze(),
    };
    match write_tx.send_item(cmd) {
        Ok(()) => Poll::Ready(Ok(n)),
        Err(_) => Poll::Ready(Err(broken_pipe())),
    }
}

/// Realize a stream pair's open on the wire if it hasn't happened yet.
/// `MuxHandle::realize_stream_pair` synchronizes purely through a
/// `parking_lot::Mutex` with no `.await` in its critical section, so this
/// always completes within a single call — there is no in-flight future to
/// store, poll again, or wake waiters for.
fn ensure_realized(
    mux: &MuxHandle, error_headers: &mut Vec<(String, String)>,
    data_headers: &mut Vec<(String, String)>, pending_data_tx: &mut Option<mpsc::Sender<Bytes>>,
    pending_error_tx: &mut Option<mpsc::Sender<Bytes>>,
) -> io::Result<OpenedStreamParts> {
    // if pending senders have already been consumed by a previous failed
    // open try, the stream is permanently broken: nothing left to
    // register with the workers.
    let (Some(data_tx), Some(error_tx)) = (pending_data_tx.take(), pending_error_tx.take()) else {
        return Err(broken_pipe());
    };
    mux.realize_stream_pair(
        std::mem::take(error_headers),
        std::mem::take(data_headers),
        data_tx,
        error_tx,
    )
    .map_err(|_| broken_pipe())
}

fn finish_unopened_stream(this: &mut Stream, parts: OpenedStreamParts) {
    let old = std::mem::replace(&mut this.state, StreamState::Transitioning);
    let StreamState::Unopened {
        mux,
        data_rx,
        error_rx,
        max_frame_size,
        read_buf,
        read_eof,
        mut release_guard,
        ..
    } = old
    else {
        unreachable!()
    };
    if let Some(g) = release_guard.as_mut() {
        g.disarm();
    }
    let graceful_shutdown = Arc::new(AtomicBool::new(false));
    let guard = StreamGuard {
        data_id: parts.data_id,
        error_id: parts.error_id,
        mux: mux.clone(),
        ctrl_permit_error: Some(parts.ctrl_permit_error),
        ctrl_permit_data: Some(parts.ctrl_permit_data),
        close_reg_permit_error: Some(parts.close_reg_permit_error),
        close_reg_permit_data: Some(parts.close_reg_permit_data),
        graceful_shutdown: Arc::clone(&graceful_shutdown),
    };
    let write_tx = PollSender::new(mux.cmd_sender());
    this.state = StreamState::Opened {
        data_id: parts.data_id,
        data_rx,
        error_rx,
        write_tx,
        send_window: parts.send_window,
        max_frame_size,
        read_buf,
        read_eof,
        graceful_shutdown,
        guard,
    };
    drop(release_guard);
}

fn ensure_open(this: &mut Stream) -> io::Result<()> {
    let parts = match &mut this.state {
        StreamState::Unopened {
            error_headers,
            data_headers,
            mux,
            pending_data_tx,
            pending_error_tx,
            ..
        } => ensure_realized(
            mux,
            error_headers,
            data_headers,
            pending_data_tx,
            pending_error_tx,
        )?,
        _ => return Ok(()),
    };
    finish_unopened_stream(this, parts);
    Ok(())
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Err(e) = ensure_open(this) {
            return Poll::Ready(Err(e));
        }
        match &mut this.state {
            StreamState::Opened {
                data_rx,
                read_buf,
                read_eof,
                ..
            } => poll_read_channel(data_rx, read_buf, read_eof, cx, buf),
            StreamState::Unopened { .. } | StreamState::Transitioning => unreachable!(),
        }
    }
}

impl AsyncBufRead for Stream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if let Err(e) = ensure_open(this) {
            return Poll::Ready(Err(e));
        }
        match &mut this.state {
            StreamState::Opened {
                data_rx,
                read_buf,
                read_eof,
                ..
            } => poll_fill_buf_channel(data_rx, read_buf, read_eof, cx),
            StreamState::Unopened { .. } | StreamState::Transitioning => unreachable!(),
        }
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        let this = self.get_mut();
        match &mut this.state {
            StreamState::Unopened { read_buf, .. } => consume_channel_buf(read_buf, amt),
            StreamState::Opened { read_buf, .. } => consume_channel_buf(read_buf, amt),
            StreamState::Transitioning => unreachable!(),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // empty writes are a no-op; never trigger lazy open.
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        if let Err(e) = ensure_open(this) {
            return Poll::Ready(Err(e));
        }

        match &mut this.state {
            StreamState::Opened {
                data_id,
                write_tx,
                send_window,
                max_frame_size,
                graceful_shutdown,
                ..
            } => {
                if graceful_shutdown.load(Ordering::Acquire) {
                    return Poll::Ready(Err(broken_pipe()));
                }
                poll_write_via_sender(write_tx, *data_id, send_window, *max_frame_size, cx, buf)
            }
            StreamState::Unopened { .. } => unreachable!("handled above"),
            StreamState::Transitioning => unreachable!(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match &mut this.state {
            // Unopened: no SPDY stream exists yet. There is nothing on the
            // wire to half-close. Drop will release the local slot when the
            // Stream goes out of scope.
            StreamState::Unopened { .. } => Poll::Ready(Ok(())),
            StreamState::Opened {
                graceful_shutdown,
                write_tx,
                data_id,
                ..
            } => poll_shutdown_opened(graceful_shutdown, write_tx, *data_id, cx),
            StreamState::Transitioning => unreachable!(),
        }
    }
}

/// State shared between `DataStream` and `ErrorStream` after `split()`.
/// The data half drives lazy open; the error half participates via a single
/// guard reference once the stream is realized.
enum SharedSplitState {
    Unopened(UnopenedShared),
    Opened(OpenedShared),
    /// Used while transferring fields out during the Unopened -> Opened
    /// transition. Shouldn't be seen by user code because the
    /// transition is performed under the parking_lot guard.
    Transitioning,
}

struct UnopenedShared {
    error_headers: Vec<(String, String)>,
    data_headers: Vec<(String, String)>,
    mux: MuxHandle,
    pending_data_tx: Option<mpsc::Sender<Bytes>>,
    pending_error_tx: Option<mpsc::Sender<Bytes>>,
    release_guard: Option<PairReleaseGuard>,
}

struct OpenedShared {
    data_id: u32,
    write_tx: PollSender<MuxCommand>,
    send_window: Arc<SendWindow>,
    graceful_shutdown: Arc<AtomicBool>,
    /// Kept alive for its `Drop` impl, which sends RST_STREAM (or skips on
    /// graceful shutdown) and notifies the workers. Never read directly.
    #[allow(dead_code)]
    guard: StreamGuard,
}

fn finish_shared_open(guard: &mut SharedSplitState, parts: OpenedStreamParts) {
    let old = std::mem::replace(guard, SharedSplitState::Transitioning);
    let SharedSplitState::Unopened(mut u) = old else {
        unreachable!()
    };
    if let Some(g) = u.release_guard.as_mut() {
        g.disarm();
    }
    let graceful_shutdown = Arc::new(AtomicBool::new(false));
    let stream_guard = StreamGuard {
        data_id: parts.data_id,
        error_id: parts.error_id,
        mux: u.mux.clone(),
        ctrl_permit_error: Some(parts.ctrl_permit_error),
        ctrl_permit_data: Some(parts.ctrl_permit_data),
        close_reg_permit_error: Some(parts.close_reg_permit_error),
        close_reg_permit_data: Some(parts.close_reg_permit_data),
        graceful_shutdown: Arc::clone(&graceful_shutdown),
    };
    let write_tx = PollSender::new(u.mux.cmd_sender());
    *guard = SharedSplitState::Opened(OpenedShared {
        data_id: parts.data_id,
        write_tx,
        send_window: parts.send_window,
        graceful_shutdown,
        guard: stream_guard,
    });
    drop(u.release_guard);
}

fn ensure_shared_open(shared: &parking_lot::Mutex<SharedSplitState>) -> io::Result<()> {
    let mut guard = shared.lock();
    let parts = match &mut *guard {
        SharedSplitState::Unopened(u) => ensure_realized(
            &u.mux,
            &mut u.error_headers,
            &mut u.data_headers,
            &mut u.pending_data_tx,
            &mut u.pending_error_tx,
        )?,
        _ => return Ok(()),
    };
    finish_shared_open(&mut guard, parts);
    Ok(())
}

/// Data half of a split SPDY stream: AsyncRead (from pod) + AsyncWrite (to
/// pod). Lazy open fires on the first non-empty write, or the first real
/// read, through this half.
pub struct DataStream {
    data_rx: mpsc::Receiver<Bytes>,
    max_frame_size: u32,
    read_buf: Option<Bytes>,
    read_eof: bool,
    shared: Arc<parking_lot::Mutex<SharedSplitState>>,
}

impl Unpin for DataStream {}

impl AsyncRead for DataStream {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Err(e) = ensure_shared_open(&this.shared) {
            return Poll::Ready(Err(e));
        }
        poll_read_channel(
            &mut this.data_rx,
            &mut this.read_buf,
            &mut this.read_eof,
            cx,
            buf,
        )
    }
}

impl AsyncBufRead for DataStream {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        let this = self.get_mut();
        if let Err(e) = ensure_shared_open(&this.shared) {
            return Poll::Ready(Err(e));
        }
        poll_fill_buf_channel(
            &mut this.data_rx,
            &mut this.read_buf,
            &mut this.read_eof,
            cx,
        )
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        consume_channel_buf(&mut self.get_mut().read_buf, amt);
    }
}

impl AsyncWrite for DataStream {
    fn poll_write(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if let Err(e) = ensure_shared_open(&this.shared) {
            return Poll::Ready(Err(e));
        }
        let mut guard = this.shared.lock();
        match &mut *guard {
            SharedSplitState::Opened(o) => {
                if o.graceful_shutdown.load(Ordering::Acquire) {
                    return Poll::Ready(Err(broken_pipe()));
                }
                poll_write_via_sender(
                    &mut o.write_tx,
                    o.data_id,
                    &o.send_window,
                    this.max_frame_size,
                    cx,
                    buf,
                )
            }
            SharedSplitState::Unopened(_) => unreachable!("handled above"),
            SharedSplitState::Transitioning => unreachable!(),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let mut guard = this.shared.lock();
        match &mut *guard {
            SharedSplitState::Unopened(_) => Poll::Ready(Ok(())),
            SharedSplitState::Opened(o) => {
                poll_shutdown_opened(&o.graceful_shutdown, &mut o.write_tx, o.data_id, cx)
            }
            SharedSplitState::Transitioning => unreachable!(),
        }
    }
}

/// Error half of a split SPDY stream: AsyncRead only (pod error messages).
/// A read through this half can also trigger the shared lazy open.
pub struct ErrorStream {
    error_rx: mpsc::Receiver<Bytes>,
    error_buf: Option<Bytes>,
    error_eof: bool,
    shared: Arc<parking_lot::Mutex<SharedSplitState>>,
}

impl Unpin for ErrorStream {}

impl AsyncRead for ErrorStream {
    fn poll_read(
        self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if let Err(e) = ensure_shared_open(&this.shared) {
            return Poll::Ready(Err(e));
        }
        poll_read_channel(
            &mut this.error_rx,
            &mut this.error_buf,
            &mut this.error_eof,
            cx,
            buf,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::DuplexStream;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::codec::Frame;
    use crate::codec::SpdyCodec;
    use crate::mux::MuxConfig;
    use crate::transport::split_raw_spdy;

    const TEST_MAX_FRAME: u32 = 1024 * 1024;
    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn test_config() -> MuxConfig {
        MuxConfig {
            ping_timeout: TEST_TIMEOUT,
            write_timeout: TEST_TIMEOUT,
            ..MuxConfig::default()
        }
    }

    struct TestPeer {
        events: mpsc::UnboundedReceiver<Frame>,
        cmds: mpsc::UnboundedSender<Vec<u8>>,
    }

    impl TestPeer {
        async fn recv(&mut self) -> Frame {
            tokio::time::timeout(TEST_TIMEOUT, self.events.recv())
                .await
                .expect("timed out waiting for a frame from the client")
                .expect("peer event channel closed")
        }

        async fn recv_syn_stream(&mut self) -> u32 {
            match self.recv().await {
                Frame::SynStream { stream_id, .. } => stream_id,
                other => panic!("expected SynStream, got {other:?}"),
            }
        }

        fn send_data(&self, stream_id: u32, payload: &[u8], fin: bool) {
            let codec = SpdyCodec::with_max_frame_size(TEST_MAX_FRAME);
            let frame = codec.encode_data(stream_id, payload, fin);
            self.cmds.send(frame).expect("peer task still running");
        }
    }

    async fn run_test_peer(
        server: DuplexStream, event_tx: mpsc::UnboundedSender<Frame>,
        mut cmd_rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let (mut read_half, mut write_half) = tokio::io::split(server);
        let mut codec = SpdyCodec::with_max_frame_size(TEST_MAX_FRAME);
        let mut buf = BytesMut::with_capacity(16 * 1024);
        let mut chunk = [0u8; 4096];
        loop {
            tokio::select! {
                biased;
                cmd = cmd_rx.recv() => {
                    match cmd {
                        Some(bytes) => {
                            if write_half.write_all(&bytes).await.is_err() {
                                return;
                            }
                        }
                        None => return,
                    }
                }
                n = read_half.read(&mut chunk) => {
                    let n = match n {
                        Ok(0) | Err(_) => return,
                        Ok(n) => n,
                    };
                    buf.extend_from_slice(&chunk[..n]);
                    loop {
                        match codec.decode_frame(&mut buf) {
                            Ok(Some(frame)) => {
                                if let Frame::Ping { id } = &frame {
                                    let pong = codec.encode_ping(*id);
                                    if write_half.write_all(&pong).await.is_err() {
                                        return;
                                    }
                                    continue;
                                }
                                if matches!(frame, Frame::Settings { .. } | Frame::WindowUpdate { stream_id: 0, .. }) {
                                    continue;
                                }
                                if event_tx.send(frame).is_err() {
                                    return;
                                }
                            }
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }
                }
            }
        }
    }

    async fn spawn_test_peer(config: MuxConfig) -> (MuxHandle, TestPeer) {
        let (client, server) = tokio::io::duplex(256 * 1024);
        let (ws_write, ws_read) = split_raw_spdy(client);
        let cancel = CancellationToken::new();

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(run_test_peer(server, event_tx, cmd_rx));

        let mux = MuxHandle::spawn(ws_write, ws_read, cancel, config)
            .await
            .expect("mux handshake");
        (
            mux,
            TestPeer {
                events: event_rx,
                cmds: cmd_tx,
            },
        )
    }

    #[tokio::test]
    async fn server_first_read_wakes_unsplit_stream_without_client_write() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let mut stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");

        let mut buf = [0u8; 16];
        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            match peer.recv().await {
                Frame::Data {
                    stream_id,
                    payload,
                    fin,
                } => {
                    assert_eq!(stream_id, error_id);
                    assert!(payload.is_empty());
                    assert!(fin);
                }
                other => panic!("expected error half-close DATA+FIN, got {other:?}"),
            }
            peer.send_data(data_id, b"hello", false);
        };

        let (read_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(stream.read(&mut buf), peer_fut)
        })
        .await
        .expect("read did not complete: server-first read still blocks forever");

        let n = read_result.expect("read failed");
        assert_eq!(&buf[..n], b"hello");
    }

    #[tokio::test]
    async fn server_first_fill_buf_wakes_unsplit_stream_without_client_write() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let mut stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, .. } if stream_id == error_id
            ));
            peer.send_data(data_id, b"greeting", false);
        };

        let (data, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            let read_fut = async {
                let bytes = stream.fill_buf().await.expect("fill_buf failed");
                let owned = bytes.to_vec();
                stream.consume(owned.len());
                owned
            };
            tokio::join!(read_fut, peer_fut)
        })
        .await
        .expect("fill_buf did not complete: server-first fill_buf still blocks forever");

        assert_eq!(data, b"greeting");
    }

    #[tokio::test]
    async fn server_first_read_wakes_split_data_stream() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut data_stream, _error_stream) = stream.split();

        let mut buf = [0u8; 16];
        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, .. } if stream_id == error_id
            ));
            peer.send_data(data_id, b"pod-out", false);
        };

        let (read_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(data_stream.read(&mut buf), peer_fut)
        })
        .await
        .expect("split DataStream read did not trigger the lazy open");

        let n = read_result.expect("read failed");
        assert_eq!(&buf[..n], b"pod-out");
    }

    #[tokio::test]
    async fn server_first_read_wakes_split_error_stream() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (_data_stream, mut error_stream) = stream.split();

        let mut buf = [0u8; 16];
        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let _data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            peer.send_data(error_id, b"pod not found", false);
        };

        let (read_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(error_stream.read(&mut buf), peer_fut)
        })
        .await
        .expect("split ErrorStream read did not trigger the lazy open");

        let n = read_result.expect("read failed");
        assert_eq!(&buf[..n], b"pod not found");
    }

    #[tokio::test]
    async fn concurrent_read_and_write_open_the_stream_exactly_once() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut data_stream, mut error_stream) = stream.split();

        let write_payload: &[u8] = b"race-safe-payload";
        let mut err_buf = [0u8; 8];

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], write_payload);
                }
                other => panic!("expected the client's write payload, got {other:?}"),
            }
            peer.send_data(error_id, b"ack", false);
        };

        let (read_result, write_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                error_stream.read(&mut err_buf),
                data_stream.write_all(write_payload),
                peer_fut,
            )
        })
        .await
        .expect("concurrent read+write did not settle: possible duplicate open or hang");

        let n = read_result.expect("error stream read failed");
        assert_eq!(&err_buf[..n], b"ack");
        write_result.expect("write_all failed");
    }

    #[tokio::test]
    async fn unused_unopened_stream_never_opens_and_releases_capacity() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        assert_eq!(mux.active_pairs(), 0);

        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        assert_eq!(mux.active_pairs(), 1);

        drop(stream);
        assert_eq!(mux.active_pairs(), 0);

        let saw_nothing = tokio::time::timeout(Duration::from_millis(200), peer.recv())
            .await
            .is_err();
        assert!(
            saw_nothing,
            "an unused, dropped stream must never send SYN_STREAM"
        );
    }

    #[tokio::test]
    async fn dropped_stream_after_read_triggered_open_sends_rst() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let mut stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            (peer, data_id)
        };

        let mut buf = [0u8; 4];
        let read_fut = stream.read(&mut buf);
        let (mut peer, data_id) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::select! {
                _ = read_fut => panic!("read resolved without a greeting from the peer"),
                result = peer_fut => result,
            }
        })
        .await
        .expect("open via read did not complete before the timeout");

        drop(stream);

        match tokio::time::timeout(TEST_TIMEOUT, peer.recv())
            .await
            .expect("timed out waiting for RST_STREAM after drop")
        {
            Frame::RstStream { stream_id, status } => {
                assert_eq!(stream_id, data_id);
                assert_eq!(status, RST_STATUS_CANCEL);
            }
            other => panic!("expected RST_STREAM for the data stream, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn first_write_after_lazy_open_is_bounded_by_frame_size() {
        let config = MuxConfig {
            max_frame_size: 16,
            ..test_config()
        };
        let (mux, mut peer) = spawn_test_peer(config).await;
        let mut stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");

        let payload = b"0123456789ABCDEFGHIJ";

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], &b"01234567"[..]);
                }
                other => panic!("expected the first DATA frame after lazy open, got {other:?}"),
            }
        };

        let (write_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(stream.write(payload), peer_fut)
        })
        .await
        .expect("first write did not complete");

        let n = write_result.expect("write failed");
        assert_eq!(n, 8, "first write must be bounded by max_frame_size - 8");
    }

    #[tokio::test]
    async fn tokio_io_split_write_then_read_open_stream_exactly_once() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut read_half, mut write_half) = tokio::io::split(stream);

        let write_payload: &[u8] = b"via-tokio-split-write-first";
        let mut read_buf = [0u8; 8];

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], write_payload);
                }
                other => panic!("expected the write payload exactly once, got {other:?}"),
            }
            peer.send_data(data_id, b"greeting", false);
        };

        let (write_result, read_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                write_half.write_all(write_payload),
                read_half.read(&mut read_buf),
                peer_fut,
            )
        })
        .await
        .expect("tokio::io::split write-then-read race did not settle");

        write_result.expect("write_all failed");
        let n = read_result.expect("read failed");
        assert_eq!(&read_buf[..n], b"greeting");
    }

    #[tokio::test]
    async fn tokio_io_split_read_then_write_open_stream_exactly_once() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut read_half, mut write_half) = tokio::io::split(stream);

        let write_payload: &[u8] = b"via-tokio-split-read-first";
        let mut read_buf = [0u8; 8];

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            peer.send_data(data_id, b"greeting", false);
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], write_payload);
                }
                other => panic!("expected the write payload exactly once, got {other:?}"),
            }
        };

        let (read_result, write_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                read_half.read(&mut read_buf),
                write_half.write_all(write_payload),
                peer_fut,
            )
        })
        .await
        .expect("tokio::io::split read-then-write race did not settle");

        let n = read_result.expect("read failed");
        assert_eq!(&read_buf[..n], b"greeting");
        write_result.expect("write_all failed");
    }

    #[tokio::test]
    async fn write_then_read_open_split_stream_exactly_once() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut data_stream, mut error_stream) = stream.split();

        let write_payload: &[u8] = b"write-branch-first";
        let mut err_buf = [0u8; 8];

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], write_payload);
                }
                other => panic!("expected the client's write payload exactly once, got {other:?}"),
            }
            peer.send_data(error_id, b"ack", false);
        };

        let (write_result, read_result, ()) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                data_stream.write_all(write_payload),
                error_stream.read(&mut err_buf),
                peer_fut,
            )
        })
        .await
        .expect("concurrent write+read did not settle: possible duplicate open or hang");

        write_result.expect("write_all failed");
        let n = read_result.expect("error stream read failed");
        assert_eq!(&err_buf[..n], b"ack");
    }

    #[tokio::test]
    async fn three_way_race_does_not_lose_a_wake() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (data_stream, mut error_stream) = stream.split();
        let (mut data_read_half, mut data_write_half) = tokio::io::split(data_stream);

        let write_payload: &[u8] = b"three-way-race";
        let mut data_buf = [0u8; 8];
        let mut err_buf = [0u8; 8];

        let peer_fut = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin, .. } if stream_id == error_id && fin
            ));
            match peer.recv().await {
                Frame::Data {
                    stream_id, payload, ..
                } => {
                    assert_eq!(stream_id, data_id);
                    assert_eq!(&payload[..], write_payload);
                }
                other => panic!("expected the write payload exactly once, got {other:?}"),
            }
            peer.send_data(data_id, b"pod-out", false);
            peer.send_data(error_id, b"pod-err", false);
        };

        let (write_result, data_read_result, err_read_result, ()) =
            tokio::time::timeout(TEST_TIMEOUT, async {
                tokio::join!(
                    data_write_half.write_all(write_payload),
                    data_read_half.read(&mut data_buf),
                    error_stream.read(&mut err_buf),
                    peer_fut,
                )
            })
            .await
            .expect("three-way race did not settle: a waiter lost its wakeup");

        write_result.expect("write_all failed");
        let n = data_read_result.expect("data read failed");
        assert_eq!(&data_buf[..n], b"pod-out");
        let n = err_read_result.expect("error read failed");
        assert_eq!(&err_buf[..n], b"pod-err");
    }

    #[tokio::test]
    async fn zero_length_read_does_not_open_unsplit_stream() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let mut stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");

        let mut empty = [0u8; 0];
        let n = tokio::time::timeout(TEST_TIMEOUT, stream.read(&mut empty))
            .await
            .expect("zero-length read must resolve immediately")
            .expect("zero-length read must not error");
        assert_eq!(n, 0);

        let saw_nothing = tokio::time::timeout(Duration::from_millis(200), peer.recv())
            .await
            .is_err();
        assert!(
            saw_nothing,
            "a zero-length read must never trigger SYN_STREAM"
        );
        assert_eq!(
            mux.active_pairs(),
            1,
            "the reserved pair stays unopened, not released"
        );
    }

    #[tokio::test]
    async fn zero_length_read_does_not_open_split_data_stream() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux
            .open_stream_pair(vec![], vec![])
            .expect("reserve stream pair");
        let (mut data_stream, _error_stream) = stream.split();

        let mut empty = [0u8; 0];
        let n = tokio::time::timeout(TEST_TIMEOUT, data_stream.read(&mut empty))
            .await
            .expect("zero-length read must resolve immediately")
            .expect("zero-length read must not error");
        assert_eq!(n, 0);

        let saw_nothing = tokio::time::timeout(Duration::from_millis(200), peer.recv())
            .await
            .is_err();
        assert!(
            saw_nothing,
            "a zero-length read on the split data half must never trigger SYN_STREAM"
        );
        assert_eq!(mux.active_pairs(), 1);
    }

    async fn exercise_write_shutdown(
        mut stream: impl AsyncRead + AsyncWrite + Unpin, peer: &mut TestPeer,
    ) {
        let request = b"request-before-fin".repeat(128);
        let reply = b"response-after-fin".repeat(128);
        let client = async {
            stream.write_all(&request).await.unwrap();
            stream.shutdown().await.unwrap();
            stream.shutdown().await.unwrap();
            assert_eq!(
                stream.write(b"not-sent").await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            let mut received = Vec::new();
            stream.read_to_end(&mut received).await.unwrap();
            assert_eq!(received, reply);
        };
        let server = async {
            let error_id = peer.recv_syn_stream().await;
            let data_id = peer.recv_syn_stream().await;
            assert!(matches!(
                peer.recv().await,
                Frame::Data { stream_id, fin: true, .. } if stream_id == error_id
            ));
            let mut received = Vec::new();
            loop {
                match peer.recv().await {
                    Frame::Data {
                        stream_id,
                        payload,
                        fin,
                    } if stream_id == data_id => {
                        received.extend_from_slice(&payload);
                        if fin {
                            break;
                        }
                    }
                    frame => panic!("unexpected frame before write-side FIN: {frame:?}"),
                }
            }
            assert_eq!(received, request);
            peer.send_data(data_id, &reply, true);
        };
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(client, server);
        })
        .await
        .expect("write shutdown must preserve response reads");
    }

    #[tokio::test]
    async fn write_shutdown_preserves_unsplit_response_reads() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let stream = mux.open_stream_pair(vec![], vec![]).unwrap();
        exercise_write_shutdown(stream, &mut peer).await;
    }

    #[tokio::test]
    async fn write_shutdown_preserves_split_response_reads() {
        let (mux, mut peer) = spawn_test_peer(test_config()).await;
        let (stream, _errors) = mux.open_stream_pair(vec![], vec![]).unwrap().split();
        exercise_write_shutdown(stream, &mut peer).await;
    }
}
