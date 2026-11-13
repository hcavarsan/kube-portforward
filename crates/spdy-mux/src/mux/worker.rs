use std::collections::{
    HashMap,
    VecDeque,
};
use std::future::poll_fn;
use std::sync::Arc;
use std::task::{
    Context,
    Poll,
};

use bytes::Bytes;
use tokio::sync::{
    mpsc,
    oneshot,
};
use tokio_util::sync::{
    CancellationToken,
    PollSender,
};

use super::commands::{
    MuxCommand,
    StreamRegistration,
};
use super::window::SendWindow;
use super::{
    RST_STATUS_CANCEL,
    RST_STATUS_FLOW_CONTROL,
    RST_STATUS_INVALID_STREAM,
};
use crate::codec::Frame;
use crate::error::Error;

/// Per-stream state owned exclusively by one frame worker.
pub(super) struct StreamState {
    /// Data sender. `None` after remote FIN (half-close) once every held
    /// payload reached the channel.
    data_tx: Option<PollSender<Bytes>>,
    /// Payloads that arrived while the per-stream channel was full, in
    /// arrival order. They get no WINDOW_UPDATE credit until they reach the
    /// channel, so a peer that respects the window keeps this under
    /// `initial_window_size` bytes.
    held: VecDeque<Bytes>,
    held_bytes: u64,
    /// Remote FIN arrived while `held` still had payloads.
    fin_after_held: bool,
    send_window: Arc<SendWindow>,
    /// Bytes delivered to the per-stream channel since the last
    /// WINDOW_UPDATE was sent.
    consumed_since_update: u64,
}

impl StreamState {
    /// Queue a DATA payload behind any held ones. Returns the RST_STREAM
    /// status when the stream has to be torn down.
    fn deliver(
        &mut self, stream_id: u32, payload: Bytes, window_tx: &mpsc::Sender<MuxCommand>,
        initial_window_size: u32,
    ) -> Option<u32> {
        if self.data_tx.is_none() || self.fin_after_held {
            return None;
        }
        let payload_len = payload.len() as u64;
        let payload = if self.held.is_empty() {
            let data_tx = self.data_tx.as_ref()?.get_ref()?;
            match data_tx.try_send(payload) {
                Ok(()) => {
                    self.credit(stream_id, payload_len, window_tx, initial_window_size);
                    return None;
                }
                Err(mpsc::error::TrySendError::Full(payload)) => payload,
                Err(mpsc::error::TrySendError::Closed(_)) => return Some(RST_STATUS_FLOW_CONTROL),
            }
        } else {
            payload
        };

        // the channel is full: hold the payload and withhold its credit so
        // the peer backs off. A peer that sends past the window it was
        // given is reset instead of growing the hold without bound.
        self.held_bytes += payload_len;
        if self.held_bytes > u64::from(initial_window_size) {
            tracing::debug!(
                stream_id,
                held_bytes = self.held_bytes,
                "SPDY peer overran the stream receive window"
            );
            return Some(RST_STATUS_FLOW_CONTROL);
        }
        self.held.push_back(payload);
        None
    }

    /// Track bytes that reached the per-stream channel and send a
    /// WINDOW_UPDATE once half the window was consumed.
    fn credit(
        &mut self, stream_id: u32, len: u64, window_tx: &mpsc::Sender<MuxCommand>,
        initial_window_size: u32,
    ) {
        self.consumed_since_update += len;
        if self.consumed_since_update >= u64::from(initial_window_size / 2) {
            let delta = self.consumed_since_update as u32;
            // only reset on success; if the channel is full, accumulate and
            // retry on the next delivery.
            if window_tx
                .try_send(MuxCommand::EncodeWindowUpdate { stream_id, delta })
                .is_ok()
            {
                self.consumed_since_update = 0;
            }
        }
    }

    /// Remote FIN: close the data sender so the consumer reads EOF, after
    /// any held payloads.
    fn half_close(&mut self) {
        if self.held.is_empty() {
            self.data_tx = None;
        } else {
            self.fin_after_held = true;
        }
    }
}

/// Move held payloads into their per-stream channels as readers make room,
/// granting WINDOW_UPDATE credit as they go. Returns `Ready` once anything
/// moved or was torn down, `Pending` with wakers registered otherwise.
fn poll_release_held(
    cx: &mut Context<'_>, streams: &mut HashMap<u32, StreamState>,
    cmd_tx: &mpsc::Sender<MuxCommand>, window_tx: &mpsc::Sender<MuxCommand>,
    initial_window_size: u32,
) -> Poll<()> {
    let mut progressed = false;
    let mut closed = Vec::new();
    for (&stream_id, state) in streams.iter_mut() {
        while !state.held.is_empty() {
            let Some(data_tx) = state.data_tx.as_mut() else {
                break;
            };
            match data_tx.poll_reserve(cx) {
                Poll::Pending => break,
                Poll::Ready(Err(_)) => {
                    closed.push(stream_id);
                    break;
                }
                Poll::Ready(Ok(())) => {
                    let Some(payload) = state.held.pop_front() else {
                        break;
                    };
                    let len = payload.len() as u64;
                    state.held_bytes -= len;
                    if data_tx.send_item(payload).is_err() {
                        closed.push(stream_id);
                        break;
                    }
                    state.credit(stream_id, len, window_tx, initial_window_size);
                    progressed = true;
                }
            }
        }
        if state.fin_after_held && state.held.is_empty() {
            state.fin_after_held = false;
            state.data_tx = None;
            progressed = true;
        }
    }
    for stream_id in closed {
        streams.remove(&stream_id);
        let _ = cmd_tx.try_send(MuxCommand::CloseStream {
            stream_id,
            status: RST_STATUS_FLOW_CONTROL,
        });
        progressed = true;
    }
    if progressed {
        Poll::Ready(())
    } else {
        Poll::Pending
    }
}

/// Owns a shard of streams partitioned by `stream_id % FRAME_WORKERS`.
/// No shared mutable state across workers.
///
/// Drains `close_reg_rx` (close registrations) before `reg_rx` (open
/// registrations) so teardown is never blocked by a burst of opens.
#[allow(clippy::too_many_arguments)]
pub(super) async fn run_frame_worker(
    worker_id: usize, mut frame_rx: mpsc::Receiver<Frame>,
    mut reg_rx: mpsc::Receiver<StreamRegistration>,
    mut close_reg_rx: mpsc::Receiver<StreamRegistration>, control_tx: mpsc::Sender<MuxCommand>,
    window_tx: mpsc::Sender<MuxCommand>, cancel: CancellationToken, initial_window_size: u32,
) {
    let mut streams: HashMap<u32, StreamState> = HashMap::new();
    let mut pending_replies: HashMap<u32, oneshot::Sender<Result<(), Error>>> = HashMap::new();

    loop {
        // drain close registrations first (non-blocking, priority).
        while let Ok(reg) = close_reg_rx.try_recv() {
            apply_registration(reg, &mut streams, &mut pending_replies, &control_tx);
        }
        // drain any pending open registrations (non-blocking) to ensure
        // Open registrations are applied before any frames for that stream.
        while let Ok(reg) = reg_rx.try_recv() {
            apply_registration(reg, &mut streams, &mut pending_replies, &control_tx);
        }

        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            // close registrations have priority over opens.
            reg = close_reg_rx.recv() => match reg {
                Some(r) => apply_registration(r, &mut streams, &mut pending_replies, &control_tx),
                None => break,
            },
            // open registrations checked before frames.
            reg = reg_rx.recv() => match reg {
                Some(r) => apply_registration(r, &mut streams, &mut pending_replies, &control_tx),
                None => break,
            },
            () = poll_fn(|cx| poll_release_held(
                cx, &mut streams, &control_tx, &window_tx, initial_window_size,
            )) => {},
            frame = frame_rx.recv() => match frame {
                Some(f) => dispatch_frame_in_worker(
                    f, &mut streams, &mut pending_replies,
                    &control_tx, &window_tx, initial_window_size,
                ),
                None => break,
            },
        }
    }

    // shutdown: fail all pending replies, poison all send windows.
    let orphaned_streams = streams.len();
    let orphaned_replies = pending_replies.len();
    if orphaned_streams > 0 || orphaned_replies > 0 {
        tracing::debug!(
            worker_id,
            orphaned_streams,
            orphaned_replies,
            "worker shutting down with in-flight streams"
        );
    }
    for (_, reply_tx) in pending_replies.drain() {
        let _ = reply_tx.send(Err(Error::MuxClosed));
    }
    for (_, state) in streams.drain() {
        state.send_window.close();
    }
}

/// Dispatches a decoded SPDY frame within a worker. Handles only stream-keyed
/// frames. Entirely synchronous and never blocks.
pub(super) fn dispatch_frame_in_worker(
    frame: Frame, streams: &mut HashMap<u32, StreamState>,
    pending_replies: &mut HashMap<u32, oneshot::Sender<Result<(), Error>>>,
    cmd_tx: &mpsc::Sender<MuxCommand>, window_tx: &mpsc::Sender<MuxCommand>,
    initial_window_size: u32,
) {
    match frame {
        Frame::Data {
            stream_id,
            payload,
            fin,
        } => {
            if !payload.is_empty() {
                // session-level recv window is tracked by the reader (not
                // here).
                if let Some(state) = streams.get_mut(&stream_id) {
                    if let Some(status) =
                        state.deliver(stream_id, payload, window_tx, initial_window_size)
                    {
                        streams.remove(&stream_id);
                        let _ = cmd_tx.try_send(MuxCommand::CloseStream { stream_id, status });
                        return;
                    }
                    // if data_tx is None (half-closed), deliver ignores the
                    // payload. The stream entry is kept for WINDOW_UPDATE
                    // processing.
                } else {
                    // trace level: routinely fires during connection teardown
                    // when late DATA frames arrive for
                    // streams we've already closed.
                    // the RST_STREAM response is correct per SPDY spec; only
                    // the log noise is undesirable at
                    // DEBUG.
                    tracing::trace!(
                        stream_id,
                        "SPDY DATA for unknown stream, sending RST_STREAM"
                    );
                    let _ = cmd_tx.try_send(MuxCommand::CloseStream {
                        stream_id,
                        status: RST_STATUS_INVALID_STREAM,
                    });
                }
            }

            // half-close: remote FIN means peer is done sending. Close our data
            // Sender so the consumer reads EOF after every held payload, but
            // keep the stream entry alive for outgoing WINDOW_UPDATE
            // processing. Full cleanup happens when StreamGuard fires Close.
            if fin {
                if let Some(state) = streams.get_mut(&stream_id) {
                    tracing::debug!(stream_id, "SPDY DATA FIN received, half-closing read side");
                    state.half_close();
                }
            }
        }
        Frame::SynReply {
            stream_id,
            headers,
            fin,
        } => {
            tracing::debug!(
                stream_id,
                num_headers = headers.len(),
                fin,
                "SPDY SYN_REPLY received"
            );
            if let Some(reply_tx) = pending_replies.remove(&stream_id) {
                let _ = reply_tx.send(Ok(()));
            } else if !streams.contains_key(&stream_id) {
                let _ = cmd_tx.try_send(MuxCommand::CloseStream {
                    stream_id,
                    status: RST_STATUS_INVALID_STREAM,
                });
            }
            // half-close on SYN_REPLY with FIN
            if fin {
                if let Some(state) = streams.get_mut(&stream_id) {
                    state.half_close();
                }
            }
        }
        Frame::RstStream { stream_id, status } => {
            // poison the send window so any in-progress poll_write on this
            // stream returns BrokenPipe immediately. Critical for the
            // no-SYN_REPLY-wait path: if the server rejects the stream
            // via RST_STREAM, the caller learns through the poisoned window,
            // not through an awaited oneshot.
            tracing::debug!(stream_id, status, "SPDY RST_STREAM received");
            if let Some(state) = streams.remove(&stream_id) {
                state.send_window.close();
            }
            if let Some(reply_tx) = pending_replies.remove(&stream_id) {
                let _ = reply_tx.send(Err(Error::StreamReset(stream_id, status)));
            }
        }
        Frame::WindowUpdate {
            stream_id,
            delta_window_size,
        } => {
            // only per-stream WINDOW_UPDATEs reach workers (stream_id == 0
            // is handled by the reader directly).
            if let Some(state) = streams.get(&stream_id) {
                state.send_window.replenish(delta_window_size);
            }
            // else: stream already closed, harmless.
        }
        // other frame types should not reach workers.
        _ => {}
    }
}

pub(super) fn apply_registration(
    reg: StreamRegistration, streams: &mut HashMap<u32, StreamState>,
    pending_replies: &mut HashMap<u32, oneshot::Sender<Result<(), Error>>>,
    cmd_tx: &mpsc::Sender<MuxCommand>,
) {
    match reg {
        StreamRegistration::Open {
            stream_id,
            data_tx,
            reply_tx,
            send_window,
        } => {
            streams.insert(
                stream_id,
                StreamState {
                    data_tx: Some(PollSender::new(data_tx)),
                    held: VecDeque::new(),
                    held_bytes: 0,
                    fin_after_held: false,
                    send_window,
                    consumed_since_update: 0,
                },
            );
            pending_replies.insert(stream_id, reply_tx);
        }
        StreamRegistration::Close { stream_id } => {
            if let Some(state) = streams.remove(&stream_id) {
                state.send_window.close();
            }
            pending_replies.remove(&stream_id);
        }
        StreamRegistration::SettingsWindowDelta { delta } => {
            for state in streams.values() {
                state.send_window.apply_delta(delta);
            }
        }
        StreamRegistration::GoAway {
            last_good_stream_id,
            status,
        } => {
            // clean up streams with id > last_good_stream_id in this shard.
            let bad_ids: Vec<u32> = streams
                .keys()
                .filter(|&&id| id > last_good_stream_id)
                .copied()
                .collect();

            for id in bad_ids {
                if let Some(state) = streams.remove(&id) {
                    state.send_window.close();
                }
                if let Some(reply_tx) = pending_replies.remove(&id) {
                    let _ = reply_tx.send(Err(Error::GoAway {
                        last_good_stream_id,
                        status,
                    }));
                }
                let _ = cmd_tx.try_send(MuxCommand::CloseStream {
                    stream_id: id,
                    status: RST_STATUS_CANCEL,
                });
            }
        }
    }
}
