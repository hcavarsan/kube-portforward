use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{
    mpsc,
    oneshot,
};

use super::window::SendWindow;

/// Command sent to the writer task via `cmd_tx`.
pub(crate) enum MuxCommand {
    /// Open a paired stream (an "error" stream half-closed at open time +
    /// a "data" stream that carries application bytes) and emit the
    /// first DATA frame on the data stream,
    OpenStreamPairAndWrite {
        error_id: u32,
        data_id: u32,
        error_headers: Vec<(String, String)>,
        data_headers: Vec<(String, String)>,
        first_payload: Bytes,
    },
    /// Send a DATA frame
    SendData {
        stream_id: u32,
        payload: Bytes,
        fin: bool,
    },
    ///SPDY DATA frame, Bypasses the codec and sent directly.
    SendRawFrame { frame: Bytes },
    /// Close a stream with a RST_STREAM frame.
    CloseStream { stream_id: u32, status: u32 },
    /// Encode and send an SPDY PING.
    EncodePing { id: u32 },
    /// Send a WebSocket-level PONG.
    SendWsPong { payload: Bytes },
    /// Encode and send a WINDOW_UPDATE frame.
    EncodeWindowUpdate { stream_id: u32, delta: u32 },
    /// Send a GOAWAY frame and prepare for graceful shutdown.
    /// Used by the open path for stream ID exhaustion and available for
    /// external graceful shutdown triggers.
    GoAway { last_good_stream_id: u32 },
}

/// Sent by the caller to the appropriate frame worker before OpenStream goes
/// to the writer, or to notify the worker that a stream has been closed from
/// the client side.
pub(crate) enum StreamRegistration {
    /// Register a new stream with the worker.
    Open {
        stream_id: u32,
        data_tx: mpsc::Sender<Bytes>,
        reply_tx: oneshot::Sender<Result<(), crate::error::Error>>,
        send_window: Arc<SendWindow>,
    },
    /// Notify the worker that a stream was closed by the client.
    Close { stream_id: u32 },
    /// Broadcast from reader when peer SETTINGS changes initial_window_size.
    /// Each worker applies the delta to its streams' send_windows.
    SettingsWindowDelta { delta: i64 },
    /// Broadcast from reader when GOAWAY is received. Each worker cleans up
    /// streams with id > last_good_stream_id in its shard.
    GoAway {
        last_good_stream_id: u32,
        status: u32,
    },
}

/// State protected by the per-handle open sequencer. Holds the
/// SPDY stream ID counter. Wrapped in `tokio::sync::Mutex` so the open
/// path can `.await` while holding it (reg_tx + cmd_tx sends are all
/// `.await`).
pub(super) struct OpenState {
    /// Next client stream ID. spdy requires client
    /// streams to be odd and monotonically increasing.
    pub next_stream_id: u32,
}
