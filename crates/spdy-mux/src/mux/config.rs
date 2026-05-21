use std::time::Duration;

/// Configurable limits for the SPDY multiplexer. Replaces all former
/// hardcoded constants. Passed through `MuxHandle::spawn()` and
/// `open_portforward_pair()`.
#[derive(Clone)]
pub struct MuxConfig {
    /// Number of parallel WebSocket connections per session
    pub pool_size: usize,
    /// Initial per-stream send/recv window size (bytes).
    pub initial_window_size: u32,
    /// Maximum concurrent stream *pairs* the SPDY peer
    /// accepts. Exceeding this is a protocol violation.
    pub max_concurrent_streams: u32,
    /// Maximum concurrent stream pairs the open path will
    /// allow before returning `CapacityExhausted`.
    pub operating_max_streams: u32,
    /// Maximum SPDY DATA frame payload size. Outgoing frames are split to
    /// respect the peer's limit; incoming frames exceeding this are rejected.
    pub max_frame_size: u32,
    /// Capacity of the data command channel .
    pub cmd_buffer_size: usize,
    /// Capacity of the control command channel
    /// Control commands (CloseStream, GoAway, PING, PONG, WINDOW_UPDATE)
    /// are drained with priority over data commands.
    pub control_buffer_size: usize,
    /// Capacity of the open-registration channel .
    /// Each of the 5 workers gets its own channel with this capacity.
    pub reg_buffer_size: usize,
    /// Capacity of the close-registration channel .
    /// Separate from open registrations so teardown is never blocked by
    /// a burst of opens.
    pub close_reg_buffer_size: usize,
    /// Per-worker bounded queue capacity for inbound frames.
    pub worker_queue_size: usize,
    /// Interval between keepalive PINGs.
    pub ping_interval: Duration,
    /// Time to wait for a PING response before tearing down.
    pub ping_timeout: Duration,
    /// Maximum duration for `sink.flush()` before tearing down.
    pub write_timeout: Duration,
    /// Time without any incoming frame before sending a probe PING.
    pub idle_timeout: Duration,
    /// Capacity of the dedicated WINDOW_UPDATE channel.
    pub window_buffer_size: usize,
    /// Per-stream inbound data channel capacity. When full, the worker
    /// withholds WINDOW_UPDATE for this stream so the peer backs off naturally.
    pub stream_data_buffer: usize,
    /// Per-stream inbound error channel capacity.
    pub stream_error_buffer: usize,
}

impl Default for MuxConfig {
    fn default() -> Self {
        Self {
            pool_size: 1,
            initial_window_size: 1024 * 1024,
            max_concurrent_streams: 100,
            operating_max_streams: 64,
            max_frame_size: 1024 * 1024,
            cmd_buffer_size: 512,
            control_buffer_size: 256,
            reg_buffer_size: 128,
            close_reg_buffer_size: 64,
            worker_queue_size: 128,
            ping_interval: Duration::from_secs(30),
            ping_timeout: Duration::from_secs(30),
            write_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(120),
            window_buffer_size: 256,
            stream_data_buffer: 64,
            stream_error_buffer: 8,
        }
    }
}
