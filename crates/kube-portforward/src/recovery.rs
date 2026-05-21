//! Session level recovery signalling.
//!
//! [`RecoverySignal`] fires when the port-forward session detects a
//! condition the caller might want to react to (typically by rebuilding the
//! session). Callers register a [`RecoveryCallback`] on the
//! [`SessionBuilder`](crate::SessionBuilder) or
//! [`ForwarderBuilder`](crate::ForwarderBuilder) to get these events.
//!
//! Callbacks run on internal tasks and should be cheap

use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RecoverySignal {
    KeepaliveTimeout {
        last_pong_age: Duration,
    },
    ServerClose,
    NetworkError(String),
    UpgradeFailed {
        status: Option<u16>,
        message: String,
    },
}

pub type RecoveryCallback = Arc<dyn Fn(RecoverySignal) + Send + Sync>;
