//! Async client for the newline-delimited JSON protocol exposed by
//! `latzero-server`.
//!
//! This crate intentionally implements server mode only. The Python client's
//! process-local shared-memory mode is Python-specific and is not part of the
//! cross-language wire protocol.

mod client;
mod error;
pub mod protocol;

pub use client::{
    CallOutcome, Client, ClientBuilder, ClientEvent, EventEmitter, EventHandlerId, Namespace,
    ProcessOptions,
};
pub use error::{Error, Result};
pub use protocol::{
    AppResult, BufferEntry, BufferUpdate, EmittedEvent, Message, MessageType, PoolStats,
    PresenceUpdate, ProcessInfo, ProcessRegistration, ProcessScale, ScanResult, WorkerKind,
    WorkerMetrics,
};
