//! Public wire and payload models for `latzero-server`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Message names accepted or emitted by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Hello,
    JoinPool,
    SwitchPool,
    LeavePool,
    SetBuffer,
    GetBuffer,
    DeleteBuffer,
    ListBuffers,
    ListClients,
    SubscribeBuffer,
    UnsubscribeBuffer,
    EmitEvent,
    CallApp,
    AppResult,
    RegisterProcess,
    UnregisterProcess,
    CallProcess,
    BroadcastProcess,
    ListProcesses,
    WorkerMetrics,
    Ack,
    Error,
    PresenceUpdate,
    BufferUpdate,
    ProcessScale,
}

impl MessageType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::JoinPool => "join_pool",
            Self::SwitchPool => "switch_pool",
            Self::LeavePool => "leave_pool",
            Self::SetBuffer => "set_buffer",
            Self::GetBuffer => "get_buffer",
            Self::DeleteBuffer => "delete_buffer",
            Self::ListBuffers => "list_buffers",
            Self::ListClients => "list_clients",
            Self::SubscribeBuffer => "subscribe_buffer",
            Self::UnsubscribeBuffer => "unsubscribe_buffer",
            Self::EmitEvent => "emit_event",
            Self::CallApp => "call_app",
            Self::AppResult => "app_result",
            Self::RegisterProcess => "register_process",
            Self::UnregisterProcess => "unregister_process",
            Self::CallProcess => "call_process",
            Self::BroadcastProcess => "broadcast_process",
            Self::ListProcesses => "list_processes",
            Self::WorkerMetrics => "worker_metrics",
            Self::Ack => "ack",
            Self::Error => "error",
            Self::PresenceUpdate => "presence_update",
            Self::BufferUpdate => "buffer_update",
            Self::ProcessScale => "process_scale",
        }
    }
}

/// One newline-delimited JSON protocol envelope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Message {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    #[serde(default)]
    pub payload: Value,
}

impl Message {
    #[must_use]
    pub fn new(
        kind: MessageType,
        request_id: Option<String>,
        client_id: Option<String>,
        pool: Option<String>,
        payload: Value,
    ) -> Self {
        Self {
            kind: kind.as_str().to_owned(),
            request_id,
            client_id,
            pool,
            payload,
        }
    }
}

/// A stored server buffer, including metadata supplied by the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BufferEntry<T = Value> {
    pub value: T,
    pub updated_at: f64,
    pub updated_by: String,
    #[serde(default)]
    pub persistent: bool,
    #[serde(default)]
    pub ttl: Option<f64>,
    #[serde(default = "default_version")]
    pub version: u64,
}

const fn default_version() -> u64 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PresenceUpdate {
    pub client_id: String,
    pub status: String,
    pub pool: String,
    #[serde(default)]
    pub clients: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BufferUpdate {
    pub key: String,
    pub operation: String,
    pub entry: BufferEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmittedEvent {
    pub event: String,
    #[serde(default)]
    pub data: Map<String, Value>,
    #[serde(default)]
    pub source_client_id: Option<String>,
    #[serde(default)]
    pub target_client_id: Option<String>,
    #[serde(default)]
    pub response_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AppResult {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_request_id: Option<String>,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub source_client_id: Option<String>,
    #[serde(default)]
    pub target_client_id: Option<String>,
    #[serde(default)]
    pub response_to: Option<String>,
    #[serde(default)]
    pub value: Value,
    #[serde(default)]
    pub error: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessScale {
    pub action: String,
    pub process_name: String,
    #[serde(default = "default_scale_count")]
    pub count: usize,
}

const fn default_scale_count() -> usize {
    1
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WorkerKind {
    #[default]
    Thread,
    Process,
    Adaptive,
}

impl WorkerKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Thread => "thread",
            Self::Process => "process",
            Self::Adaptive => "adaptive",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProcessInfo {
    pub client_id: String,
    pub process_name: String,
    #[serde(default)]
    pub worker_kind: WorkerKind,
    #[serde(default)]
    pub worker_count: usize,
    #[serde(default)]
    pub queue_depth: usize,
    #[serde(default)]
    pub avg_latency: f64,
    #[serde(default)]
    pub completed_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProcessRegistration {
    pub process_id: String,
    #[serde(default)]
    pub group_id: String,
    #[serde(default)]
    pub worker_kind: WorkerKind,
    #[serde(default = "default_scale_count")]
    pub min_workers: usize,
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,
    #[serde(default)]
    pub scale: bool,
    #[serde(default)]
    pub max_replicas: Option<usize>,
}

const fn default_max_workers() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WorkerMetrics {
    pub process_name: String,
    pub active_workers: usize,
    pub queue_depth: usize,
    pub avg_latency: f64,
    pub completed_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanResult {
    pub next_cursor: usize,
    pub keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolStats {
    pub name: String,
    pub client_id: String,
    pub server_mode: bool,
    pub key_count: usize,
}

pub type ProcessMap = HashMap<String, ProcessInfo>;
