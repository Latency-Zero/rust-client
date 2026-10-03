#![recursion_limit = "256"]

use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, Read, Write},
    process::ExitCode,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use latzero::{AppResult, CallOutcome, Client, ClientEvent, Error, ProcessOptions};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{broadcast, watch},
    task::{JoinHandle, JoinSet},
    time::{self, Instant},
};

const MIB: usize = 1024 * 1024;
const MAX_COMMAND_BYTES: u64 = 1024 * 1024;
const MAX_RUNS: usize = 128;
const MAX_ERRORS: usize = 8;

#[derive(Clone, Deserialize)]
#[serde(default)]
struct Config {
    port: u16,
    client_id: String,
    pool: String,
    timeout_ms: u64,
    worker_delay_ms: u64,
    role: String,
    min_workers: usize,
    max_workers: usize,
    handler_workers: Option<usize>,
    runtime_threads: usize,
    max_pending_requests: usize,
    writer_capacity: usize,
    max_handler_tasks: usize,
    max_frame_bytes: usize,
    max_queued_bytes: usize,
    max_handler_bytes: usize,
    event_capacity: usize,
    history_limit: usize,
    record_limit: usize,
    sample_limit: usize,
    shutdown_timeout_ms: u64,
    include_result_payload: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 14_130,
            client_id: String::new(),
            pool: String::new(),
            timeout_ms: 5000,
            worker_delay_ms: 0,
            role: "worker".to_owned(),
            min_workers: 1,
            max_workers: 64,
            handler_workers: None,
            runtime_threads: 2,
            max_pending_requests: 512,
            writer_capacity: 512,
            max_handler_tasks: 512,
            max_frame_bytes: MIB,
            max_queued_bytes: 64 * MIB,
            max_handler_bytes: 64 * MIB,
            event_capacity: 4096,
            history_limit: 300_000,
            record_limit: 100_000,
            sample_limit: 100_000,
            shutdown_timeout_ms: 2000,
            include_result_payload: false,
        }
    }
}

impl Config {
    fn validate(&self) -> Result<(), String> {
        identifier("client_id", &self.client_id, 128)?;
        identifier("pool", &self.pool, 512)?;
        if self.port == 0 || !["worker", "load", "mesh"].contains(&self.role.as_str()) {
            return Err("port must be positive and role must be worker, load, or mesh".to_owned());
        }
        for (name, value, minimum, maximum) in [
            ("timeout_ms", self.timeout_ms as usize, 1, 30_000),
            ("worker_delay_ms", self.worker_delay_ms as usize, 0, 30_000),
            ("min_workers", self.min_workers, 1, 64),
            ("max_workers", self.max_workers, 1, 64),
            ("runtime_threads", self.runtime_threads, 1, 4),
            ("max_pending_requests", self.max_pending_requests, 1, 1024),
            ("writer_capacity", self.writer_capacity, 1, 1024),
            ("max_handler_tasks", self.max_handler_tasks, 1, 1024),
            ("max_frame_bytes", self.max_frame_bytes, 1024, 16 * MIB),
            ("max_queued_bytes", self.max_queued_bytes, 1024, 64 * MIB),
            ("max_handler_bytes", self.max_handler_bytes, 1024, 64 * MIB),
            ("event_capacity", self.event_capacity, 1, 100_000),
            ("history_limit", self.history_limit, 1, 300_000),
            ("record_limit", self.record_limit, 1, 100_000),
            ("sample_limit", self.sample_limit, 1, 100_000),
            (
                "shutdown_timeout_ms",
                self.shutdown_timeout_ms as usize,
                1,
                10_000,
            ),
        ] {
            if !(minimum..=maximum).contains(&value) {
                return Err(format!("{name} must be between {minimum} and {maximum}"));
            }
        }
        if self.min_workers > self.max_workers {
            return Err("min_workers must not exceed max_workers".to_owned());
        }
        Ok(())
    }

    fn limits(&self) -> Value {
        json!({
            "max_pending_requests": self.max_pending_requests,
            "writer_capacity": self.writer_capacity,
            "max_handler_tasks": self.max_handler_tasks,
            "max_frame_bytes": self.max_frame_bytes,
            "max_queued_bytes": self.max_queued_bytes,
            "max_handler_bytes": self.max_handler_bytes,
            "event_capacity": self.event_capacity,
            "history_limit": self.history_limit,
            "record_limit": self.record_limit,
            "sample_limit": self.sample_limit,
            "runtime_threads": self.runtime_threads,
            "min_workers": self.min_workers,
            "max_workers": if self.role == "load" { self.min_workers } else { self.max_workers },
            "scale": false,
        })
    }
}

#[derive(Clone, Deserialize)]
struct Edge {
    target: String,
    #[serde(default)]
    response_to: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(default)]
struct Settings {
    duration: f64,
    concurrency: usize,
    mode: String,
    rate: f64,
    payload_bytes: usize,
    max_operations: u64,
    target: Option<String>,
    response_to: Option<String>,
    edges: Vec<Edge>,
    start_at_unix_ms: Option<f64>,
    warmup: f64,
    timeout_ms: Option<u64>,
    rpc_kind: String,
    max_schedule_lag_ms: f64,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            duration: 1.0,
            concurrency: 1,
            mode: "closed".to_owned(),
            rate: 1000.0,
            payload_bytes: 0,
            max_operations: 200_000,
            target: None,
            response_to: None,
            edges: Vec::new(),
            start_at_unix_ms: None,
            warmup: 0.0,
            timeout_ms: None,
            rpc_kind: "process".to_owned(),
            max_schedule_lag_ms: 50.0,
        }
    }
}

impl Settings {
    fn validate(&mut self, config: &Config) -> Result<(), String> {
        for (name, value, minimum, maximum) in [
            ("duration", self.duration, 0.2, 120.0),
            ("warmup", self.warmup, 0.0, 10.0),
            ("rate", self.rate, 1.0, 100_000.0),
            ("max_schedule_lag_ms", self.max_schedule_lag_ms, 0.0, 1000.0),
        ] {
            if !value.is_finite() || !(minimum..=maximum).contains(&value) {
                return Err(format!(
                    "{name} must be finite and between {minimum} and {maximum}"
                ));
            }
        }
        if !(1..=1024).contains(&self.concurrency)
            || self.payload_bytes > 65_536
            || !(1..=500_000).contains(&self.max_operations)
            || !["closed", "open"].contains(&self.mode.as_str())
            || !["app", "process"].contains(&self.rpc_kind.as_str())
        {
            return Err(
                "invalid concurrency, payload_bytes, max_operations, mode, or rpc_kind".to_owned(),
            );
        }
        let timeout = self.timeout_ms.unwrap_or(config.timeout_ms);
        if !(1..=30_000).contains(&timeout) {
            return Err("timeout_ms must be between 1 and 30000".to_owned());
        }
        self.timeout_ms = Some(timeout);
        if let Some(start) = self.start_at_unix_ms {
            let difference = start - unix_ms();
            if !start.is_finite() || !(-120_000.0..=30_000.0).contains(&difference) {
                return Err("start_at_unix_ms must be within 120s past and 30s future".to_owned());
            }
        }
        if self.edges.is_empty() {
            self.edges.push(Edge {
                target: self
                    .target
                    .clone()
                    .ok_or("target or nonempty edges is required")?,
                response_to: self.response_to.clone(),
            });
        }
        if self.edges.len() > 1024 {
            return Err("edges must contain at most 1024 entries".to_owned());
        }
        for edge in &mut self.edges {
            identifier("target", &edge.target, 512)?;
            if edge.response_to.is_none() {
                edge.response_to.clone_from(&self.response_to);
            }
            if let Some(recipient) = &edge.response_to {
                identifier("response_to", recipient, 512)?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct Command {
    operation: String,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    settings: Option<Settings>,
}

#[derive(Default)]
struct LedgerRun {
    effects: u64,
    unique_effects: u64,
    incoming_results: u64,
    delivered_success: u64,
    duplicates: u64,
    effect_duplicates: u64,
    result_duplicates: u64,
    misroutes: u64,
    malformed: u64,
    application_errors: u64,
    completed_within_window: u64,
    results_without_window: u64,
    correlation_errors: u64,
    active: u64,
    max_active: u64,
    cancelled_handlers: u64,
    clock_skew_samples: u64,
    effect_records: Vec<Value>,
    result_records: Vec<Value>,
    samples_latency_ms: Vec<f64>,
    samples_scheduled_latency_ms: Vec<f64>,
    error_samples: Vec<Value>,
    history_limit_hit: bool,
}

struct Ledger {
    runs: HashMap<String, LedgerRun>,
    effects_seen: HashSet<String>,
    results_seen: HashSet<String>,
    records: usize,
    samples: usize,
    event_gaps: u64,
    handler_failures: u64,
    disconnected: bool,
    event_receiver_closed: bool,
    history_limit_hit: bool,
    unattributed_results: Vec<Value>,
    error_samples: Vec<Value>,
    config: Arc<Config>,
}

impl Ledger {
    fn new(config: Arc<Config>) -> Self {
        Self {
            runs: HashMap::new(),
            effects_seen: HashSet::new(),
            results_seen: HashSet::new(),
            records: 0,
            samples: 0,
            event_gaps: 0,
            handler_failures: 0,
            disconnected: false,
            event_receiver_closed: false,
            history_limit_hit: false,
            unattributed_results: Vec::new(),
            error_samples: Vec::new(),
            config,
        }
    }

    fn admit_run(&mut self, run_id: &str) -> bool {
        if self.runs.contains_key(run_id) {
            return true;
        }
        if self.runs.len() == MAX_RUNS {
            self.history_limit_hit = true;
            return false;
        }
        self.runs.insert(run_id.to_owned(), LedgerRun::default());
        true
    }

    fn observe_effect(&mut self, data: &Map<String, Value>) -> Option<String> {
        if data.get("warmup").and_then(Value::as_bool) == Some(true) {
            return None;
        }
        let Some(run_id) = text(data.get("run_id")).filter(|id| !id.is_empty() && id.len() <= 128)
        else {
            error_sample(
                &mut self.error_samples,
                json!({"kind":"malformed_invocation","message":"missing valid run_id"}),
            );
            return None;
        };
        if !self.admit_run(run_id) {
            return None;
        }
        let token = text(data.get("token")).filter(|token| !token.is_empty() && token.len() <= 512);
        let duplicate = token.is_some_and(|token| self.effects_seen.contains(token));
        let can_remember =
            self.effects_seen.len() + self.results_seen.len() < self.config.history_limit;
        if let Some(token) = token.filter(|_| !duplicate && can_remember) {
            self.effects_seen.insert(token.to_owned());
        }
        let malformed = token.is_none()
            || text(data.get("origin")).is_none()
            || text(data.get("padding")).is_none()
            || number(data.get("sent_at_unix_ms")).is_none()
            || scheduled_ms(data).is_none()
            || !valid_token(data);
        let misroute = text(data.get("expected_target")) != Some(self.config.client_id.as_str());
        let run = self.runs.get_mut(run_id).expect("admitted ledger run");
        run.effects += 1;
        run.active += 1;
        run.max_active = run.max_active.max(run.active);
        if duplicate {
            run.duplicates += 1;
            run.effect_duplicates += 1;
        } else {
            run.unique_effects += 1;
        }
        run.malformed += u64::from(malformed);
        run.misroutes += u64::from(misroute);
        if !can_remember && !duplicate {
            run.history_limit_hit = true;
            self.history_limit_hit = true;
        }
        if run.effect_records.len() < self.config.record_limit
            && self.records < self.config.history_limit
        {
            run.effect_records.push(json!({
                "token": token,
                "origin": data.get("origin"),
                "target": self.config.client_id,
                "handled_by": self.config.client_id,
                "expected_target": data.get("expected_target"),
                "recipient": data.get("expected_recipient"),
                "run_id": run_id,
                "sent_at_unix_ms": data.get("sent_at_unix_ms"),
                "scheduled_at_unix_ms": scheduled_ms(data),
                "received_at_unix_ms": unix_ms(),
                "executed_at_unix_ms": unix_ms(),
                "padding_bytes": text(data.get("padding")).map(str::len),
                "duplicate": duplicate,
                "valid": !malformed && !misroute,
            }));
            self.records += 1;
        } else {
            run.history_limit_hit = true;
            self.history_limit_hit = true;
        }
        Some(run_id.to_owned())
    }

    fn observe_result(&mut self, request_id: String, result: AppResult, kind: &str) {
        let envelope_observed = kind != "typed_terminal_value";
        let received = unix_ms();
        let object = result.value.as_object();
        if object
            .and_then(|data| data.get("warmup"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            return;
        }
        let token = object.and_then(|data| text(data.get("token")));
        let run_id = object
            .and_then(|data| text(data.get("run_id")))
            .filter(|id| !id.is_empty() && id.len() <= 128);
        let origin = object.and_then(|data| text(data.get("origin")));
        let expected_target = object.and_then(|data| text(data.get("expected_target")));
        let expected_recipient = object.and_then(|data| text(data.get("expected_recipient")));
        let handled_by = object.and_then(|data| text(data.get("handled_by")));
        let sent = object.and_then(|data| number(data.get("sent_at_unix_ms")));
        let scheduled = object.and_then(scheduled_ms);
        let padding = object.and_then(|data| text(data.get("padding")));
        let latency = sent.map(|sent| received - sent);
        let scheduled_latency = scheduled.map(|scheduled| received - scheduled);
        let malformed = token.is_none_or(|token| token.is_empty() || token.len() > 512)
            || origin.is_none()
            || expected_target.is_none()
            || expected_recipient.is_none()
            || sent.is_none()
            || scheduled.is_none()
            || padding.is_none_or(|padding| !padding.bytes().all(|byte| byte == b'x'))
            || (envelope_observed && request_id.is_empty())
            || object.is_some_and(|data| {
                data.get("payload_bytes").is_some_and(|bytes| {
                    bytes.as_u64().is_none_or(|bytes| {
                        padding.is_none_or(|padding| padding.len() as u64 != bytes)
                    })
                })
            });
        let correlation_error = object.is_none_or(|data| !valid_token(data))
            || result
                .request_id
                .as_deref()
                .is_some_and(|id| id != request_id);
        let misroute = expected_target != handled_by
            || expected_recipient != Some(self.config.client_id.as_str())
            || (envelope_observed
                && (expected_target != result.target_client_id.as_deref()
                    || origin != result.source_client_id.as_deref()
                    || result.response_to.as_deref() != Some(self.config.client_id.as_str())));
        let application_error = result.error.as_ref().is_some_and(|error| !error.is_null());
        let key = token.unwrap_or(&request_id);
        let duplicate = self.results_seen.contains(key);
        let can_remember =
            self.effects_seen.len() + self.results_seen.len() < self.config.history_limit;
        if !duplicate && can_remember {
            self.results_seen.insert(key.to_owned());
        }
        let mut record = json!({
            "type": kind,
            "token": token,
            "origin": origin.or(result.source_client_id.as_deref()),
            "target": handled_by.or(result.target_client_id.as_deref()),
            "handled_by": handled_by,
            "expected_target": expected_target,
            "recipient": self.config.client_id,
            "expected_recipient": expected_recipient,
            "run_id": run_id,
            "request_id": if envelope_observed { Some(request_id.as_str()) } else { None },
            "payload_request_id": result.request_id,
            "parent_request_id": result.parent_request_id,
            "source_client_id": result.source_client_id,
            "target_client_id": result.target_client_id,
            "response_to": result.response_to,
            "event": result.event,
            "envelope_observed": envelope_observed,
            "correlation_source": if envelope_observed { "unsolicited_app_result_envelope" } else { "typed_sdk_future_binding" },
            "sent_at_unix_ms": sent,
            "scheduled_at_unix_ms": scheduled,
            "received_at_unix_ms": received,
            "padding_bytes": padding.map(str::len),
            "latency_ms": latency,
            "scheduled_latency_ms": scheduled_latency,
            "error": result.error,
            "duplicate": duplicate,
            "valid": !malformed && !misroute && !application_error && !correlation_error,
            "latency_clock": "system_time_unix_ms_cross_process_estimate",
            "window_start_at_unix_ms": object.and_then(|data| number(data.get("window_start_at_unix_ms"))),
            "window_end_at_unix_ms": object.and_then(|data| number(data.get("window_end_at_unix_ms"))),
        });
        if self.config.include_result_payload {
            if envelope_observed {
                record["payload"] = serde_json::to_value(&result).unwrap_or(Value::Null);
            } else {
                record["value"] = result.value.clone();
            }
        }
        let Some(run_id) = run_id.filter(|run_id| self.admit_run(run_id)) else {
            if self.unattributed_results.len() < self.config.record_limit
                && self.records < self.config.history_limit
            {
                self.unattributed_results.push(record);
                self.records += 1;
            } else {
                self.history_limit_hit = true;
            }
            return;
        };
        let run = self.runs.get_mut(run_id).expect("admitted ledger run");
        run.incoming_results += 1;
        run.application_errors += u64::from(application_error);
        run.malformed += u64::from(malformed);
        run.misroutes += u64::from(misroute);
        run.correlation_errors += u64::from(correlation_error);
        if duplicate {
            run.duplicates += 1;
            run.result_duplicates += 1;
        }
        if !malformed && !misroute && !application_error && !duplicate && !correlation_error {
            run.delivered_success += 1;
            if let Some((start, end)) = object.and_then(|data| {
                Some((
                    number(data.get("window_start_at_unix_ms"))?,
                    number(data.get("window_end_at_unix_ms"))?,
                ))
            }) {
                run.completed_within_window += u64::from(start <= received && received < end);
            } else {
                run.results_without_window += 1;
            }
            if let (Some(latency), Some(scheduled_latency)) = (latency, scheduled_latency) {
                if latency >= 0.0 && scheduled_latency >= 0.0 {
                    if self.samples + 2 <= self.config.history_limit {
                        let previous =
                            run.samples_latency_ms.len() + run.samples_scheduled_latency_ms.len();
                        bounded_sample(
                            &mut run.samples_latency_ms,
                            latency,
                            self.config.sample_limit,
                            &mut run.history_limit_hit,
                        );
                        bounded_sample(
                            &mut run.samples_scheduled_latency_ms,
                            scheduled_latency,
                            self.config.sample_limit,
                            &mut run.history_limit_hit,
                        );
                        self.samples += run.samples_latency_ms.len()
                            + run.samples_scheduled_latency_ms.len()
                            - previous;
                    } else {
                        run.history_limit_hit = true;
                    }
                } else {
                    run.clock_skew_samples += 1;
                }
            }
        }
        if malformed || misroute || application_error || duplicate || correlation_error {
            error_sample(&mut run.error_samples, record.clone());
        }
        if run.result_records.len() < self.config.record_limit
            && self.records < self.config.history_limit
        {
            run.result_records.push(record);
            self.records += 1;
        } else {
            run.history_limit_hit = true;
        }
        run.history_limit_hit |= !can_remember && !duplicate;
        self.history_limit_hit |= run.history_limit_hit;
    }

    fn snapshot(&self, selected: Option<&str>, running: bool) -> Value {
        let runs: Map<String, Value> = self
            .runs
            .iter()
            .filter(|(id, _)| selected.is_none_or(|selected| selected == id.as_str()))
            .map(|(id, run)| {
                (
                    id.clone(),
                    json!({
                            "effects": run.effects,
                            "unique_effects": run.unique_effects,
                            "incoming_results": run.incoming_results,
                            "delivered_success": run.delivered_success,
                            "duplicates": run.duplicates,
                            "effect_duplicates": run.effect_duplicates,
                            "result_duplicates": run.result_duplicates,
                            "misroutes": run.misroutes,
                            "malformed": run.malformed,
                    "application_errors": run.application_errors,
                    "correlation_errors": run.correlation_errors,
                    "completed_within_window": run.completed_within_window,
                    "results_without_window": run.results_without_window,
                            "active": run.active,
                            "max_active": run.max_active,
                            "cancelled_handlers": run.cancelled_handlers,
                            "clock_skew_samples": run.clock_skew_samples,
                            "effect_records": run.effect_records,
                            "result_records": run.result_records,
                            "samples_latency_ms": run.samples_latency_ms,
                            "samples_scheduled_latency_ms": run.samples_scheduled_latency_ms,
                            "latency_ms": distribution(&run.samples_latency_ms),
                            "scheduled_latency_ms": distribution(&run.samples_scheduled_latency_ms),
                            "latency_clock": "system_time_unix_ms_cross_process_estimate",
                            "error_samples": run.error_samples,
                            "history_limit_hit": run.history_limit_hit,
                        }),
                )
            })
            .collect();
        json!({
            "operation": "stats",
            "client_id": self.config.client_id,
            "run_id": selected,
            "run_in_progress": running,
            "runs": runs,
            "unattributed_results": self.unattributed_results,
            "event_gaps": self.event_gaps,
            "handler_failures": self.handler_failures,
            "disconnected": self.disconnected,
            "event_receiver_closed": self.event_receiver_closed,
            "history_limit_hit": self.history_limit_hit,
            "error_samples": self.error_samples,
            "limits": self.config.limits(),
        })
    }
}

struct HandlerGuard {
    ledger: Arc<Mutex<Ledger>>,
    run_id: Option<String>,
    completed: bool,
}

impl Drop for HandlerGuard {
    fn drop(&mut self) {
        if let Some(id) = self.run_id.as_ref() {
            if let Some(run) = lock(&self.ledger).runs.get_mut(id) {
                run.active = run.active.saturating_sub(1);
                run.cancelled_handlers += u64::from(!self.completed);
            }
        }
    }
}

async fn echo(ledger: Arc<Mutex<Ledger>>, mut data: Map<String, Value>) -> Result<Value, String> {
    let (run_id, id, delay) = {
        let mut state = lock(&ledger);
        (
            state.observe_effect(&data),
            state.config.client_id.clone(),
            state.config.worker_delay_ms,
        )
    };
    let mut guard = HandlerGuard {
        ledger,
        run_id,
        completed: false,
    };
    if delay > 0 {
        time::sleep(Duration::from_millis(delay)).await;
    }
    data.insert("handled_by".to_owned(), Value::String(id));
    guard.completed = true;
    Ok(Value::Object(data))
}

async fn collect_events(
    mut receiver: broadcast::Receiver<ClientEvent>,
    ledger: Arc<Mutex<Ledger>>,
) {
    loop {
        match receiver.recv().await {
            Ok(ClientEvent::AppResult { request_id, result }) => {
                lock(&ledger).observe_result(request_id, result, "app_result")
            }
            Ok(ClientEvent::Unknown(message)) if message.kind == "error" => {
                if let Ok(mut result) = serde_json::from_value::<AppResult>(message.payload.clone())
                {
                    result.error = Some(message.payload.clone());
                    lock(&ledger).observe_result(
                        message.request_id.unwrap_or_default(),
                        result,
                        "error",
                    );
                }
            }
            Ok(ClientEvent::HandlerFailed { event, error }) => {
                let mut state = lock(&ledger);
                state.handler_failures += 1;
                error_sample(
                    &mut state.error_samples,
                    json!({"event":event,"error":short(&error)}),
                );
            }
            Ok(ClientEvent::Disconnected) => lock(&ledger).disconnected = true,
            Ok(_) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                lock(&ledger).event_gaps += skipped
            }
            Err(broadcast::error::RecvError::Closed) => {
                lock(&ledger).event_receiver_closed = true;
                break;
            }
        }
    }
}

#[derive(Default)]
struct Measurement {
    offered: u64,
    started: u64,
    accepted: u64,
    success: u64,
    completed_within_window: u64,
    errors: u64,
    timeouts: u64,
    rejected: u64,
    mismatches: u64,
    duplicates: u64,
    routed_accepted: u64,
    scheduler_rejected: u64,
    overdue_rejected: u64,
    concurrency_rejected: u64,
    active: u64,
    max_active: u64,
    task_panics: u64,
    accepted_within_window: u64,
    samples_service_latency_ms: Vec<f64>,
    error_kinds: HashMap<String, u64>,
    issued: Vec<Value>,
    seen_terminal_tokens: HashSet<String>,
    samples_latency_ms: Vec<f64>,
    samples_scheduled_latency_ms: Vec<f64>,
    acceptance_latency_ms: Vec<f64>,
    samples_issue_lag_ms: Vec<f64>,
    error_samples: Vec<Value>,
    operation_limit_hit: bool,
    history_limit_hit: bool,
    offer_lag_count: u64,
    max_offer_lag_ms: f64,
    last_issued_at_unix_ms: Option<f64>,
}

#[derive(Clone)]
struct Phase {
    client: Client,
    ledger: Arc<Mutex<Ledger>>,
    config: Arc<Config>,
    settings: Arc<Settings>,
    run_id: Arc<str>,
    padding: Arc<str>,
    start: Instant,
    end: Instant,
    start_unix_ms: f64,
    warmup: bool,
    measurement: Arc<Mutex<Measurement>>,
}

struct AttemptGuard {
    measurement: Arc<Mutex<Measurement>>,
    record_index: Option<usize>,
    token: String,
    completed: bool,
}

impl Drop for AttemptGuard {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = lock(&self.measurement);
            state.active = state.active.saturating_sub(1);
            state.errors += 1;
            *state.error_kinds.entry("cancelled".to_owned()).or_default() += 1;
            if let Some(record) = self
                .record_index
                .and_then(|index| state.issued.get_mut(index))
            {
                record["outcome"] = json!("cancelled");
            }
            error_sample(
                &mut state.error_samples,
                json!({"kind":"cancelled","token":self.token}),
            );
        }
    }
}

async fn attempt(phase: Phase, sequence: u64, due: Instant, scheduled: f64) {
    let issued_at = Instant::now();
    let lag = milliseconds(issued_at.saturating_duration_since(due));
    let edge = &phase.settings.edges[sequence as usize % phase.settings.edges.len()];
    let recipient = edge
        .response_to
        .as_deref()
        .unwrap_or(phase.client.client_id());
    let token = if phase.warmup {
        format!(
            "{}:warmup:{}:{sequence}",
            phase.run_id,
            phase.client.client_id()
        )
    } else {
        format!("{}:{}:{sequence}", phase.run_id, phase.client.client_id())
    };
    let sent = unix_ms();
    let packet = json!({
        "token": token,
        "padding": phase.padding.as_ref(),
        "origin": phase.client.client_id(),
        "expected_target": edge.target,
        "expected_recipient": recipient,
        "run_id": phase.run_id.as_ref(),
        "sent_at_unix_ms": sent,
        "scheduled_at_unix_ms": scheduled,
        "scheduled_at": scheduled,
        "payload_bytes": phase.settings.payload_bytes,
        "window_start_at_unix_ms": phase.start_unix_ms,
        "window_end_at_unix_ms": phase.start_unix_ms + phase.end.duration_since(phase.start).as_secs_f64() * 1000.0,
        "warmup": phase.warmup,
    });
    let record_index = {
        let mut state = lock(&phase.measurement);
        if phase.settings.mode == "closed" {
            state.offer_lag_count += 1;
            state.max_offer_lag_ms = state.max_offer_lag_ms.max(lag);
        }
        if !phase.warmup {
            let limit = phase.config.sample_limit;
            let mut hit = state.history_limit_hit;
            bounded_sample(&mut state.samples_issue_lag_ms, lag, limit, &mut hit);
            state.history_limit_hit = hit;
        }
        if issued_at >= phase.end
            || (phase.settings.mode == "open" && lag > phase.settings.max_schedule_lag_ms)
        {
            state.rejected += 1;
            state.scheduler_rejected += 1;
            state.overdue_rejected += 1;
            return;
        }
        state.started += 1;
        state.last_issued_at_unix_ms = Some(sent);
        state.active += 1;
        state.max_active = state.max_active.max(state.active);
        if !phase.warmup && state.issued.len() < phase.config.record_limit {
            let index = state.issued.len();
            state.issued.push(json!({
                "token": token,
                "origin": phase.client.client_id(),
                "target": edge.target,
                "recipient": recipient,
                "run_id": phase.run_id.as_ref(),
                "request_id": null,
                "sent_at_unix_ms": sent,
                "scheduled_at_unix_ms": scheduled,
                "deadline_at_unix_ms": sent + phase.settings.timeout_ms.expect("validated timeout") as f64,
                "outcome": "pending",
            }));
            Some(index)
        } else {
            state.history_limit_hit |= !phase.warmup;
            None
        }
    };
    let mut guard = AttemptGuard {
        measurement: Arc::clone(&phase.measurement),
        record_index,
        token: token.clone(),
        completed: false,
    };
    let timeout = Duration::from_millis(phase.settings.timeout_ms.expect("validated timeout"));
    let outcome = if phase.settings.rpc_kind == "app" {
        phase
            .client
            .call_app_with_options::<_, Value>(
                &edge.target,
                "echo",
                &packet,
                timeout,
                edge.response_to.as_deref(),
            )
            .await
    } else {
        phase
            .client
            .call_process_with_options::<_, Value>(
                &format!("{}:echo", edge.target),
                &packet,
                timeout,
                edge.response_to.as_deref(),
            )
            .await
    };
    let finished = Instant::now();
    let latency = milliseconds(finished.saturating_duration_since(issued_at));
    let scheduled_latency = milliseconds(finished.saturating_duration_since(due));
    let mut state = lock(&phase.measurement);
    state.active = state.active.saturating_sub(1);
    let (name, request_id) = match outcome {
        Ok(CallOutcome::Routed { request_id }) if recipient != phase.client.client_id() => {
            state.accepted += 1;
            state.routed_accepted += 1;
            state.accepted_within_window +=
                u64::from(finished >= phase.start && finished < phase.end);
            if !phase.warmup {
                let mut hit = state.history_limit_hit;
                bounded_sample(
                    &mut state.acceptance_latency_ms,
                    latency,
                    phase.config.sample_limit,
                    &mut hit,
                );
                state.history_limit_hit = hit;
            }
            ("accepted", Some(request_id))
        }
        Ok(CallOutcome::Result(value)) if recipient == phase.client.client_id() => {
            state.accepted += 1;
            let valid = validate_echo(&packet, &value, &edge.target);
            if !phase.warmup {
                lock(&phase.ledger).observe_result(
                    String::new(),
                    AppResult {
                        request_id: None,
                        parent_request_id: None,
                        event: None,
                        source_client_id: None,
                        target_client_id: None,
                        response_to: None,
                        value: value.clone(),
                        error: None,
                    },
                    "typed_terminal_value",
                );
            }
            let returned_token = text(value.get("token"));
            let duplicate =
                returned_token.is_some_and(|token| state.seen_terminal_tokens.contains(token));
            if let Some(returned_token) = returned_token.filter(|_| !duplicate) {
                if state.seen_terminal_tokens.len() < phase.config.history_limit {
                    state.seen_terminal_tokens.insert(returned_token.to_owned());
                } else {
                    state.history_limit_hit = true;
                }
            }
            state.duplicates += u64::from(duplicate);
            if valid && !duplicate {
                state.success += 1;
                state.completed_within_window +=
                    u64::from(finished >= phase.start && finished < phase.end);
                if !phase.warmup {
                    let mut hit = state.history_limit_hit;
                    bounded_sample(
                        &mut state.samples_latency_ms,
                        if phase.settings.mode == "open" {
                            scheduled_latency
                        } else {
                            latency
                        },
                        phase.config.sample_limit,
                        &mut hit,
                    );
                    bounded_sample(
                        &mut state.samples_scheduled_latency_ms,
                        scheduled_latency,
                        phase.config.sample_limit,
                        &mut hit,
                    );
                    bounded_sample(
                        &mut state.samples_service_latency_ms,
                        latency,
                        phase.config.sample_limit,
                        &mut hit,
                    );
                    state.history_limit_hit = hit;
                }
                ("success", None)
            } else {
                state.errors += 1;
                state.mismatches += u64::from(!valid);
                *state
                    .error_kinds
                    .entry("validation".to_owned())
                    .or_default() += 1;
                error_sample(
                    &mut state.error_samples,
                    json!({"kind":"validation","token":token,"returned_token":returned_token,"handled_by":value.get("handled_by"),"duplicate":duplicate,"differences":echo_differences(&packet,&value)}),
                );
                ("error", None)
            }
        }
        Ok(_) => {
            state.errors += 1;
            state.mismatches += 1;
            *state.error_kinds.entry("outcome".to_owned()).or_default() += 1;
            error_sample(
                &mut state.error_samples,
                json!({"kind":"outcome","token":token,"message":"terminal/acceptance outcome did not match recipient"}),
            );
            ("error", None)
        }
        Err(error) => {
            let (kind, timed_out, rejected) = classify_error(&error);
            state.errors += 1;
            state.timeouts += u64::from(timed_out);
            state.rejected += u64::from(rejected);
            state.accepted += u64::from(matches!(error, Error::Handler(_)));
            *state.error_kinds.entry(kind.to_owned()).or_default() += 1;
            let request_id = match &error {
                Error::Timeout { request_id, .. } => Some(request_id.clone()),
                _ => None,
            };
            error_sample(
                &mut state.error_samples,
                json!({"kind":kind,"token":token,"error":short(&error.to_string())}),
            );
            (
                if timed_out {
                    "timeout"
                } else if rejected {
                    "rejected"
                } else {
                    "error"
                },
                request_id,
            )
        }
    };
    if let Some(record) = record_index.and_then(|index| state.issued.get_mut(index)) {
        record["outcome"] = json!(name);
        record["request_id"] = json!(request_id);
        record["completed_at_unix_ms"] = json!(unix_ms());
        record["latency_ms"] = json!(latency);
        record["scheduled_latency_ms"] = json!(scheduled_latency);
    }
    guard.completed = true;
}

async fn run_phase(phase: Phase, tasks: &mut JoinSet<()>) -> Result<(), String> {
    guarded_wait(phase.start).await;
    if phase.settings.mode == "closed" {
        for _ in 0..phase.settings.concurrency {
            let phase = phase.clone();
            tasks.spawn(async move {
                loop {
                    let now = Instant::now();
                    if now >= phase.end {
                        break;
                    }
                    let sequence = {
                        let mut state = lock(&phase.measurement);
                        if state.offered == phase.settings.max_operations {
                            state.operation_limit_hit = true;
                            break;
                        }
                        let sequence = state.offered;
                        state.offered += 1;
                        sequence
                    };
                    let scheduled = phase.start_unix_ms
                        + milliseconds(now.saturating_duration_since(phase.start));
                    attempt(phase.clone(), sequence, now, scheduled).await;
                    if sequence % 64 == 63 {
                        tokio::task::yield_now().await;
                    }
                }
            });
        }
    } else {
        let planned = (phase.end.duration_since(phase.start).as_secs_f64() * phase.settings.rate)
            .ceil() as u64;
        let total = planned.min(phase.settings.max_operations);
        lock(&phase.measurement).operation_limit_hit = planned > phase.settings.max_operations;
        for sequence in 0..total {
            let offset = Duration::from_secs_f64(sequence as f64 / phase.settings.rate);
            let due = phase.start + offset;
            guarded_wait(due).await;
            while let Some(result) = tasks.try_join_next() {
                observe_join(result, &phase.measurement);
            }
            let now = Instant::now();
            let admitted = {
                let mut state = lock(&phase.measurement);
                let admitted = offer_open(
                    &phase.settings,
                    phase.end,
                    due,
                    now,
                    tasks.len(),
                    &mut state,
                );
                if !admitted && !phase.warmup {
                    let mut hit = state.history_limit_hit;
                    bounded_sample(
                        &mut state.samples_issue_lag_ms,
                        milliseconds(now.saturating_duration_since(due)),
                        phase.config.sample_limit,
                        &mut hit,
                    );
                    state.history_limit_hit = hit;
                }
                admitted
            };
            if admitted {
                tasks.spawn(attempt(
                    phase.clone(),
                    sequence,
                    due,
                    phase.start_unix_ms + milliseconds(offset),
                ));
            }
            if sequence % 128 == 127 {
                tokio::task::yield_now().await;
            }
        }
    }
    let drain_deadline = phase.end
        + Duration::from_millis(phase.settings.timeout_ms.expect("validated timeout") + 2000);
    time::timeout_at(drain_deadline, async {
        while let Some(result) = tasks.join_next().await {
            observe_join(result, &phase.measurement);
        }
    })
    .await
    .map_err(|_| "run drain exceeded the bounded RPC timeout".to_owned())?;
    guarded_wait(phase.end).await;
    Ok(())
}

async fn run_load(
    client: Client,
    config: Arc<Config>,
    ledger: Arc<Mutex<Ledger>>,
    run_id: String,
    settings: Settings,
    mut cancel: watch::Receiver<bool>,
) {
    let began = Instant::now();
    let reference_unix = unix_ms();
    let start_unix = settings
        .start_at_unix_ms
        .unwrap_or(reference_unix + settings.warmup * 1000.0);
    let delta = start_unix - reference_unix;
    let start = if delta >= 0.0 {
        began + Duration::from_secs_f64(delta / 1000.0)
    } else {
        began - Duration::from_secs_f64(-delta / 1000.0)
    };
    let end = start + Duration::from_secs_f64(settings.duration);
    let settings = Arc::new(settings);
    let measurement = Arc::new(Mutex::new(Measurement::default()));
    let warmup_measurement = Arc::new(Mutex::new(Measurement::default()));
    let phase = Phase {
        client,
        ledger: Arc::clone(&ledger),
        config: Arc::clone(&config),
        settings: Arc::clone(&settings),
        run_id: Arc::from(run_id.as_str()),
        padding: Arc::from("x".repeat(settings.payload_bytes)),
        start,
        end,
        start_unix_ms: start_unix,
        warmup: false,
        measurement: Arc::clone(&measurement),
    };
    let mut tasks = JoinSet::new();
    let mut observed_start = None;
    let result = tokio::select! {
        biased;
        _ = cancel.changed() => Err("run cancelled by shutdown or stdin EOF".to_owned()),
        result = async {
            let warmup_duration = settings.warmup.min(start.saturating_duration_since(Instant::now()).as_secs_f64());
            if warmup_duration > 0.0 {
                let warmup_start = Instant::now();
                let warmup = Phase {
                    start: warmup_start,
                    end: warmup_start + Duration::from_secs_f64(warmup_duration),
                    start_unix_ms: unix_ms(),
                    warmup: true,
                    measurement: Arc::clone(&warmup_measurement),
                    ..phase.clone()
                };
                run_phase(warmup, &mut tasks).await?;
            }
            guarded_wait(start).await;
            observed_start = Some(Instant::now());
            run_phase(phase, &mut tasks).await
        } => result,
    };
    tasks.abort_all();
    let cleanup = time::timeout(Duration::from_millis(config.shutdown_timeout_ms), async {
        while let Some(result) = tasks.join_next().await {
            observe_join(result, &measurement);
        }
    })
    .await;
    let finished = Instant::now();
    let (ledger_history_limit_hit, event_gaps) = {
        let ledger = lock(&ledger);
        (ledger.history_limit_hit, ledger.event_gaps)
    };
    let state = lock(&measurement);
    let warmup = lock(&warmup_measurement);
    let report = json!({
        "operation": "run",
        "run_id": run_id,
        "client_id": config.client_id,
        "driver": "rust",
        "offered": state.offered,
        "started": state.started,
        "accepted": state.accepted,
        "accepted_definition": "routed_acceptance_ack_or_direct_terminal_implies_acceptance",
        "success": state.success,
        "completed_within_window": state.completed_within_window,
        "errors": state.errors,
        "timeouts": state.timeouts,
        "rejected": state.rejected,
        "mismatches": state.mismatches,
        "duplicates": state.duplicates,
        "routed_accepted": state.routed_accepted,
        "accepted_within_window": state.accepted_within_window,
        "scheduler_rejected": state.scheduler_rejected,
        "overdue_rejected": state.overdue_rejected,
        "concurrency_rejected": state.concurrency_rejected,
        "error_kinds": state.error_kinds,
        "error_samples": state.error_samples,
        "max_active": state.max_active,
        "active": state.active,
        "task_panics": state.task_panics,
        "duration": settings.duration,
        "concurrency": settings.concurrency,
        "mode": settings.mode,
        "rpc_kind": settings.rpc_kind,
        "rate": settings.rate,
        "payload_bytes": settings.payload_bytes,
        "timeout_ms": settings.timeout_ms,
        "max_operations": settings.max_operations,
        "concurrency_scope": if settings.edges.iter().any(|edge| edge.response_to.as_deref().is_some_and(|recipient| recipient != config.client_id)) { "sdk_admission_futures_not_end_to_end_routed_calls" } else { "terminal_sdk_calls" },
        "elapsed_seconds": finished.saturating_duration_since(began).as_secs_f64(),
        "drain_duration": finished.saturating_duration_since(end).as_secs_f64(),
        "window_unix_ms_start": start_unix,
        "window_unix_ms_end": start_unix + settings.duration * 1000.0,
        "start_skew_ms": observed_start.map(|observed| milliseconds(observed.saturating_duration_since(start))),
        "goodput_rps": state.completed_within_window as f64 / settings.duration,
        "issued": state.issued,
        "samples_latency_ms": state.samples_latency_ms,
        "samples_scheduled_latency_ms": state.samples_scheduled_latency_ms,
        "samples_service_latency_ms": state.samples_service_latency_ms,
        "acceptance_latency_ms": state.acceptance_latency_ms,
        "samples_issue_lag_ms": state.samples_issue_lag_ms,
        "offer_lag_count": state.offer_lag_count,
        "max_offer_lag_ms": state.max_offer_lag_ms,
        "latency_ms": distribution(&state.samples_latency_ms),
        "scheduled_latency_ms": distribution(&state.samples_scheduled_latency_ms),
        "service_latency_ms": distribution(&state.samples_service_latency_ms),
        "issue_lag_ms": distribution(&state.samples_issue_lag_ms),
        "latency_clock": "same_process_monotonic_instant",
        "timestamp_equality_tolerance_ms": 1.0,
        "routed_latency_clock": "system_time_unix_ms_cross_process_estimate",
        "latency_boundary": if settings.mode == "open" { "scheduled_offer_to_terminal" } else { "sdk_api_issue_to_terminal" },
        "sent_at_boundary": "first_polled_sdk_call_not_socket_transmission",
        "max_schedule_lag_ms": settings.max_schedule_lag_ms,
        "operation_limit_hit": state.operation_limit_hit,
        "operation_cap_reached": state.offered >= settings.max_operations,
        "sample_policy": "first_n_bounded",
        "direct_request_id": "not_exposed_by_typed_sdk_return",
        "history_limit_hit": state.history_limit_hit || ledger_history_limit_hit,
        "event_gaps": event_gaps,
        "cleanup_timed_out": cleanup.is_err(),
        "run_error": result.err(),
        "errors_include_timeouts_and_sdk_rejections": true,
        "third_party_result_deadline_at_unix_ms": state.last_issued_at_unix_ms.map(|sent| sent + settings.timeout_ms.expect("validated timeout") as f64),
        "warmup": {"offered":warmup.offered,"started":warmup.started,"success":warmup.success,"accepted":warmup.accepted,"errors":warmup.errors,"timeouts":warmup.timeouts,"rejected":warmup.rejected},
        "limits": config.limits(),
    });
    let _ = emit(&report);
}

fn offer_open(
    settings: &Settings,
    end: Instant,
    due: Instant,
    now: Instant,
    active: usize,
    state: &mut Measurement,
) -> bool {
    let lag = milliseconds(now.saturating_duration_since(due));
    let overdue = now >= end || lag > settings.max_schedule_lag_ms;
    let saturated = active >= settings.concurrency;
    state.offered += 1;
    state.offer_lag_count += 1;
    state.max_offer_lag_ms = state.max_offer_lag_ms.max(lag);
    if overdue || saturated {
        state.rejected += 1;
        state.scheduler_rejected += 1;
        state.overdue_rejected += u64::from(overdue);
        state.concurrency_rejected += u64::from(!overdue && saturated);
        return false;
    }
    true
}

fn observe_join(result: Result<(), tokio::task::JoinError>, measurement: &Mutex<Measurement>) {
    if let Err(error) = result {
        if error.is_panic() {
            let mut state = lock(measurement);
            state.task_panics += 1;
            error_sample(
                &mut state.error_samples,
                json!({"kind":"task_panic","error":short(&error.to_string())}),
            );
        }
    }
}

fn classify_error(error: &Error) -> (&'static str, bool, bool) {
    match error {
        Error::Timeout { .. } => ("timeout", true, false),
        Error::Overloaded { .. } => ("overload", false, true),
        Error::FrameTooLarge { .. } => ("frame_limit", false, true),
        Error::Handler(_) => ("application", false, false),
        Error::Server { code, .. } => (
            "server",
            code == "timeout" || code == "rpc_timeout",
            code == "overloaded" || code.ends_with("_limit") || code.ends_with("_full"),
        ),
        Error::Disconnected | Error::Connection { .. } | Error::Io(_) => {
            ("transport", false, false)
        }
        Error::Serialization(_) => ("serialization", false, false),
        _ => ("protocol", false, false),
    }
}

async fn guarded_wait(deadline: Instant) {
    // Windows timer granularity must not issue an offer before its due time.
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        time::sleep(
            remaining
                .min(Duration::from_millis(100))
                .max(Duration::from_millis(1)),
        )
        .await;
    }
}

fn identifier(name: &str, value: &str, limit: usize) -> Result<(), String> {
    if value.is_empty() || value.len() > limit {
        Err(format!("{name} must be nonempty and at most {limit} bytes"))
    } else {
        Ok(())
    }
}

fn text(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

fn number(value: Option<&Value>) -> Option<f64> {
    value
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite())
}

fn scheduled_ms(data: &Map<String, Value>) -> Option<f64> {
    number(data.get("scheduled_at_unix_ms")).or_else(|| number(data.get("scheduled_at")))
}

fn valid_token(data: &Map<String, Value>) -> bool {
    let (Some(run_id), Some(origin), Some(token)) = (
        text(data.get("run_id")),
        text(data.get("origin")),
        text(data.get("token")),
    ) else {
        return false;
    };
    token
        .strip_prefix(&format!("{run_id}:{origin}:"))
        .is_some_and(|suffix| {
            !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
        })
}

fn echo_field_equal(key: &str, expected: &Value, returned: Option<&Value>) -> bool {
    if [
        "sent_at_unix_ms",
        "scheduled_at_unix_ms",
        "scheduled_at",
        "window_start_at_unix_ms",
        "window_end_at_unix_ms",
    ]
    .contains(&key)
    {
        // Cross-language decimal round trips can change the low bit of epoch milliseconds.
        return number(Some(expected))
            .zip(number(returned))
            .is_some_and(|(a, b)| (a - b).abs() <= 1.0);
    }
    returned == Some(expected)
}

fn validate_echo(packet: &Value, returned: &Value, target: &str) -> bool {
    packet
        .as_object()
        .zip(returned.as_object())
        .is_some_and(|(expected, actual)| {
            actual.len() == expected.len() + 1
                && expected
                    .iter()
                    .all(|(key, value)| echo_field_equal(key, value, actual.get(key)))
                && text(actual.get("handled_by")) == Some(target)
        })
}

fn echo_differences(packet: &Value, returned: &Value) -> Vec<Value> {
    let Some(expected) = packet.as_object() else {
        return Vec::new();
    };
    expected.iter().filter(|(key, value)| !echo_field_equal(key, value, returned.get(key)))
        .take(MAX_ERRORS).map(|(key, value)| json!({"field":key,"expected":if key == "padding" { json!(value.as_str().map(str::len)) } else { value.clone() },"returned":if key == "padding" { json!(returned.get(key).and_then(Value::as_str).map(str::len)) } else { returned.get(key).cloned().unwrap_or(Value::Null) }})).collect()
}

fn unix_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
        * 1000.0
}

fn milliseconds(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn short(message: &str) -> String {
    message.chars().take(512).collect()
}

fn error_sample(samples: &mut Vec<Value>, value: Value) {
    if samples.len() < MAX_ERRORS {
        samples.push(value);
    }
}

fn bounded_sample(samples: &mut Vec<f64>, value: f64, limit: usize, hit: &mut bool) {
    if samples.len() < limit {
        samples.push(value);
    } else {
        *hit = true;
    }
}

fn distribution(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({"count":0,"p50":null,"p95":null,"p99":null,"p99.9":null,"max":null});
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let percentile = |percent: f64| {
        let position = (sorted.len() - 1) as f64 * percent / 100.0;
        let lower = position.floor() as usize;
        let upper = position.ceil() as usize;
        sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower as f64)
    };
    json!({
        "count":sorted.len(),
        "p50":percentile(50.0),
        "p95":percentile(95.0),
        "p99":percentile(99.0),
        "p99.9":percentile(99.9),
        "max":sorted.last(),
    })
}

fn emit(value: &Value) -> io::Result<()> {
    let mut output = io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    output.flush()
}

type Running = (JoinHandle<()>, watch::Sender<bool>);

fn stop_run(runtime: &Runtime, running: &mut Option<Running>, config: &Config) {
    if let Some((mut task, cancel)) = running.take() {
        let _ = cancel.send(true);
        runtime.block_on(async {
            if time::timeout(
                Duration::from_millis(config.shutdown_timeout_ms + 3000),
                &mut task,
            )
            .await
            .is_err()
            {
                task.abort();
                let _ = time::timeout(Duration::from_millis(config.shutdown_timeout_ms), &mut task)
                    .await;
            }
        });
    }
}

fn run(mut config: Config) -> Result<(), String> {
    if let Some(workers) = config.handler_workers {
        config.min_workers = workers;
        config.max_workers = workers;
    }
    config.validate()?;
    let config = Arc::new(config);
    let runtime = Builder::new_multi_thread()
        .worker_threads(config.runtime_threads)
        .enable_all()
        .thread_name("benchmark-peer")
        .build()
        .map_err(|error| error.to_string())?;
    let client = runtime
        .block_on(
            Client::builder(format!("latzero://{}", config.client_id), &config.pool)
                .port(config.port)
                .timeout(Duration::from_millis(config.timeout_ms))
                .write_timeout(Duration::from_millis(config.timeout_ms))
                .shutdown_timeout(Duration::from_millis(config.shutdown_timeout_ms))
                .event_capacity(config.event_capacity)
                .max_pending_requests(config.max_pending_requests)
                .writer_capacity(config.writer_capacity)
                .max_handler_tasks(config.max_handler_tasks)
                .max_frame_bytes(config.max_frame_bytes)
                .max_queued_bytes(config.max_queued_bytes)
                .max_handler_bytes(config.max_handler_bytes)
                .connect(),
        )
        .map_err(|error| error.to_string())?;
    let ledger = Arc::new(Mutex::new(Ledger::new(Arc::clone(&config))));
    let mut collector = runtime.spawn(collect_events(client.events(), Arc::clone(&ledger)));
    let registration = runtime.block_on(async {
        let event_ledger = Arc::clone(&ledger);
        client
            .try_on_event("echo", move |data| echo(Arc::clone(&event_ledger), data))
            .await?;
        let process_ledger = Arc::clone(&ledger);
        client
            .register_process(
                "echo",
                ProcessOptions {
                    min_workers: config.min_workers,
                    max_workers: if config.role == "load" {
                        config.min_workers
                    } else {
                        config.max_workers
                    },
                    scale: false,
                    max_replicas: 1,
                    ..ProcessOptions::default()
                },
                move |data| echo(Arc::clone(&process_ledger), data),
            )
            .await
    });
    let mut running: Option<Running> = None;
    let outcome = (|| -> Result<(), String> {
        let registration = registration.map_err(|error| error.to_string())?;
        emit(&json!({"ready":true,"pid":std::process::id(),"client_id":config.client_id,"process_id":registration.process_id,"limits":config.limits()})).map_err(|error| error.to_string())?;
        // Keep stdin on the main thread: Tokio stdin leaves an uncancellable blocking read.
        let stdin = io::stdin();
        let mut reader = stdin.lock();
        loop {
            let mut line = Vec::new();
            let length = (&mut reader)
                .take(MAX_COMMAND_BYTES + 1)
                .read_until(b'\n', &mut line)
                .map_err(|error| error.to_string())?;
            if length == 0 {
                break;
            }
            if length as u64 > MAX_COMMAND_BYTES {
                return Err("stdin command exceeds the bounded 1MiB line limit".to_owned());
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            if running.as_ref().is_some_and(|(task, _)| task.is_finished()) {
                if let Some((task, _)) = running.take() {
                    runtime.block_on(task).map_err(|error| error.to_string())?;
                }
            }
            let command: Command = match serde_json::from_slice(&line) {
                Ok(command) => command,
                Err(error) => {
                    emit(&json!({"operation":"error","error":short(&error.to_string())}))
                        .map_err(|error| error.to_string())?;
                    continue;
                }
            };
            match command.operation.as_str() {
                "run" => {
                    let validated = (|| {
                        if running.is_some() {
                            return Err("only one run may be in progress".to_owned());
                        }
                        let run_id = command.run_id.clone().ok_or("run_id is required")?;
                        identifier("run_id", &run_id, 128)?;
                        let mut settings = command.settings.ok_or("settings are required")?;
                        settings.validate(&config)?;
                        Ok((run_id, settings))
                    })();
                    match validated {
                        Ok((run_id, settings)) => {
                            let (cancel, receiver) = watch::channel(false);
                            running = Some((
                                runtime.spawn(run_load(
                                    client.clone(),
                                    Arc::clone(&config),
                                    Arc::clone(&ledger),
                                    run_id,
                                    settings,
                                    receiver,
                                )),
                                cancel,
                            ));
                        }
                        Err(error) => emit(
                            &json!({"operation":"run","run_id":command.run_id,"run_error":error}),
                        )
                        .map_err(|error| error.to_string())?,
                    }
                }
                "stats" => {
                    let snapshot =
                        lock(&ledger).snapshot(command.run_id.as_deref(), running.is_some());
                    emit(&snapshot).map_err(|error| error.to_string())?;
                }
                "reset" => {
                    let mut state = lock(&ledger);
                    if running.is_some() || state.runs.values().any(|run| run.active > 0) {
                        emit(&json!({"operation":"reset","error":"cannot reset an active run or worker"})).map_err(|error| error.to_string())?;
                    } else {
                        *state = Ledger::new(Arc::clone(&config));
                        emit(&json!({"operation":"reset","reset":true}))
                            .map_err(|error| error.to_string())?;
                    }
                }
                "shutdown" => break,
                _ => emit(&json!({"operation":command.operation,"error":"unknown operation"}))
                    .map_err(|error| error.to_string())?,
            }
        }
        Ok(())
    })();
    stop_run(&runtime, &mut running, &config);
    let disconnected = runtime.block_on(async {
        time::timeout(
            Duration::from_millis(config.shutdown_timeout_ms * 4 + 500),
            client.disconnect(),
        )
        .await
    });
    collector.abort();
    runtime.block_on(async {
        let _ = time::timeout(
            Duration::from_millis(config.shutdown_timeout_ms),
            &mut collector,
        )
        .await;
    });
    drop(client);
    runtime.shutdown_timeout(Duration::from_millis(config.shutdown_timeout_ms));
    outcome?;
    match disconnected {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error.to_string()),
        Err(_) => Err("disconnect exceeded the finite cleanup deadline".to_owned()),
    }
}

fn main() -> ExitCode {
    let result = (|| {
        let argument = std::env::args()
            .nth(1)
            .ok_or("expected one JSON configuration argument")?;
        if argument.len() > 65_536 {
            return Err("configuration exceeds 65536 bytes".to_owned());
        }
        let config = serde_json::from_str(&argument).map_err(|error| error.to_string())?;
        run(config)
    })();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = emit(&json!({"operation":"error","error":short(&error)}));
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(id: &str) -> Arc<Config> {
        Arc::new(Config {
            client_id: id.to_owned(),
            pool: "bench".to_owned(),
            ..Config::default()
        })
    }

    fn packet(sequence: u64, target: &str, recipient: &str) -> Value {
        let now = unix_ms();
        json!({
            "token":format!("test:origin:{sequence}"),
            "padding":"xxxx",
            "payload_bytes":4,
            "origin":"origin",
            "expected_target":target,
            "expected_recipient":recipient,
            "run_id":"test",
            "sent_at_unix_ms":now - 10.0,
            "scheduled_at_unix_ms":now - 20.0,
            "window_start_at_unix_ms":now - 1000.0,
            "window_end_at_unix_ms":now + 1000.0,
            "warmup":false,
        })
    }

    fn result(value: Value, id: &str) -> AppResult {
        AppResult {
            request_id: Some(id.to_owned()),
            parent_request_id: Some("opaque-parent".to_owned()),
            event: Some("target:echo".to_owned()),
            source_client_id: Some("origin".to_owned()),
            target_client_id: Some("target".to_owned()),
            response_to: Some("recipient".to_owned()),
            value,
            error: None,
        }
    }

    #[test]
    fn percentiles_match_the_shared_linear_interpolation() {
        assert_eq!(distribution(&[])["p50"], Value::Null);
        let result = distribution(&[4.0, 1.0, 3.0, 2.0]);
        assert_eq!(result["p50"], json!(2.5));
        assert_eq!(result["p95"], json!(3.85));
        assert_eq!(result["max"], json!(4.0));
    }

    #[test]
    fn settings_reject_fractional_dimensions_and_unbounded_values() {
        assert!(serde_json::from_value::<Settings>(json!({"concurrency":1.5})).is_err());
        assert!(serde_json::from_value::<Settings>(json!({"max_operations":1.5})).is_err());
        let config = Config {
            client_id: "rust".to_owned(),
            pool: "bench".to_owned(),
            ..Config::default()
        };
        let mut settings = Settings {
            target: Some("worker".to_owned()),
            duration: 121.0,
            ..Settings::default()
        };
        assert!(settings.validate(&config).is_err());
    }

    #[test]
    fn clock_tolerance_never_masks_changed_payload_or_missing_fields() {
        let original = packet(0, "target", "recipient");
        let mut returned = original.clone();
        returned["handled_by"] = json!("target");
        returned["sent_at_unix_ms"] =
            json!(original["sent_at_unix_ms"].as_f64().unwrap() + 0.000244140625);
        assert!(validate_echo(&original, &returned, "target"));
        returned["sent_at_unix_ms"] = json!(original["sent_at_unix_ms"].as_f64().unwrap() + 1.1);
        assert!(!validate_echo(&original, &returned, "target"));
        returned["sent_at_unix_ms"] = original["sent_at_unix_ms"].clone();
        returned["padding"] = json!("xxxz");
        assert!(!validate_echo(&original, &returned, "target"));
        assert_eq!(
            echo_differences(&original, &returned)[0]["field"],
            json!("padding")
        );
        returned = original.clone();
        returned["handled_by"] = json!("target");
        returned["extra"] = Value::Null;
        assert!(!validate_echo(&original, &returned, "target"));
    }

    #[test]
    fn routed_result_before_ack_is_self_contained_and_keeps_both_ids() {
        let mut ledger = Ledger::new(config("recipient"));
        let mut value = packet(0, "target", "recipient");
        value["handled_by"] = json!("target");
        ledger.observe_result(
            "opaque-origin-id".to_owned(),
            result(value.clone(), "opaque-origin-id"),
            "app_result",
        );
        let state = &ledger.runs["test"];
        assert_eq!(state.incoming_results, 1);
        assert_eq!(state.delivered_success, 1);
        assert_eq!(state.completed_within_window, 1);
        assert_eq!(
            state.correlation_errors + state.malformed + state.misroutes,
            0
        );
        assert_eq!(
            state.result_records[0]["request_id"],
            json!("opaque-origin-id")
        );
        assert_eq!(
            state.result_records[0]["payload_request_id"],
            json!("opaque-origin-id")
        );
        assert_eq!(
            state.result_records[0]["parent_request_id"],
            json!("opaque-parent")
        );
        ledger.observe_result(
            "opaque-origin-id".to_owned(),
            result(value, "opaque-origin-id"),
            "app_result",
        );
        assert_eq!(ledger.runs["test"].duplicates, 1);
        assert_eq!(ledger.runs["test"].delivered_success, 1);
    }

    #[test]
    fn typed_terminal_value_is_recorded_without_inventing_an_envelope_id() {
        let mut ledger = Ledger::new(config("recipient"));
        let mut value = packet(0, "target", "recipient");
        value["handled_by"] = json!("target");
        let mut terminal = result(value, "discarded-id");
        terminal.request_id = None;
        terminal.parent_request_id = None;
        terminal.source_client_id = None;
        terminal.target_client_id = None;
        terminal.response_to = None;
        ledger.observe_result(String::new(), terminal, "typed_terminal_value");
        let state = &ledger.runs["test"];
        assert_eq!(state.delivered_success, 1);
        assert_eq!(state.result_records[0]["request_id"], Value::Null);
        assert_eq!(state.result_records[0]["target"], json!("target"));
        assert_eq!(state.result_records[0]["envelope_observed"], json!(false));
    }

    #[test]
    fn wrong_origin_target_recipient_and_payload_id_are_not_success() {
        for field in ["source", "target", "recipient", "id", "token", "error"] {
            let mut ledger = Ledger::new(config("recipient"));
            let mut value = packet(0, "target", "recipient");
            value["handled_by"] = json!("target");
            let mut response = result(value, "origin-id");
            match field {
                "source" => response.source_client_id = Some("wrong".to_owned()),
                "target" => response.target_client_id = Some("wrong".to_owned()),
                "recipient" => response.response_to = Some("wrong".to_owned()),
                "id" => response.request_id = Some("wrong".to_owned()),
                "token" => response.value["token"] = json!("wrong:origin:0"),
                "error" => response.error = Some(json!({"code":"application"})),
                _ => unreachable!(),
            }
            ledger.observe_result("origin-id".to_owned(), response, "app_result");
            assert_eq!(ledger.runs["test"].delivered_success, 0, "{field}");
            assert_eq!(
                ledger.runs["test"].result_records[0]["valid"],
                json!(false),
                "{field}"
            );
        }
    }

    #[test]
    fn bounded_ledgers_keep_counts_and_explicitly_report_truncation() {
        let mut options = (*config("recipient")).clone();
        options.history_limit = 3;
        options.record_limit = 2;
        options.sample_limit = 1;
        let mut ledger = Ledger::new(Arc::new(options));
        for sequence in 0..20 {
            let mut value = packet(sequence, "target", "recipient");
            value["handled_by"] = json!("target");
            ledger.observe_result(
                format!("id{sequence}"),
                result(value, &format!("id{sequence}")),
                "app_result",
            );
        }
        assert_eq!(ledger.runs["test"].incoming_results, 20);
        assert_eq!(ledger.runs["test"].result_records.len(), 2);
        assert_eq!(ledger.runs["test"].samples_latency_ms.len(), 1);
        assert!(ledger.history_limit_hit);
        assert!(ledger.runs["test"].history_limit_hit);
        assert_eq!(ledger.results_seen.len(), 3);
        assert_eq!(ledger.runs["test"].error_samples.len(), 0);
    }

    #[test]
    fn open_schedule_accounts_for_late_and_saturated_offers_without_catchup() {
        let settings = Settings {
            concurrency: 2,
            mode: "open".to_owned(),
            ..Settings::default()
        };
        let start = Instant::now();
        let end = start + Duration::from_secs(1);
        let mut measurement = Measurement::default();
        for sequence in 0..20 {
            let due = start + Duration::from_millis(sequence * 10);
            assert!(!offer_open(
                &settings,
                end,
                due,
                start + Duration::from_secs(2),
                0,
                &mut measurement
            ));
        }
        assert_eq!(measurement.offered, 20);
        assert_eq!(measurement.rejected, 20);
        assert_eq!(measurement.overdue_rejected, 20);
        assert_eq!(measurement.offer_lag_count, 20);
        assert!(!offer_open(
            &settings,
            end,
            start,
            start + Duration::from_millis(1),
            2,
            &mut measurement
        ));
        assert_eq!(measurement.concurrency_rejected, 1);
        assert!(offer_open(
            &settings,
            end,
            start,
            start + Duration::from_millis(1),
            1,
            &mut measurement
        ));
        assert_eq!(measurement.offered, 22);
        assert_eq!(measurement.started, 0);
    }

    #[tokio::test]
    async fn event_broadcast_gaps_are_counted_instead_of_silently_dropped() {
        let ledger = Arc::new(Mutex::new(Ledger::new(config("recipient"))));
        let (sender, receiver) = broadcast::channel(2);
        for _ in 0..6 {
            sender.send(ClientEvent::Disconnected).unwrap();
        }
        drop(sender);
        collect_events(receiver, Arc::clone(&ledger)).await;
        let state = lock(&ledger);
        assert_eq!(state.event_gaps, 4);
        assert!(state.event_receiver_closed);
        assert!(state.disconnected);
    }

    #[tokio::test]
    async fn worker_echo_preserves_arbitrary_json_and_cancellation_releases_active() {
        let ledger = Arc::new(Mutex::new(Ledger::new(config("target"))));
        let mut original = packet(0, "target", "recipient");
        original["extra"] = json!({"null":null,"bool":false,"zero":0,"array":["",{},[]]});
        let returned = echo(Arc::clone(&ledger), original.as_object().unwrap().clone())
            .await
            .unwrap();
        assert!(validate_echo(&original, &returned, "target"));
        assert_eq!(lock(&ledger).runs["test"].active, 0);
        assert_eq!(lock(&ledger).runs["test"].cancelled_handlers, 0);
        let mut options = (*config("target")).clone();
        options.worker_delay_ms = 30_000;
        let ledger = Arc::new(Mutex::new(Ledger::new(Arc::new(options))));
        let worker_ledger = Arc::clone(&ledger);
        let task = tokio::spawn(echo(worker_ledger, original.as_object().unwrap().clone()));
        tokio::task::yield_now().await;
        assert_eq!(lock(&ledger).runs["test"].active, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(lock(&ledger).runs["test"].active, 0);
        assert_eq!(lock(&ledger).runs["test"].cancelled_handlers, 1);
    }

    #[test]
    fn warmup_does_not_contaminate_the_measured_ledgers() {
        let mut ledger = Ledger::new(config("target"));
        let mut value = packet(0, "target", "recipient");
        value["warmup"] = json!(true);
        assert!(ledger.observe_effect(value.as_object().unwrap()).is_none());
        ledger.observe_result("id".to_owned(), result(value, "id"), "app_result");
        assert!(ledger.runs.is_empty());
        assert_eq!(ledger.records, 0);
    }
}
