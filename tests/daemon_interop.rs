use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    io::{BufRead, BufReader as StdBufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
    thread::JoinHandle,
    time::Duration,
};

use latzero::{AppResult, CallOutcome, Client, ClientEvent, Error, Message, ProcessOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{broadcast, mpsc},
    time,
};
use uuid::Uuid;

const POOL: &str = "rust-daemon-interop";
const DEADLINE: Duration = Duration::from_secs(10);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_pods_router_affinity_cross_owner_switch_and_same_owner_rejoin() {
    let Some(mut daemon) = Daemon::start_pods().await else {
        return;
    };
    assert_eq!(daemon.ready["pod_count"], 4);
    assert_eq!(daemon.ready["children"].as_array().unwrap().len(), 4);
    let pids: HashSet<_> = daemon.ready["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|child| child["pid"].as_u64().unwrap())
        .collect();
    assert_eq!(pids.len(), 4);
    assert!(!pids.contains(&daemon.ready["pid"].as_u64().unwrap()));
    assert_ne!(daemon.ready["initial_owner"], daemon.ready["other_owner"]);
    let other_pool = daemon.ready["other_pool"].as_str().unwrap().to_owned();
    let source = daemon.client("rust-pod-source").await;
    let clone = source.clone();
    let target = daemon.client("rust-pod-target").await;
    target
        .on_event("echo", |data| async move {
            Ok::<_, String>(data["value"].clone())
        })
        .await;
    source
        .register_process("retained", ProcessOptions::default(), |_| async {
            Ok::<_, String>("same-owner")
        })
        .await
        .unwrap();
    let value = json!({"nested": [1, null, false, {"pod": "owner"}]});
    source.set("isolation", &value).await.unwrap();
    assert_eq!(
        source
            .call_app::<_, Value>("rust-pod-target", "echo", &json!({"value": value}))
            .await
            .unwrap(),
        value
    );
    let raw = daemon
        .command(
            "start_raw",
            json!({"pool": POOL, "client_id": "raw-pod-peer"}),
        )
        .await;
    assert_eq!(raw["client_id"], "raw-pod-peer");
    assert_eq!(
        source
            .call_process::<_, Value>("raw-pod-peer:echo", &json!({"value": value}))
            .await
            .unwrap(),
        value
    );
    let invocation = daemon.command("next_raw_call", json!({})).await;
    assert_eq!(invocation["invocation"]["pool"], POOL);
    let mut events = clone.events();
    source.switch_pool(&other_pool, None).await.unwrap();
    assert!(source.is_connected() && clone.is_connected());
    assert_eq!(clone.pool_name().await, other_pool);
    assert_eq!(clone.get::<Value>("isolation").await.unwrap(), None);
    clone.subscribe_buffer("changed-pool").await.unwrap();
    clone
        .set("changed-pool", &json!({"clone": true}))
        .await
        .unwrap();
    let update = bounded(async {
        for _ in 0..64 {
            if let ClientEvent::Buffer(update) = events.recv().await.unwrap() {
                if update.key == "changed-pool" {
                    return update;
                }
            }
        }
        panic!("new pool buffer event did not reach the original event subscription");
    })
    .await;
    assert_eq!(update.entry.value, json!({"clone": true}));
    clone.switch_pool(POOL, None).await.unwrap();
    assert_eq!(source.pool_name().await, POOL);
    assert_eq!(
        clone.get::<Value>("isolation").await.unwrap(),
        Some(value.clone())
    );
    assert!(
        matches!(target.call_process::<_, Value>("rust-pod-source:retained", &json!({})).await,
        Err(Error::Server { code, .. }) if code == "process_not_found")
    );
    source
        .register_process("retained", ProcessOptions::default(), |_| async {
            Ok::<_, String>("same-owner")
        })
        .await
        .unwrap();
    source.switch_pool(POOL, None).await.unwrap();
    assert_eq!(
        target
            .call_process::<_, String>("rust-pod-source:retained", &json!({}))
            .await
            .unwrap(),
        "same-owner"
    );
    source.disconnect().await.unwrap();
    assert!(!clone.is_connected());
    target.disconnect().await.unwrap();
    drop(clone);
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_pods_python_bidirectional_rpc_and_auth_denial_after_redirect() {
    let Some(mut daemon) = Daemon::start_pods().await else {
        return;
    };
    let python = daemon.command("start_python", json!({"pool": POOL})).await;
    if let Some(reason) = python.get("skip").and_then(Value::as_str) {
        eprintln!("SKIP real-pod Python SDK: {reason}");
        daemon.shutdown().await;
        return;
    }
    assert_eq!(python["client_id"], "python-peer");
    let rust = daemon.client("rust-pod-worker").await;
    rust.on_event("echo", |data| async move {
        Ok::<_, String>(data["value"].clone())
    })
    .await;
    rust.register_process("echo", ProcessOptions::default(), |data| async move {
        Ok::<_, String>(data["value"].clone())
    })
    .await
    .unwrap();
    let value = json!({"list": [null, true, 1, 2.5], "string": "owner-affine"});
    for kind in ["app", "process"] {
        let reply = if kind == "app" {
            rust.call_app::<_, Value>("python-peer", "echo", &json!({"value": value}))
                .await
                .unwrap()
        } else {
            rust.call_process::<_, Value>("python-peer:echo", &json!({"value": value}))
                .await
                .unwrap()
        };
        assert_eq!(
            reply,
            json!({"owner": "python-peer", "kind": kind, "value": value})
        );
        let returned = daemon
            .command(
                "python_call",
                json!({"kind": kind, "target": "rust-pod-worker", "value": value}),
            )
            .await;
        assert_eq!(returned, value);
    }
    let other_pool = daemon.ready["other_pool"].as_str().unwrap();
    let owner = Client::builder("latzero://secure-owner", other_pool)
        .port(daemon.port)
        .auth_token("owner-secret")
        .timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    let denied = Client::builder("latzero://denied", other_pool)
        .port(daemon.port)
        .auth_token("wrong")
        .timeout(Duration::from_secs(5))
        .connect()
        .await;
    assert!(matches!(denied, Err(Error::Authentication(_))));
    let clone = rust.clone();
    assert!(matches!(
        rust.switch_pool(other_pool, Some("wrong")).await,
        Err(Error::Authentication(_))
    ));
    assert!(!rust.is_connected() && !clone.is_connected());
    assert_eq!(clone.pool_name().await, POOL);
    owner.disconnect().await.unwrap();
    daemon.shutdown().await;
}

async fn bounded<F: Future>(future: F) -> F::Output {
    time::timeout(DEADLINE, future)
        .await
        .expect("real-daemon operation exceeded its deadline")
}

struct FixtureProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    output: mpsc::Receiver<std::io::Result<String>>,
    reader: Option<JoinHandle<()>>,
    sequence: u64,
    response_timeout: Duration,
}

impl FixtureProcess {
    fn spawn(mut command: Command) -> std::io::Result<Self> {
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn()?;
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, output) = mpsc::channel(32);
        let mut process = Self {
            child,
            stdin: Some(stdin),
            output,
            reader: None,
            sequence: 0,
            response_timeout: DEADLINE,
        };
        process.reader = Some(std::thread::Builder::new().spawn(move || {
            for line in StdBufReader::new(stdout).lines() {
                if sender.blocking_send(line).is_err() {
                    break;
                }
            }
        })?);
        Ok(process)
    }

    async fn receive(&mut self) -> Value {
        let line = time::timeout(self.response_timeout, self.output.recv())
            .await
            .expect("fixture response exceeded its finite deadline")
            .expect("fixture child exited before its JSON response")
            .expect("fixture child stdout failed");
        let message: Value = serde_json::from_str(&line).expect("fixture output was not JSON");
        assert_eq!(message["ok"], true, "fixture child failed: {message}");
        message
    }

    async fn command(&mut self, operation: &str, fields: Value) -> Value {
        self.sequence += 1;
        let mut command = fields.as_object().unwrap().clone();
        command.insert("id".to_owned(), json!(self.sequence));
        command.insert("operation".to_owned(), json!(operation));
        let mut encoded = serde_json::to_vec(&command).unwrap();
        encoded.push(b'\n');
        // Serial commands fit in one pipe buffer; no unacknowledged command backlog.
        assert!(encoded.len() < 4096);
        let stdin = self.stdin.as_mut().unwrap();
        stdin.write_all(&encoded).unwrap();
        stdin.flush().unwrap();
        let response = self.receive().await;
        assert_eq!(response["id"], self.sequence);
        response["result"].clone()
    }

    async fn wait_success(&mut self) {
        self.stdin.take();
        let status = bounded(async {
            let mut exit_check = time::interval(Duration::from_millis(10));
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                exit_check.tick().await;
            }
        })
        .await;
        assert!(status.success(), "fixture child exited with {status}");
    }

    fn terminate(&mut self) {
        self.stdin.take();
        self.output.close();
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        if let Some(reader) = self.reader.take() {
            let _ = reader.join();
        }
    }
}

impl Drop for FixtureProcess {
    fn drop(&mut self) {
        self.terminate();
    }
}

struct Daemon {
    process: FixtureProcess,
    temp_root: PathBuf,
    port: u16,
    ready: Value,
}

impl Daemon {
    async fn start(controlled_clock: bool) -> Option<Self> {
        Self::start_mode(controlled_clock, 1).await
    }

    async fn start_pods() -> Option<Self> {
        if std::env::var("LATZERO_TEST_PODS").as_deref() != Ok("1") {
            eprintln!("SKIP real-pod smoke: set LATZERO_TEST_PODS=1 after the single-daemon suite");
            return None;
        }
        Self::start_mode(false, 4).await
    }

    async fn start_mode(controlled_clock: bool, pods: usize) -> Option<Self> {
        let Some(python) = std::env::var_os("LATZERO_TEST_PYTHON") else {
            eprintln!(
                "SKIP real-daemon smoke: set LATZERO_TEST_PYTHON to an existing server-deps interpreter"
            );
            return None;
        };
        if !Path::new(&python).is_file() {
            eprintln!("SKIP real-daemon smoke: LATZERO_TEST_PYTHON interpreter is unavailable");
            return None;
        }
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let server_root = manifest.parent().unwrap().join("latzero-server");
        if !server_root.join("latzero_server/server.py").is_file() {
            eprintln!("SKIP real-daemon smoke: sibling latzero-server checkout is unavailable");
            return None;
        }
        let temp_parent = std::env::temp_dir().join("kilo");
        if !temp_parent.is_dir() {
            eprintln!("SKIP real-daemon smoke: isolated kilo temporary parent is unavailable");
            return None;
        }
        let temp_root = temp_parent.join(format!("latzero-rust-daemon-{}", Uuid::new_v4()));
        std::fs::create_dir(&temp_root).unwrap();
        let mut command = Command::new(python);
        command
            .arg("-B")
            .arg("-u")
            .arg(manifest.join("tests/daemon_fixture.py"))
            .arg("--server-root")
            .arg(&server_root)
            .arg("--temp-root")
            .arg(&temp_root)
            .current_dir(&server_root)
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .env("PYTHONIOENCODING", "utf-8");
        if controlled_clock {
            command.arg("--controlled-clock");
        }
        if pods > 1 {
            command.arg("--pods").arg(pods.to_string());
        }
        let mut process = match FixtureProcess::spawn(command) {
            Ok(process) => process,
            Err(error) => {
                std::fs::remove_dir_all(&temp_root).unwrap();
                eprintln!("SKIP real-daemon smoke: interpreter could not start: {error}");
                return None;
            }
        };
        if pods > 1 {
            process.response_timeout = Duration::from_secs(32);
        }
        let mut daemon = Self {
            process,
            temp_root,
            port: 0,
            ready: Value::Null,
        };
        let ready = daemon.process.receive().await;
        if let Some(reason) = ready.get("skip").and_then(Value::as_str) {
            eprintln!("SKIP real-daemon smoke: {reason}");
            return None;
        }
        assert_eq!(ready["ready"], true);
        daemon.port = u16::try_from(ready["port"].as_u64().unwrap()).unwrap();
        assert_ne!(daemon.port, 0);
        assert!(Path::new(ready["data_dir"].as_str().unwrap()).starts_with(&daemon.temp_root));
        daemon.ready = ready;
        Some(daemon)
    }

    async fn command(&mut self, operation: &str, fields: Value) -> Value {
        self.process.command(operation, fields).await
    }

    async fn client(&self, client_id: &str) -> Client {
        Client::builder(format!("latzero://{client_id}"), POOL)
            .port(self.port)
            .timeout(Duration::from_secs(5))
            .connect()
            .await
            .unwrap()
    }

    async fn shutdown(mut self) {
        let result = self.command("shutdown", json!({})).await;
        assert_eq!(result, json!({"stopped": true, "data_removed": true}));
        self.process.wait_success().await;
        let temporary = self.temp_root.clone();
        drop(self);
        assert!(
            !temporary.exists(),
            "daemon temporary directory was not removed"
        );
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.process.terminate();
        if let Err(error) = std::fs::remove_dir_all(&self.temp_root) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!("fixture temporary cleanup failed: {error}");
            }
        }
    }
}

struct NodePeer(FixtureProcess);

impl NodePeer {
    async fn start(port: u16) -> Option<Self> {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let sdk = manifest.parent().unwrap().join("node-client/index.cjs");
        if !sdk.is_file() {
            eprintln!("SKIP Node SDK smoke: sibling node-client checkout is unavailable");
            return None;
        }
        let node = std::env::var_os("LATZERO_TEST_NODE").unwrap_or_else(|| "node".into());
        let mut command = Command::new(node);
        command
            .arg("--unhandled-rejections=strict")
            .arg(manifest.join("tests/daemon_node.cjs"))
            .arg(json!({"sdk": sdk, "port": port, "pool": POOL}).to_string())
            .current_dir(manifest);
        let mut peer = match FixtureProcess::spawn(command) {
            Ok(process) => Self(process),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!(
                    "SKIP Node SDK smoke: LATZERO_TEST_NODE/default node executable is unavailable"
                );
                return None;
            }
            Err(error) => panic!("Node fixture could not start: {error}"),
        };
        let ready = peer.0.receive().await;
        assert_eq!(ready["ready"], true);
        assert_eq!(ready["client_id"], "node-peer");
        assert_eq!(ready["process_id"], "node-peer:echo");
        Some(peer)
    }

    async fn command(&mut self, operation: &str, fields: Value) -> Value {
        self.0.command(operation, fields).await
    }

    async fn shutdown(mut self) {
        assert_eq!(
            self.command("shutdown", json!({})).await,
            json!({"stopped": true})
        );
        self.0.wait_success().await;
    }
}

struct RawClient {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
    pending: VecDeque<Message>,
}

impl RawClient {
    async fn connect(port: u16, client_id: &str) -> Self {
        let stream = bounded(TcpStream::connect(("127.0.0.1", port)))
            .await
            .unwrap();
        let (reader, writer) = stream.into_split();
        let mut client = Self {
            reader: BufReader::new(reader),
            writer,
            pending: VecDeque::new(),
        };
        client.request("hello", json!({}), "raw-hello").await;
        client
            .request(
                "join_pool",
                json!({"client_id": client_id, "pool": POOL}),
                "raw-join",
            )
            .await;
        client
    }

    async fn request(&mut self, kind: &str, payload: Value, request_id: &str) -> Message {
        let mut frame = serde_json::to_vec(&json!({"type": kind, "request_id": request_id,
                                                 "pool": null, "payload": payload}))
        .unwrap();
        frame.push(b'\n');
        bounded(self.writer.write_all(&frame)).await.unwrap();
        self.receive("ack", request_id).await
    }

    async fn receive(&mut self, kind: &str, request_id: &str) -> Message {
        if let Some(index) = self.pending.iter().position(|message| {
            message.kind == kind && message.request_id.as_deref() == Some(request_id)
        }) {
            return self.pending.remove(index).unwrap();
        }
        bounded(async {
            for _ in 0..64 {
                let mut line = String::new();
                assert_ne!(
                    self.reader.read_line(&mut line).await.unwrap(),
                    0,
                    "raw peer reached EOF"
                );
                let message: Message = serde_json::from_str(&line).unwrap();
                assert_ne!(message.kind, "error", "raw peer received {message:?}");
                if message.kind == kind && message.request_id.as_deref() == Some(request_id) {
                    return message;
                }
                assert!(self.pending.len() < 64);
                self.pending.push_back(message);
            }
            panic!("raw peer exceeded its bounded response reads");
        })
        .await
    }

    async fn close(mut self) {
        self.request("leave_pool", json!({}), "raw-leave").await;
        bounded(self.writer.shutdown()).await.unwrap();
    }
}

async fn app_result(
    events: &mut broadcast::Receiver<ClientEvent>,
    id: Option<&str>,
) -> (String, AppResult) {
    bounded(async {
        for _ in 0..64 {
            match events.recv().await.unwrap() {
                ClientEvent::AppResult { request_id, result }
                    if id.is_none_or(|id| id == request_id) =>
                {
                    return (request_id, result);
                }
                ClientEvent::HandlerFailed { event, error } => {
                    panic!("handler failed: {event}: {error}")
                }
                _ => {}
            }
        }
        panic!("expected app_result was absent from bounded event reads");
    })
    .await
}

async fn buffer_update(events: &mut broadcast::Receiver<ClientEvent>, operation: &str) {
    bounded(async {
        for _ in 0..64 {
            match events.recv().await.unwrap() {
                ClientEvent::Buffer(update)
                    if update.key == "typed" && update.operation == operation =>
                {
                    return;
                }
                ClientEvent::HandlerFailed { event, error } => {
                    panic!("handler failed: {event}: {error}")
                }
                _ => {}
            }
        }
        panic!("expected buffer update was absent from bounded event reads");
    })
    .await;
}

fn result_value(owner: &str, kind: &str, value: Value) -> Value {
    json!({"owner": owner, "kind": kind, "value": value})
}

async fn install_echo(client: &Client) {
    let app_owner = client.client_id().to_owned();
    client
        .on_event("echo", move |data| {
            let owner = app_owner.clone();
            async move { Ok::<_, String>(result_value(&owner, "app", data["value"].clone())) }
        })
        .await;
    let process_owner = client.client_id().to_owned();
    let registration = client
        .register_process("echo", ProcessOptions::default(), move |data| {
            let owner = process_owner.clone();
            async move { Ok::<_, String>(result_value(&owner, "process", data["value"].clone())) }
        })
        .await
        .unwrap();
    assert_eq!(
        registration.process_id,
        format!("{}:echo", client.client_id())
    );
}

fn routed(outcome: CallOutcome<Value>) -> String {
    match outcome {
        CallOutcome::Routed { request_id } => {
            assert!(!request_id.is_empty());
            request_id
        }
        CallOutcome::Result(value) => panic!("third-party call returned a terminal value: {value}"),
    }
}

fn assert_receipt(
    result: &AppResult,
    id: &str,
    origin: &str,
    target: &str,
    kind: &str,
    value: Value,
) {
    assert_eq!(result.request_id.as_deref(), Some(id));
    assert_eq!(result.source_client_id.as_deref(), Some(origin));
    assert_eq!(result.target_client_id.as_deref(), Some(target));
    assert_eq!(
        result.event.as_deref(),
        Some(if kind == "app" {
            "echo".to_owned()
        } else {
            format!("{target}:echo")
        })
        .as_deref()
    );
    assert_eq!(result.error, None);
    assert_eq!(result.value, value);
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Document {
    text: String,
    fraction: f64,
    values: Vec<Value>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_typed_buffers_and_controlled_fractional_ttl() {
    let Some(mut daemon) = Daemon::start(true).await else {
        return;
    };
    let writer = daemon.client("rust-writer").await;
    let observer = daemon.client("rust-observer").await;
    let mut raw = RawClient::connect(daemon.port, "raw-buffer").await;
    let mut events = observer.events();
    observer.subscribe_buffer("typed").await.unwrap();
    let document = Document {
        text: "unicode \u{03bb} / \u{1f680}".to_owned(),
        fraction: 1.25,
        values: vec![Value::Null, json!(false), json!(0), json!("")],
    };
    writer
        .set_with_options("typed", &document, Some(Duration::from_millis(1250)), true)
        .await
        .unwrap();
    buffer_update(&mut events, "set").await;
    let entry = observer
        .get_entry::<Document>("typed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.value, document);
    assert_eq!(entry.ttl, Some(1.25));
    assert_eq!(entry.version, 1);
    assert_eq!(entry.updated_by, writer.client_id());
    assert!(entry.persistent);
    assert_eq!(observer.get::<Value>("missing").await.unwrap(), None);
    for (key, value) in [
        ("null", Value::Null),
        ("false", json!(false)),
        ("zero", json!(0)),
        ("empty", json!("")),
    ] {
        writer.set(key, &value).await.unwrap();
        assert_eq!(
            observer.get::<Value>(key).await.unwrap(),
            Some(value.clone())
        );
        let response = raw
            .request("get_buffer", json!({"key": key}), &format!("raw-get-{key}"))
            .await;
        assert_eq!(response.payload["exists"], true);
        assert_eq!(response.payload["entry"]["value"], value);
    }
    assert_eq!(
        observer.get::<Option<i64>>("null").await.unwrap(),
        Some(None)
    );
    assert_eq!(observer.get::<bool>("false").await.unwrap(), Some(false));
    assert_eq!(observer.get::<i64>("zero").await.unwrap(), Some(0));
    assert_eq!(
        observer.get::<String>("empty").await.unwrap(),
        Some(String::new())
    );
    raw.request(
        "set_buffer",
        json!({"key": "from-raw", "value": "\u{00e9}\u{96ea}"}),
        "raw-set-opaque",
    )
    .await;
    assert_eq!(
        writer.get::<String>("from-raw").await.unwrap(),
        Some("\u{00e9}\u{96ea}".to_owned())
    );
    daemon
        .command("advance_clock", json!({"seconds": 1.5}))
        .await;
    buffer_update(&mut events, "expired").await;
    assert_eq!(observer.get::<Document>("typed").await.unwrap(), None);
    observer.unsubscribe_buffer("typed").await.unwrap();
    assert!(writer.delete("from-raw").await.unwrap());
    assert!(!writer.delete("from-raw").await.unwrap());
    raw.close().await;
    writer.disconnect().await.unwrap();
    observer.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_self_cross_app_and_canonical_process_calls() {
    let Some(daemon) = Daemon::start(false).await else {
        return;
    };
    let caller = daemon.client("rust-caller").await;
    let worker = daemon.client("rust-worker").await;
    install_echo(&caller).await;
    install_echo(&worker).await;
    for target in [caller.client_id(), worker.client_id()] {
        for response_to in [None, Some(caller.client_id())] {
            let app: CallOutcome<Value> = caller
                .call_app_with_options(
                    target,
                    "echo",
                    &json!({"value": false}),
                    DEADLINE,
                    response_to,
                )
                .await
                .unwrap();
            assert_eq!(
                app,
                CallOutcome::Result(result_value(target, "app", json!(false)))
            );
            let process: CallOutcome<Value> = caller
                .call_process_with_options(
                    &format!("{target}:echo"),
                    &json!({"value": null}),
                    DEADLINE,
                    response_to,
                )
                .await
                .unwrap();
            assert_eq!(
                process,
                CallOutcome::Result(result_value(target, "process", Value::Null))
            );
        }
    }
    let value: Value = caller
        .call_app(worker.client_id(), "echo", &json!({"value": "direct"}))
        .await
        .unwrap();
    assert_eq!(
        value,
        result_value(worker.client_id(), "app", json!("direct"))
    );
    let value: Value = caller
        .call_process("rust-worker:echo", &json!({"value": 1.5}))
        .await
        .unwrap();
    assert_eq!(
        value,
        result_value(worker.client_id(), "process", json!(1.5))
    );
    worker
        .on_event("fail", |_| async {
            Err::<Value, _>("intentional application failure")
        })
        .await;
    let error = caller
        .call_app::<_, Value>(worker.client_id(), "fail", &json!({}))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Handler(message) if message.contains("intentional application failure"))
    );
    let error = caller
        .call_process::<_, Value>("rust-worker:missing", &json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Server { code, .. } if code == "process_not_found"));
    caller.disconnect().await.unwrap();
    worker.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_third_party_and_broadcast_ids_are_correlated() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let caller = daemon.client("rust-origin").await;
    let worker = daemon.client("rust-target").await;
    let second = daemon.client("rust-second").await;
    let recipient = daemon.client("rust-recipient").await;
    for client in [&caller, &worker, &second] {
        install_echo(client).await;
    }
    let mut events = recipient.events();
    for target in [worker.client_id(), caller.client_id()] {
        for kind in ["app", "process"] {
            let outcome = if kind == "app" {
                caller
                    .call_app_with_options(
                        target,
                        "echo",
                        &json!({"value": 7}),
                        DEADLINE,
                        Some(recipient.client_id()),
                    )
                    .await
                    .unwrap()
            } else {
                caller
                    .call_process_with_options(
                        &format!("{target}:echo"),
                        &json!({"value": 7}),
                        DEADLINE,
                        Some(recipient.client_id()),
                    )
                    .await
                    .unwrap()
            };
            let id = routed(outcome);
            let (received_id, result) = app_result(&mut events, Some(&id)).await;
            assert_eq!(received_id, id);
            assert_eq!(result.response_to.as_deref(), Some(recipient.client_id()));
            assert_receipt(
                &result,
                &id,
                caller.client_id(),
                target,
                kind,
                result_value(target, kind, json!(7)),
            );
        }
    }
    let targets = caller
        .broadcast_process(
            "echo",
            &json!({"value": "broadcast"}),
            Some(recipient.client_id()),
        )
        .await
        .unwrap();
    assert_eq!(
        targets.iter().cloned().collect::<HashSet<_>>(),
        HashSet::from([
            "rust-origin:echo".to_owned(),
            "rust-target:echo".to_owned(),
            "rust-second:echo".to_owned()
        ])
    );
    let mut children = HashSet::new();
    let mut parents = HashSet::new();
    for _ in 0..targets.len() {
        let (id, result) = app_result(&mut events, None).await;
        assert!(
            children.insert(id.clone()),
            "broadcast reused a child request ID"
        );
        let parent = result
            .parent_request_id
            .clone()
            .expect("broadcast result omitted parent metadata");
        assert_ne!(id, parent);
        parents.insert(parent);
        let target = result.target_client_id.as_deref().unwrap();
        assert_receipt(
            &result,
            &id,
            caller.client_id(),
            target,
            "process",
            result_value(target, "process", json!("broadcast")),
        );
    }
    assert_eq!(parents.len(), 1);
    let mut raw = RawClient::connect(daemon.port, "raw-broadcast").await;
    let ack = raw
        .request(
            "broadcast_process",
            json!({"process_name": "echo", "data": {"value": 0}}),
            "opaque-broadcast-parent",
        )
        .await;
    let raw_children = ack.payload["request_ids"].as_array().unwrap();
    assert_eq!(raw_children.len(), 3);
    assert_eq!(raw_children.iter().collect::<HashSet<_>>().len(), 3);
    for id in raw_children {
        let id = id.as_str().unwrap();
        let result = raw.receive("app_result", id).await;
        assert_eq!(result.payload["request_id"], id);
        assert_eq!(
            result.payload["parent_request_id"],
            "opaque-broadcast-parent"
        );
        assert_ne!(id, "opaque-broadcast-parent");
    }
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    raw.close().await;
    for client in [&caller, &worker, &second, &recipient] {
        client.disconnect().await.unwrap();
    }
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_raw_peer_bidirectional_opaque_ids() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let worker = daemon.client("rust-raw-worker").await;
    let recipient = daemon.client("rust-raw-recipient").await;
    install_echo(&worker).await;
    let mut raw = RawClient::connect(daemon.port, "raw-origin").await;
    for kind in ["app", "process"] {
        let id = format!("opaque/{kind}:origin-not-a-uuid");
        let payload = if kind == "app" {
            json!({"target_client_id": worker.client_id(), "event": "echo", "data": {"value": "\u{03bb}"}})
        } else {
            json!({"process_id": "rust-raw-worker:echo", "data": {"value": "\u{03bb}"}})
        };
        let ack = raw
            .request(
                if kind == "app" {
                    "call_app"
                } else {
                    "call_process"
                },
                payload,
                &id,
            )
            .await;
        assert_eq!(ack.payload["queued"], true);
        assert_eq!(ack.payload["request_id"], id);
        let result = raw.receive("app_result", &id).await;
        let typed: AppResult = serde_json::from_value(result.payload).unwrap();
        assert_receipt(
            &typed,
            &id,
            "raw-origin",
            worker.client_id(),
            kind,
            result_value(worker.client_id(), kind, json!("\u{03bb}")),
        );
    }
    daemon
        .command(
            "start_raw",
            json!({"pool": POOL, "client_id": "legacy-worker"}),
        )
        .await;
    let mut events = recipient.events();
    for kind in ["app", "process"] {
        let outcome = if kind == "app" {
            worker
                .call_app_with_options(
                    "legacy-worker",
                    "echo",
                    &json!({"value": false}),
                    DEADLINE,
                    Some(recipient.client_id()),
                )
                .await
                .unwrap()
        } else {
            worker
                .call_process_with_options(
                    "legacy-worker:echo",
                    &json!({"value": false}),
                    DEADLINE,
                    Some(recipient.client_id()),
                )
                .await
                .unwrap()
        };
        let id = routed(outcome);
        let record = daemon.command("next_raw_call", json!({})).await;
        let invocation = &record["invocation"];
        let hop_id = invocation["request_id"].as_str().unwrap();
        assert_ne!(hop_id, id);
        assert_eq!(
            invocation["payload"]["event"],
            if kind == "app" {
                "echo"
            } else {
                "legacy-worker:echo"
            }
        );
        assert_eq!(record["reply"]["request_id"], hop_id);
        assert_eq!(
            record["reply"]["payload"],
            json!({"value": false, "error": null})
        );
        let (_, result) = app_result(&mut events, Some(&id)).await;
        assert_receipt(
            &result,
            &id,
            worker.client_id(),
            "legacy-worker",
            kind,
            json!(false),
        );
    }
    for kind in ["app", "process"] {
        for response_to in [None, Some(worker.client_id())] {
            let data = json!({"value": "", "hold": true});
            let mut call = Box::pin(async {
                if kind == "app" {
                    worker
                        .call_app_with_options::<_, Value>(
                            "legacy-worker",
                            "echo",
                            &data,
                            DEADLINE,
                            response_to,
                        )
                        .await
                } else {
                    worker
                        .call_process_with_options::<_, Value>(
                            "legacy-worker:echo",
                            &data,
                            DEADLINE,
                            response_to,
                        )
                        .await
                }
            });
            let record = tokio::select! {
                result = call.as_mut() => panic!("legacy call settled before its callee completed: {result:?}"),
                record = daemon.command("next_raw_call", json!({})) => record,
            };
            assert_eq!(record["held"], true);
            // A same-connection round trip fences delivery of the earlier RPC ACK.
            assert!(!worker.exists("legacy-acceptance-barrier").await.unwrap());
            std::future::poll_fn(|cx| {
                assert!(
                    call.as_mut().poll(cx).is_pending(),
                    "acceptance ACK settled a terminal call"
                );
                std::task::Poll::Ready(())
            })
            .await;
            daemon
                .command(
                    "complete_raw",
                    json!({"request_id": record["invocation"]["request_id"]}),
                )
                .await;
            assert_eq!(bounded(call).await.unwrap(), CallOutcome::Result(json!("")));
        }
    }
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    raw.close().await;
    worker.disconnect().await.unwrap();
    recipient.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_registration_rejoin_switch_and_disconnect() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let worker = daemon.client("rust-lifecycle-worker").await;
    let caller = daemon.client("rust-lifecycle-caller").await;
    worker
        .register_process("versioned", ProcessOptions::default(), |_| async {
            Ok::<_, String>(1)
        })
        .await
        .unwrap();
    assert_eq!(
        caller
            .call_process::<_, i64>("rust-lifecycle-worker:versioned", &json!({}))
            .await
            .unwrap(),
        1
    );
    worker
        .register_process("versioned", ProcessOptions::default(), |_| async {
            Ok::<_, String>(2)
        })
        .await
        .unwrap();
    let rejected = worker
        .register_process(
            "versioned",
            ProcessOptions {
                group_id: Some("x".repeat(513)),
                ..ProcessOptions::default()
            },
            |_| async { Ok::<_, String>(99) },
        )
        .await;
    assert!(matches!(rejected, Err(Error::Protocol(_))));
    worker.switch_pool(POOL, None).await.unwrap();
    assert_eq!(
        caller
            .call_process::<_, i64>("rust-lifecycle-worker:versioned", &json!({}))
            .await
            .unwrap(),
        2
    );
    assert_eq!(caller.list_processes(None).await.unwrap().len(), 1);
    worker.unregister_process("versioned").await.unwrap();
    let error = caller
        .call_process::<_, Value>("rust-lifecycle-worker:versioned", &json!({}))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Server { code, .. } if code == "process_not_found"));
    caller.set("pool-local", &true).await.unwrap();
    worker.switch_pool("rust-other-pool", None).await.unwrap();
    assert!(worker.list_processes(None).await.unwrap().is_empty());
    assert_eq!(worker.get::<bool>("pool-local").await.unwrap(), None);
    assert_eq!(caller.get::<bool>("pool-local").await.unwrap(), Some(true));
    caller.switch_pool("rust-other-pool", None).await.unwrap();
    let (started, mut starts) = mpsc::channel(1);
    worker
        .register_process("blocked", ProcessOptions::default(), move |_| {
            let started = started.clone();
            async move {
                started.send(()).await.unwrap();
                std::future::pending::<()>().await;
                Ok::<_, String>(Value::Null)
            }
        })
        .await
        .unwrap();
    let call = {
        let caller = caller.clone();
        tokio::spawn(async move {
            caller
                .call_process_with_options::<_, Value>(
                    "rust-lifecycle-worker:blocked",
                    &json!({}),
                    DEADLINE,
                    None,
                )
                .await
        })
    };
    assert_eq!(bounded(starts.recv()).await, Some(()));
    bounded(worker.disconnect()).await.unwrap();
    let error = bounded(call).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Server { code, .. } if code == "peer_disconnected"));
    assert!(!worker.is_connected());
    assert!(matches!(
        worker.get::<Value>("anything").await,
        Err(Error::Disconnected)
    ));
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    let replacement = Client::builder("latzero://rust-lifecycle-worker", "rust-other-pool")
        .port(daemon.port)
        .connect()
        .await
        .unwrap();
    assert!(replacement.list_processes(None).await.unwrap().is_empty());
    replacement.disconnect().await.unwrap();
    caller.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_pool_change_terminates_an_in_flight_route() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    daemon
        .command(
            "start_raw",
            json!({"pool": POOL, "client_id": "held-worker"}),
        )
        .await;
    let caller = daemon.client("rust-switching-caller").await;
    let call = {
        let caller = caller.clone();
        tokio::spawn(async move {
            caller
                .call_app_with_options::<_, Value>(
                    "held-worker",
                    "echo",
                    &json!({"value": 1, "hold": true}),
                    DEADLINE,
                    None,
                )
                .await
        })
    };
    let record = daemon.command("next_raw_call", json!({})).await;
    assert_eq!(record["held"], true);
    assert!(
        !call.is_finished(),
        "call settled before the held callee completed"
    );
    bounded(caller.switch_pool("after-switch", None))
        .await
        .unwrap();
    let error = bounded(call).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Disconnected | Error::Server { .. }));
    assert_eq!(caller.pool_name().await, "after-switch");
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    caller.set("still-usable", &0).await.unwrap();
    assert_eq!(caller.get::<i64>("still-usable").await.unwrap(), Some(0));
    caller.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_python_sdk_bidirectional_calls_and_buffers() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let ready = daemon.command("start_python", json!({"pool": POOL})).await;
    if let Some(reason) = ready["skip"].as_str() {
        eprintln!("SKIP optional real Python SDK interop: {reason}");
        daemon.shutdown().await;
        return;
    }
    assert_eq!(ready["client_id"], "python-peer");
    let rust = daemon.client("rust-python-peer").await;
    let recipient = daemon.client("rust-python-recipient").await;
    install_echo(&rust).await;
    let mut events = recipient.events();
    for kind in ["app", "process"] {
        for response_to in [None, Some(rust.client_id())] {
            let result: CallOutcome<Value> = if kind == "app" {
                rust.call_app_with_options(
                    "python-peer",
                    "echo",
                    &json!({"value": "\u{96ea}"}),
                    DEADLINE,
                    response_to,
                )
                .await
                .unwrap()
            } else {
                rust.call_process_with_options(
                    "python-peer:echo",
                    &json!({"value": "\u{96ea}"}),
                    DEADLINE,
                    response_to,
                )
                .await
                .unwrap()
            };
            assert_eq!(
                result,
                CallOutcome::Result(result_value("python-peer", kind, json!("\u{96ea}")))
            );
        }
        for response_to in [None, Some("python-peer")] {
            let result = daemon.command("python_call", json!({"kind": kind, "target": rust.client_id(), "value": null, "response_to": response_to})).await;
            assert_eq!(result, result_value(rust.client_id(), kind, Value::Null));
        }
        let acceptance = daemon.command("python_call", json!({"kind": kind, "target": rust.client_id(), "value": false, "response_to": recipient.client_id()})).await;
        assert_eq!(acceptance["queued"], true);
        let id = acceptance["request_id"].as_str().unwrap();
        let (_, result) = app_result(&mut events, Some(id)).await;
        assert_receipt(
            &result,
            id,
            "python-peer",
            rust.client_id(),
            kind,
            result_value(rust.client_id(), kind, json!(false)),
        );
        let outcome = if kind == "app" {
            rust.call_app_with_options(
                "python-peer",
                "echo",
                &json!({"value": 0}),
                DEADLINE,
                Some("python-peer"),
            )
            .await
            .unwrap()
        } else {
            rust.call_process_with_options(
                "python-peer:echo",
                &json!({"value": 0}),
                DEADLINE,
                Some("python-peer"),
            )
            .await
            .unwrap()
        };
        let id = routed(outcome);
        let hook = daemon.command("python_hook", json!({})).await;
        let result: AppResult = serde_json::from_value(hook).unwrap();
        assert_receipt(
            &result,
            &id,
            rust.client_id(),
            "python-peer",
            kind,
            result_value("python-peer", kind, json!(0)),
        );
    }
    let scalars = json!([null, false, 0, "", "\u{03bb}", 1.5]);
    daemon
        .command(
            "python_set",
            json!({"key": "python-buffer", "value": scalars}),
        )
        .await;
    assert_eq!(
        rust.get::<Value>("python-buffer").await.unwrap(),
        Some(scalars)
    );
    daemon
        .command("python_subscribe", json!({"key": "rust-buffer"}))
        .await;
    rust.set("rust-buffer", &Value::Null).await.unwrap();
    let update = daemon.command("python_update", json!({})).await;
    assert_eq!(update["key"], "rust-buffer");
    assert_eq!(update["entry"]["value"], Value::Null);
    assert_eq!(
        daemon
            .command("python_get", json!({"key": "rust-buffer"}))
            .await,
        json!({"exists": true, "value": null})
    );
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    rust.disconnect().await.unwrap();
    recipient.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_node_sdk_bidirectional_rpc_and_buffer_fidelity() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let Some(mut node) = NodePeer::start(daemon.port).await else {
        daemon.shutdown().await;
        return;
    };
    let rust = daemon.client("rust-node-peer").await;
    install_echo(&rust).await;
    let mut events = rust.events();
    for value in [
        Value::Null,
        json!(false),
        json!(0),
        json!(""),
        json!("\u{03bb}\u{96ea}"),
    ] {
        let result: Value = rust
            .call_process("node-peer:echo", &json!({"value": value}))
            .await
            .unwrap();
        assert_eq!(result, result_value("node-peer", "process", value.clone()));
        for kind in ["app", "process"] {
            let response = node
                .command(
                    "call",
                    json!({"kind": kind, "target": rust.client_id(), "value": value}),
                )
                .await;
            let message: Message = serde_json::from_value(response).unwrap();
            assert_eq!(message.kind, "app_result");
            let id = message.request_id.unwrap();
            assert!(!id.is_empty());
            let result: AppResult = serde_json::from_value(message.payload).unwrap();
            assert_receipt(
                &result,
                &id,
                "node-peer",
                rust.client_id(),
                kind,
                result_value(rust.client_id(), kind, value.clone()),
            );
        }
        node.command("set", json!({"key": "node-scalar", "value": value}))
            .await;
        assert_eq!(
            rust.get::<Value>("node-scalar").await.unwrap(),
            Some(value.clone())
        );
        rust.set("rust-scalar", &value).await.unwrap();
        assert_eq!(
            node.command("get", json!({"key": "rust-scalar"})).await,
            json!({"exists": true, "value": value})
        );
    }
    assert_eq!(
        node.command("get", json!({"key": "missing-scalar"})).await,
        json!({"exists": false, "value": "missing"})
    );
    for kind in ["app", "process"] {
        let response = node.command("call", json!({"kind": kind, "target": rust.client_id(), "value": false, "response_to": rust.client_id()})).await;
        assert_eq!(response["type"], "ack");
        assert_eq!(response["payload"]["queued"], true);
        let id = response["request_id"].as_str().unwrap();
        assert_eq!(response["payload"]["request_id"], id);
        let (_, result) = app_result(&mut events, Some(id)).await;
        assert_eq!(result.response_to.as_deref(), Some(rust.client_id()));
        assert_receipt(
            &result,
            id,
            "node-peer",
            rust.client_id(),
            kind,
            result_value(rust.client_id(), kind, json!(false)),
        );
    }
    assert_eq!(daemon.command("barrier", json!({})).await["routes"], 0);
    node.shutdown().await;
    rust.disconnect().await.unwrap();
    daemon.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_daemon_guard_cleans_up_without_a_shutdown_command() {
    let Some(mut daemon) = Daemon::start(false).await else {
        return;
    };
    let temporary = daemon.temp_root.clone();
    daemon.command("barrier", json!({})).await;
    assert!(temporary.is_dir());
    drop(daemon);
    assert!(
        !temporary.exists(),
        "RAII failure path left daemon state behind"
    );
}
