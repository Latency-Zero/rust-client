# LatZero Rust Client

[![Crates.io](https://img.shields.io/crates/v/latzero.svg)](https://crates.io/crates/latzero)
[![Documentation](https://docs.rs/latzero/badge.svg)](https://docs.rs/latzero)
[![License](https://img.shields.io/crates/l/latzero.svg)](LICENSE)

`latzero` is the asynchronous Rust client for `latzero-server`. It provides
JSON buffers, subscriptions, events, RPC, and registered worker processes over
the server's newline-delimited JSON TCP protocol.

Rust and Python applications can use the same server and pool to exchange
JSON-compatible data and call one another. The Python client's standalone
shared-memory mode is Python-specific and is not implemented by this crate.

## Features

- Tokio-based asynchronous TCP client
- Pool isolation and optional pool authentication
- Typed JSON buffers with TTL and persistence options
- Buffer subscriptions and presence notifications
- Targeted and broadcast events
- Bidirectional application RPC
- Registered processes with direct or round-robin routing
- Third-party response routing
- Namespaced buffers and event emitters
- Concurrent request correlation and typed errors
- Automatic transport cleanup when the final client handle is dropped

## Requirements

- Rust 1.85 or newer
- A running `latzero-server`
- Tokio when using the asynchronous examples
- JSON-serializable values for server communication

The client connects to `127.0.0.1:14130` by default.

## Installation

Add the published crate:

```console
cargo add latzero
```

Most applications will also need Tokio and a serialization format:

```console
cargo add tokio --features macros,rt-multi-thread
cargo add serde_json
```

Equivalent `Cargo.toml` configuration:

```toml
[dependencies]
latzero = "0.1.0"
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

For local development against this repository instead of crates.io:

```toml
[dependencies]
latzero = { path = "../rust-client" }
```

## Start The Server

Start `latzero-server` before connecting clients:

```console
latzero-server --headless
```

On Windows, the standalone executable can be started directly:

```powershell
& ".\latzero-server.exe" --headless
```

The default TCP endpoint is `127.0.0.1:14130`. The Rust client uses the TCP
endpoint, not the server's WebSocket endpoint.

## Quick Start

```rust,no_run
use latzero::Client;
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let client = Client::connect("latzero://rust-client", "example").await?;

    client
        .set("greeting", &json!({"message": "hello from Rust"}))
        .await?;

    let greeting: Option<Value> = client.get("greeting").await?;
    println!("{greeting:?}");

    client.disconnect().await
}
```

The DSN must use the form `latzero://client-id`. Client IDs should be unique
within a pool. Clients can communicate only when they are connected to the
same pool.

## Connection Configuration

`Client::connect` uses the standard host, port, and five-second request
timeout. Use `Client::builder` to override them:

```rust,no_run
use std::time::Duration;
use latzero::Client;

# async fn connect() -> latzero::Result<()> {
let client = Client::builder("latzero://worker-1", "production")
    .host("127.0.0.1")
    .port(14_130)
    .auth_token("optional-pool-token")
    .timeout(Duration::from_secs(10))
    .event_capacity(512)
    .connect()
    .await?;
# client.disconnect().await
# }
```

An established client can move to another pool with `switch_pool`. Registered
processes are cleared when switching pools and must be registered again.

```rust,no_run
# use latzero::Client;
# async fn switch(client: &Client) -> latzero::Result<()> {
client.switch_pool("another-pool", None).await?;
# Ok(())
# }
```

Call `disconnect` for an orderly pool leave. Dropping the final clone of a
`Client` still closes its transport if explicit disconnection is not possible.

## Buffers

Buffer values may be any Serde value that can be represented as JSON.

```rust,no_run
use std::time::Duration;
use latzero::Client;
use serde_json::{Value, json};

# async fn buffers(client: &Client) -> latzero::Result<()> {
client.set("config", &json!({"enabled": true})).await?;

client
    .set_with_options(
        "session",
        &json!({"user_id": 42}),
        Some(Duration::from_secs(60)),
        false,
    )
    .await?;

let config: Option<Value> = client.get("config").await?;
let exists = client.exists("config").await?;
let keys = client.keys(Some("con")).await?;
let deleted = client.delete("config").await?;

println!("config={config:?} exists={exists} keys={keys:?} deleted={deleted}");
# Ok(())
# }
```

The fourth argument to `set_with_options` controls persistence. Persistent
buffers can be restored by a server configured with persistent storage. TTL is
optional and uses `std::time::Duration`.

`get::<T>` returns `Option<T>`: `None` means the key does not exist. For
metadata such as `updated_at`, `updated_by`, version, TTL, and persistence,
use `get_entry::<T>`.

Additional buffer helpers include:

| Operation | Methods |
| --- | --- |
| Read and write | `set`, `set_with_options`, `get`, `get_entry` |
| Inspect | `exists`, `keys`, `values`, `items`, `size`, `stats`, `scan` |
| Delete | `delete`, `delete_many` |
| Client-side batches | `mset`, `mget` |
| Subscriptions | `subscribe_buffer`, `unsubscribe_buffer` |

Batch helpers currently issue one protocol request per key.

## Namespaced Buffers

Namespaces prefix keys as `namespace:key` while exposing unprefixed names to
the caller:

```rust,no_run
# use latzero::Client;
# use serde_json::{Value, json};
# async fn namespaces(client: &Client) -> latzero::Result<()> {
let users = client.namespace("users");

users.set("42", &json!({"name": "Ada"})).await?;
let user: Option<Value> = users.get("42").await?;
let user_keys = users.keys(None).await?;

let counters = client.namespace("counters");
let total = counters.increment("jobs", 1).await?;

println!("user={user:?} keys={user_keys:?} jobs={total}");
# Ok(())
# }
```

`increment` and `decrement` are client-side read-modify-write operations and
are not atomic across multiple clients.

## Buffer Subscriptions

Create an event receiver before subscribing so no update is missed:

```rust,no_run
use latzero::{Client, ClientEvent};

# async fn subscribe(client: &Client) -> latzero::Result<()> {
let mut events = client.events();
client.subscribe_buffer("jobs:status").await?;

while let Ok(event) = events.recv().await {
    match event {
        ClientEvent::Buffer(update) if update.key == "jobs:status" => {
            println!("operation={} value={}", update.operation, update.entry.value);
            break;
        }
        ClientEvent::Disconnected => break,
        _ => {}
    }
}

client.unsubscribe_buffer("jobs:status").await?;
# Ok(())
# }
```

`Client::events()` returns a Tokio broadcast receiver. Each receiver observes
its own stream, and slow receivers can report Tokio broadcast lag errors.

## Events

Event payloads must serialize to a JSON object. Scalars and arrays are rejected
because handlers receive `serde_json::Map<String, Value>`.

Register handlers with `on_event`:

```rust,no_run
use latzero::Client;
use serde_json::{Map, Value};

# async fn register(client: &Client) {
let handler_id = client
    .on_event("notifications:show", |data: Map<String, Value>| async move {
        println!("notification: {}", data["message"]);
        Ok::<_, String>(())
    })
    .await;

// Remove it later if it is no longer needed.
client.remove_event_handler("notifications:show", handler_id).await;
# }
```

Emit a fire-and-forget event to one client:

```rust,no_run
# use latzero::Client;
# use serde_json::json;
# async fn emit(client: &Client) -> latzero::Result<()> {
client
    .emit_event(
        "notifications:show",
        &json!({"message": "build complete"}),
        Some("worker-1"),
        None,
    )
    .await?;
# Ok(())
# }
```

Pass `None` as `target_client_id` to broadcast to the other clients in the
same pool. `emit_app` is a targeted convenience wrapper around `emit_event`.

## Application RPC

Handlers registered with `on_event` also service incoming application calls.
The returned value can be any JSON-serializable type.

```rust,no_run
use latzero::Client;
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let worker = Client::connect("latzero://worker-1", "app").await?;
    worker
        .on_event("math:add", |data: Map<String, Value>| async move {
            let x = data["x"].as_i64().ok_or("x must be an integer")?;
            let y = data["y"].as_i64().ok_or("y must be an integer")?;
            Ok::<_, String>(x + y)
        })
        .await;

    let caller = Client::connect("latzero://caller-1", "app").await?;
    let sum: i64 = caller
        .call_app("worker-1", "math:add", &json!({"x": 20, "y": 22}))
        .await?;

    assert_eq!(sum, 42);
    caller.disconnect().await?;
    worker.disconnect().await
}
```

`call_app_with_options` accepts a custom timeout and an optional `response_to`
client. When a different response client is selected, it returns
`CallOutcome::Routed { request_id }` after server acknowledgement instead of
waiting locally. The response client receives `ClientEvent::AppResult`.

## Namespaced Events

An event emitter prefixes names as `namespace:event`:

```rust,no_run
# use latzero::Client;
# use serde_json::{Map, Value, json};
# async fn event_namespace(client: &Client) -> latzero::Result<()> {
let math = client.event_emitter("math");

math.on("double", |data: Map<String, Value>| async move {
    Ok::<_, String>(data["value"].as_i64().unwrap_or_default() * 2)
})
.await;

let result: i64 = math
    .call("double", "worker-1", &json!({"value": 21}))
    .await?;
assert_eq!(result, 42);
# Ok(())
# }
```

## Registered Processes

A process is a named handler advertised to the server. Calls can target its
canonical `client-id:process-name` ID or, on current servers, its short name.

```rust,no_run
use latzero::{Client, ProcessOptions};
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let worker = Client::connect("latzero://worker-1", "app").await?;
    let registration = worker
        .register_process(
            "multiply",
            ProcessOptions::default(),
            |data: Map<String, Value>| async move {
                let x = data["x"].as_i64().ok_or("x must be an integer")?;
                let y = data["y"].as_i64().ok_or("y must be an integer")?;
                Ok::<_, String>(x * y)
            },
        )
        .await?;

    let caller = Client::connect("latzero://caller-1", "app").await?;
    let product: i64 = caller
        .call_process(&registration.process_id, &json!({"x": 6, "y": 7}))
        .await?;

    assert_eq!(product, 42);
    caller.disconnect().await?;
    worker.disconnect().await
}
```

Process operations include:

| Operation | Method |
| --- | --- |
| Register | `register_process` |
| Unregister | `unregister_process` |
| Call one process | `call_process`, `call_process_with_options` |
| Call every matching process | `broadcast_process` |
| Discover processes | `list_processes` |
| Report capacity and latency | `report_worker_metrics` |

Short-name calls let the server select a matching worker using round-robin
routing. Canonical process IDs work with all released server revisions.

`ProcessOptions` configures scaling metadata and local concurrency:

```rust
use latzero::{ProcessOptions, WorkerKind};

let options = ProcessOptions {
    scale: true,
    max_replicas: 4,
    group_id: Some("math-workers".to_owned()),
    worker_kind: WorkerKind::Thread,
    min_workers: 1,
    max_workers: 8,
};
```

Rust handlers execute as asynchronous Tokio tasks. `min_workers` and
`max_workers` control local concurrent handler execution. `WorkerKind` is also
sent as protocol metadata; selecting `WorkerKind::Process` does not move a Rust
closure into an operating-system child process.

## Client Event Stream

`Client::events()` exposes unsolicited server messages:

| Variant | Meaning |
| --- | --- |
| `ClientEvent::Presence` | A client joined or left the pool |
| `ClientEvent::Buffer` | A subscribed buffer changed |
| `ClientEvent::Event` | An emitted event was received |
| `ClientEvent::AppResult` | A third-party-routed result arrived |
| `ClientEvent::ProcessScale` | The server requested a process capacity change |
| `ClientEvent::HandlerFailed` | A payload or local handler failed |
| `ClientEvent::Disconnected` | The transport closed |
| `ClientEvent::Unknown` | A forward-compatible unknown message arrived |

Registered event handlers and the event stream are independent. An emitted
event is published to the stream and then dispatched to registered handlers.

## Python Interoperability

Use the Python server-mode `LatZero` client, the same pool, and unique client
IDs. Values and payloads must be JSON-compatible.

Python handler:

```python
from latzero import LatZero

client = LatZero("latzero://python-worker", pool="app")

@client.on_event("math:add")
def add(x, y):
    return x + y
```

Rust caller:

```rust,no_run
# use latzero::Client;
# use serde_json::json;
# async fn call_python(client: &Client) -> latzero::Result<()> {
let result: i64 = client
    .call_app(
        "python-worker",
        "math:add",
        &json!({"x": 20, "y": 22}),
    )
    .await?;
assert_eq!(result, 42);
# Ok(())
# }
```

The repository's `example/python-rust-comms` directory contains a complete
bidirectional example covering shared buffers, subscriptions, events, RPC, and
process calls.

## Error Handling

All fallible operations return `latzero::Result<T>`. The public `Error` enum
distinguishes:

| Error | Meaning |
| --- | --- |
| `InvalidDsn` | The DSN is not a valid `latzero://client-id` value |
| `Connection` | The TCP connection could not be opened |
| `Disconnected` | An operation used a closed client |
| `Timeout` | A correlated request did not finish in time |
| `Authentication` | Pool authentication failed |
| `Server` | The server rejected a request |
| `Protocol` | Local or remote protocol requirements were violated |
| `Serialization` | JSON encoding or decoding failed |
| `Io` | The socket or frame writer failed |
| `Handler` | A remote event or process handler returned an error |

Errors can be matched directly:

```rust,no_run
use latzero::{Client, Error};

# async fn connect() {
match Client::connect("latzero://worker-1", "app").await {
    Ok(client) => println!("connected as {}", client.client_id()),
    Err(Error::Connection { endpoint, source }) => {
        eprintln!("could not connect to {endpoint}: {source}");
    }
    Err(error) => eprintln!("LatZero error: {error}"),
}
# }
```

## Protocol Compatibility

The client implements the currently dispatched `latzero-server` TCP messages:

```text
hello, join_pool, switch_pool, leave_pool,
set_buffer, get_buffer, delete_buffer, list_buffers, list_clients,
subscribe_buffer, unsubscribe_buffer,
emit_event, call_app, app_result,
register_process, unregister_process, call_process, broadcast_process,
list_processes, worker_metrics
```

Older server executables may not support every operation. If the server returns
`Unsupported message type`, upgrade the server or avoid that newer operation.

The protocol currently does not provide version negotiation, TLS,
reconnection, or resumable sessions. A closed connection fails pending
requests. Create a new `Client` and re-register event handlers and processes to
reconnect.

## Security Notes

- Pool authentication controls admission but does not encrypt traffic.
- The default transport is plaintext TCP intended for trusted networks.
- Use network-level encryption or a secure tunnel outside trusted local
  environments.
- Do not place secrets in client IDs, pool names, or unencrypted buffer values.

## Examples

Run the included Rust example after starting the server:

```console
cargo run --example server_mode
```

Run the full Python/Rust example from `example/python-rust-comms` in two
terminals:

```powershell
py "__python.py"
```

```powershell
cargo run
```

Either side can be started first.

## Development And Verification

```console
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
cargo package
```

## License

Licensed under the MIT License. See [LICENSE](LICENSE).
