# LatZero Rust Client

Async Rust client for `latzero-server`. It implements the same newline-delimited
JSON TCP protocol as the Python server-mode `LatZero` client.

The Python client's standalone shared-memory implementation is intentionally
out of scope. Rust and Python applications interoperate through
`latzero-server`.

## Features

- Tokio-based TCP transport with `hello`, pool join/switch/leave, request IDs,
  timeouts, and typed server errors
- Concurrent request correlation, including two-stage `ack` and `app_result`
  calls
- JSON buffers, TTL, persistence, listing, client-derived batches and scans
- Buffer subscriptions and presence/update event streams
- Targeted and broadcast events, app RPC, third-party response routing
- Process registration, direct/short-name calls, broadcast, discovery, worker
  metrics, and server-directed concurrency scaling
- Namespaced buffers and event emitters
- Cancellation-safe frame writer and automatic socket cleanup when the final
  client handle is dropped

## Install

```toml
[dependencies]
latzero = { path = "../rust-client" }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Start the server first:

```text
latzero-server --headless
```

## Buffers

```rust,no_run
use std::time::Duration;
use latzero::Client;
use serde_json::{Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let client = Client::builder("latzero://rust-api", "app")
        .auth_token("optional-pool-token")
        .connect()
        .await?;

    client.set("config", &json!({"enabled": true})).await?;
    client
        .set_with_options("session", &json!({"user": 42}), Some(Duration::from_secs(60)), false)
        .await?;

    let config: Option<Value> = client.get("config").await?;
    println!("{config:?}");

    let users = client.namespace("users");
    users.set("42", &json!({"name": "Ada"})).await?;

    client.disconnect().await
}
```

`get` returns `Option<T>`, so a missing buffer is distinct from a present JSON
`null` when `T` can represent null, such as `serde_json::Value`.

## Events And RPC

Handlers receive a JSON object and may return any serializable value.

```rust,no_run
use latzero::Client;
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let worker = Client::connect("latzero://worker-1", "app").await?;
    let process = worker
        .on_event("add", |data: Map<String, Value>| async move {
            let value = data["x"].as_i64().unwrap() + data["y"].as_i64().unwrap();
            Ok::<_, String>(value)
        })
        .await;

    let caller = Client::connect("latzero://caller-1", "app").await?;
    let result: i64 = caller
        .call_app("worker-1", "add", &json!({"x": 3, "y": 4}))
        .await?;
    assert_eq!(result, 7);

    caller.disconnect().await?;
    worker.disconnect().await
}
```

Use `Client::events()` for unsolicited `presence_update`, `buffer_update`,
emitted events, third-party app results, scaling commands, disconnects, and
unknown forward-compatible protocol messages. `CallOutcome::Routed` preserves
the request ID when `response_to` sends a result to another client.

## Processes

```rust,no_run
use latzero::{Client, ProcessOptions};
use serde_json::{Map, Value, json};

#[tokio::main]
async fn main() -> latzero::Result<()> {
    let worker = Client::connect("latzero://worker-1", "app").await?;
    worker
        .register_process(
            "multiply",
            ProcessOptions::default(),
            |data: Map<String, Value>| async move {
                Ok::<_, String>(data["x"].as_i64().unwrap() * data["y"].as_i64().unwrap())
            },
        )
        .await?;

    let caller = Client::connect("latzero://caller-1", "app").await?;
    let value: i64 = caller
        .call_process(&process.process_id, &json!({"x": 6, "y": 7}))
        .await?;
    assert_eq!(value, 42);
    Ok(())
}
```

Calling the canonical `process_id` works with all released server revisions;
current servers additionally accept a short process name and select a worker
using round-robin routing.

`WorkerKind` values are sent as protocol metadata for compatibility. Rust
handlers execute as asynchronous Tokio tasks; `min_workers` and `max_workers`
control local concurrent handler execution. Selecting `Process` does not move a
Rust closure into an operating-system child process.

## Compatibility

The client covers every message currently dispatched by `latzero-server`:

```text
hello, join_pool, switch_pool, leave_pool,
set_buffer, get_buffer, delete_buffer, list_buffers, list_clients,
subscribe_buffer, unsubscribe_buffer,
emit_event, call_app, app_result,
register_process, unregister_process, call_process, broadcast_process,
list_processes, worker_metrics
```

The server protocol currently has no version negotiation, TLS, reconnection,
or resumable sessions. A closed connection fails pending requests; construct a
new `Client` to reconnect and re-register handlers/processes.

## Verification

```text
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo doc --no-deps
```
