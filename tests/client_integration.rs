use std::{collections::HashMap, time::Duration};

use latzero::{CallOutcome, Client, ClientEvent, Error, Message, ProcessOptions, WorkerKind};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    time,
};

async fn read_message(reader: &mut BufReader<OwnedReadHalf>) -> Message {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    assert!(!line.is_empty(), "client closed unexpectedly");
    serde_json::from_str(&line).unwrap()
}

async fn write_message(writer: &mut OwnedWriteHalf, message: &Message) {
    let mut bytes = serde_json::to_vec(message).unwrap();
    bytes.push(b'\n');
    writer.write_all(&bytes).await.unwrap();
}

fn response(request: &Message, kind: &str, payload: Value) -> Message {
    Message {
        kind: kind.to_owned(),
        request_id: request.request_id.clone(),
        client_id: request.client_id.clone(),
        pool: request.pool.clone(),
        payload,
    }
}

async fn accept_handshake(
    listener: TcpListener,
    join_payload: Value,
) -> (BufReader<OwnedReadHalf>, OwnedWriteHalf) {
    let (stream, _) = listener.accept().await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);

    let hello = read_message(&mut reader).await;
    assert_eq!(hello.kind, "hello");
    write_message(
        &mut writer,
        &response(&hello, "ack", json!({"server": "test"})),
    )
    .await;

    let join = read_message(&mut reader).await;
    assert_eq!(join.kind, "join_pool");
    write_message(&mut writer, &response(&join, "ack", join_payload)).await;
    (reader, writer)
}

#[tokio::test]
async fn buffers_namespaces_subscriptions_and_out_of_order_rpc() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut reader, mut writer) = accept_handshake(
            listener,
            json!({"pool": "alpha", "auth_required": false, "clients": ["rust-1"]}),
        )
        .await;
        let mut values = HashMap::<String, Value>::new();
        loop {
            let request = read_message(&mut reader).await;
            match request.kind.as_str() {
                "set_buffer" => {
                    let key = request.payload["key"].as_str().unwrap().to_owned();
                    values.insert(key.clone(), request.payload["value"].clone());
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"key": key, "version": 1})),
                    )
                    .await;
                }
                "get_buffer" => {
                    let key = request.payload["key"].as_str().unwrap();
                    let payload = values.get(key).map_or_else(
                        || json!({"key": key, "exists": false, "entry": null}),
                        |value| {
                            json!({
                                "key": key,
                                "exists": true,
                                "entry": {
                                    "value": value,
                                    "updated_at": 1.0,
                                    "updated_by": "rust-1",
                                    "persistent": false,
                                    "ttl": null,
                                    "version": 1
                                }
                            })
                        },
                    );
                    write_message(&mut writer, &response(&request, "ack", payload)).await;
                }
                "list_buffers" => {
                    let prefix = request.payload["pattern"].as_str();
                    let mut keys: Vec<_> = values
                        .keys()
                        .filter(|key| prefix.is_none_or(|prefix| key.starts_with(prefix)))
                        .cloned()
                        .collect();
                    keys.sort();
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"keys": keys})),
                    )
                    .await;
                }
                "subscribe_buffer" => {
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"subscribed": true})),
                    )
                    .await;
                    write_message(
                        &mut writer,
                        &Message {
                            kind: "buffer_update".to_owned(),
                            request_id: None,
                            client_id: Some("other".to_owned()),
                            pool: Some("alpha".to_owned()),
                            payload: json!({
                                "key": "users:1",
                                "operation": "set",
                                "entry": {
                                    "value": {"name": "Ada"},
                                    "updated_at": 2.0,
                                    "updated_by": "other",
                                    "persistent": false,
                                    "ttl": null,
                                    "version": 2
                                }
                            }),
                        },
                    )
                    .await;
                }
                "call_app" if request.payload["response_to"] == Value::Null => {
                    // A fast target can reply before the origin receives its ack.
                    write_message(
                        &mut writer,
                        &response(&request, "app_result", json!({"value": 7, "error": null})),
                    )
                    .await;
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"queued": true})),
                    )
                    .await;
                }
                "call_app" => {
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"queued": true})),
                    )
                    .await;
                }
                "leave_pool" => {
                    write_message(
                        &mut writer,
                        &response(&request, "ack", json!({"left_pool": true})),
                    )
                    .await;
                    break;
                }
                other => panic!("unexpected request: {other}"),
            }
        }
    });

    let client = Client::builder("latzero://rust-1", "alpha")
        .port(port)
        .connect()
        .await
        .unwrap();
    let mut events = client.events();
    let users = client.namespace("users");
    users.set("1", &json!({"name": "Ada"})).await.unwrap();
    assert_eq!(
        users.get::<Value>("1").await.unwrap(),
        Some(json!({"name": "Ada"}))
    );
    assert_eq!(users.keys(None).await.unwrap(), vec!["1"]);
    assert_eq!(users.increment("counter", 2).await.unwrap(), 2);
    assert_eq!(users.decrement("counter", 1).await.unwrap(), 1);

    client.subscribe_buffer("users:1").await.unwrap();
    let event = time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, ClientEvent::Buffer(update) if update.key == "users:1"));

    let result: i64 = client
        .call_app("worker", "sum", &json!({"x": 3, "y": 4}))
        .await
        .unwrap();
    assert_eq!(result, 7);

    let outcome: CallOutcome<Value> = client
        .call_app_with_options(
            "worker",
            "sum",
            &json!({"x": 1}),
            Duration::from_secs(1),
            Some("third"),
        )
        .await
        .unwrap();
    assert!(matches!(outcome, CallOutcome::Routed { request_id } if !request_id.is_empty()));

    client.disconnect().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn authentication_errors_are_typed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = read_message(&mut reader).await;
        write_message(&mut writer, &response(&hello, "ack", json!({}))).await;
        let join = read_message(&mut reader).await;
        write_message(
            &mut writer,
            &response(
                &join,
                "error",
                json!({"code": "auth_failed", "message": "bad token"}),
            ),
        )
        .await;
    });

    let result = Client::builder("latzero://rust-auth", "secure")
        .auth_token("wrong")
        .port(port)
        .connect()
        .await;
    let error = match result {
        Ok(_) => panic!("authentication unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(matches!(error, Error::Authentication(message) if message == "bad token"));
    server.await.unwrap();
}

#[tokio::test]
async fn incoming_event_and_process_calls_return_results() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (finished, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut reader, mut writer) = accept_handshake(
            listener,
            json!({"pool": "alpha", "auth_required": false, "clients": ["worker"]}),
        )
        .await;
        let register = read_message(&mut reader).await;
        assert_eq!(register.kind, "register_process");
        write_message(
            &mut writer,
            &response(
                &register,
                "ack",
                json!({
                    "process_id": "worker:add",
                    "group_id": "g1",
                    "worker_kind": "thread",
                    "min_workers": 1,
                    "max_workers": 4
                }),
            ),
        )
        .await;

        for (request_id, event, expected) in [
            ("event-call", "add", json!(99)),
            ("process-call", "worker:add", json!(7)),
        ] {
            write_message(
                &mut writer,
                &Message {
                    kind: "call_app".to_owned(),
                    request_id: Some(request_id.to_owned()),
                    client_id: Some("caller".to_owned()),
                    pool: Some("alpha".to_owned()),
                    payload: json!({"event": event, "data": {"x": 3, "y": 4}}),
                },
            )
            .await;
            loop {
                let result = read_message(&mut reader).await;
                if result.kind == "worker_metrics" {
                    write_message(
                        &mut writer,
                        &response(&result, "ack", json!({"received": 1})),
                    )
                    .await;
                    continue;
                }
                assert_eq!(result.kind, "app_result");
                assert_eq!(result.request_id.as_deref(), Some(request_id));
                assert_eq!(result.payload["value"], expected);
                break;
            }
        }
        finished.send(()).unwrap();

        let leave = read_message(&mut reader).await;
        assert_eq!(leave.kind, "leave_pool");
        write_message(
            &mut writer,
            &response(&leave, "ack", json!({"left_pool": true})),
        )
        .await;
    });

    let client = Client::builder("latzero://worker", "alpha")
        .port(port)
        .connect()
        .await
        .unwrap();
    client
        .on_event("add", |_| async { Ok::<_, String>(99) })
        .await;
    client
        .register_process(
            "add",
            ProcessOptions {
                max_workers: 4,
                worker_kind: WorkerKind::Thread,
                ..ProcessOptions::default()
            },
            |data| async move {
                let x = data["x"].as_i64().unwrap();
                let y = data["y"].as_i64().unwrap();
                Ok::<_, String>(x + y)
            },
        )
        .await
        .unwrap();
    time::timeout(Duration::from_secs(1), received)
        .await
        .expect("handler replies were not received")
        .unwrap();
    client.disconnect().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn dropping_last_client_handle_closes_the_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut reader, _writer) = accept_handshake(listener, json!({})).await;
        let mut line = String::new();
        let read = time::timeout(Duration::from_secs(1), reader.read_line(&mut line))
            .await
            .expect("socket remained open after Client was dropped")
            .unwrap();
        assert_eq!(read, 0);
    });

    let client = Client::builder("latzero://drop-test", "alpha")
        .port(port)
        .connect()
        .await
        .unwrap();
    drop(client);
    server.await.unwrap();
}
