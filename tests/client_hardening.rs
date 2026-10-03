use std::{
    fmt::Debug,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use latzero::{
    CallOutcome, Client, ClientBuilder, ClientEvent, Error, Message, ProcessOptions, WorkerMetrics,
};
use serde::{Serialize, Serializer, ser::Error as _};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpSocket, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Semaphore, broadcast, mpsc},
    time,
};

const WAIT: Duration = Duration::from_secs(3);
const CLIENT_ID: &str = "worker";
const POOL: &str = "alpha";

async fn bounded<F: Future>(future: F) -> F::Output {
    time::timeout(WAIT, future)
        .await
        .expect("test barrier did not complete within its deadline")
}

struct Peer {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Peer {
    async fn accept(listener: TcpListener) -> Self {
        let (stream, _) = bounded(listener.accept()).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let (reader, writer) = stream.into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn handshake(&mut self) {
        let hello = self.read().await;
        assert_eq!(hello.kind, "hello");
        assert_eq!(hello.payload["client_id"], CLIENT_ID);
        self.ack(&hello, json!({"server": "hardening-fixture"}))
            .await;
        let join = self.read().await;
        assert_eq!(join.kind, "join_pool");
        assert_eq!(join.payload["client_id"], CLIENT_ID);
        assert_eq!(join.pool.as_deref(), Some(POOL));
        self.ack(
            &join,
            json!({"pool": POOL, "auth_required": false, "clients": [CLIENT_ID]}),
        )
        .await;
    }

    async fn read_optional(&mut self) -> Option<Message> {
        let mut line = String::new();
        let count = bounded(self.reader.read_line(&mut line)).await.unwrap();
        if count == 0 {
            return None;
        }
        assert!(line.ends_with('\n'), "client sent an incomplete JSON frame");
        Some(serde_json::from_str(&line).expect("client emitted invalid JSON"))
    }

    async fn read(&mut self) -> Message {
        self.read_optional()
            .await
            .expect("client closed before the expected frame")
    }

    async fn application_frame(&mut self) -> Message {
        bounded(async {
            loop {
                let message = self.read().await;
                if message.kind == "worker_metrics" {
                    self.ack(&message, json!({"received": 1})).await;
                } else {
                    return message;
                }
            }
        })
        .await
    }

    async fn send(&mut self, message: &Message) {
        let mut bytes = serde_json::to_vec(message).unwrap();
        bytes.push(b'\n');
        self.raw(&bytes).await;
    }

    async fn raw(&mut self, bytes: &[u8]) {
        bounded(self.writer.write_all(bytes)).await.unwrap();
    }

    async fn reply(&mut self, request: &Message, kind: &str, payload: Value) {
        self.send(&response(request, kind, payload)).await;
    }

    async fn ack(&mut self, request: &Message, payload: Value) {
        self.reply(request, "ack", payload).await;
    }

    async fn invoke(&mut self, request_id: &str, event: &str, data: Value) {
        self.send(&incoming(request_id, event, data, POOL)).await;
    }

    async fn closed(&mut self) {
        let mut bytes = Vec::new();
        bounded(self.reader.read_to_end(&mut bytes)).await.unwrap();
        assert!(
            bytes.is_empty(),
            "unexpected frames after transport cleanup: {bytes:?}"
        );
    }
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

fn incoming(request_id: &str, event: &str, data: Value, pool: &str) -> Message {
    Message {
        kind: "call_app".to_owned(),
        request_id: Some(request_id.to_owned()),
        client_id: Some("origin".to_owned()),
        pool: Some(pool.to_owned()),
        payload: json!({
            "event": event,
            "data": data,
            "source_client_id": "origin",
            "target_client_id": CLIENT_ID,
            "response_to": "origin",
        }),
    }
}

async fn pair(configure: impl FnOnce(ClientBuilder) -> ClientBuilder) -> (Client, Peer) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    pair_on(listener, configure).await
}

async fn pair_on(
    listener: TcpListener,
    configure: impl FnOnce(ClientBuilder) -> ClientBuilder,
) -> (Client, Peer) {
    let port = listener.local_addr().unwrap().port();
    let builder = configure(
        Client::builder(format!("latzero://{CLIENT_ID}"), POOL)
            .port(port)
            .timeout(Duration::from_secs(2)),
    );
    let (client, peer) = bounded(async {
        tokio::join!(builder.connect(), async {
            let mut peer = Peer::accept(listener).await;
            peer.handshake().await;
            peer
        })
    })
    .await;
    (client.unwrap(), peer)
}

async fn request_for<F, T>(peer: &mut Peer, operation: &mut Pin<Box<F>>) -> Message
where
    F: Future<Output = latzero::Result<T>>,
    T: Debug,
{
    bounded(async {
        tokio::select! {
            result = operation.as_mut() => panic!("operation completed before its reply: {result:?}"),
            request = peer.application_frame() => request,
        }
    })
    .await
}

async fn poll_pending<F: Future>(future: Pin<&mut F>) {
    let mut future = future;
    poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "operation settled before the barrier"
        );
        Poll::Ready(())
    })
    .await;
}

async fn register_constant(client: &Client, peer: &mut Peer, value: &str) {
    let value = value.to_owned();
    let mut registration =
        Box::pin(
            client.register_process("job", ProcessOptions::default(), move |_| {
                let value = value.clone();
                async move { Ok::<_, String>(value) }
            }),
        );
    let request = request_for(peer, &mut registration).await;
    assert_eq!(request.kind, "register_process");
    peer.ack(&request, registration_payload()).await;
    assert_eq!(
        bounded(registration).await.unwrap().process_id,
        "worker:job"
    );
}

fn registration_payload() -> Value {
    json!({
        "process_id": "worker:job",
        "group_id": "hardening-group",
        "worker_kind": "thread",
        "min_workers": 1,
        "max_workers": 1,
    })
}

async fn finish(client: Client, mut peer: Peer) {
    if client.is_connected() {
        let mut disconnect = Box::pin(client.disconnect());
        let request = request_for(&mut peer, &mut disconnect).await;
        assert_eq!(request.kind, "leave_pool");
        peer.ack(&request, json!({"left_pool": true})).await;
        bounded(disconnect).await.unwrap();
    }
    peer.closed().await;
}

async fn fence(client: &Client, peer: &mut Peer) -> Vec<Message> {
    let mut operation = Box::pin(client.clients());
    let mut preceding = Vec::new();
    loop {
        let request = request_for(peer, &mut operation).await;
        if request.kind == "list_clients" {
            peer.ack(&request, json!({"clients": [CLIENT_ID]})).await;
            assert_eq!(bounded(operation).await.unwrap(), [CLIENT_ID]);
            return preceding;
        }
        preceding.push(request);
    }
}

#[tokio::test]
async fn process_response_to_own_client_waits_for_terminal_result_in_both_orders() {
    for result_first in [false, true] {
        let (client, mut peer) = pair(|builder| builder).await;
        let data = json!({"input": 1});
        let mut call = Box::pin(client.call_process_with_options::<_, Value>(
            "remote:job",
            &data,
            Duration::from_secs(1),
            Some(CLIENT_ID),
        ));
        let request = request_for(&mut peer, &mut call).await;
        assert_eq!(request.kind, "call_process");
        assert_eq!(request.payload["response_to"], CLIENT_ID);
        let ack = response(
            &request,
            "ack",
            json!({"queued": true, "request_id": request.request_id}),
        );
        let result = response(
            &request,
            "app_result",
            json!({"value": {"answer": 42}, "error": null}),
        );
        for message in if result_first {
            [result, ack]
        } else {
            [ack, result]
        } {
            peer.send(&message).await;
        }
        assert_eq!(
            bounded(call).await.unwrap(),
            CallOutcome::Result(json!({"answer": 42}))
        );
        finish(client, peer).await;
    }
}

async fn self_call(process: bool, opaque_hop: bool, result_first: bool) {
    let (client, mut peer) = pair(|builder| builder.max_pending_requests(1)).await;
    client
        .on_event("echo", |data| async move { Ok::<_, String>(data) })
        .await;
    if process {
        register_constant(&client, &mut peer, "process-result").await;
    }
    let data = json!({"value": 7});
    let mut call: Pin<Box<dyn Future<Output = latzero::Result<CallOutcome<Value>>> + Send + '_>> =
        if process {
            Box::pin(client.call_process_with_options(
                "worker:job",
                &data,
                Duration::from_secs(1),
                None,
            ))
        } else {
            Box::pin(client.call_app_with_options(
                CLIENT_ID,
                "echo",
                &data,
                Duration::from_secs(1),
                None,
            ))
        };
    let request = bounded(async {
        tokio::select! {
            result = call.as_mut() => panic!("self call completed before invocation: {result:?}"),
            request = peer.application_frame() => request,
        }
    })
    .await;
    let origin_id = request.request_id.as_deref().unwrap();
    let hop_id = if opaque_hop {
        "opaque:server/hop-id"
    } else {
        origin_id
    };
    peer.invoke(
        hop_id,
        if process { "worker:job" } else { "echo" },
        data.clone(),
    )
    .await;
    let submission = bounded(async {
        tokio::select! {
            result = call.as_mut() => panic!("self call waiter consumed its incoming invocation: {result:?}"),
            result = peer.application_frame() => result,
        }
    }).await;
    assert_eq!(submission.kind, "app_result");
    assert_eq!(submission.request_id.as_deref(), Some(hop_id));
    assert_eq!(submission.pool.as_deref(), Some(POOL));
    assert!(submission.payload["error"].is_null());
    assert_eq!(
        submission.payload["value"],
        if process {
            json!("process-result")
        } else {
            data.clone()
        }
    );

    // A worker's result-submission ACK is not the origin call's acceptance ACK.
    peer.ack(&submission, json!({"delivered": true})).await;
    let terminal = response(
        &request,
        "app_result",
        json!({
            "request_id": origin_id,
            "value": submission.payload["value"],
            "error": null,
        }),
    );
    let acceptance = response(
        &request,
        "ack",
        json!({"queued": true, "request_id": origin_id}),
    );
    for message in if result_first {
        [terminal, acceptance]
    } else {
        [acceptance, terminal]
    } {
        peer.send(&message).await;
    }
    assert_eq!(
        bounded(call).await.unwrap(),
        CallOutcome::Result(submission.payload["value"].clone())
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn app_self_calls_preserve_opaque_and_legacy_push_ids_in_both_reply_orders() {
    for opaque_hop in [false, true] {
        for result_first in [false, true] {
            self_call(false, opaque_hop, result_first).await;
        }
    }
}

#[tokio::test]
async fn process_self_calls_preserve_opaque_and_legacy_push_ids_in_both_reply_orders() {
    for opaque_hop in [false, true] {
        for result_first in [false, true] {
            self_call(true, opaque_hop, result_first).await;
        }
    }
}

#[tokio::test]
async fn third_party_self_call_ignores_delivery_ack_until_real_acceptance() {
    let (client, mut peer) = pair(|builder| builder).await;
    client
        .on_event("echo", |_| async { Ok::<_, String>(7) })
        .await;
    let data = json!({});
    let mut call = Box::pin(client.call_app_with_options::<_, Value>(
        CLIENT_ID,
        "echo",
        &data,
        Duration::from_secs(1),
        Some("third"),
    ));
    let request = request_for(&mut peer, &mut call).await;
    peer.invoke(request.request_id.as_deref().unwrap(), "echo", json!({}))
        .await;
    let submission = request_for(&mut peer, &mut call).await;
    assert_eq!(submission.kind, "app_result");
    peer.ack(&submission, json!({"delivered": true})).await;
    // A reader-side push is a barrier proving the delivery ACK was processed.
    let mut events = client.events();
    peer.send(&presence("after-hop-ack", POOL)).await;
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    poll_pending(call.as_mut()).await;
    peer.ack(
        &request,
        json!({"queued": true, "request_id": request.request_id}),
    )
    .await;
    assert!(
        matches!(bounded(call).await.unwrap(), CallOutcome::Routed { request_id } if Some(request_id.as_str()) == request.request_id.as_deref())
    );
    finish(client, peer).await;
}

fn presence(client_id: &str, pool: &str) -> Message {
    Message {
        kind: "presence_update".to_owned(),
        request_id: None,
        client_id: Some(client_id.to_owned()),
        pool: Some(pool.to_owned()),
        payload: json!({"client_id": client_id, "status": "joined", "pool": pool, "clients": [CLIENT_ID]}),
    }
}

#[tokio::test]
async fn unsolicited_result_with_matching_id_is_published_without_consuming_ack_waiter() {
    let (client, mut peer) = pair(|builder| builder).await;
    let mut events = client.events();
    let mut get = Box::pin(client.get::<Value>("key"));
    let request = request_for(&mut peer, &mut get).await;
    peer.reply(
        &request,
        "app_result",
        json!({"value": "unsolicited", "error": null}),
    )
    .await;
    peer.send(&presence("result-barrier", POOL)).await;
    let first = bounded(events.recv()).await.unwrap();
    assert!(
        matches!(first, ClientEvent::AppResult { request_id, result }
        if Some(request_id.as_str()) == request.request_id.as_deref() && result.value == "unsolicited")
    );
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    poll_pending(get.as_mut()).await;
    peer.ack(&request, json!({"exists": false})).await;
    assert_eq!(bounded(get).await.unwrap(), None);
    finish(client, peer).await;
}

#[tokio::test]
async fn pending_flood_is_typed_bounded_and_cancelled_waiters_release_admission() {
    let (client, mut peer) = pair(|builder| builder.max_pending_requests(2)).await;
    let mut first = Box::pin(client.get::<Value>("first"));
    let first_request = request_for(&mut peer, &mut first).await;
    let mut second = Box::pin(client.get::<Value>("second"));
    let second_request = request_for(&mut peer, &mut second).await;
    for _ in 0..8 {
        let error = bounded(client.get::<Value>("excess")).await.unwrap_err();
        assert!(matches!(error, Error::Overloaded { resource } if !resource.is_empty()));
    }
    drop(first);
    let mut replacement = Box::pin(client.get::<Value>("replacement"));
    let replacement_request = request_for(&mut peer, &mut replacement).await;
    assert_eq!(replacement_request.payload["key"], "replacement");
    // Replies to cancelled IDs cannot settle the newly admitted operation.
    peer.ack(
        &first_request,
        json!({"exists": true, "entry": {"value": "stale"}}),
    )
    .await;
    peer.ack(&replacement_request, json!({"exists": false}))
        .await;
    assert_eq!(bounded(replacement).await.unwrap(), None);
    peer.ack(&second_request, json!({"exists": false})).await;
    assert_eq!(bounded(second).await.unwrap(), None);
    finish(client, peer).await;
}

#[tokio::test]
async fn request_timeout_releases_pending_admission_without_replaying_the_request() {
    let (client, mut peer) = pair(|builder| builder.max_pending_requests(1)).await;
    let data = json!({});
    let mut call = Box::pin(client.call_app_with_options::<_, Value>(
        "remote",
        "effect",
        &data,
        Duration::from_millis(40),
        None,
    ));
    let request = request_for(&mut peer, &mut call).await;
    peer.ack(&request, json!({"queued": true})).await;
    assert!(
        matches!(bounded(call).await, Err(Error::Timeout { timeout, .. }) if timeout == Duration::from_millis(40))
    );
    let preceding = fence(&client, &mut peer).await;
    assert!(
        preceding.iter().all(|message| message.kind != "call_app"),
        "timed-out effectful call was replayed"
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn failed_reregistration_restores_previous_handler_after_pre_ack_replacement() {
    let (client, mut peer) = pair(|builder| builder).await;
    register_constant(&client, &mut peer, "old").await;
    let mut replacement = Box::pin(client.register_process(
        "job",
        ProcessOptions::default(),
        |_| async { Ok::<_, String>("new") },
    ));
    let request = request_for(&mut peer, &mut replacement).await;
    assert_eq!(request.kind, "register_process");
    peer.invoke("pre-ack-hop", "worker:job", json!({})).await;
    let result = request_for(&mut peer, &mut replacement).await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.request_id.as_deref(), Some("pre-ack-hop"));
    assert_eq!(
        result.payload["value"], "new",
        "replacement must be installed before registration ACK"
    );
    peer.reply(
        &request,
        "error",
        json!({"code": "registration_rejected", "message": "fixture rejected replacement"}),
    )
    .await;
    assert!(bounded(replacement).await.is_err());
    peer.invoke("rollback-hop", "worker:job", json!({})).await;
    let result = peer.application_frame().await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.request_id.as_deref(), Some("rollback-hop"));
    assert_eq!(
        result.payload["value"], "old",
        "failed replacement removed the previous handler"
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn same_identity_same_pool_switch_is_idempotent_and_keeps_process_handler() {
    let (client, mut peer) = pair(|builder| builder).await;
    register_constant(&client, &mut peer, "retained").await;
    let mut switch = Box::pin(client.switch_pool(POOL, None));
    let request = request_for(&mut peer, &mut switch).await;
    assert_eq!(request.kind, "switch_pool");
    assert_eq!(request.payload["client_id"], CLIENT_ID);
    peer.ack(&request, json!({"pool": POOL, "clients": [CLIENT_ID]}))
        .await;
    bounded(switch).await.unwrap();
    assert_eq!(client.client_id(), CLIENT_ID);
    assert_eq!(client.pool_name().await, POOL);
    peer.invoke("retained-hop", "worker:job", json!({})).await;
    let result = peer.application_frame().await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.payload["value"], "retained");
    finish(client, peer).await;
}

#[tokio::test]
async fn unacknowledged_pool_switch_closes_uncertain_membership() {
    let (client, mut peer) = pair(|builder| builder.timeout(Duration::from_millis(100))).await;
    let mut events = client.events();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    assert_eq!(request.kind, "switch_pool");
    assert!(matches!(bounded(switch).await, Err(Error::Timeout { .. })));
    assert!(
        !client.is_connected(),
        "membership is uncertain after transmitted switch timeout"
    );
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Disconnected
    ));
    assert!(matches!(
        bounded(client.clients()).await,
        Err(Error::Disconnected)
    ));
    peer.closed().await;
}

#[tokio::test]
async fn pool_transition_rejects_or_times_out_calls_within_their_admission_deadline() {
    let (client, mut peer) = pair(|builder| builder.timeout(Duration::from_secs(2))).await;
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    let data = json!({});
    let call = Box::pin(client.call_process_with_options::<_, Value>(
        "remote:job",
        &data,
        Duration::from_millis(40),
        None,
    ));
    let result = time::timeout(Duration::from_millis(500), call)
        .await
        .expect("call deadline did not bound operation-gate admission");
    match result {
        Err(Error::Timeout { timeout, .. }) => assert_eq!(timeout, Duration::from_millis(40)),
        Err(Error::Overloaded { resource }) => assert!(!resource.is_empty()),
        other => {
            panic!("pool-transition admission neither rejected nor expired the call: {other:?}")
        }
    }
    peer.ack(&request, json!({"pool": "beta", "clients": [CLIENT_ID]}))
        .await;
    bounded(switch).await.unwrap();
    assert!(
        fence(&client, &mut peer)
            .await
            .iter()
            .all(|message| message.kind != "call_process")
    );
    finish(client, peer).await;
}

struct Dropped {
    id: usize,
    sender: mpsc::UnboundedSender<usize>,
}

impl Drop for Dropped {
    fn drop(&mut self) {
        let _ = self.sender.send(self.id);
    }
}

struct HandlerProbe {
    started: mpsc::UnboundedReceiver<usize>,
    dropped: mpsc::UnboundedReceiver<usize>,
    release: Arc<Semaphore>,
    invoked: Arc<AtomicUsize>,
}

async fn blocked_process(client: &Client, peer: &mut Peer) -> HandlerProbe {
    let (started_sender, started) = mpsc::unbounded_channel();
    let (dropped_sender, dropped) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let invoked = Arc::new(AtomicUsize::new(0));
    let handler_release = release.clone();
    let handler_invoked = invoked.clone();
    let mut registration = Box::pin(client.register_process(
        "job",
        ProcessOptions {
            max_workers: 1,
            ..ProcessOptions::default()
        },
        move |_| {
            let started_sender = started_sender.clone();
            let dropped_sender = dropped_sender.clone();
            let release = handler_release.clone();
            let invoked = handler_invoked.clone();
            async move {
                let id = invoked.fetch_add(1, Ordering::SeqCst);
                let _guard = Dropped {
                    id,
                    sender: dropped_sender,
                };
                started_sender.send(id).unwrap();
                release.acquire().await.unwrap().forget();
                Ok::<_, String>(id)
            }
        },
    ));
    let request = request_for(peer, &mut registration).await;
    peer.ack(&request, registration_payload()).await;
    bounded(registration).await.unwrap();
    HandlerProbe {
        started,
        dropped,
        release,
        invoked,
    }
}

#[tokio::test]
async fn unregister_cancels_running_and_queued_process_work_and_quiesces_replies() {
    let (client, mut peer) = pair(|builder| builder.max_handler_tasks(4)).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("running-hop", "worker:job", json!({})).await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    peer.invoke("queued-hop", "worker:job", json!({})).await;
    let mut events = client.events();
    peer.send(&presence("queue-barrier", POOL)).await;
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    let mut unregister = Box::pin(client.unregister_process("job"));
    let request = request_for(&mut peer, &mut unregister).await;
    assert_eq!(request.kind, "unregister_process");
    peer.ack(
        &request,
        json!({"removed": true, "process_id": "worker:job"}),
    )
    .await;
    bounded(unregister).await.unwrap();
    assert_eq!(
        bounded(probe.dropped.recv()).await,
        Some(0),
        "running handler did not receive cancellation"
    );
    probe.release.add_permits(2);
    assert_eq!(
        probe.invoked.load(Ordering::SeqCst),
        1,
        "queued handler executed after unregister"
    );
    assert!(
        fence(&client, &mut peer)
            .await
            .iter()
            .all(|message| message.kind != "app_result"),
        "unregistered handler emitted a stale reply"
    );
    register_constant(&client, &mut peer, "fresh").await;
    peer.invoke("fresh-hop", "worker:job", json!({})).await;
    assert_eq!(peer.application_frame().await.payload["value"], "fresh");
    finish(client, peer).await;
}

#[tokio::test]
async fn pool_change_cancels_old_generation_handlers_before_new_membership_replies() {
    let (client, mut peer) =
        pair(|builder| builder.shutdown_timeout(Duration::from_millis(100))).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("old-pool-hop", "worker:job", json!({})).await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    assert_eq!(request.kind, "switch_pool");
    peer.ack(&request, json!({"pool": "beta", "clients": [CLIENT_ID]}))
        .await;
    bounded(switch).await.unwrap();
    assert_eq!(bounded(probe.dropped.recv()).await, Some(0));
    probe.release.add_permits(1);
    assert_eq!(client.pool_name().await, "beta");
    assert!(
        fence(&client, &mut peer)
            .await
            .iter()
            .all(|message| message.kind != "app_result"),
        "old handler replied through new membership"
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn cancelling_switch_during_handler_quiescence_closes_before_membership_is_sent() {
    let (client, mut peer) = pair(|builder| builder).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("quiescence-hop", "worker:job", json!({})).await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    let mut events = client.events();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    // Poll once: the current-thread runtime cannot reap the aborted handler
    // until we yield, so cancellation occurs inside the quiescence barrier.
    poll_pending(switch.as_mut()).await;
    drop(switch);
    assert!(
        !client.is_connected(),
        "cancelled quiescence left a half-live membership"
    );
    assert_eq!(bounded(probe.dropped.recv()).await, Some(0));
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Disconnected
    ));
    peer.closed().await;
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn disconnect_cancels_handlers_and_closes_even_while_user_future_is_pending() {
    let (client, mut peer) =
        pair(|builder| builder.shutdown_timeout(Duration::from_millis(100))).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("disconnect-hop", "worker:job", json!({})).await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    let mut events = client.events();
    let mut disconnect = Box::pin(client.disconnect());
    let request = request_for(&mut peer, &mut disconnect).await;
    assert_eq!(request.kind, "leave_pool");
    peer.ack(&request, json!({"left_pool": true})).await;
    bounded(disconnect).await.unwrap();
    assert_eq!(bounded(probe.dropped.recv()).await, Some(0));
    assert!(!client.is_connected());
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Disconnected
    ));
    probe.release.add_permits(1);
    peer.closed().await;
    bounded(client.disconnect()).await.unwrap();
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
}

struct Unencodable;

impl Serialize for Unencodable {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom("encoding failed: \"quoted\"\\path\nline"))
    }
}

#[tokio::test]
async fn synchronous_panic_future_panic_and_encoding_error_return_json_safe_handler_errors() {
    let (client, mut peer) = pair(|builder| builder).await;
    client
        .on_event("sync-panic", |data| {
            assert!(!data.is_empty(), "sync panic: \"quoted\"\\path\nline");
            std::future::ready(Ok::<_, String>(Value::Null))
        })
        .await;
    client
        .on_event("future-panic", |data| async move {
            assert!(!data.is_empty(), "future panic: \"quoted\"\\path\nline");
            Ok::<_, String>(Value::Null)
        })
        .await;
    client
        .on_event("encoding-error", |_| async { Ok::<_, String>(Unencodable) })
        .await;
    client
        .on_event("healthy", |_| async { Ok::<_, String>(42) })
        .await;
    for event in ["sync-panic", "future-panic", "encoding-error"] {
        peer.invoke(event, event, json!({})).await;
        let result = peer.application_frame().await;
        assert_eq!(result.kind, "app_result");
        assert_eq!(result.request_id.as_deref(), Some(event));
        assert!(result.payload["value"].is_null());
        assert!(result.payload["error"].is_object());
        assert!(!result.payload["error"]["type"].as_str().unwrap().is_empty());
        assert!(
            !result.payload["error"]["message"]
                .as_str()
                .unwrap()
                .is_empty()
        );
    }
    peer.invoke("healthy-hop", "healthy", json!({})).await;
    assert_eq!(peer.application_frame().await.payload["value"], 42);
    assert!(client.is_connected());
    finish(client, peer).await;
}

#[tokio::test]
async fn oversized_outgoing_request_is_typed_and_does_not_leak_pending_capacity() {
    let limit = 512;
    let (client, mut peer) =
        pair(|builder| builder.max_frame_bytes(limit).max_pending_requests(1)).await;
    let error = bounded(client.set("too-large", &"x".repeat(1024)))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::FrameTooLarge { size, limit: actual_limit } if size > limit && actual_limit == limit)
    );
    assert!(client.is_connected());
    let preceding = fence(&client, &mut peer).await;
    assert!(
        preceding.is_empty(),
        "rejected oversized request reached the wire"
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn broadcast_receiver_explicitly_reports_lag_using_existing_api() {
    let (client, mut peer) = pair(|builder| builder.event_capacity(2)).await;
    let mut events = client.events();
    for index in 0..6 {
        peer.send(&presence(&format!("peer-{index}"), POOL)).await;
    }
    // The ACK barrier is read after every preceding push, without consuming events.
    assert!(fence(&client, &mut peer).await.is_empty());
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(4))
    ));
    assert!(
        matches!(events.try_recv().unwrap(), ClientEvent::Presence(update) if update.client_id == "peer-4")
    );
    assert!(
        matches!(events.try_recv().unwrap(), ClientEvent::Presence(update) if update.client_id == "peer-5")
    );
    assert!(matches!(
        events.try_recv(),
        Err(broadcast::error::TryRecvError::Empty)
    ));
    finish(client, peer).await;
}

async fn disconnected(events: &mut broadcast::Receiver<ClientEvent>) {
    bounded(async {
        loop {
            if matches!(events.recv().await.unwrap(), ClientEvent::Disconnected) {
                return;
            }
        }
    })
    .await;
}

fn assert_handler_error(message: &Message) {
    assert_eq!(message.kind, "app_result");
    assert!(message.payload["value"].is_null());
    assert!(message.payload["error"].is_object());
    assert!(
        !message.payload["error"]["type"]
            .as_str()
            .unwrap()
            .is_empty()
    );
    assert!(
        !message.payload["error"]["message"]
            .as_str()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn malformed_binary_oversize_and_unterminated_eof_frames_clean_up_all_transport_work() {
    for (name, bytes, eof) in [
        ("malformed", b"{broken}\n".to_vec(), false),
        ("binary", vec![0xff, 0xfe, b'\n'], false),
        ("non-object", b"[]\n".to_vec(), false),
        (
            "invalid-type",
            b"{\"type\":\"\",\"payload\":{}}\n".to_vec(),
            false,
        ),
        (
            "invalid-payload",
            b"{\"type\":\"ack\",\"payload\":7}\n".to_vec(),
            false,
        ),
        ("oversize-line", vec![b'x'; 1025], false),
        ("partial-json-eof", b"{\"type\":\"ack\"".to_vec(), true),
        (
            "complete-json-without-newline-eof",
            b"{\"type\":\"ack\",\"payload\":{}}".to_vec(),
            true,
        ),
        ("clean-eof", Vec::new(), true),
    ] {
        let (client, mut peer) =
            pair(|builder| builder.max_frame_bytes(512).max_pending_requests(2)).await;
        let mut probe = blocked_process(&client, &mut peer).await;
        peer.invoke("cleanup-hop", "worker:job", json!({})).await;
        assert_eq!(bounded(probe.started.recv()).await, Some(0));
        let mut pending = Box::pin(client.get::<Value>("pending-at-cleanup"));
        let request = request_for(&mut peer, &mut pending).await;
        assert_eq!(request.kind, "get_buffer");
        let mut events = client.events();
        peer.raw(&bytes).await;
        if eof {
            bounded(peer.writer.shutdown()).await.unwrap();
        }
        disconnected(&mut events).await;
        assert!(
            !client.is_connected(),
            "{name} left the transport connected"
        );
        assert!(
            matches!(bounded(pending).await, Err(Error::Disconnected)),
            "{name} did not fail pending requests as disconnected"
        );
        assert_eq!(
            bounded(probe.dropped.recv()).await,
            Some(0),
            "{name} did not cancel the running handler"
        );
        probe.release.add_permits(1);
        peer.closed().await;
        assert!(matches!(
            bounded(client.clients()).await,
            Err(Error::Disconnected)
        ));
    }
}

#[tokio::test]
async fn dropping_last_public_handle_cancels_running_handler_and_closes_transport() {
    let (client, mut peer) = pair(|builder| builder).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("last-handle-hop", "worker:job", json!({}))
        .await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    drop(client);
    assert_eq!(bounded(probe.dropped.recv()).await, Some(0));
    peer.closed().await;
}

#[tokio::test]
async fn handler_task_and_byte_admission_excess_returns_safe_rpc_errors_and_recovers() {
    for byte_budget in [false, true] {
        let (client, mut peer) = pair(|builder| {
            if byte_budget {
                builder.max_handler_tasks(4).max_handler_bytes(1024)
            } else {
                builder.max_handler_tasks(1)
            }
        })
        .await;
        let mut probe = blocked_process(&client, &mut peer).await;
        let data = if byte_budget {
            json!({"bytes": "x".repeat(600)})
        } else {
            json!({})
        };
        peer.invoke("admitted-hop", "worker:job", data.clone())
            .await;
        assert_eq!(bounded(probe.started.recv()).await, Some(0));
        for index in 0..6 {
            let request_id = format!("excess-hop-{index}");
            peer.invoke(&request_id, "worker:job", data.clone()).await;
            let error = peer.application_frame().await;
            assert_eq!(error.request_id.as_deref(), Some(request_id.as_str()));
            assert_handler_error(&error);
            assert!(
                error.payload["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("handler")
            );
        }
        assert_eq!(probe.invoked.load(Ordering::SeqCst), 1);
        assert!(
            client.is_connected(),
            "small overload errors must not be silently dropped"
        );
        probe.release.add_permits(2);
        let completed = peer.application_frame().await;
        assert_eq!(completed.request_id.as_deref(), Some("admitted-hop"));
        assert_eq!(completed.payload["value"], 0);
        peer.invoke("recovered-hop", "worker:job", json!({})).await;
        let recovered = peer.application_frame().await;
        assert_eq!(recovered.request_id.as_deref(), Some("recovered-hop"));
        assert_eq!(recovered.payload["value"], 1);
        assert_eq!(probe.invoked.load(Ordering::SeqCst), 2);
        finish(client, peer).await;
    }
}

#[tokio::test]
async fn notification_handler_admission_excess_closes_instead_of_silently_losing_work() {
    let (client, mut peer) = pair(|builder| builder.max_handler_tasks(1)).await;
    let (started_sender, mut started) = mpsc::unbounded_channel();
    let (dropped_sender, mut dropped) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let handler_release = release.clone();
    client
        .on_event("hold", move |_| {
            let started_sender = started_sender.clone();
            let dropped_sender = dropped_sender.clone();
            let release = handler_release.clone();
            async move {
                let _guard = Dropped {
                    id: 0,
                    sender: dropped_sender,
                };
                started_sender.send(()).unwrap();
                release.acquire().await.unwrap().forget();
                Ok::<_, String>(Value::Null)
            }
        })
        .await;
    let event = Message {
        kind: "emit_event".to_owned(),
        request_id: None,
        client_id: Some("origin".to_owned()),
        pool: Some(POOL.to_owned()),
        payload: json!({"event": "hold", "data": {}}),
    };
    peer.send(&event).await;
    assert_eq!(bounded(started.recv()).await, Some(()));
    let mut events = client.events();
    peer.send(&event).await;
    disconnected(&mut events).await;
    assert_eq!(bounded(dropped.recv()).await, Some(0));
    assert!(!client.is_connected());
    peer.closed().await;
}

#[tokio::test]
async fn fallible_event_registration_limit_is_explicit_and_reusable() {
    let (client, mut peer) = pair(|builder| builder.max_batch_size(1)).await;
    let id = client
        .try_on_event("first", |_| async { Ok::<_, String>(42) })
        .await
        .unwrap();
    assert!(matches!(
        client
            .try_on_event("excess", |_| async { Ok::<_, String>(0) })
            .await,
        Err(Error::Overloaded {
            resource: "event handlers"
        })
    ));
    assert!(client.is_connected());
    peer.invoke("first-hop", "first", json!({})).await;
    assert_eq!(peer.application_frame().await.payload["value"], 42);
    assert!(client.remove_event_handler("first", id).await);
    client
        .try_on_event("replacement", |_| async { Ok::<_, String>(7) })
        .await
        .unwrap();
    peer.invoke("replacement-hop", "replacement", json!({}))
        .await;
    assert_eq!(peer.application_frame().await.payload["value"], 7);
    finish(client, peer).await;
}

#[tokio::test]
async fn legacy_infallible_registration_failure_does_not_claim_a_live_handler() {
    let (client, mut peer) = pair(|builder| builder.max_batch_size(1)).await;
    client
        .on_event("first", |_| async { Ok::<_, String>(42) })
        .await;
    let mut events = client.events();
    client
        .on_event("excess", |_| async { Ok::<_, String>(0) })
        .await;
    assert!(!client.is_connected());
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::HandlerFailed { event, .. } if event == "excess")
    );
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Disconnected
    ));
    peer.closed().await;
}

#[tokio::test]
async fn oversized_handler_result_is_replaced_by_small_json_safe_error() {
    let (client, mut peer) = pair(|builder| builder.max_frame_bytes(512)).await;
    client
        .on_event("large", |_| async { Ok::<_, String>("x".repeat(2048)) })
        .await;
    peer.invoke("large-result-hop", "large", json!({})).await;
    let result = peer.application_frame().await;
    assert_eq!(result.request_id.as_deref(), Some("large-result-hop"));
    assert_handler_error(&result);
    assert!(serde_json::to_vec(&result).unwrap().len() <= 512);
    assert!(client.is_connected());
    finish(client, peer).await;
}

#[tokio::test]
async fn malformed_registration_ack_closes_uncertain_registration_state() {
    let (client, mut peer) = pair(|builder| builder).await;
    register_constant(&client, &mut peer, "old").await;
    let mut events = client.events();
    let mut registration = Box::pin(client.register_process(
        "job",
        ProcessOptions::default(),
        |_| async { Ok::<_, String>("new") },
    ));
    let request = request_for(&mut peer, &mut registration).await;
    peer.ack(&request, json!({"process_id": 1})).await;
    assert!(bounded(registration).await.is_err());
    disconnected(&mut events).await;
    assert!(
        !client.is_connected(),
        "bad ACK leaves server-side registration ownership uncertain"
    );
    peer.closed().await;
}

#[tokio::test]
async fn cancelled_pool_transition_closes_transmitted_uncertain_membership() {
    let (client, mut peer) = pair(|builder| builder).await;
    let mut events = client.events();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    assert_eq!(request.kind, "switch_pool");
    drop(switch);
    disconnected(&mut events).await;
    assert!(!client.is_connected());
    peer.closed().await;
}

#[tokio::test]
async fn reader_commits_switch_ack_before_following_old_pool_pushes() {
    let (client, mut peer) = pair(|builder| builder).await;
    let invoked = Arc::new(AtomicUsize::new(0));
    let counter = invoked.clone();
    client
        .on_event("old-event", move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok::<_, String>("stale") }
        })
        .await;
    let mut events = client.events();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    peer.ack(&request, json!({"pool": "beta", "clients": [CLIENT_ID]}))
        .await;
    peer.invoke("stale-pool-hop", "old-event", json!({})).await;
    peer.send(&presence("stale-presence", POOL)).await;
    peer.send(&presence("new-presence", "beta")).await;
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Presence(update)
        if update.pool == "beta" && update.client_id == "new-presence")
    );
    bounded(switch).await.unwrap();
    assert_eq!(invoked.load(Ordering::SeqCst), 0);
    assert_eq!(client.pool_name().await, "beta");
    assert!(fence(&client, &mut peer).await.is_empty());
    finish(client, peer).await;
}

#[tokio::test]
async fn optional_null_pool_and_opaque_hop_id_remain_compatible() {
    let (client, mut peer) = pair(|builder| builder).await;
    client
        .on_event("legacy", |_| async { Ok::<_, String>("compatible") })
        .await;
    let mut invocation = incoming("opaque/no-required-fields:1", "legacy", json!({}), POOL);
    invocation.pool = None;
    peer.send(&invocation).await;
    let result = peer.application_frame().await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.request_id, invocation.request_id);
    assert_eq!(result.pool.as_deref(), Some(POOL));
    assert_eq!(result.payload["value"], "compatible");
    assert!(result.payload["error"].is_null());
    finish(client, peer).await;
}

#[tokio::test]
async fn writer_count_and_byte_budgets_reject_before_send_and_release_cancelled_frames() {
    for byte_budget in [false, true] {
        let (client, mut peer) = pair(|builder| {
            builder
                .max_pending_requests(8)
                .writer_capacity(if byte_budget { 4 } else { 2 })
                .max_queued_bytes(if byte_budget { 600 } else { 2048 })
                .max_frame_bytes(512)
        })
        .await;
        let data = "x".repeat(240);
        let mut first = Box::pin(client.set("cancel-before-send", &data));
        poll_pending(first.as_mut()).await;
        let mut second = Box::pin(client.set("kept", &data));
        if byte_budget {
            let error = bounded(second).await.unwrap_err();
            assert!(matches!(error, Error::Overloaded { resource } if !resource.is_empty()));
            drop(first);
            assert!(
                fence(&client, &mut peer).await.is_empty(),
                "cancelled byte-reserved frame was transmitted"
            );
        } else {
            poll_pending(second.as_mut()).await;
            let error = bounded(client.set("excess", &data)).await.unwrap_err();
            assert!(matches!(error, Error::Overloaded { resource } if !resource.is_empty()));
            drop(first);
            let request = request_for(&mut peer, &mut second).await;
            assert_eq!(
                request.payload["key"], "kept",
                "cancelled frame remained in writer FIFO"
            );
            peer.ack(&request, json!({"version": 1})).await;
            bounded(second).await.unwrap();
        }
        let mut recovered = Box::pin(client.set("recovered", &data));
        let request = request_for(&mut peer, &mut recovered).await;
        assert_eq!(request.payload["key"], "recovered");
        peer.ack(&request, json!({"version": 1})).await;
        bounded(recovered).await.unwrap();
        finish(client, peer).await;
    }
}

#[tokio::test]
async fn oversized_batch_is_typed_and_does_not_partially_execute() {
    let (client, mut peer) = pair(|builder| builder.max_batch_size(2)).await;
    let keys = ["one".to_owned(), "two".to_owned(), "three".to_owned()];
    let error = bounded(client.mget::<Value>(&keys)).await.unwrap_err();
    assert!(matches!(error, Error::Overloaded { resource } if !resource.is_empty()));
    assert!(
        fence(&client, &mut peer).await.is_empty(),
        "over-limit batch performed partial reads"
    );
    finish(client, peer).await;
}

#[tokio::test]
async fn hello_and_join_share_one_builder_deadline() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let started = time::Instant::now();
    let mut connect = Box::pin(
        Client::builder(format!("latzero://{CLIENT_ID}"), POOL)
            .port(port)
            .timeout(Duration::from_millis(700))
            .connect(),
    );
    let mut peer = bounded(async {
        tokio::select! {
            _ = connect.as_mut() => panic!("connect completed before hello ACK"),
            peer = Peer::accept(listener) => peer,
        }
    })
    .await;
    let hello = bounded(async {
        tokio::select! {
            _ = connect.as_mut() => panic!("connect completed before hello ACK"),
            request = peer.read() => request,
        }
    })
    .await;
    assert_eq!(hello.kind, "hello");
    // Withhold hello ACK while actively polling the original connect future.
    assert!(
        time::timeout_at(started + Duration::from_millis(500), connect.as_mut())
            .await
            .is_err()
    );
    peer.ack(&hello, json!({})).await;
    let join = bounded(async {
        tokio::select! {
            _ = connect.as_mut() => panic!("connect completed before join ACK"),
            request = peer.read() => request,
        }
    })
    .await;
    assert_eq!(join.kind, "join_pool");
    let result = time::timeout_at(started + Duration::from_millis(950), connect)
        .await
        .expect("connect reset its deadline between hello and join");
    assert!(matches!(result, Err(Error::Timeout { .. })));
    peer.closed().await;
}

async fn small_receive_window() -> TcpListener {
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    socket.listen(1).unwrap()
}

#[tokio::test]
async fn stalled_writer_deadline_disconnects_even_with_reader_half_open() {
    let listener = small_receive_window().await;
    let (client, mut peer) = pair_on(listener, |builder| {
        builder
            .write_timeout(Duration::from_millis(150))
            .writer_capacity(8)
            .max_pending_requests(8)
            .max_queued_bytes(5 * 1024 * 1024)
    })
    .await;
    let mut events = client.events();
    peer.send(&presence("reader-half-open", POOL)).await;
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    let data = "x".repeat(512 * 1024);
    let mut calls: Vec<_> = (0..8)
        .map(|_| Box::pin(client.set("stalled-writer", &data)))
        .collect();
    // Poll admission without yielding to the writer so all eight frames are queued.
    for call in &mut calls {
        poll_pending(call.as_mut()).await;
    }
    disconnected(&mut events).await;
    assert!(
        !client.is_connected(),
        "writer failure left the still-open reader authoritative"
    );
    for call in calls {
        assert!(matches!(bounded(call).await, Err(Error::Disconnected)));
    }
    let mut partial_output = Vec::new();
    bounded(peer.reader.read_to_end(&mut partial_output))
        .await
        .unwrap();
    assert!(
        !partial_output.is_empty(),
        "fixture never exercised actual transport writing"
    );
    assert!(
        partial_output.len() < 8 * 512 * 1024,
        "fixture did not produce socket backpressure"
    );
}

#[test]
fn queued_call_expires_once_before_writer_is_resumed_and_is_not_transmitted() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (client, mut peer) = runtime.block_on(pair(|builder| {
        builder.writer_capacity(1).max_pending_requests(1)
    }));
    let call_data = json!({"effect": "must-not-run"});
    let mut queued = Box::pin(client.call_app_with_options::<_, Value>(
        "remote",
        "effect",
        &call_data,
        Duration::from_millis(40),
        None,
    ));
    runtime.block_on(poll_pending(queued.as_mut()));
    // The current-thread writer cannot run while only this separate deadline clock is driven.
    let clock = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    clock.block_on(async {
        assert!(
            time::timeout(Duration::from_millis(40), std::future::pending::<()>())
                .await
                .is_err()
        );
    });
    assert!(matches!(
        runtime.block_on(queued),
        Err(Error::Timeout { .. })
    ));
    runtime.block_on(async {
        assert!(client.is_connected());
        assert!(
            fence(&client, &mut peer)
                .await
                .iter()
                .all(|message| message.kind != "call_app"),
            "expired unsent effectful request executed after writer resumed"
        );
        finish(client, peer).await;
    });
}

async fn next_metrics(peer: &mut Peer) -> WorkerMetrics {
    let request = peer.read().await;
    assert_eq!(
        request.kind, "worker_metrics",
        "unexpected frame while awaiting metrics"
    );
    let metrics: Vec<WorkerMetrics> =
        serde_json::from_value(request.payload["metrics"].clone()).unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].process_name, "job");
    peer.ack(&request, json!({"received": 1})).await;
    metrics.into_iter().next().unwrap()
}

#[tokio::test]
async fn process_metrics_reflect_real_backlog_and_only_finished_work() {
    let (client, mut peer) = pair(|builder| builder.max_handler_tasks(4)).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("metrics-active-hop", "worker:job", json!({}))
        .await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    peer.invoke("metrics-queued-hop", "worker:job", json!({}))
        .await;
    let mut events = client.events();
    peer.send(&presence("metrics-queue-barrier", POOL)).await;
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    let metrics = next_metrics(&mut peer).await;
    assert_eq!(metrics.active_workers, 1);
    assert_eq!(
        metrics.queue_depth, 1,
        "local queue metrics omitted admitted but not running work"
    );
    assert_eq!(metrics.completed_count, 0);
    assert_eq!(probe.invoked.load(Ordering::SeqCst), 1);
    probe.release.add_permits(2);
    let first = peer.application_frame().await;
    let second = peer.application_frame().await;
    let ids = [first.request_id.as_deref(), second.request_id.as_deref()];
    assert!(ids.contains(&Some("metrics-active-hop")));
    assert!(ids.contains(&Some("metrics-queued-hop")));
    assert!(first.payload["error"].is_null() && second.payload["error"].is_null());
    let metrics = next_metrics(&mut peer).await;
    assert_eq!(metrics.queue_depth, 0);
    assert_eq!(metrics.completed_count, 2);
    assert!(metrics.avg_latency.is_finite() && metrics.avg_latency >= 0.0);
    finish(client, peer).await;
}

#[tokio::test]
async fn rejected_unregister_restores_handler_but_cancelled_backlog_does_not_leak_metrics() {
    let (client, mut peer) = pair(|builder| builder.max_handler_tasks(4)).await;
    let mut probe = blocked_process(&client, &mut peer).await;
    peer.invoke("cancelled-active-hop", "worker:job", json!({}))
        .await;
    assert_eq!(bounded(probe.started.recv()).await, Some(0));
    peer.invoke("cancelled-queue-hop", "worker:job", json!({}))
        .await;
    let metrics = next_metrics(&mut peer).await;
    assert_eq!(metrics.queue_depth, 1);
    assert_eq!(metrics.completed_count, 0);
    let mut unregister = Box::pin(client.unregister_process("job"));
    let request = request_for(&mut peer, &mut unregister).await;
    assert_eq!(request.kind, "unregister_process");
    assert_eq!(bounded(probe.dropped.recv()).await, Some(0));
    peer.reply(
        &request,
        "error",
        json!({"code": "unregister_rejected", "message": "fixture rejected unregister"}),
    )
    .await;
    assert!(
        matches!(bounded(unregister).await, Err(Error::Server { code, .. }) if code == "unregister_rejected")
    );
    assert!(client.is_connected());
    let metrics = next_metrics(&mut peer).await;
    assert_eq!(
        metrics.queue_depth, 0,
        "cancelling queue admission leaked queue depth"
    );
    assert_eq!(
        metrics.completed_count, 0,
        "cancelled handler was reported as completed execution"
    );
    assert_eq!(probe.invoked.load(Ordering::SeqCst), 1);
    probe.release.add_permits(2);
    peer.invoke("restored-handler-hop", "worker:job", json!({}))
        .await;
    let result = peer.application_frame().await;
    assert_eq!(result.request_id.as_deref(), Some("restored-handler-hop"));
    assert_eq!(result.payload["value"], 1);
    let metrics = next_metrics(&mut peer).await;
    assert_eq!(metrics.queue_depth, 0);
    assert_eq!(metrics.completed_count, 1);
    finish(client, peer).await;
}

#[tokio::test]
async fn control_result_reserve_survives_full_regular_writer_and_pending_admission() {
    let (client, mut peer) = pair(|builder| {
        builder
            .writer_capacity(1)
            .max_pending_requests(1)
            .max_queued_bytes(256)
            .control_reserve(1)
    })
    .await;
    let holder = Arc::new(Mutex::new(Some(client.clone())));
    let handler_holder = holder.clone();
    client
        .on_event("control-result", move |_| {
            let client = handler_holder.lock().unwrap().as_ref().unwrap().clone();
            async move {
                // Fill regular admission within this handler poll, before the writer can run.
                let mut regular = Box::pin(client.clients());
                poll_pending(regular.as_mut()).await;
                assert!(matches!(
                    client.clients().await,
                    Err(Error::Overloaded { .. })
                ));
                drop(regular);
                // The cancelled ordinary frame still owns its writer count/bytes until drained.
                Ok::<_, String>(42)
            }
        })
        .await;
    peer.invoke("reserved-result-hop", "control-result", json!({}))
        .await;
    let result = peer.application_frame().await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.request_id.as_deref(), Some("reserved-result-hop"));
    assert_eq!(result.payload["value"], 42);
    assert!(result.payload["error"].is_null());
    holder.lock().unwrap().take();
    assert!(client.is_connected());
    assert!(fence(&client, &mut peer).await.is_empty());
    finish(client, peer).await;
}

#[tokio::test]
async fn definitive_ack_and_terminal_result_survive_immediately_following_eof() {
    for rpc in [false, true] {
        let (client, mut peer) = pair(|builder| builder).await;
        let mut events = client.events();
        if rpc {
            let data = json!({});
            let mut call = Box::pin(client.call_app_with_options::<_, Value>(
                "remote",
                "reply-before-eof",
                &data,
                Duration::from_secs(1),
                None,
            ));
            let request = request_for(&mut peer, &mut call).await;
            peer.ack(&request, json!({"queued": true})).await;
            peer.reply(&request, "app_result", json!({"value": 42, "error": null}))
                .await;
            bounded(peer.writer.shutdown()).await.unwrap();
            disconnected(&mut events).await;
            assert_eq!(bounded(call).await.unwrap(), CallOutcome::Result(json!(42)));
        } else {
            let mut clients = Box::pin(client.clients());
            let request = request_for(&mut peer, &mut clients).await;
            peer.ack(&request, json!({"clients": ["definitive"]})).await;
            bounded(peer.writer.shutdown()).await.unwrap();
            disconnected(&mut events).await;
            assert_eq!(bounded(clients).await.unwrap(), ["definitive"]);
        }
        assert!(!client.is_connected());
        peer.closed().await;
    }
}
