use std::{
    fmt::Debug,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use latzero::{CallOutcome, Client, ClientBuilder, ClientEvent, Error, Message, ProcessOptions};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    sync::{Semaphore, broadcast, mpsc},
    time,
};

const WAIT: Duration = Duration::from_secs(4);
const ID: &str = "redirect-worker";
const POOL: &str = "alpha";

async fn bounded<F: Future>(future: F) -> F::Output {
    time::timeout(WAIT, future)
        .await
        .expect("redirect test barrier timed out")
}

struct Peer {
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

impl Peer {
    async fn accept(listener: &TcpListener) -> Self {
        let (socket, _) = bounded(listener.accept()).await.unwrap();
        socket.set_nodelay(true).unwrap();
        let (reader, writer) = socket.into_split();
        Self {
            reader: BufReader::new(reader),
            writer,
        }
    }

    async fn read_optional(&mut self) -> Option<Message> {
        let mut line = String::new();
        if bounded(self.reader.read_line(&mut line)).await.unwrap() == 0 {
            return None;
        }
        assert!(line.ends_with('\n'));
        Some(serde_json::from_str(&line).unwrap())
    }

    async fn read(&mut self) -> Message {
        self.read_optional()
            .await
            .expect("client closed before expected frame")
    }

    async fn application(&mut self) -> Message {
        bounded(async {
            loop {
                let message = self.read().await;
                if message.kind == "worker_metrics" {
                    self.reply(&message, "ack", json!({"received": 1})).await;
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
        bounded(self.writer.write_all(&bytes)).await.unwrap();
    }

    async fn reply(&mut self, request: &Message, kind: &str, payload: Value) {
        self.send(&response(request, kind, payload)).await;
    }

    async fn hello(&mut self) {
        let hello = self.read().await;
        assert_eq!(hello.kind, "hello");
        assert_eq!(hello.client_id.as_deref(), Some(ID));
        assert_eq!(
            hello.payload,
            json!({"client_id": ID, "capabilities": ["pool_redirect_v1"]})
        );
        self.reply(&hello, "ack", json!({"server": "legacy-or-pod"}))
            .await;
    }

    async fn membership(&mut self, pool: &str, auth: Option<&str>) -> Message {
        let join = self.read().await;
        assert_eq!(join.kind, "join_pool");
        assert_eq!(join.client_id.as_deref(), Some(ID));
        assert_eq!(join.pool.as_deref(), Some(pool));
        assert_eq!(
            join.payload,
            json!({"client_id": ID, "pool": pool, "auth_token": auth})
        );
        join
    }

    async fn joined(&mut self, pool: &str, auth: Option<&str>) {
        self.hello().await;
        let join = self.membership(pool, auth).await;
        self.reply(&join, "ack", json!({"pool": pool, "clients": [ID]}))
            .await;
    }

    async fn closed(&mut self) {
        let mut bytes = Vec::new();
        bounded(self.reader.read_to_end(&mut bytes)).await.unwrap();
        assert!(bytes.is_empty(), "stale frames after cleanup: {bytes:?}");
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

fn redirect(request: &Message, port: u16, pool: &str) -> Message {
    response(
        request,
        "redirect",
        json!({
            "protocol": "pool_redirect_v1", "host": "127.0.0.1", "port": port,
            "ws_port": null, "pool": pool, "pod_index": 1, "pod_count": 4,
            "router_host": "127.0.0.1", "router_port": 12345,
            "router_ws_port": null, "cluster_id": "ephemeral-test-cluster",
        }),
    )
}

fn presence(pool: &str, marker: &str) -> Message {
    Message {
        kind: "presence_update".to_owned(),
        request_id: None,
        client_id: Some(marker.to_owned()),
        pool: Some(pool.to_owned()),
        payload: json!({"client_id": marker, "status": "joined", "pool": pool, "clients": [ID]}),
    }
}

fn invocation(pool: &str, id: &str, event: &str) -> Message {
    Message {
        kind: "call_app".to_owned(),
        request_id: Some(id.to_owned()),
        client_id: Some("caller".to_owned()),
        pool: Some(pool.to_owned()),
        payload: json!({"event": event, "data": {}, "response_to": "caller"}),
    }
}

async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}

fn builder(port: u16) -> ClientBuilder {
    Client::builder(format!("latzero://{ID}"), POOL)
        .port(port)
        .timeout(Duration::from_secs(2))
}

async fn pair(configure: impl FnOnce(ClientBuilder) -> ClientBuilder) -> (Client, Peer) {
    let listener = listener().await;
    let port = listener.local_addr().unwrap().port();
    let (client, peer) = bounded(async {
        tokio::join!(configure(builder(port)).connect(), async {
            let mut peer = Peer::accept(&listener).await;
            peer.joined(POOL, None).await;
            peer
        })
    })
    .await;
    (client.unwrap(), peer)
}

async fn request_for<F, T>(peer: &mut Peer, future: &mut Pin<Box<F>>) -> Message
where
    F: Future<Output = latzero::Result<T>>,
    T: Debug,
{
    bounded(async {
        tokio::select! {
            result = future.as_mut() => panic!("request completed before reply: {result:?}"),
            message = peer.application() => message,
        }
    })
    .await
}

async fn next_connect<F: Future<Output = latzero::Result<Client>>>(
    peer: &mut Peer,
    future: &mut Pin<Box<F>>,
) -> Message {
    bounded(async {
        tokio::select! {
            _ = future.as_mut() => panic!("connect completed before handshake reply"),
            message = peer.read() => message,
        }
    })
    .await
}

async fn accept_connect<F: Future<Output = latzero::Result<Client>>>(
    listener: &TcpListener,
    future: &mut Pin<Box<F>>,
) -> Peer {
    bounded(async {
        tokio::select! {
            _ = future.as_mut() => panic!("connect completed before owner connection"),
            peer = Peer::accept(listener) => peer,
        }
    })
    .await
}

async fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

async fn finish(client: Client, mut peer: Peer) {
    let mut disconnect = Box::pin(client.disconnect());
    let leave = request_for(&mut peer, &mut disconnect).await;
    assert_eq!(leave.kind, "leave_pool");
    peer.reply(&leave, "ack", json!({"left_pool": true})).await;
    bounded(disconnect).await.unwrap();
    peer.closed().await;
}

async fn redirected_pair() -> (Client, Peer, u16) {
    let router = listener().await;
    let owner = listener().await;
    let entry_port = router.local_addr().unwrap().port();
    let owner_port = owner.local_addr().unwrap().port();
    let (result, peer) = bounded(async {
        tokio::join!(
            builder(entry_port).auth_token("initial-token").connect(),
            async {
                let mut router = Peer::accept(&router).await;
                router.hello().await;
                let join = router.membership(POOL, Some("initial-token")).await;
                router.send(&redirect(&join, owner_port, POOL)).await;
                router.closed().await;
                let mut peer = Peer::accept(&owner).await;
                peer.joined(POOL, Some("initial-token")).await;
                peer
            }
        )
    })
    .await;
    (result.unwrap(), peer, entry_port)
}

#[tokio::test]
async fn router_owner_handshake_preserves_identity_auth_and_final_buffered_push() {
    let (client, mut owner, _) = redirected_pair().await;
    assert!(client.is_connected());
    assert_eq!(client.pool_name().await, POOL);
    let mut events = client.events();
    owner.send(&presence(POOL, "owner-ready")).await;
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Presence(value) if value.client_id == "owner-ready")
    );
    let mut get = Box::pin(client.get::<Value>("only-on-owner"));
    let request = request_for(&mut owner, &mut get).await;
    assert_eq!(request.kind, "get_buffer");
    assert_eq!(request.pool.as_deref(), Some(POOL));
    owner.reply(&request, "ack", json!({"exists": false})).await;
    assert_eq!(bounded(get).await.unwrap(), None);
    finish(client, owner).await;
}

#[tokio::test]
async fn final_join_ack_and_following_push_in_one_write_keep_buffered_input() {
    let owner = listener().await;
    let port = owner.local_addr().unwrap().port();
    let (client, peer) = bounded(async {
        tokio::join!(builder(port).connect(), async {
            let mut peer = Peer::accept(&owner).await;
            peer.hello().await;
            let join = peer.membership(POOL, None).await;
            let mut bytes =
                serde_json::to_vec(&response(&join, "ack", json!({"pool": POOL}))).unwrap();
            bytes.push(b'\n');
            bytes.extend(serde_json::to_vec(&presence(POOL, "buffered-after-join")).unwrap());
            bytes.push(b'\n');
            bounded(peer.writer.write_all(&bytes)).await.unwrap();
            peer
        })
    })
    .await;
    let client = client.unwrap();
    let mut events = client.events();
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Presence(value) if value.client_id == "buffered-after-join")
    );
    finish(client, peer).await;
}

async fn hop_chain(redirects: usize, limit: Option<usize>) {
    let mut listeners = Vec::new();
    for _ in 0..=redirects {
        listeners.push(listener().await);
    }
    let ports: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    let maximum = limit.unwrap_or(4);
    let builder = limit.map_or_else(
        || builder(ports[0]),
        |limit| builder(ports[0]).max_redirects(limit),
    );
    let (result, peer) = bounded(async {
        tokio::join!(builder.connect(), async {
            for index in 0..=redirects.min(maximum) {
                let mut peer = Peer::accept(&listeners[index]).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                if index == redirects {
                    peer.reply(&join, "ack", json!({"pool": POOL})).await;
                    return Some(peer);
                }
                peer.send(&redirect(&join, ports[index + 1], POOL)).await;
                peer.closed().await;
            }
            None
        })
    })
    .await;
    if redirects <= maximum {
        finish(result.unwrap(), peer.unwrap()).await;
    } else {
        assert!(matches!(result, Err(Error::Protocol(message)) if message.contains("limit")));
        assert!(peer.is_none());
        let mut accept = Box::pin(listeners[maximum + 1].accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn default_four_hops_and_explicit_redirect_limits_are_bounded() {
    for (hops, limit) in [
        (4, None),
        (5, None),
        (2, Some(2)),
        (3, Some(2)),
        (1, Some(0)),
        (5, Some(5)),
    ] {
        hop_chain(hops, limit).await;
    }
}

#[tokio::test]
async fn unsafe_malformed_redirect_fields_fail_before_target_connection() {
    for (field, value) in [
        ("protocol", json!("other")),
        ("pool", json!("beta")),
        ("host", json!("localhost")),
        ("host", json!("example.invalid")),
        ("host", json!("192.0.2.1")),
        ("host", json!("0.0.0.0")),
        ("host", json!("::ffff:127.0.0.1")),
        ("host", json!("127.1")),
        ("port", json!(0)),
        ("port", json!(65536)),
        ("port", json!(-1)),
        ("port", json!(true)),
        ("port", json!(123.0)),
        ("port", json!("123")),
        ("ws_port", json!(0)),
        ("router_port", json!(0)),
        ("router_ws_port", json!(false)),
        ("router_host", json!("remote.invalid")),
        ("pod_index", json!(-1)),
        ("pod_index", json!(4)),
        ("pod_index", json!(0.0)),
        ("pod_index", json!(false)),
        ("pod_count", json!(0)),
        ("pod_count", json!(65)),
        ("pod_count", json!(4.0)),
        ("pod_count", json!(true)),
        ("cluster_id", json!("")),
        ("cluster_id", Value::Null),
    ] {
        let router = listener().await;
        let owner = listener().await;
        let port = router.local_addr().unwrap().port();
        let owner_port = owner.local_addr().unwrap().port();
        let (result, ()) = bounded(async {
            tokio::join!(builder(port).connect(), async {
                let mut peer = Peer::accept(&router).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                let mut redirect = redirect(&join, owner_port, POOL);
                redirect.payload[field] = value;
                peer.send(&redirect).await;
                peer.closed().await;
            })
        })
        .await;
        assert!(
            matches!(result, Err(Error::Protocol(_))),
            "accepted invalid field {field}"
        );
        let mut accept = Box::pin(owner.accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn redirect_envelope_requires_requested_client_and_pool() {
    for wrong_client in [false, true] {
        let router = listener().await;
        let owner = listener().await;
        let port = router.local_addr().unwrap().port();
        let owner_port = owner.local_addr().unwrap().port();
        let (result, ()) = bounded(async {
            tokio::join!(builder(port).connect(), async {
                let mut peer = Peer::accept(&router).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                let mut redirect = redirect(&join, owner_port, POOL);
                if wrong_client {
                    redirect.client_id = Some("other".to_owned());
                } else {
                    redirect.pool = Some("other".to_owned());
                }
                peer.send(&redirect).await;
                peer.closed().await;
            })
        })
        .await;
        assert!(matches!(result, Err(Error::Protocol(_))));
        let mut accept = Box::pin(owner.accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn localhost_alias_and_two_endpoint_cycles_are_rejected_without_reconnect() {
    for two_hops in [false, true] {
        let first = listener().await;
        let second = listener().await;
        let first_port = first.local_addr().unwrap().port();
        let second_port = second.local_addr().unwrap().port();
        let (result, ()) = bounded(async {
            tokio::join!(builder(first_port).host("localhost").connect(), async {
                let mut peer = Peer::accept(&first).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                peer.send(&redirect(
                    &join,
                    if two_hops { second_port } else { first_port },
                    POOL,
                ))
                .await;
                peer.closed().await;
                if two_hops {
                    let mut peer = Peer::accept(&second).await;
                    peer.hello().await;
                    let join = peer.membership(POOL, None).await;
                    peer.send(&redirect(&join, first_port, POOL)).await;
                    peer.closed().await;
                }
            })
        })
        .await;
        assert!(matches!(result, Err(Error::Protocol(message)) if message.contains("cycle")));
        let mut accept = Box::pin(first.accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn ownership_metadata_changes_within_chain_are_not_authentication() {
    for (field, value) in [
        ("cluster_id", json!("another")),
        ("pod_count", json!(3)),
        ("router_host", json!("127.0.0.2")),
        ("router_port", json!(12346)),
        ("router_ws_port", json!(12347)),
    ] {
        let first = listener().await;
        let second = listener().await;
        let target = listener().await;
        let port = first.local_addr().unwrap().port();
        let second_port = second.local_addr().unwrap().port();
        let target_port = target.local_addr().unwrap().port();
        let (result, ()) = bounded(async {
            tokio::join!(builder(port).connect(), async {
                let mut peer = Peer::accept(&first).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                peer.send(&redirect(&join, second_port, POOL)).await;
                peer.closed().await;
                let mut peer = Peer::accept(&second).await;
                peer.hello().await;
                let join = peer.membership(POOL, None).await;
                let mut redirect = redirect(&join, target_port, POOL);
                redirect.payload[field] = value;
                peer.send(&redirect).await;
                peer.closed().await;
            })
        })
        .await;
        assert!(matches!(result, Err(Error::Protocol(_))));
        let mut accept = Box::pin(target.accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn hello_and_uncorrelated_membership_redirects_do_not_move_transport() {
    let router = listener().await;
    let owner = listener().await;
    let port = router.local_addr().unwrap().port();
    let owner_port = owner.local_addr().unwrap().port();
    let (client, peer) = bounded(async {
        tokio::join!(builder(port).connect(), async {
            let mut peer = Peer::accept(&router).await;
            let hello = peer.read().await;
            peer.send(&redirect(&hello, owner_port, POOL)).await;
            peer.reply(&hello, "ack", json!({})).await;
            let join = peer.membership(POOL, None).await;
            let mut wrong_id = redirect(&join, owner_port, POOL);
            wrong_id.request_id = Some("not-the-pending-membership".to_owned());
            peer.send(&wrong_id).await;
            peer.reply(&join, "ack", json!({"pool": POOL})).await;
            peer
        })
    })
    .await;
    let mut accept = Box::pin(owner.accept());
    poll_pending(accept.as_mut()).await;
    finish(client.unwrap(), peer).await;
}

#[tokio::test]
async fn rpc_and_unsolicited_redirects_never_replay_or_replace_transport() {
    let (client, mut peer) = pair(|builder| builder).await;
    let owner = listener().await;
    let owner_port = owner.local_addr().unwrap().port();
    let mut events = client.events();
    let data = json!({"effect": "once"});
    let mut call =
        Box::pin(client.call_app_with_options::<_, Value>("remote", "effect", &data, WAIT, None));
    let request = request_for(&mut peer, &mut call).await;
    peer.send(&redirect(&request, owner_port, POOL)).await;
    peer.send(&presence(POOL, "processed-redirect")).await;
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Unknown(message) if message.kind == "redirect")
    );
    assert!(matches!(
        bounded(events.recv()).await.unwrap(),
        ClientEvent::Presence(_)
    ));
    poll_pending(call.as_mut()).await;
    peer.reply(&request, "app_result", json!({"value": 42, "error": null}))
        .await;
    assert_eq!(bounded(call).await.unwrap(), CallOutcome::Result(json!(42)));
    let mut accept = Box::pin(owner.accept());
    poll_pending(accept.as_mut()).await;
    finish(client, peer).await;
}

#[tokio::test]
async fn definitive_membership_ack_cannot_be_replaced_by_late_duplicate_redirect() {
    let (client, mut peer) = pair(|builder| builder).await;
    let owner = listener().await;
    let port = owner.local_addr().unwrap().port();
    let mut events = client.events();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut peer, &mut switch).await;
    // Keep the switch unpolled until the reader consumes all three frames.
    // Its ACK is definitive even though its pending slot has not dropped yet.
    peer.reply(&request, "ack", json!({"pool": "beta"})).await;
    peer.send(&redirect(&request, port, "beta")).await;
    peer.send(&presence("beta", "duplicate-fenced")).await;
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Presence(value) if value.client_id == "duplicate-fenced")
    );
    bounded(switch).await.unwrap();
    assert!(client.is_connected());
    assert_eq!(client.pool_name().await, "beta");
    let mut accept = Box::pin(owner.accept());
    poll_pending(accept.as_mut()).await;
    finish(client, peer).await;
}

#[tokio::test]
async fn redirect_hops_share_the_original_connect_deadline() {
    let router = listener().await;
    let owner = listener().await;
    let port = router.local_addr().unwrap().port();
    let owner_port = owner.local_addr().unwrap().port();
    let started = time::Instant::now();
    let mut connect = Box::pin(builder(port).timeout(Duration::from_millis(700)).connect());
    let mut first = accept_connect(&router, &mut connect).await;
    let hello = next_connect(&mut first, &mut connect).await;
    first.reply(&hello, "ack", json!({})).await;
    let join = next_connect(&mut first, &mut connect).await;
    assert!(
        time::timeout_at(started + Duration::from_millis(450), connect.as_mut())
            .await
            .is_err()
    );
    first.send(&redirect(&join, owner_port, POOL)).await;
    let mut target = accept_connect(&owner, &mut connect).await;
    let hello = next_connect(&mut target, &mut connect).await;
    target.reply(&hello, "ack", json!({})).await;
    let join = next_connect(&mut target, &mut connect).await;
    assert_eq!(join.kind, "join_pool");
    let result = time::timeout_at(started + Duration::from_millis(950), connect)
        .await
        .expect("deadline reset at redirect");
    assert!(matches!(result, Err(Error::Timeout { .. })));
    first.closed().await;
    target.closed().await;
}

#[tokio::test]
async fn switch_handoff_hello_join_share_the_original_switch_deadline() {
    let (client, mut old) = pair(|builder| builder.timeout(Duration::from_millis(700))).await;
    let owner = listener().await;
    let port = owner.local_addr().unwrap().port();
    let started = time::Instant::now();
    let mut switch = Box::pin(client.switch_pool("beta", None));
    let request = request_for(&mut old, &mut switch).await;
    assert!(
        time::timeout_at(started + Duration::from_millis(450), switch.as_mut())
            .await
            .is_err()
    );
    old.send(&redirect(&request, port, "beta")).await;
    let mut target = bounded(async {
        tokio::select! {
            _ = switch.as_mut() => panic!("switch ended before owner"),
            peer = Peer::accept(&owner) => peer,
        }
    })
    .await;
    let hello = request_for(&mut target, &mut switch).await;
    target.reply(&hello, "ack", json!({})).await;
    let join = request_for(&mut target, &mut switch).await;
    assert_eq!(join.kind, "join_pool");
    let result = time::timeout_at(started + Duration::from_millis(950), switch)
        .await
        .expect("switch reset the redirect deadline");
    assert!(matches!(result, Err(Error::Timeout { .. })));
    assert!(!client.is_connected());
    old.closed().await;
    target.closed().await;
}

#[tokio::test]
async fn rejected_switch_redirect_closes_uncertain_old_membership_without_target_connect() {
    for invalid in ["disabled", "zero-port", "cycle", "wrong-pool"] {
        let (client, mut old) =
            pair(|builder| builder.max_redirects(if invalid == "disabled" { 0 } else { 4 })).await;
        let clone = client.clone();
        let owner = listener().await;
        let owner_port = owner.local_addr().unwrap().port();
        let mut events = clone.events();
        let mut switch = Box::pin(client.switch_pool("beta", None));
        let request = request_for(&mut old, &mut switch).await;
        let mut redirect = redirect(&request, owner_port, "beta");
        match invalid {
            "zero-port" => redirect.payload["port"] = json!(0),
            "cycle" => redirect.payload["port"] = json!(old.writer.local_addr().unwrap().port()),
            "wrong-pool" => redirect.payload["pool"] = json!("other"),
            _ => {}
        }
        old.send(&redirect).await;
        assert!(matches!(bounded(switch).await, Err(Error::Protocol(_))));
        assert!(!client.is_connected() && !clone.is_connected());
        assert_eq!(clone.pool_name().await, POOL);
        assert!(matches!(
            bounded(events.recv()).await.unwrap(),
            ClientEvent::Disconnected
        ));
        old.closed().await;
        let mut accept = Box::pin(owner.accept());
        poll_pending(accept.as_mut()).await;
    }
}

struct Dropped(mpsc::UnboundedSender<()>);
impl Drop for Dropped {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

#[tokio::test]
async fn switch_updates_all_clones_and_event_api_without_old_reply_or_rpc_replay() {
    let (client, mut old) = pair(|builder| builder).await;
    let clone = client.clone();
    let namespace = clone.namespace("shared");
    let mut events = clone.events();
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (dropped_tx, mut dropped) = mpsc::unbounded_channel();
    let release = Arc::new(Semaphore::new(0));
    let handler_release = release.clone();
    client
        .on_event("held", move |_| {
            let started = started_tx.clone();
            let dropped = dropped_tx.clone();
            let release = handler_release.clone();
            async move {
                let _guard = Dropped(dropped);
                started.send(()).unwrap();
                release.acquire().await.unwrap().forget();
                Ok::<_, String>("old-result")
            }
        })
        .await;
    old.send(&invocation(POOL, "old-opaque-hop", "held")).await;
    assert_eq!(bounded(started.recv()).await, Some(()));
    let data = json!({});
    let mut pending =
        Box::pin(clone.call_app_with_options::<_, Value>("remote", "effect", &data, WAIT, None));
    let effect = request_for(&mut old, &mut pending).await;
    old.reply(&effect, "ack", json!({"queued": true})).await;
    let owner = listener().await;
    let port = owner.local_addr().unwrap().port();
    let mut switch = Box::pin(client.switch_pool("beta", Some("new-token")));
    let request = request_for(&mut old, &mut switch).await;
    assert_eq!(request.kind, "switch_pool");
    assert_eq!(request.payload["auth_token"], "new-token");
    assert_eq!(bounded(dropped.recv()).await, Some(()));
    let message = redirect(&request, port, "beta");
    old.send(&message).await;
    let mut target = bounded(async {
        tokio::select! {
            _ = switch.as_mut() => panic!("switch completed before owner join"),
            target = Peer::accept(&owner) => target,
        }
    })
    .await;
    let (result, ()) =
        bounded(async { tokio::join!(switch, target.joined("beta", Some("new-token"))) }).await;
    result.unwrap();
    old.closed().await;
    assert!(matches!(bounded(pending).await, Err(Error::Disconnected)));
    assert!(client.is_connected() && clone.is_connected());
    assert_eq!(client.pool_name().await, "beta");
    assert_eq!(clone.pool_name().await, "beta");
    release.add_permits(1);
    target.send(&presence("beta", "new-generation")).await;
    assert!(
        matches!(bounded(events.recv()).await.unwrap(), ClientEvent::Presence(value) if value.pool == "beta")
    );
    let mut get = Box::pin(namespace.get::<Value>("fence"));
    let request = request_for(&mut target, &mut get).await;
    assert_eq!(
        request.kind, "get_buffer",
        "old result or effect replay escaped to owner"
    );
    assert_eq!(request.payload["key"], "shared:fence");
    assert_eq!(request.pool.as_deref(), Some("beta"));
    target
        .reply(&request, "ack", json!({"exists": false}))
        .await;
    assert_eq!(bounded(get).await.unwrap(), None);
    drop(namespace);
    drop(clone);
    finish(client, target).await;
}

#[tokio::test]
async fn same_pool_owner_rejoin_retains_process_handler_and_pending_route() {
    let (client, mut peer, _) = redirected_pair().await;
    let mut register = Box::pin(client.register_process(
        "job",
        ProcessOptions::default(),
        |_| async { Ok::<_, String>("retained") },
    ));
    let request = request_for(&mut peer, &mut register).await;
    peer.reply(&request, "ack", json!({"process_id": format!("{ID}:job")}))
        .await;
    bounded(register).await.unwrap();
    let data = json!({});
    let mut pending =
        Box::pin(client.call_app_with_options::<_, Value>("remote", "held", &data, WAIT, None));
    let call = request_for(&mut peer, &mut pending).await;
    peer.reply(&call, "ack", json!({"queued": true})).await;
    let mut switch = Box::pin(client.switch_pool(POOL, Some("same-pool-token")));
    let request = request_for(&mut peer, &mut switch).await;
    peer.reply(&request, "ack", json!({"pool": POOL})).await;
    bounded(switch).await.unwrap();
    peer.reply(&call, "app_result", json!({"value": 7, "error": null}))
        .await;
    assert_eq!(
        bounded(pending).await.unwrap(),
        CallOutcome::Result(json!(7))
    );
    peer.send(&invocation(
        POOL,
        "retained-opaque-hop",
        &format!("{ID}:job"),
    ))
    .await;
    let result = peer.application().await;
    assert_eq!(result.kind, "app_result");
    assert_eq!(result.request_id.as_deref(), Some("retained-opaque-hop"));
    assert_eq!(result.payload, json!({"value": "retained", "error": null}));
    finish(client, peer).await;
}

#[tokio::test]
async fn final_owner_authentication_denial_is_typed_and_switch_closes_uncertain_state() {
    for switching in [false, true] {
        let owner = listener().await;
        let owner_port = owner.local_addr().unwrap().port();
        if switching {
            let (client, mut old) = pair(|builder| builder).await;
            let clone = client.clone();
            let mut events = clone.events();
            let mut switch = Box::pin(client.switch_pool("secure", Some("denied")));
            let request = request_for(&mut old, &mut switch).await;
            old.send(&redirect(&request, owner_port, "secure")).await;
            let (result, ()) = bounded(async {
                tokio::join!(switch, async {
                    let mut peer = Peer::accept(&owner).await;
                    peer.hello().await;
                    let join = peer.membership("secure", Some("denied")).await;
                    peer.reply(
                        &join,
                        "error",
                        json!({"code": "auth_failed", "message": "bad pool token"}),
                    )
                    .await;
                    peer.closed().await;
                })
            })
            .await;
            assert!(
                matches!(result, Err(Error::Authentication(message)) if message == "bad pool token")
            );
            assert!(!client.is_connected() && !clone.is_connected());
            assert_eq!(clone.pool_name().await, POOL);
            assert!(matches!(
                bounded(events.recv()).await.unwrap(),
                ClientEvent::Disconnected
            ));
            assert!(matches!(
                events.try_recv(),
                Err(broadcast::error::TryRecvError::Empty)
            ));
            old.closed().await;
        } else {
            let router = listener().await;
            let entry = router.local_addr().unwrap().port();
            let (result, ()) = bounded(async {
                tokio::join!(builder(entry).auth_token("denied").connect(), async {
                    let mut old = Peer::accept(&router).await;
                    old.hello().await;
                    let join = old.membership(POOL, Some("denied")).await;
                    old.send(&redirect(&join, owner_port, POOL)).await;
                    old.closed().await;
                    let mut peer = Peer::accept(&owner).await;
                    peer.hello().await;
                    let join = peer.membership(POOL, Some("denied")).await;
                    peer.reply(
                        &join,
                        "error",
                        json!({"code": "auth_failed", "message": "denied"}),
                    )
                    .await;
                    peer.closed().await;
                })
            })
            .await;
            assert!(matches!(result, Err(Error::Authentication(_))));
        }
    }
}

#[tokio::test]
async fn cancelling_initial_redirect_handshake_drops_socket_without_background_tasks() {
    for during_join in [false, true] {
        let router = listener().await;
        let owner = listener().await;
        let entry = router.local_addr().unwrap().port();
        let owner_port = owner.local_addr().unwrap().port();
        let mut connect = Box::pin(builder(entry).connect());
        let mut first = accept_connect(&router, &mut connect).await;
        let hello = next_connect(&mut first, &mut connect).await;
        first.reply(&hello, "ack", json!({})).await;
        let join = next_connect(&mut first, &mut connect).await;
        first.send(&redirect(&join, owner_port, POOL)).await;
        let mut target = accept_connect(&owner, &mut connect).await;
        let hello = next_connect(&mut target, &mut connect).await;
        if during_join {
            target.reply(&hello, "ack", json!({})).await;
            let join = next_connect(&mut target, &mut connect).await;
            assert_eq!(join.kind, "join_pool");
        }
        drop(connect);
        first.closed().await;
        target.closed().await;
    }
}

#[tokio::test]
async fn cancelling_switch_at_owner_hello_or_join_closes_every_clone_and_transport() {
    for during_join in [false, true] {
        let (client, mut old) = pair(|builder| builder).await;
        let clone = client.clone();
        let mut events = clone.events();
        let owner = listener().await;
        let port = owner.local_addr().unwrap().port();
        let mut switch = Box::pin(client.switch_pool("beta", None));
        let request = request_for(&mut old, &mut switch).await;
        old.send(&redirect(&request, port, "beta")).await;
        let mut target = bounded(async {
            tokio::select! {
                _ = switch.as_mut() => panic!("switch ended before owner"),
                peer = Peer::accept(&owner) => peer,
            }
        })
        .await;
        let hello = request_for(&mut target, &mut switch).await;
        assert_eq!(hello.kind, "hello");
        if during_join {
            target.reply(&hello, "ack", json!({})).await;
            let join = request_for(&mut target, &mut switch).await;
            assert_eq!(join.kind, "join_pool");
        }
        drop(switch);
        assert!(!client.is_connected() && !clone.is_connected());
        assert!(matches!(
            bounded(events.recv()).await.unwrap(),
            ClientEvent::Disconnected
        ));
        old.closed().await;
        target.closed().await;
        assert!(matches!(clone.clients().await, Err(Error::Disconnected)));
    }
}

#[tokio::test]
async fn cancelling_retired_router_handoff_before_owner_dial_closes_shared_state() {
    for pool in [POOL, "beta"] {
        let (client, mut old) = pair(|builder| builder).await;
        let clone = client.clone();
        let owner = listener().await;
        let port = owner.local_addr().unwrap().port();
        let mut events = clone.events();
        let mut switch = Box::pin(client.switch_pool(pool, None));
        let request = request_for(&mut old, &mut switch).await;
        old.send(&redirect(&request, port, pool)).await;
        // Retirement closes the old socket even when the switch future stays
        // unpolled at its membership waiter, before any owner dial.
        old.closed().await;
        drop(switch);
        assert!(!client.is_connected() && !clone.is_connected());
        assert!(matches!(
            bounded(events.recv()).await.unwrap(),
            ClientEvent::Disconnected
        ));
        let mut accept = Box::pin(owner.accept());
        poll_pending(accept.as_mut()).await;
    }
}

#[tokio::test]
async fn redirected_connection_lives_until_last_public_clone_and_cancels_internal_handler() {
    let (client, mut owner, _) = redirected_pair().await;
    let clone = client.clone();
    let (started_tx, mut started) = mpsc::unbounded_channel();
    let (dropped_tx, mut dropped) = mpsc::unbounded_channel();
    client
        .on_event("never", move |_| {
            let started = started_tx.clone();
            let dropped = dropped_tx.clone();
            async move {
                let _guard = Dropped(dropped);
                started.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok::<_, String>(Value::Null)
            }
        })
        .await;
    drop(client);
    let mut operation = Box::pin(clone.clients());
    let request = request_for(&mut owner, &mut operation).await;
    owner.reply(&request, "ack", json!({"clients": [ID]})).await;
    assert_eq!(bounded(operation).await.unwrap(), [ID]);
    owner
        .send(&invocation(POOL, "last-public-handle", "never"))
        .await;
    assert_eq!(bounded(started.recv()).await, Some(()));
    drop(clone);
    assert_eq!(bounded(dropped.recv()).await, Some(()));
    owner.closed().await;
}

#[tokio::test]
async fn redirected_owner_starts_one_metrics_stream_and_closes_it_on_last_handle() {
    let (client, mut peer, _) = redirected_pair().await;
    let effects = Arc::new(AtomicUsize::new(0));
    let counter = effects.clone();
    let mut register =
        Box::pin(
            client.register_process("metrics", ProcessOptions::default(), move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, String>(42) }
            }),
        );
    let request = request_for(&mut peer, &mut register).await;
    peer.reply(
        &request,
        "ack",
        json!({"process_id": format!("{ID}:metrics")}),
    )
    .await;
    bounded(register).await.unwrap();
    peer.send(&invocation(POOL, "metrics-hop", &format!("{ID}:metrics")))
        .await;
    assert_eq!(peer.application().await.payload["value"], 42);
    let metrics = peer.read().await;
    assert_eq!(metrics.kind, "worker_metrics");
    assert_eq!(metrics.payload["metrics"].as_array().unwrap().len(), 1);
    assert_eq!(metrics.payload["metrics"][0]["completed_count"], 1);
    peer.reply(&metrics, "ack", json!({"received": 1})).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    drop(client);
    peer.closed().await;
}
