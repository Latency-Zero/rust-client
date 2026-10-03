use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    future::{Future, poll_fn},
    net::{IpAddr, SocketAddr},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Notify, OwnedSemaphorePermit, RwLock, Semaphore, broadcast, mpsc},
    task::{JoinHandle, JoinSet},
    time,
};
use uuid::Uuid;

use crate::{
    Error, Result,
    protocol::{
        AppResult, BufferEntry, BufferUpdate, EmittedEvent, Message, MessageType, PoolStats,
        PresenceUpdate, ProcessMap, ProcessRegistration, ProcessScale, ScanResult, WorkerKind,
        WorkerMetrics,
    },
};

type HandlerFuture = Pin<Box<dyn Future<Output = std::result::Result<Value, String>> + Send>>;
type Handler = Arc<dyn Fn(Map<String, Value>) -> HandlerFuture + Send + Sync>;

/// Identifier returned when an event handler is installed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventHandlerId(Uuid);

/// Unsolicited messages and lifecycle changes received from the server.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ClientEvent {
    Presence(PresenceUpdate),
    Buffer(BufferUpdate),
    Event(EmittedEvent),
    AppResult {
        request_id: String,
        result: AppResult,
    },
    ProcessScale(ProcessScale),
    HandlerFailed {
        event: String,
        error: String,
    },
    Disconnected,
    Unknown(Message),
}

/// Result of a call that may route its response to a third client.
#[derive(Debug, Clone, PartialEq)]
pub enum CallOutcome<T> {
    Result(T),
    Routed { request_id: String },
}

/// Options for a server-side process registration and its local task pool.
#[derive(Debug, Clone)]
pub struct ProcessOptions {
    pub scale: bool,
    pub max_replicas: usize,
    pub group_id: Option<String>,
    pub worker_kind: WorkerKind,
    pub min_workers: usize,
    pub max_workers: usize,
}

impl Default for ProcessOptions {
    fn default() -> Self {
        Self {
            scale: false,
            max_replicas: 10,
            group_id: None,
            worker_kind: WorkerKind::Thread,
            min_workers: 1,
            max_workers: 10,
        }
    }
}

/// Builder for [`Client`].
#[derive(Debug, Clone)]
pub struct ClientBuilder {
    dsn: String,
    pool: String,
    auth_token: Option<String>,
    host: String,
    port: u16,
    timeout: Duration,
    event_capacity: usize,
    max_pending_requests: usize,
    writer_capacity: usize,
    max_queued_bytes: usize,
    max_frame_bytes: usize,
    max_handler_tasks: usize,
    max_handler_bytes: usize,
    max_batch_size: usize,
    control_reserve: usize,
    control_reserve_bytes: usize,
    write_timeout: Duration,
    shutdown_timeout: Duration,
    max_redirects: usize,
}

impl ClientBuilder {
    #[must_use]
    pub fn new(dsn: impl Into<String>, pool: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            pool: pool.into(),
            auth_token: None,
            host: "127.0.0.1".to_owned(),
            port: 14_130,
            timeout: Duration::from_secs(5),
            event_capacity: 256,
            max_pending_requests: 256,
            writer_capacity: 256,
            max_queued_bytes: 1024 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_handler_tasks: 256,
            max_handler_bytes: 1024 * 1024,
            max_batch_size: 256,
            control_reserve: 32,
            control_reserve_bytes: 64 * 1024,
            write_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(1),
            max_redirects: 4,
        }
    }

    #[must_use]
    pub fn auth_token(mut self, auth_token: impl Into<String>) -> Self {
        self.auth_token = Some(auth_token.into());
        self
    }

    #[must_use]
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    #[must_use]
    pub const fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    #[must_use]
    pub const fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub const fn event_capacity(mut self, capacity: usize) -> Self {
        self.event_capacity = capacity;
        self
    }

    #[must_use]
    pub const fn max_pending_requests(mut self, limit: usize) -> Self {
        self.max_pending_requests = limit;
        self
    }

    #[must_use]
    pub const fn writer_capacity(mut self, limit: usize) -> Self {
        self.writer_capacity = limit;
        self
    }

    #[must_use]
    pub const fn max_queued_bytes(mut self, limit: usize) -> Self {
        self.max_queued_bytes = limit;
        self
    }

    #[must_use]
    pub const fn max_frame_bytes(mut self, limit: usize) -> Self {
        self.max_frame_bytes = limit;
        self
    }

    #[must_use]
    pub const fn max_handler_tasks(mut self, limit: usize) -> Self {
        self.max_handler_tasks = limit;
        self
    }

    #[must_use]
    pub const fn max_handler_bytes(mut self, limit: usize) -> Self {
        self.max_handler_bytes = limit;
        self
    }

    #[must_use]
    pub const fn max_batch_size(mut self, limit: usize) -> Self {
        self.max_batch_size = limit;
        self
    }

    #[must_use]
    pub const fn control_reserve(mut self, limit: usize) -> Self {
        self.control_reserve = limit;
        self
    }

    #[must_use]
    pub const fn control_reserve_bytes(mut self, limit: usize) -> Self {
        self.control_reserve_bytes = limit;
        self
    }

    #[must_use]
    pub const fn write_timeout(mut self, timeout: Duration) -> Self {
        self.write_timeout = timeout;
        self
    }

    #[must_use]
    pub const fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Bound local pool-owner redirects per connect or explicit pool switch.
    /// Defaults to four; accepts zero through sixteen. Zero disables following
    /// redirects. No application request is replayed.
    #[must_use]
    pub const fn max_redirects(mut self, limit: usize) -> Self {
        self.max_redirects = limit;
        self
    }

    /// Open TCP, perform `hello`, and join, sharing one deadline across all hops.
    pub async fn connect(self) -> Result<Client> {
        let client_id = parse_dsn(&self.dsn)?;
        require_nonempty("pool", &self.pool)?;
        if self.host.is_empty()
            || self.timeout.is_zero()
            || self.write_timeout.is_zero()
            || self.shutdown_timeout.is_zero()
        {
            return Err(Error::Protocol(
                "host and timeouts must be nonempty/positive".to_owned(),
            ));
        }
        for (name, limit) in [
            ("event_capacity", self.event_capacity),
            ("max_pending_requests", self.max_pending_requests),
            ("writer_capacity", self.writer_capacity),
            ("max_queued_bytes", self.max_queued_bytes),
            ("max_frame_bytes", self.max_frame_bytes),
            ("max_handler_tasks", self.max_handler_tasks),
            ("max_handler_bytes", self.max_handler_bytes),
            ("max_batch_size", self.max_batch_size),
            ("control_reserve", self.control_reserve),
            ("control_reserve_bytes", self.control_reserve_bytes),
        ] {
            if limit == 0 || limit > Semaphore::MAX_PERMITS {
                return Err(Error::Protocol(format!(
                    "{name} must be positive and within Tokio's capacity limit"
                )));
            }
        }
        if self.max_redirects > 16 {
            return Err(Error::Protocol("max_redirects must be between 0 and 16".to_owned()));
        }
        deadline(self.write_timeout)?;
        deadline(self.shutdown_timeout)?;
        let deadline = deadline(self.timeout)?;

        let channel_capacity = self
            .writer_capacity
            .checked_add(self.control_reserve)
            .filter(|limit| *limit <= Semaphore::MAX_PERMITS)
            .ok_or_else(|| {
                Error::Protocol("writer capacity plus control reserve is too large".to_owned())
            })?;
        let entry_endpoint = Endpoint {
            host: self.host.clone(),
            port: self.port,
        };
        let options = HandshakeOptions {
            deadline,
            timeout: self.timeout,
            write_timeout: self.write_timeout,
            max_frame_bytes: self.max_frame_bytes,
            max_queued_bytes: self.max_queued_bytes,
        };
        let connection = open_pool_connection(
            entry_endpoint.clone(),
            &client_id,
            &self.pool,
            self.auth_token.as_deref(),
            &options,
            &mut RedirectChain::new(&self.host, self.max_redirects),
        )
        .await?;
        let (event_sender, _) = broadcast::channel(self.event_capacity);

        let client = Client {
            inner: Arc::new(Inner {
                client_id,
                pool: StdMutex::new(self.pool.clone()),
                auth_token: StdMutex::new(self.auth_token.clone()),
                timeout: self.timeout,
                write_timeout: self.write_timeout,
                shutdown_timeout: self.shutdown_timeout,
                max_pending_requests: self.max_pending_requests,
                max_frame_bytes: self.max_frame_bytes,
                max_batch_size: self.max_batch_size,
                max_queued_bytes: self.max_queued_bytes,
                control_reserve_bytes: self.control_reserve_bytes,
                max_handler_bytes: self.max_handler_bytes,
                entry_endpoint,
                max_redirects: self.max_redirects,
                writer_channel_capacity: channel_capacity,
                writer_slots: Arc::new(Semaphore::new(self.writer_capacity)),
                handler_slots: Arc::new(Semaphore::new(self.max_handler_tasks)),
                queued_bytes: Arc::new(AtomicUsize::new(0)),
                handler_bytes: Arc::new(AtomicUsize::new(0)),
                transport: StdMutex::new(Transport {
                    endpoint: connection.endpoint.clone(),
                    peer_addr: connection.peer_addr,
                    sender: None,
                }),
                pending: StdMutex::new(HashMap::new()),
                event_handlers: RwLock::new(HashMap::new()),
                processes: StdMutex::new(HashMap::new()),
                events: event_sender,
                connected: AtomicBool::new(true),
                closing: AtomicBool::new(false),
                generation: AtomicU64::new(0),
                connection_generation: AtomicU64::new(0),
                switching: AtomicBool::new(false),
                operation_gate: RwLock::new(()),
                transition_slots: Arc::new(Semaphore::new(self.max_pending_requests)),
                registration_gate: tokio::sync::Mutex::new(()),
                handler_tasks: StdMutex::new(JoinSet::new()),
                reader_task: StdMutex::new(None),
                writer_task: StdMutex::new(None),
                metrics_task: StdMutex::new(None),
            }),
            owner: None,
        };
        let mut client = client;
        client.owner = Some(Arc::new(ConnectionOwner(Arc::downgrade(&client.inner))));

        client.attach_transport(connection, &self.pool, self.auth_token.as_deref())?;

        Ok(client)
    }
}

/// Cloneable, asynchronous client for one `latzero-server` connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
    // Only public handles own the connection. Internal handler tasks must not
    // keep a socket alive after the last application handle is dropped.
    owner: Option<Arc<ConnectionOwner>>,
}

struct ConnectionOwner(Weak<Inner>);

impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        if let Some(inner) = self.0.upgrade() {
            inner.close();
        }
    }
}

struct Inner {
    client_id: String,
    pool: StdMutex<String>,
    auth_token: StdMutex<Option<String>>,
    timeout: Duration,
    write_timeout: Duration,
    shutdown_timeout: Duration,
    max_pending_requests: usize,
    max_frame_bytes: usize,
    max_batch_size: usize,
    max_queued_bytes: usize,
    control_reserve_bytes: usize,
    max_handler_bytes: usize,
    entry_endpoint: Endpoint,
    max_redirects: usize,
    writer_channel_capacity: usize,
    writer_slots: Arc<Semaphore>,
    handler_slots: Arc<Semaphore>,
    queued_bytes: Arc<AtomicUsize>,
    handler_bytes: Arc<AtomicUsize>,
    transport: StdMutex<Transport>,
    pending: StdMutex<HashMap<String, PendingSlot>>,
    event_handlers: RwLock<HashMap<String, Vec<(EventHandlerId, Handler)>>>,
    processes: StdMutex<HashMap<String, Arc<ProcessRuntime>>>,
    events: broadcast::Sender<ClientEvent>,
    connected: AtomicBool,
    closing: AtomicBool,
    generation: AtomicU64,
    connection_generation: AtomicU64,
    switching: AtomicBool,
    operation_gate: RwLock<()>,
    transition_slots: Arc<Semaphore>,
    registration_gate: tokio::sync::Mutex<()>,
    handler_tasks: StdMutex<JoinSet<()>>,
    reader_task: StdMutex<Option<JoinHandle<()>>>,
    writer_task: StdMutex<Option<JoinHandle<()>>>,
    metrics_task: StdMutex<Option<JoinHandle<()>>>,
}

impl Inner {
    fn close(&self) {
        let mut transport = lock(&self.transport);
        self.close_locked(&mut transport);
    }

    fn close_connection(&self, generation: u64) {
        let mut transport = lock(&self.transport);
        if self.connection_generation.load(Ordering::Acquire) == generation {
            self.close_locked(&mut transport);
        }
    }

    fn close_work_generation(&self, generation: u64) {
        let mut transport = lock(&self.transport);
        if self.generation.load(Ordering::Acquire) == generation {
            self.close_locked(&mut transport);
        }
    }

    fn close_locked(&self, transport: &mut Transport) {
        self.closing.store(true, Ordering::Release);
        if self.connected.swap(false, Ordering::AcqRel) {
            transport.sender.take();
            lock(&self.pending).clear();
            self.writer_slots.close();
            self.handler_slots.close();
            self.transition_slots.close();
            lock(&self.handler_tasks).abort_all();
            for process in lock(&self.processes).values() {
                process.close();
            }
            for task in [&self.reader_task, &self.writer_task, &self.metrics_task] {
                if let Some(handle) = lock(task).as_ref() {
                    handle.abort();
                }
            }
            let _ = self.events.send(ClientEvent::Disconnected);
        }
    }

    fn connection_is_current(&self, generation: u64) -> bool {
        self.connected.load(Ordering::Acquire)
            && self.connection_generation.load(Ordering::Acquire) == generation
    }

    fn retire_connection(&self, generation: u64) -> bool {
        let mut transport = lock(&self.transport);
        if !self.connection_is_current(generation) {
            return false;
        }
        self.connection_generation.fetch_add(1, Ordering::AcqRel);
        self.switching.store(true, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        lock(&self.handler_tasks).abort_all();
        for process in lock(&self.processes).values() {
            process.close();
        }
        lock(&self.processes).clear();
        transport.sender.take();
        lock(&self.pending).clear();
        for stored in [&self.reader_task, &self.writer_task] {
            if let Some(task) = lock(stored).as_ref() {
                task.abort();
            }
        }
        true
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        lock(&self.handler_tasks).abort_all();
        for task in [&self.reader_task, &self.writer_task, &self.metrics_task] {
            if let Some(handle) = lock(task).take() {
                handle.abort();
            }
        }
    }
}

enum WriterCommand {
    Frame {
        bytes: Vec<u8>,
        deadline: Instant,
        generation: u64,
        connection_generation: u64,
        request: Option<Arc<AtomicUsize>>,
        _slot: Option<OwnedSemaphorePermit>,
        _bytes: ByteReservation,
    },
}

struct ByteReservation {
    used: Arc<AtomicUsize>,
    bytes: usize,
}

impl ByteReservation {
    fn acquire(
        used: &Arc<AtomicUsize>,
        bytes: usize,
        limit: usize,
        resource: &'static str,
    ) -> Result<Self> {
        used.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            current
                .checked_add(bytes)
                .filter(|updated| *updated <= limit)
        })
        .map_err(|_| Error::Overloaded { resource })?;
        Ok(Self {
            used: Arc::clone(used),
            bytes,
        })
    }
}

impl Drop for ByteReservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

struct PendingSlot {
    sender: mpsc::Sender<Message>,
    kind: MessageType,
    generation: u64,
    connection_generation: u64,
    acknowledgement: bool,
    terminal: bool,
    membership: Option<(String, Option<String>)>,
    expects_result: bool,
}

const REDIRECT_PROTOCOL: &str = "pool_redirect_v1";

#[derive(Clone)]
struct Endpoint {
    host: String,
    port: u16,
}

impl Endpoint {
    fn address(&self) -> String {
        match self.host.parse::<IpAddr>() {
            Ok(ip) => SocketAddr::new(ip, self.port).to_string(),
            Err(_) => format!("{}:{}", self.host, self.port),
        }
    }
}

struct Transport {
    endpoint: Endpoint,
    peer_addr: SocketAddr,
    sender: Option<mpsc::Sender<WriterCommand>>,
}

struct PreparedConnection {
    endpoint: Endpoint,
    peer_addr: SocketAddr,
    reader: BufReader<OwnedReadHalf>,
    writer: OwnedWriteHalf,
}

struct HandshakeOptions {
    deadline: Instant,
    timeout: Duration,
    write_timeout: Duration,
    max_frame_bytes: usize,
    max_queued_bytes: usize,
}

struct RedirectChain {
    local_entry: bool,
    limit: usize,
    redirects: usize,
    visited: HashSet<SocketAddr>,
    ownership: Option<(String, u64)>,
}

impl RedirectChain {
    fn new(host: &str, limit: usize) -> Self {
        Self {
            local_entry: host.eq_ignore_ascii_case("localhost")
                || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()),
            limit,
            redirects: 0,
            visited: HashSet::new(),
            ownership: None,
        }
    }

    fn follow(&mut self, message: &Message, client_id: &str, pool: &str) -> Result<Endpoint> {
        let invalid = |field: &str| Error::Protocol(format!("invalid pool redirect: {field}"));
        if message.kind != MessageType::Redirect.as_str()
            || message.client_id.as_deref() != Some(client_id)
            || message.pool.as_deref() != Some(pool)
            || message.payload["protocol"].as_str() != Some(REDIRECT_PROTOCOL)
            || message.payload["pool"].as_str() != Some(pool)
        {
            return Err(invalid("protocol, client, or requested pool mismatch"));
        }
        if !self.local_entry {
            return Err(invalid("configured entry host is not local"));
        }
        let host = message.payload["host"]
            .as_str()
            .ok_or_else(|| invalid("host"))?;
        let ip = host
            .parse::<IpAddr>()
            .ok()
            .filter(IpAddr::is_loopback)
            .ok_or_else(|| invalid("target must be a numeric loopback address"))?;
        let port = redirect_port(&message.payload, "port", false)?.unwrap();
        redirect_port(&message.payload, "ws_port", true)?;
        redirect_port(&message.payload, "router_port", false)?;
        redirect_port(&message.payload, "router_ws_port", true)?;
        message.payload["router_host"]
            .as_str()
            .and_then(|host| host.parse::<IpAddr>().ok())
            .filter(IpAddr::is_loopback)
            .ok_or_else(|| invalid("router_host must be numeric loopback"))?;
        let count = message.payload["pod_count"]
            .as_u64()
            .filter(|count| (1..=64).contains(count))
            .ok_or_else(|| invalid("pod_count must be an integer between 1 and 64"))?;
        if message.payload["pod_index"]
            .as_u64()
            .is_none_or(|index| index >= count)
        {
            return Err(invalid("pod_index must be an integer below pod_count"));
        }
        let cluster = message.payload["cluster_id"]
            .as_str()
            .filter(|cluster| !cluster.is_empty() && cluster.len() <= 512)
            .ok_or_else(|| invalid("cluster_id"))?;
        if self
            .ownership
            .as_ref()
            .is_some_and(|(known, pods)| known != cluster || *pods != count)
        {
            return Err(invalid("cluster or pod count changed within redirect chain"));
        }
        if self.redirects >= self.limit {
            return Err(invalid("redirect limit exceeded"));
        }
        let endpoint = SocketAddr::new(ip, port);
        if !self.visited.insert(endpoint) {
            return Err(invalid("endpoint cycle"));
        }
        self.ownership = Some((cluster.to_owned(), count));
        self.redirects += 1;
        Ok(Endpoint {
            host: ip.to_string(),
            port,
        })
    }
}

fn redirect_port(payload: &Value, name: &str, optional: bool) -> Result<Option<u16>> {
    let value = &payload[name];
    if optional && value.is_null() {
        return Ok(None);
    }
    value
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port != 0)
        .map(Some)
        .ok_or_else(|| Error::Protocol(format!("invalid pool redirect: {name} must be a nonzero u16")))
}

async fn open_pool_connection(
    mut endpoint: Endpoint,
    client_id: &str,
    pool: &str,
    auth_token: Option<&str>,
    options: &HandshakeOptions,
    chain: &mut RedirectChain,
) -> Result<PreparedConnection> {
    loop {
        if Instant::now() >= options.deadline {
            return Err(request_timeout("pool connection", options.timeout));
        }
        let address = endpoint.address();
        let connect = async {
            if endpoint.host.eq_ignore_ascii_case("localhost") {
                // Race the two numeric local addresses: a stalled IPv6 connect
                // must not consume the whole deadline before IPv4 is attempted.
                let ipv4 = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, endpoint.port));
                let ipv6 = TcpStream::connect((std::net::Ipv6Addr::LOCALHOST, endpoint.port));
                tokio::pin!(ipv4, ipv6);
                tokio::select! {
                    result = &mut ipv4 => match result {
                        Ok(stream) => Ok(stream),
                        Err(_) => ipv6.await,
                    },
                    result = &mut ipv6 => match result {
                        Ok(stream) => Ok(stream),
                        Err(_) => ipv4.await,
                    },
                }
            } else {
                TcpStream::connect(&address).await
            }
        };
        let stream = match time::timeout_at(options.deadline.into(), connect).await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(source)) => return Err(Error::Connection { endpoint: address, source }),
            Err(_) => return Err(Error::Connection {
                endpoint: address,
                source: std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out"),
            }),
        };
        let peer_addr = stream.peer_addr()?;
        chain.visited.insert(peer_addr);
        let _ = stream.set_nodelay(true);
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let hello = Message::new(
            MessageType::Hello,
            Some(Uuid::new_v4().to_string()),
            Some(client_id.to_owned()),
            None,
            json!({"client_id": client_id, "capabilities": [REDIRECT_PROTOCOL]}),
        );
        handshake_request(&mut reader, &mut writer, &hello, options).await?;
        let join = Message::new(
            MessageType::JoinPool,
            Some(Uuid::new_v4().to_string()),
            Some(client_id.to_owned()),
            Some(pool.to_owned()),
            json!({"client_id": client_id, "pool": pool, "auth_token": auth_token}),
        );
        let reply = handshake_request(&mut reader, &mut writer, &join, options).await?;
        if reply.kind == MessageType::Redirect.as_str() {
            endpoint = chain.follow(&reply, client_id, pool)?;
            // Drop both halves before opening the owner. No old buffered work
            // or application request is carried over to the new connection.
            drop(reader);
            drop(writer);
            continue;
        }
        return Ok(PreparedConnection { endpoint, peer_addr, reader, writer });
    }
}

async fn handshake_request(
    reader: &mut BufReader<OwnedReadHalf>,
    writer: &mut OwnedWriteHalf,
    request: &Message,
    options: &HandshakeOptions,
) -> Result<Message> {
    let request_id = request.request_id.as_deref().unwrap();
    if Instant::now() >= options.deadline {
        return Err(request_timeout(request_id, options.timeout));
    }
    let mut encoded = serde_json::to_vec(request)?;
    if encoded.len() > options.max_frame_bytes {
        return Err(Error::FrameTooLarge { size: encoded.len(), limit: options.max_frame_bytes });
    }
    encoded.push(b'\n');
    if encoded.len() > options.max_queued_bytes {
        return Err(Error::Overloaded { resource: "writer bytes" });
    }
    let write_deadline = options.deadline.min(deadline(options.write_timeout)?);
    time::timeout_at(write_deadline.into(), writer.write_all(&encoded))
        .await
        .map_err(|_| request_timeout(request_id, options.timeout))??;
    loop {
        if Instant::now() >= options.deadline {
            return Err(request_timeout(request_id, options.timeout));
        }
        let frame = time::timeout_at(options.deadline.into(), read_frame(reader, options.max_frame_bytes))
            .await
            .map_err(|_| request_timeout(request_id, options.timeout))??
            .ok_or(Error::Disconnected)?;
        let message: Message = serde_json::from_slice(&frame)?;
        if message.kind.is_empty() || !(message.payload.is_object() || message.payload.is_null()) {
            return Err(Error::Protocol("invalid handshake envelope".to_owned()));
        }
        if message.request_id.as_deref() == Some(request_id) {
            match message.kind.as_str() {
                "ack" | "error" => return check_response(message),
                "redirect" if request.kind == MessageType::JoinPool.as_str() => return Ok(message),
                _ => {}
            }
        }
    }
}

impl Client {
    /// Connect with the standard host, port, and timeout.
    pub async fn connect(dsn: impl Into<String>, pool: impl Into<String>) -> Result<Self> {
        ClientBuilder::new(dsn, pool).connect().await
    }

    #[must_use]
    pub fn builder(dsn: impl Into<String>, pool: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(dsn, pool)
    }

    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.inner.client_id
    }

    pub async fn pool_name(&self) -> String {
        lock(&self.inner.pool).clone()
    }

    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.inner.connected.load(Ordering::Acquire)
    }

    /// Subscribe to presence, buffer, event, result, scaling, and lifecycle pushes.
    #[must_use]
    pub fn events(&self) -> broadcast::Receiver<ClientEvent> {
        self.inner.events.subscribe()
    }

    /// Switch pools, following bounded local owner redirects on the same client.
    pub async fn switch_pool(
        &self,
        pool: impl Into<String>,
        auth_token: Option<&str>,
    ) -> Result<()> {
        let timeout = self.inner.timeout;
        let deadline = deadline(timeout)?;
        let _admission = Arc::clone(&self.inner.transition_slots)
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded {
                resource: "pool transitions",
            })?;
        let _transition = time::timeout_at(deadline.into(), self.inner.operation_gate.write())
            .await
            .map_err(|_| request_timeout("switch_pool", timeout))?;
        let pool = pool.into();
        require_nonempty("pool", &pool)?;
        if !self.is_connected() || self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::Disconnected);
        }
        let peer_addr = lock(&self.inner.transport).peer_addr;
        let changed = pool != *lock(&self.inner.pool);
        let mut transition = TransitionGuard {
            inner: Arc::clone(&self.inner),
            armed: true,
        };
        if changed {
            self.inner.switching.store(true, Ordering::Release);
            self.inner.generation.fetch_add(1, Ordering::AcqRel);
            lock(&self.inner.pending).clear();
            self.cancel_handlers(deadline).await?;
        }
        let mut result = self
            .request_in_pool(
                MessageType::SwitchPool,
                json!({
                    "client_id": self.inner.client_id,
                    "pool": pool,
                    "auth_token": auth_token,
                }),
                Some(pool.clone()),
                deadline.saturating_duration_since(Instant::now()),
            )
            .await;
        let redirected = result
            .as_ref()
            .is_ok_and(|message| message.kind == MessageType::Redirect.as_str());
        if redirected {
            let options = HandshakeOptions {
                deadline,
                timeout,
                write_timeout: self.inner.write_timeout,
                max_frame_bytes: self.inner.max_frame_bytes,
                max_queued_bytes: self.inner.max_queued_bytes,
            };
            let mut chain =
                RedirectChain::new(&self.inner.entry_endpoint.host, self.inner.max_redirects);
            chain.visited.insert(peer_addr);
            result = async {
                let endpoint = chain.follow(result.as_ref().unwrap(), self.client_id(), &pool)?;
                self.cancel_handlers(deadline).await?;
                self.stop_transport(deadline).await?;
                let connection = open_pool_connection(
                    endpoint,
                    self.client_id(),
                    &pool,
                    auth_token,
                    &options,
                    &mut chain,
                )
                .await?;
                self.attach_transport(connection, &pool, auth_token)?;
                Ok(Message::new(MessageType::Ack, None, None, None, json!({})))
            }
            .await;
        }
        if !matches!(
            result,
            Err(Error::Timeout { .. } | Error::Disconnected | Error::Io(_))
        ) && (!redirected || result.is_ok())
        {
            transition.armed = false;
            self.inner.switching.store(false, Ordering::Release);
        }
        result.map(|_| ())
    }

    /// Leave the pool and close the connection. Calling this more than once is safe.
    pub async fn disconnect(&self) -> Result<()> {
        if !self.is_connected() {
            return Ok(());
        }
        if self.inner.closing.swap(true, Ordering::AcqRel) {
            self.force_close().await;
            return Ok(());
        }
        let timeout = self.inner.shutdown_timeout;
        let deadline = deadline(timeout)?;
        let mut teardown = TransitionGuard {
            inner: Arc::clone(&self.inner),
            armed: true,
        };
        let leave_result =
            match time::timeout_at(deadline.into(), self.inner.operation_gate.write()).await {
                Ok(_transition) if self.is_connected() => self
                    .request_in_pool(
                        MessageType::LeavePool,
                        json!({}),
                        Some(self.pool_name().await),
                        deadline.saturating_duration_since(Instant::now()),
                    )
                    .await
                    .map(|_| ()),
                Ok(_) => Ok(()),
                Err(_) => Err(request_timeout("disconnect", timeout)),
            };
        self.force_close().await;
        teardown.armed = false;
        leave_result
    }

    async fn force_close(&self) {
        self.inner.close();
        let _ = self.cancel_handlers(Instant::now() + self.inner.shutdown_timeout).await;
        for stored in [
            &self.inner.metrics_task,
            &self.inner.writer_task,
            &self.inner.reader_task,
        ] {
            let task = lock(stored).take();
            if let Some(mut task) = task {
                task.abort();
                let _ = time::timeout(self.inner.shutdown_timeout, &mut task).await;
            }
        }
        lock(&self.inner.processes).clear();
    }

    async fn cancel_handlers(&self, deadline: Instant) -> Result<()> {
        let mut tasks = std::mem::take(&mut *lock(&self.inner.handler_tasks));
        tasks.abort_all();
        time::timeout_at(deadline.into(), async {
            while tasks.join_next().await.is_some() {}
        })
        .await
        .map_err(|_| request_timeout("handler quiescence", self.inner.timeout))
    }

    async fn stop_transport(&self, deadline: Instant) -> Result<()> {
        for stored in [&self.inner.reader_task, &self.inner.writer_task] {
            let task = lock(stored).take();
            if let Some(mut task) = task {
                task.abort();
                time::timeout_at(deadline.into(), &mut task)
                    .await
                    .map_err(|_| request_timeout("pool redirect handoff", self.inner.timeout))?
                    .ok();
            }
        }
        if Instant::now() >= deadline {
            return Err(request_timeout("pool redirect handoff", self.inner.timeout));
        }
        Ok(())
    }

    fn attach_transport(
        &self,
        connection: PreparedConnection,
        pool: &str,
        auth_token: Option<&str>,
    ) -> Result<()> {
        let mut transport = lock(&self.inner.transport);
        if !self.is_connected() || self.inner.closing.load(Ordering::Acquire) {
            return Err(Error::Disconnected);
        }
        let mut reader_task = lock(&self.inner.reader_task);
        let mut writer_task = lock(&self.inner.writer_task);
        if reader_task.is_some() || writer_task.is_some() {
            return Err(Error::Protocol("old transport tasks were not reaped".to_owned()));
        }
        let changed = pool != *lock(&self.inner.pool);
        *lock(&self.inner.pool) = pool.to_owned();
        *lock(&self.inner.auth_token) = auth_token.map(str::to_owned);
        if changed {
            for process in lock(&self.inner.processes).values() {
                process.close();
            }
            lock(&self.inner.processes).clear();
        }
        let (sender, receiver) = mpsc::channel(self.inner.writer_channel_capacity);
        transport.endpoint = connection.endpoint;
        transport.peer_addr = connection.peer_addr;
        transport.sender = Some(sender);
        let generation = self.inner.connection_generation.load(Ordering::Acquire);
        *writer_task = Some(tokio::spawn(writer_loop(
            Arc::downgrade(&self.inner),
            connection.writer,
            receiver,
            generation,
        )));
        *reader_task = Some(tokio::spawn(read_loop(
            Arc::downgrade(&self.inner),
            connection.reader,
            generation,
        )));
        let mut metrics_task = lock(&self.inner.metrics_task);
        if metrics_task.is_none() {
            *metrics_task = Some(tokio::spawn(metrics_loop(Arc::downgrade(&self.inner))));
        }
        self.inner.switching.store(false, Ordering::Release);
        Ok(())
    }

    // Buffer operations -------------------------------------------------

    pub async fn set<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> Result<()> {
        self.set_with_options(key, value, None, false).await
    }

    pub async fn set_with_options<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        ttl: Option<Duration>,
        persistent: bool,
    ) -> Result<()> {
        require_nonempty("key", key)?;
        self.request(
            MessageType::SetBuffer,
            json!({
                "key": key,
                "value": serde_json::to_value(value)?,
                "ttl": ttl.map(|value| value.as_secs_f64()),
                "persistent": persistent,
            }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        Ok(self.get_entry(key).await?.map(|entry| entry.value))
    }

    pub async fn get_entry<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<BufferEntry<T>>> {
        require_nonempty("key", key)?;
        let reply = self
            .request(
                MessageType::GetBuffer,
                json!({ "key": key }),
                self.inner.timeout,
            )
            .await?;
        if !reply
            .payload
            .get("exists")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return Ok(None);
        }
        let entry = reply
            .payload
            .get("entry")
            .cloned()
            .ok_or_else(|| Error::Protocol("get_buffer ack omitted entry".to_owned()))?;
        Ok(Some(serde_json::from_value(entry)?))
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        require_nonempty("key", key)?;
        let reply = self
            .request(
                MessageType::GetBuffer,
                json!({ "key": key }),
                self.inner.timeout,
            )
            .await?;
        Ok(reply
            .payload
            .get("exists")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        require_nonempty("key", key)?;
        let reply = self
            .request(
                MessageType::DeleteBuffer,
                json!({ "key": key }),
                self.inner.timeout,
            )
            .await?;
        Ok(reply
            .payload
            .get("deleted")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }

    pub async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let reply = self
            .request(
                MessageType::ListBuffers,
                json!({ "pattern": prefix }),
                self.inner.timeout,
            )
            .await?;
        serde_json::from_value(
            reply
                .payload
                .get("keys")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(Into::into)
    }

    pub async fn clients(&self) -> Result<Vec<String>> {
        let reply = self
            .request(MessageType::ListClients, json!({}), self.inner.timeout)
            .await?;
        serde_json::from_value(
            reply
                .payload
                .get("clients")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(Into::into)
    }

    pub async fn values<T: DeserializeOwned>(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<Option<T>>> {
        let keys = self.keys(prefix).await?;
        self.require_batch(keys.len())?;
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get(&key).await?);
        }
        Ok(values)
    }

    pub async fn items<T: DeserializeOwned>(
        &self,
        prefix: Option<&str>,
    ) -> Result<Vec<(String, Option<T>)>> {
        let keys = self.keys(prefix).await?;
        self.require_batch(keys.len())?;
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self.get(&key).await?;
            items.push((key, value));
        }
        Ok(items)
    }

    pub async fn mset<T: Serialize>(
        &self,
        values: &HashMap<String, T>,
        ttl: Option<Duration>,
        persistent: bool,
    ) -> Result<()> {
        self.require_batch(values.len())?;
        for (key, value) in values {
            self.set_with_options(key, value, ttl, persistent).await?;
        }
        Ok(())
    }

    pub async fn mget<T: DeserializeOwned>(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<T>>> {
        self.require_batch(keys.len())?;
        let mut values = HashMap::with_capacity(keys.len());
        for key in keys {
            values.insert(key.clone(), self.get(key).await?);
        }
        Ok(values)
    }

    pub async fn delete_many(&self, keys: &[String]) -> Result<usize> {
        self.require_batch(keys.len())?;
        let mut deleted = 0;
        for key in keys {
            deleted += usize::from(self.delete(key).await?);
        }
        Ok(deleted)
    }

    pub async fn size(&self) -> Result<usize> {
        Ok(self.keys(None).await?.len())
    }

    pub async fn stats(&self) -> Result<PoolStats> {
        Ok(PoolStats {
            name: self.pool_name().await,
            client_id: self.client_id().to_owned(),
            server_mode: true,
            key_count: self.size().await?,
        })
    }

    pub async fn scan(&self, cursor: usize, count: usize) -> Result<ScanResult> {
        let keys = self.keys(None).await?;
        let start = cursor.min(keys.len());
        let end = start.saturating_add(count).min(keys.len());
        Ok(ScanResult {
            next_cursor: if end < keys.len() { end } else { 0 },
            keys: keys[start..end].to_vec(),
        })
    }

    pub async fn subscribe_buffer(&self, key: &str) -> Result<()> {
        require_nonempty("key", key)?;
        self.request(
            MessageType::SubscribeBuffer,
            json!({ "key": key }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    pub async fn unsubscribe_buffer(&self, key: &str) -> Result<()> {
        require_nonempty("key", key)?;
        self.request(
            MessageType::UnsubscribeBuffer,
            json!({ "key": key }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    #[must_use]
    pub fn namespace(&self, prefix: impl Into<String>) -> Namespace {
        Namespace {
            client: self.clone(),
            prefix: format!("{}:", prefix.into()),
        }
    }

    // Event and app RPC operations -------------------------------------

    pub async fn emit_event<T: Serialize + ?Sized>(
        &self,
        event: &str,
        data: &T,
        target_client_id: Option<&str>,
        response_to: Option<&str>,
    ) -> Result<()> {
        require_nonempty("event", event)?;
        if let Some(target) = target_client_id {
            require_nonempty("target_client_id", target)?;
        }
        if let Some(response) = response_to {
            require_nonempty("response_to", response)?;
        }
        let data = to_object(data)?;
        self.request(
            MessageType::EmitEvent,
            json!({
                "event": event,
                "data": data,
                "target_client_id": target_client_id,
                "response_to": response_to,
            }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    pub async fn call_app<T, R>(&self, target_client_id: &str, event: &str, data: &T) -> Result<R>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        match self
            .call_app_with_options(target_client_id, event, data, self.inner.timeout, None)
            .await?
        {
            CallOutcome::Result(value) => Ok(value),
            CallOutcome::Routed { .. } => Err(Error::Protocol(
                "call was routed to another response client".to_owned(),
            )),
        }
    }

    /// Call another client. If `response_to` names a different client, returns
    /// the request ID after acknowledgement and does not wait for a result.
    pub async fn call_app_with_options<T, R>(
        &self,
        target_client_id: &str,
        event: &str,
        data: &T,
        timeout: Duration,
        response_to: Option<&str>,
    ) -> Result<CallOutcome<R>>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        require_nonempty("target_client_id", target_client_id)?;
        require_nonempty("event", event)?;
        self.call(
            MessageType::CallApp,
            json!({
                "target_client_id": target_client_id, "event": event,
                "data": to_object(data)?, "response_to": response_to,
            }),
            timeout,
            response_to,
        )
        .await
    }

    pub async fn emit_app<T: Serialize + ?Sized>(
        &self,
        target_client_id: &str,
        event: &str,
        data: &T,
        response_to: Option<&str>,
    ) -> Result<()> {
        self.emit_event(event, data, Some(target_client_id), response_to)
            .await
    }

    /// Register an asynchronous handler for emitted events and incoming app calls.
    pub async fn on_event<F, Fut, T, E>(
        &self,
        event: impl Into<String>,
        handler: F,
    ) -> EventHandlerId
    where
        F: Fn(Map<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Serialize,
        E: Display,
    {
        let event = event.into();
        match self.try_on_event(event.clone(), handler).await {
            Ok(id) => id,
            Err(error) => {
                let _ = self.inner.events.send(ClientEvent::HandlerFailed {
                    event,
                    error: error.to_string(),
                });
                // This infallible legacy API has no rejection return value.
                // Fail the connection rather than pretending a handler exists.
                self.inner.close();
                EventHandlerId(Uuid::new_v4())
            }
        }
    }

    /// Install a handler with explicit local registration admission errors.
    pub async fn try_on_event<F, Fut, T, E>(
        &self,
        event: impl Into<String>,
        handler: F,
    ) -> Result<EventHandlerId>
    where
        F: Fn(Map<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Serialize,
        E: Display,
    {
        let event = event.into();
        require_nonempty("event", &event)?;
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let mut handlers = self.inner.event_handlers.write().await;
        if handlers.values().map(Vec::len).sum::<usize>() >= self.inner.max_batch_size {
            return Err(Error::Overloaded {
                resource: "event handlers",
            });
        }
        let id = EventHandlerId(Uuid::new_v4());
        handlers
            .entry(event)
            .or_default()
            .push((id, adapt_handler(handler)));
        Ok(id)
    }

    pub async fn remove_event_handler(&self, event: &str, id: EventHandlerId) -> bool {
        let mut all_handlers = self.inner.event_handlers.write().await;
        let Some(handlers) = all_handlers.get_mut(event) else {
            return false;
        };
        let old_len = handlers.len();
        handlers.retain(|(handler_id, _)| *handler_id != id);
        let removed = handlers.len() != old_len;
        if handlers.is_empty() {
            all_handlers.remove(event);
        }
        removed
    }

    #[must_use]
    pub fn event_emitter(&self, namespace: impl Into<String>) -> EventEmitter {
        EventEmitter {
            client: self.clone(),
            namespace: namespace.into(),
        }
    }

    // Process operations ------------------------------------------------

    pub async fn register_process<F, Fut, T, E>(
        &self,
        name: impl Into<String>,
        options: ProcessOptions,
        handler: F,
    ) -> Result<ProcessRegistration>
    where
        F: Fn(Map<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Serialize,
        E: Display,
    {
        let deadline = deadline(self.inner.timeout)?;
        let _admission = Arc::clone(&self.inner.transition_slots)
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded {
                resource: "registrations",
            })?;
        let _operation = time::timeout_at(deadline.into(), self.inner.operation_gate.read())
            .await
            .map_err(|_| request_timeout("register_process", self.inner.timeout))?;
        let _registration = time::timeout_at(deadline.into(), self.inner.registration_gate.lock())
            .await
            .map_err(|_| request_timeout("register_process", self.inner.timeout))?;
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let name = name.into();
        require_nonempty("process_name", &name)?;
        if let Some(group) = options.group_id.as_deref() {
            require_nonempty("group_id", group)?;
        }
        if options.min_workers == 0
            || options.min_workers > options.max_workers
            || options.max_replicas == 0
            || options.max_workers > Semaphore::MAX_PERMITS
        {
            return Err(Error::Protocol(
                "min_workers must be between 1 and max_workers".to_owned(),
            ));
        }
        let runtime = Arc::new(ProcessRuntime::new(
            name.clone(),
            options.clone(),
            adapt_handler(handler),
        ));
        let previous = {
            let mut processes = lock(&self.inner.processes);
            if !processes.contains_key(&name) && processes.len() >= self.inner.max_batch_size {
                return Err(Error::Overloaded {
                    resource: "registered processes",
                });
            }
            processes.insert(name.clone(), Arc::clone(&runtime))
        };
        if let Some(previous) = previous.as_ref() {
            previous.pause();
        }
        let mut replacement = RegistrationGuard {
            inner: Arc::clone(&self.inner),
            name: name.clone(),
            runtime,
            previous,
            committed: false,
            resolved: false,
        };

        let payload = json!({
            "process_name": name,
            "scale": options.scale,
            "max_replicas": options.max_replicas,
            "group_id": options.group_id,
            "worker_kind": options.worker_kind.as_str(),
            "min_workers": options.min_workers,
            "max_workers": options.max_workers,
        });
        match self
            .request_in_pool(
                MessageType::RegisterProcess,
                payload,
                Some(self.pool_name().await),
                deadline.saturating_duration_since(Instant::now()),
            )
            .await
        {
            Ok(message) => match serde_json::from_value(message.payload) {
                Ok(registration) => {
                    replacement.committed = true;
                    if let Some(previous) = replacement.previous.as_ref() {
                        previous.close();
                    }
                    Ok(registration)
                }
                Err(error) => {
                    self.inner.close();
                    Err(error.into())
                }
            },
            Err(error) => {
                replacement.resolved = true;
                if matches!(
                    error,
                    Error::Timeout { .. } | Error::Io(_) | Error::Disconnected
                ) {
                    self.inner.close();
                }
                Err(error)
            }
        }
    }

    pub async fn unregister_process(&self, name: &str) -> Result<()> {
        let deadline = deadline(self.inner.timeout)?;
        let _admission = Arc::clone(&self.inner.transition_slots)
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded {
                resource: "registrations",
            })?;
        let _operation = time::timeout_at(deadline.into(), self.inner.operation_gate.read())
            .await
            .map_err(|_| request_timeout("unregister_process", self.inner.timeout))?;
        let _registration = time::timeout_at(deadline.into(), self.inner.registration_gate.lock())
            .await
            .map_err(|_| request_timeout("unregister_process", self.inner.timeout))?;
        require_nonempty("process_name", name)?;
        let runtime = lock(&self.inner.processes).get(name).cloned();
        if let Some(runtime) = runtime.as_ref() {
            runtime.pause();
        }
        let mut cancellation = TransitionGuard {
            inner: Arc::clone(&self.inner),
            armed: true,
        };
        let result = self
            .request_in_pool(
                MessageType::UnregisterProcess,
                json!({ "process_name": name }),
                Some(self.pool_name().await),
                deadline.saturating_duration_since(Instant::now()),
            )
            .await;
        match result {
            Ok(_) => {
                if let Some(runtime) = lock(&self.inner.processes).remove(name) {
                    runtime.close();
                }
                cancellation.armed = false;
                Ok(())
            }
            Err(error) => {
                if matches!(
                    error,
                    Error::Timeout { .. } | Error::Disconnected | Error::Io(_)
                ) {
                    self.inner.close();
                } else if let Some(runtime) = runtime {
                    runtime.resume();
                }
                cancellation.armed = false;
                Err(error)
            }
        }
    }

    pub async fn call_process<T, R>(&self, process_id: &str, data: &T) -> Result<R>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        match self
            .call_process_with_options(process_id, data, self.inner.timeout, None)
            .await?
        {
            CallOutcome::Result(value) => Ok(value),
            CallOutcome::Routed { .. } => Err(Error::Protocol(
                "process result was routed elsewhere".to_owned(),
            )),
        }
    }

    pub async fn call_process_with_options<T, R>(
        &self,
        process_id: &str,
        data: &T,
        timeout: Duration,
        response_to: Option<&str>,
    ) -> Result<CallOutcome<R>>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        require_nonempty("process_id", process_id)?;
        self.call(
            MessageType::CallProcess,
            json!({
                "process_id": process_id, "data": to_object(data)?, "response_to": response_to,
            }),
            timeout,
            response_to,
        )
        .await
    }

    async fn call<R: DeserializeOwned>(
        &self,
        kind: MessageType,
        mut payload: Value,
        timeout: Duration,
        response_to: Option<&str>,
    ) -> Result<CallOutcome<R>> {
        let deadline = deadline(timeout)?;
        let request_id = Uuid::new_v4().to_string();
        let operation = match self.inner.operation_gate.try_read() {
            Ok(operation) => operation,
            Err(_) => {
                return Err(Error::Overloaded {
                    resource: "pool transition",
                });
            }
        };
        if let Some(response_to) = response_to {
            require_nonempty("response_to", response_to)?;
        }
        payload["timeout"] = json!(
            deadline
                .saturating_duration_since(Instant::now())
                .as_secs_f64()
        );
        let message = Message::new(
            kind,
            Some(request_id.clone()),
            Some(self.client_id().to_owned()),
            Some(self.pool_name().await),
            payload,
        );
        let mut pending = self
            .open_request(message, request_id.clone(), kind, deadline)
            .await?;
        drop(operation);
        if response_to.is_some_and(|target| target != self.client_id()) {
            self.wait_for(
                &request_id,
                &mut pending,
                deadline,
                timeout,
                &[MessageType::Ack],
            )
            .await?;
            return Ok(CallOutcome::Routed { request_id });
        }
        let message = self
            .wait_for(
                &request_id,
                &mut pending,
                deadline,
                timeout,
                &[MessageType::AppResult],
            )
            .await?;
        decode_app_result(message).map(CallOutcome::Result)
    }

    pub async fn broadcast_process<T: Serialize + ?Sized>(
        &self,
        process_name: &str,
        data: &T,
        response_to: Option<&str>,
    ) -> Result<Vec<String>> {
        require_nonempty("process_name", process_name)?;
        if let Some(response_to) = response_to {
            require_nonempty("response_to", response_to)?;
        }
        let reply = self
            .request(
                MessageType::BroadcastProcess,
                json!({
                    "process_name": process_name,
                    "data": to_object(data)?,
                    "response_to": response_to,
                    "timeout": self.inner.timeout.as_secs_f64(),
                }),
                self.inner.timeout,
            )
            .await?;
        serde_json::from_value(
            reply
                .payload
                .get("targets")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(Into::into)
    }

    pub async fn list_processes(&self, pattern: Option<&str>) -> Result<ProcessMap> {
        let reply = self
            .request(
                MessageType::ListProcesses,
                json!({ "pattern": pattern }),
                self.inner.timeout,
            )
            .await?;
        serde_json::from_value(
            reply
                .payload
                .get("processes")
                .cloned()
                .unwrap_or_else(|| json!({})),
        )
        .map_err(Into::into)
    }

    pub async fn report_worker_metrics(&self, metrics: &[WorkerMetrics]) -> Result<()> {
        self.require_batch(metrics.len())?;
        if metrics
            .iter()
            .any(|metric| !metric.avg_latency.is_finite() || metric.avg_latency < 0.0)
        {
            return Err(Error::Protocol(
                "worker latency must be finite and nonnegative".to_owned(),
            ));
        }
        self.request(
            MessageType::WorkerMetrics,
            json!({ "metrics": metrics }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    fn require_batch(&self, size: usize) -> Result<()> {
        if size > self.inner.max_batch_size {
            Err(Error::Overloaded {
                resource: "batch size",
            })
        } else {
            Ok(())
        }
    }

    // Transport ---------------------------------------------------------

    async fn request(
        &self,
        kind: MessageType,
        payload: Value,
        timeout: Duration,
    ) -> Result<Message> {
        let deadline = deadline(timeout)?;
        let request_id = Uuid::new_v4().to_string();
        let operation = match self.inner.operation_gate.try_read() {
            Ok(operation) => operation,
            Err(_) => {
                return Err(Error::Overloaded {
                    resource: "pool transition",
                });
            }
        };
        let message = Message::new(
            kind,
            Some(request_id.clone()),
            Some(self.client_id().to_owned()),
            Some(self.pool_name().await),
            payload,
        );
        let mut pending = self
            .open_request(message, request_id.clone(), kind, deadline)
            .await?;
        drop(operation);
        self.wait_for(
            &request_id,
            &mut pending,
            deadline,
            timeout,
            &[MessageType::Ack],
        )
        .await
    }

    async fn request_in_pool(
        &self,
        kind: MessageType,
        payload: Value,
        pool: Option<String>,
        timeout: Duration,
    ) -> Result<Message> {
        let request_id = Uuid::new_v4().to_string();
        let deadline = deadline(timeout)?;
        let mut pending = self
            .open_request(
                Message::new(
                    kind,
                    Some(request_id.clone()),
                    Some(self.client_id().to_owned()),
                    pool,
                    payload,
                ),
                request_id.clone(),
                kind,
                deadline,
            )
            .await?;
        self.wait_for(
            &request_id,
            &mut pending,
            deadline,
            timeout,
            if matches!(kind, MessageType::JoinPool | MessageType::SwitchPool) {
                &[MessageType::Ack, MessageType::Redirect]
            } else {
                &[MessageType::Ack]
            },
        )
        .await
    }

    async fn open_request(
        &self,
        message: Message,
        request_id: String,
        kind: MessageType,
        deadline: Instant,
    ) -> Result<PendingResponse> {
        if !self.is_connected()
            || self.inner.closing.load(Ordering::Acquire) && kind != MessageType::LeavePool
        {
            return Err(Error::Disconnected);
        }
        let (sender, receiver) = mpsc::channel(2);
        let generation = self.inner.generation.load(Ordering::Acquire);
        let connection_generation = self.inner.connection_generation.load(Ordering::Acquire);
        let membership = if matches!(kind, MessageType::JoinPool | MessageType::SwitchPool) {
            Some((
                message.payload["pool"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                message.payload["auth_token"].as_str().map(str::to_owned),
            ))
        } else {
            None
        };
        let is_membership = membership.is_some();
        {
            let mut slots = lock(&self.inner.pending);
            if !self.is_connected() {
                return Err(Error::Disconnected);
            }
            if slots.len() >= self.inner.max_pending_requests {
                return Err(Error::Overloaded {
                    resource: "pending requests",
                });
            }
            slots.insert(
                request_id.clone(),
                PendingSlot {
                    sender,
                    kind,
                    generation,
                    connection_generation,
                    acknowledgement: false,
                    terminal: false,
                    membership,
                    expects_result: matches!(kind, MessageType::CallApp | MessageType::CallProcess)
                        && message.payload["response_to"]
                            .as_str()
                            .is_none_or(|target| target == self.client_id()),
                },
            );
        }
        let status = Arc::new(AtomicUsize::new(0));
        let pending = PendingResponse {
            receiver,
            request_id,
            inner: Arc::downgrade(&self.inner),
            status: Arc::clone(&status),
            generation,
            connection_generation,
            membership: is_membership,
        };
        self.send_message(
            &message,
            deadline,
            generation,
            Some(status),
            kind == MessageType::LeavePool,
        )?;
        Ok(pending)
    }

    async fn wait_for(
        &self,
        request_id: &str,
        pending: &mut PendingResponse,
        deadline: Instant,
        timeout: Duration,
        expected: &[MessageType],
    ) -> Result<Message> {
        loop {
            if !pending.membership
                && pending.generation != self.inner.generation.load(Ordering::Acquire)
            {
                return Err(Error::Disconnected);
            }
            if !pending.membership
                && pending.connection_generation
                    != self.inner.connection_generation.load(Ordering::Acquire)
            {
                return Err(Error::Disconnected);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Timeout {
                    request_id: request_id.to_owned(),
                    timeout,
                });
            }
            let message = match time::timeout(remaining, pending.receiver.recv()).await {
                Ok(Some(message)) => message,
                Ok(None) => return Err(Error::Disconnected),
                Err(_) => {
                    return Err(Error::Timeout {
                        request_id: request_id.to_owned(),
                        timeout,
                    });
                }
            };
            if !pending.membership
                && pending.generation != self.inner.generation.load(Ordering::Acquire)
            {
                return Err(Error::Disconnected);
            }
            if !pending.membership
                && pending.connection_generation
                    != self.inner.connection_generation.load(Ordering::Acquire)
            {
                return Err(Error::Disconnected);
            }
            if message.kind == MessageType::Error.as_str()
                || expected.iter().any(|kind| message.kind == kind.as_str())
            {
                return check_response(message);
            }
            // Acceptance ACKs are informational for direct RPC completion.
            // The reader admits at most one ACK and one terminal response.
        }
    }

    fn send_message(
        &self,
        message: &Message,
        deadline: Instant,
        generation: u64,
        request: Option<Arc<AtomicUsize>>,
        control: bool,
    ) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let mut encoded = serde_json::to_vec(message)?;
        if encoded.len() > self.inner.max_frame_bytes {
            return Err(Error::FrameTooLarge {
                size: encoded.len(),
                limit: self.inner.max_frame_bytes,
            });
        }
        if deadline <= Instant::now() {
            return Err(request_timeout(
                message.request_id.as_deref().unwrap_or("send"),
                Duration::ZERO,
            ));
        }
        encoded.push(b'\n');
        let slot = if control {
            None
        } else {
            Some(
                Arc::clone(&self.inner.writer_slots)
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded {
                        resource: "writer messages",
                    })?,
            )
        };
        let byte_limit = self.inner.max_queued_bytes.saturating_add(if control {
            self.inner.control_reserve_bytes
        } else {
            0
        });
        let bytes = ByteReservation::acquire(
            &self.inner.queued_bytes,
            encoded.len(),
            byte_limit,
            "writer bytes",
        )?;
        let transport = lock(&self.inner.transport);
        if !self.is_connected() || generation != self.inner.generation.load(Ordering::Acquire) {
            return Err(Error::Disconnected);
        }
        transport
            .sender
            .as_ref()
            .ok_or(Error::Disconnected)?
            .try_send(WriterCommand::Frame {
                bytes: encoded,
                deadline,
                generation,
                connection_generation: self.inner.connection_generation.load(Ordering::Acquire),
                request,
                _slot: slot,
                _bytes: bytes,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Overloaded {
                    resource: "writer control capacity",
                },
                mpsc::error::TrySendError::Closed(_) => Error::Disconnected,
            })
    }

    async fn dispatch_message(&self, message: Message) {
        if message
            .pool
            .as_ref()
            .is_some_and(|pool| pool != &*lock(&self.inner.pool))
        {
            return;
        }
        match message.kind.as_str() {
            "presence_update" => match serde_json::from_value(message.payload) {
                Ok(value) => {
                    let _ = self.inner.events.send(ClientEvent::Presence(value));
                }
                Err(error) => self.publish_decode_error("presence_update", error),
            },
            "buffer_update" => match serde_json::from_value(message.payload) {
                Ok(value) => {
                    let _ = self.inner.events.send(ClientEvent::Buffer(value));
                }
                Err(error) => self.publish_decode_error("buffer_update", error),
            },
            "emit_event" => match serde_json::from_value::<EmittedEvent>(message.payload) {
                Ok(event) => {
                    let _ = self.inner.events.send(ClientEvent::Event(event.clone()));
                    let handlers = self
                        .inner
                        .event_handlers
                        .read()
                        .await
                        .get(&event.event)
                        .cloned()
                        .unwrap_or_default();
                    if !handlers.is_empty() {
                        self.spawn_handler(event.event, event.data, handlers, None, None)
                            .await;
                    }
                }
                Err(error) => self.publish_decode_error("emit_event", error),
            },
            "call_app" => {
                self.admit_call(message).await;
            }
            "app_result" => match serde_json::from_value(message.payload) {
                Ok(value) => {
                    let _ = self.inner.events.send(ClientEvent::AppResult {
                        request_id: message.request_id.unwrap_or_default(),
                        result: value,
                    });
                }
                Err(error) => self.publish_decode_error("app_result", error),
            },
            "process_scale" => match serde_json::from_value::<ProcessScale>(message.payload) {
                Ok(scale) => {
                    if let Some(runtime) = lock(&self.inner.processes)
                        .get(&scale.process_name)
                        .cloned()
                    {
                        runtime.scale(&scale.action, scale.count);
                    }
                    let _ = self.inner.events.send(ClientEvent::ProcessScale(scale));
                }
                Err(error) => self.publish_decode_error("process_scale", error),
            },
            _ => {
                if message.kind != "ack" {
                    let _ = self.inner.events.send(ClientEvent::Unknown(message));
                }
            }
        }
    }

    fn publish_decode_error(&self, event: &str, error: serde_json::Error) {
        let _ = self.inner.events.send(ClientEvent::HandlerFailed {
            event: event.to_owned(),
            error: error.to_string(),
        });
    }

    async fn admit_call(&self, message: Message) {
        let Some(request_id) = message.request_id else {
            return;
        };
        let event = message.payload["event"]
            .as_str()
            .filter(|value| !value.is_empty());
        let data = message.payload["data"].as_object();
        let (Some(event), Some(data)) = (event, data) else {
            self.reply_call(
                request_id,
                "",
                Err("incoming call requires a non-empty event and object data".to_owned()),
                self.inner.generation.load(Ordering::Acquire),
                self.pool_name().await,
            );
            return;
        };
        let process = event
            .strip_prefix(&format!("{}:", self.client_id()))
            .and_then(|name| lock(&self.inner.processes).get(name).cloned());
        let handlers = self
            .inner
            .event_handlers
            .read()
            .await
            .get(event)
            .cloned()
            .unwrap_or_default();
        self.spawn_handler(
            event.to_owned(),
            data.clone(),
            handlers,
            process,
            Some(request_id),
        )
        .await;
    }

    async fn spawn_handler(
        &self,
        event: String,
        data: Map<String, Value>,
        handlers: Vec<(EventHandlerId, Handler)>,
        process: Option<Arc<ProcessRuntime>>,
        request_id: Option<String>,
    ) {
        let generation = self.inner.generation.load(Ordering::Acquire);
        let pool = self.pool_name().await;
        let admission = if self.inner.switching.load(Ordering::Acquire)
            || self.inner.closing.load(Ordering::Acquire)
        {
            Err(Error::Disconnected)
        } else {
            Arc::clone(&self.inner.handler_slots)
                .try_acquire_owned()
                .map_err(|_| Error::Overloaded {
                    resource: "handler tasks",
                })
        };
        let byte_size = serde_json::to_vec(&data)
            .map_or(self.inner.max_handler_bytes.saturating_add(1), |value| {
                value.len()
            })
            .saturating_add(event.len())
            .saturating_add(request_id.as_ref().map_or(0, String::len))
            .saturating_add(128);
        let admission = admission.and_then(|slot| {
            ByteReservation::acquire(
                &self.inner.handler_bytes,
                byte_size,
                self.inner.max_handler_bytes,
                "handler bytes",
            )
            .map(|bytes| (slot, bytes))
        });
        let (slot, bytes) = match admission {
            Ok(admission) => admission,
            Err(error) => {
                if let Some(request_id) = request_id {
                    self.reply_call(request_id, &event, Err(error.to_string()), generation, pool);
                } else {
                    self.inner.close();
                }
                return;
            }
        };
        let client = Self {
            inner: Arc::clone(&self.inner),
            owner: None,
        };
        let process_generation = process
            .as_ref()
            .map(|runtime| runtime.generation.load(Ordering::Acquire));
        let mut tasks = lock(&self.inner.handler_tasks);
        if !self.is_connected()
            || self.inner.closing.load(Ordering::Acquire)
            || self.inner.switching.load(Ordering::Acquire)
            || self.inner.generation.load(Ordering::Acquire) != generation
        {
            return;
        }
        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                let _ = self.inner.events.send(ClientEvent::HandlerFailed {
                    event: "task".to_owned(),
                    error: error.to_string(),
                });
            }
        }
        tasks.spawn(async move {
            let _admission = (slot, bytes);
            if !client.is_connected()
                || client.inner.closing.load(Ordering::Acquire)
                || client.inner.switching.load(Ordering::Acquire)
                || client.inner.generation.load(Ordering::Acquire) != generation
            {
                return;
            }
            let result = if let Some(process) = process.as_ref() {
                process.invoke(data, process_generation.unwrap_or(0)).await
            } else {
                let mut result = Ok(Value::Null);
                for (_, handler) in handlers {
                    result = handler(data.clone()).await;
                    if result.is_err() {
                        break;
                    }
                }
                result
            };
            if !client.is_connected()
                || client.inner.closing.load(Ordering::Acquire)
                || client.inner.generation.load(Ordering::Acquire) != generation
                || process.as_ref().is_some_and(|runtime| {
                    !runtime.accepting.load(Ordering::Acquire)
                        || Some(runtime.generation.load(Ordering::Acquire)) != process_generation
                })
            {
                return;
            }
            if let Some(request_id) = request_id {
                client.reply_call(request_id, &event, result, generation, pool);
            } else if let Err(error) = result {
                let _ = client
                    .inner
                    .events
                    .send(ClientEvent::HandlerFailed { event, error });
            }
        });
    }

    fn reply_call(
        &self,
        request_id: String,
        event: &str,
        result: std::result::Result<Value, String>,
        generation: u64,
        pool: String,
    ) {
        if generation != self.inner.generation.load(Ordering::Acquire)
            || !self.is_connected()
            || self.inner.closing.load(Ordering::Acquire)
        {
            return;
        }
        let payload = match result {
            Ok(value) => json!({ "value": value, "error": null }),
            Err(error) => json!({
                "value": null,
                "error": { "type": "HandlerError", "message": error },
            }),
        };
        let mut response = Message::new(
            MessageType::AppResult,
            Some(request_id),
            Some(self.client_id().to_owned()),
            Some(pool),
            payload,
        );
        let deadline = match deadline(self.inner.timeout) {
            Ok(value) => value,
            Err(_) => {
                self.inner.close_work_generation(generation);
                return;
            }
        };
        let mut sent = self.send_message(&response, deadline, generation, None, true);
        if matches!(sent, Err(Error::FrameTooLarge { .. })) {
            response.payload = json!({"value": null, "error": {"type": "HandlerError", "message": "Handler result exceeds configured frame maximum"}});
            sent = self.send_message(&response, deadline, generation, None, true);
        }
        if let Err(error) = sent {
            if generation != self.inner.generation.load(Ordering::Acquire)
                || !self.is_connected()
            {
                return;
            }
            let _ = self.inner.events.send(ClientEvent::HandlerFailed {
                event: event.to_owned(),
                error: error.to_string(),
            });
            self.inner.close_work_generation(generation);
        }
    }
}

async fn writer_loop(
    inner: Weak<Inner>,
    mut writer: OwnedWriteHalf,
    mut receiver: mpsc::Receiver<WriterCommand>,
    connection_generation: u64,
) {
    while let Some(command) = receiver.recv().await {
        let Some(state) = inner.upgrade() else {
            break;
        };
        let WriterCommand::Frame {
            bytes,
            deadline,
            generation,
            connection_generation: frame_connection_generation,
            request,
            _slot,
            _bytes,
        } = command;
        if !state.connection_is_current(connection_generation) {
            break;
        }
        if state.generation.load(Ordering::Acquire) != generation
            || frame_connection_generation != connection_generation
        {
            continue;
        }
        if deadline <= Instant::now() {
            if request.is_none() {
                state.close_connection(connection_generation);
                break;
            }
            continue;
        }
        if let Some(request) = request.as_ref() {
            if request
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                continue;
            }
        }
        let limit = deadline.min(Instant::now() + state.write_timeout);
        if !matches!(
            time::timeout_at(limit.into(), writer.write_all(&bytes)).await,
            Ok(Ok(()))
        ) {
            state.close_connection(connection_generation);
            break;
        }
    }
    let _ = writer.shutdown().await;
}

async fn read_loop(
    inner: Weak<Inner>,
    mut reader: BufReader<OwnedReadHalf>,
    connection_generation: u64,
) {
    loop {
        let Some(state) = inner.upgrade() else {
            return;
        };
        if !state.connection_is_current(connection_generation) {
            return;
        }
        let limit = state.max_frame_bytes;
        drop(state);

        let line = match read_frame(&mut reader, limit).await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break,
        };
        let Some(state) = inner.upgrade() else {
            return;
        };
        if !state.connection_is_current(connection_generation) {
            return;
        }
        let client = Client {
            inner: Arc::clone(&state),
            owner: None,
        };
        let message: Message = match serde_json::from_slice::<Message>(&line) {
            Ok(message)
                if !message.kind.is_empty()
                    && (message.payload.is_object() || message.payload.is_null()) =>
            {
                message
            }
            Ok(_) => {
                state.close_connection(connection_generation);
                break;
            }
            Err(error) => {
                let _ = state.events.send(ClientEvent::HandlerFailed {
                    event: "protocol".to_owned(),
                    error: error.to_string(),
                });
                state.close_connection(connection_generation);
                break;
            }
        };
        let mut correlated = false;
        let mut redirect_sender = None;
        if let Some(request_id) = message.request_id.as_ref() {
            let mut pending = lock(&state.pending);
            if let Some(slot) = pending.get_mut(request_id) {
                let reply = message.kind.as_str();
                let is_rpc = matches!(slot.kind, MessageType::CallApp | MessageType::CallProcess);
                let is_membership =
                    matches!(slot.kind, MessageType::JoinPool | MessageType::SwitchPool);
                if slot.generation == state.generation.load(Ordering::Acquire)
                    && slot.connection_generation == connection_generation
                    && (reply == "error"
                        || reply == "app_result" && slot.expects_result
                        || reply == "redirect" && is_membership
                        || reply == "ack"
                            && (!is_rpc || message.payload["queued"].as_bool() == Some(true)))
                {
                    correlated = true;
                    let duplicate = slot.terminal || reply == "ack" && slot.acknowledgement;
                    if !duplicate {
                        if reply == "ack" {
                            slot.acknowledgement = true;
                            if let Some((pool, auth_token)) = slot.membership.take() {
                                let changed = pool != *lock(&state.pool);
                                *lock(&state.pool) = pool;
                                *lock(&state.auth_token) = auth_token;
                                if changed {
                                    for process in lock(&state.processes).values() {
                                        process.close();
                                    }
                                    lock(&state.processes).clear();
                                }
                                state.switching.store(false, Ordering::Release);
                            }
                        } else {
                            slot.terminal = true;
                        }
                        if reply == "redirect" {
                            redirect_sender = Some(slot.sender.clone());
                        } else if slot.sender.try_send(message.clone()).is_err() {
                            drop(pending);
                            state.close_connection(connection_generation);
                            break;
                        }
                    }
                }
            }
        }
        if let Some(sender) = redirect_sender {
            // The router closes this socket. Fence it before EOF or an old
            // writer failure can touch the replacement connection.
            if state.retire_connection(connection_generation)
                && sender.try_send(message).is_err()
            {
                state.close();
            }
            return;
        }
        if !correlated {
            client.dispatch_message(message).await;
        }
    }

    if let Some(state) = inner.upgrade() {
        state.close_connection(connection_generation);
    }
}

async fn read_frame(
    reader: &mut BufReader<OwnedReadHalf>,
    limit: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let chunk = reader.fill_buf().await?;
        if chunk.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "partial JSON frame",
                ))
            };
        }
        let newline = chunk.iter().position(|byte| *byte == b'\n');
        let length = newline.unwrap_or(chunk.len());
        if frame.len().saturating_add(length) > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "frame exceeds configured maximum",
            ));
        }
        frame.extend_from_slice(&chunk[..length]);
        reader.consume(length + usize::from(newline.is_some()));
        if newline.is_some() {
            return Ok(Some(frame));
        }
    }
}

async fn metrics_loop(inner: Weak<Inner>) {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.tick().await;
    loop {
        interval.tick().await;
        let Some(state) = inner.upgrade() else {
            return;
        };
        let client = Client {
            inner: state,
            owner: None,
        };
        if !client.is_connected() {
            return;
        }
        let Ok(deadline) = deadline(client.inner.timeout) else { return; };
        let Ok(_admission) = time::timeout_at(deadline.into(), client.inner.operation_gate.read()).await
        else { continue; };
        if !client.is_connected() || client.inner.closing.load(Ordering::Acquire) {
            return;
        }
        let processes: Vec<_> = lock(&client.inner.processes).values().cloned().collect();
        if !processes.is_empty() {
            let metrics: Vec<_> = processes.iter().map(|runtime| runtime.metrics()).collect();
            let pool = lock(&client.inner.pool).clone();
            let _ = client.request_in_pool(
                MessageType::WorkerMetrics,
                json!({"metrics": metrics}),
                Some(pool),
                deadline.saturating_duration_since(Instant::now()),
            ).await;
        }
    }
}

struct PendingResponse {
    receiver: mpsc::Receiver<Message>,
    request_id: String,
    inner: Weak<Inner>,
    status: Arc<AtomicUsize>,
    generation: u64,
    connection_generation: u64,
    membership: bool,
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        let _ = self
            .status
            .compare_exchange(0, 2, Ordering::AcqRel, Ordering::Acquire);
        if let Some(inner) = self.inner.upgrade() {
            lock(&inner.pending).remove(&self.request_id);
        }
    }
}

struct TransitionGuard {
    inner: Arc<Inner>,
    armed: bool,
}

impl Drop for TransitionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.inner.close();
        }
    }
}

struct RegistrationGuard {
    inner: Arc<Inner>,
    name: String,
    runtime: Arc<ProcessRuntime>,
    previous: Option<Arc<ProcessRuntime>>,
    committed: bool,
    resolved: bool,
}

impl Drop for RegistrationGuard {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        if !self.resolved {
            self.inner.close();
        }
        self.runtime.close();
        let mut processes = lock(&self.inner.processes);
        if processes
            .get(&self.name)
            .is_some_and(|runtime| Arc::ptr_eq(runtime, &self.runtime))
        {
            if let Some(previous) = self.previous.take() {
                previous.resume();
                processes.insert(self.name.clone(), previous);
            } else {
                processes.remove(&self.name);
            }
        }
    }
}

struct ProcessRuntime {
    name: String,
    options: ProcessOptions,
    handler: Handler,
    capacity: AtomicUsize,
    active: AtomicUsize,
    queued: AtomicUsize,
    completed: AtomicU64,
    total_latency_micros: AtomicU64,
    notify: Notify,
    quiesced: Notify,
    accepting: AtomicBool,
    closed: AtomicBool,
    generation: AtomicU64,
}

impl ProcessRuntime {
    fn new(name: String, options: ProcessOptions, handler: Handler) -> Self {
        Self {
            name,
            capacity: AtomicUsize::new(options.min_workers),
            options,
            handler,
            active: AtomicUsize::new(0),
            queued: AtomicUsize::new(0),
            completed: AtomicU64::new(0),
            total_latency_micros: AtomicU64::new(0),
            notify: Notify::new(),
            quiesced: Notify::new(),
            accepting: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            generation: AtomicU64::new(0),
        }
    }

    async fn invoke(
        &self,
        data: Map<String, Value>,
        generation: u64,
    ) -> std::result::Result<Value, String> {
        let _permit = self.acquire(generation).await?;
        let started = Instant::now();
        let mut future = (self.handler)(data);
        let result = loop {
            let notified = self.quiesced.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.accepting.load(Ordering::Acquire)
                || self.generation.load(Ordering::Acquire) != generation
            {
                return Err("Process handler was quiesced".to_owned());
            }
            tokio::select! {
                result = &mut future => break result,
                _ = notified => {}
            }
        };
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.total_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.completed.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn acquire(&self, generation: u64) -> std::result::Result<ProcessPermit<'_>, String> {
        self.queued.fetch_add(1, Ordering::Relaxed);
        let queued = ProcessQueueGuard(self);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.accepting.load(Ordering::Acquire)
                || self.generation.load(Ordering::Acquire) != generation
            {
                return Err("Process handler is unavailable".to_owned());
            }
            let active = self.active.load(Ordering::Acquire);
            let capacity = self.capacity.load(Ordering::Acquire);
            if active < capacity
                && self
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                drop(queued);
                return Ok(ProcessPermit { runtime: self });
            }
            notified.await;
        }
    }

    fn scale(&self, action: &str, count: usize) {
        let current = self.capacity.load(Ordering::Acquire);
        let updated = match action {
            "up" => current.saturating_add(count).min(self.options.max_workers),
            "down" => current.saturating_sub(count).max(self.options.min_workers),
            _ => current,
        };
        self.capacity.store(updated, Ordering::Release);
        self.notify.notify_waiters();
    }

    fn pause(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.accepting.store(false, Ordering::Release);
        self.notify.notify_waiters();
        self.quiesced.notify_waiters();
    }

    fn resume(&self) {
        if !self.closed.load(Ordering::Acquire) {
            self.accepting.store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.pause();
    }

    fn metrics(&self) -> WorkerMetrics {
        let completed = self.completed.load(Ordering::Relaxed);
        let total_micros = self.total_latency_micros.load(Ordering::Relaxed);
        WorkerMetrics {
            process_name: self.name.clone(),
            active_workers: self.capacity.load(Ordering::Relaxed),
            queue_depth: self.queued.load(Ordering::Relaxed),
            avg_latency: if completed == 0 {
                0.0
            } else {
                (total_micros as f64 / completed as f64) / 1_000_000.0
            },
            completed_count: completed,
        }
    }
}

struct ProcessPermit<'a> {
    runtime: &'a ProcessRuntime,
}

struct ProcessQueueGuard<'a>(&'a ProcessRuntime);

impl Drop for ProcessQueueGuard<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::Relaxed);
        // Forward a wakeup if a selected waiter was cancelled before acquiring.
        self.0.notify.notify_one();
    }
}

impl Drop for ProcessPermit<'_> {
    fn drop(&mut self) {
        self.runtime.active.fetch_sub(1, Ordering::AcqRel);
        self.runtime.notify.notify_one();
    }
}

/// A key prefix applied as `prefix:key`.
#[derive(Clone)]
pub struct Namespace {
    client: Client,
    prefix: String,
}

impl Namespace {
    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    pub async fn set<T: Serialize + ?Sized>(&self, key: &str, value: &T) -> Result<()> {
        self.client.set(&self.key(key), value).await
    }

    pub async fn set_with_options<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        ttl: Option<Duration>,
        persistent: bool,
    ) -> Result<()> {
        self.client
            .set_with_options(&self.key(key), value, ttl, persistent)
            .await
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.client.get(&self.key(key)).await
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        self.client.exists(&self.key(key)).await
    }

    pub async fn delete(&self, key: &str) -> Result<bool> {
        self.client.delete(&self.key(key)).await
    }

    pub async fn increment(&self, key: &str, delta: i64) -> Result<i64> {
        let value = self.get::<i64>(key).await?.unwrap_or(0);
        let updated = value
            .checked_add(delta)
            .ok_or_else(|| Error::Protocol("integer increment overflowed".to_owned()))?;
        self.set(key, &updated).await?;
        Ok(updated)
    }

    pub async fn decrement(&self, key: &str, delta: i64) -> Result<i64> {
        let delta = delta
            .checked_neg()
            .ok_or_else(|| Error::Protocol("integer decrement overflowed".to_owned()))?;
        self.increment(key, delta).await
    }

    pub async fn keys(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let full_prefix = format!("{}{}", self.prefix, prefix.unwrap_or_default());
        Ok(self
            .client
            .keys(Some(&full_prefix))
            .await?
            .into_iter()
            .filter_map(|key| key.strip_prefix(&self.prefix).map(str::to_owned))
            .collect())
    }

    pub async fn mset<T: Serialize>(
        &self,
        values: &HashMap<String, T>,
        ttl: Option<Duration>,
        persistent: bool,
    ) -> Result<()> {
        self.client.require_batch(values.len())?;
        for (key, value) in values {
            self.set_with_options(key, value, ttl, persistent).await?;
        }
        Ok(())
    }

    pub async fn mget<T: DeserializeOwned>(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<T>>> {
        self.client.require_batch(keys.len())?;
        let mut values = HashMap::with_capacity(keys.len());
        for key in keys {
            values.insert(key.clone(), self.get(key).await?);
        }
        Ok(values)
    }
}

/// Event API that prefixes event names as `namespace:event`.
#[derive(Clone)]
pub struct EventEmitter {
    client: Client,
    namespace: String,
}

impl EventEmitter {
    fn event(&self, event: &str) -> String {
        if self.namespace.is_empty() {
            event.to_owned()
        } else {
            format!("{}:{event}", self.namespace)
        }
    }

    pub async fn on<F, Fut, T, E>(&self, event: &str, handler: F) -> EventHandlerId
    where
        F: Fn(Map<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
        T: Serialize,
        E: Display,
    {
        self.client.on_event(self.event(event), handler).await
    }

    pub async fn emit<T: Serialize + ?Sized>(&self, event: &str, data: &T) -> Result<()> {
        self.client
            .emit_event(&self.event(event), data, None, None)
            .await
    }

    pub async fn call<T, R>(&self, event: &str, target_client_id: &str, data: &T) -> Result<R>
    where
        T: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.client
            .call_app(target_client_id, &self.event(event), data)
            .await
    }
}

fn adapt_handler<F, Fut, T, E>(handler: F) -> Handler
where
    F: Fn(Map<String, Value>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = std::result::Result<T, E>> + Send + 'static,
    T: Serialize,
    E: Display,
{
    Arc::new(move |data| {
        let future = catch_unwind(AssertUnwindSafe(|| handler(data)));
        Box::pin(async move {
            let mut future = Box::pin(future.map_err(|_| "Handler panicked".to_owned())?);
            poll_fn(move |context| {
                match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(context))) {
                    Ok(Poll::Pending) => Poll::Pending,
                    Ok(Poll::Ready(result)) => Poll::Ready(
                        catch_unwind(AssertUnwindSafe(|| {
                            let value = result.map_err(|error| error.to_string())?;
                            serde_json::to_value(value).map_err(|error| error.to_string())
                        }))
                        .unwrap_or_else(|_| Err("Handler result encoding panicked".to_owned())),
                    ),
                    Err(_) => Poll::Ready(Err("Handler panicked".to_owned())),
                }
            })
            .await
        })
    })
}

fn check_response(message: Message) -> Result<Message> {
    if message.kind != MessageType::Error.as_str() {
        return Ok(message);
    }
    let code = message
        .payload
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or("server_error")
        .to_owned();
    let text = message
        .payload
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Unknown server error")
        .to_owned();
    match code.as_str() {
        "auth_failed" => Err(Error::Authentication(text)),
        "timeout" => Err(Error::Timeout {
            request_id: message.request_id.unwrap_or_default(),
            timeout: Duration::ZERO,
        }),
        "connection_closed" => Err(Error::Disconnected),
        "partial_delivery" => Err(Error::PartialDelivery {
            message: text,
            accepted: serde_json::from_value(
                message
                    .payload
                    .get("accepted")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?,
            failed: serde_json::from_value(
                message
                    .payload
                    .get("failed")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?,
            request_ids: serde_json::from_value(
                message
                    .payload
                    .get("request_ids")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )?,
        }),
        _ => Err(Error::Server {
            code,
            message: text,
        }),
    }
}

fn decode_app_result<R: DeserializeOwned>(message: Message) -> Result<R> {
    if let Some(error) = message
        .payload
        .get("error")
        .filter(|value| !value.is_null())
    {
        return Err(Error::Handler(error.to_string()));
    }
    serde_json::from_value(message.payload.get("value").cloned().unwrap_or(Value::Null))
        .map_err(Into::into)
}

fn to_object<T: Serialize + ?Sized>(value: &T) -> Result<Map<String, Value>> {
    match serde_json::to_value(value)? {
        Value::Object(object) => Ok(object),
        _ => Err(Error::Protocol(
            "event and process data must serialize to a JSON object".to_owned(),
        )),
    }
}

fn require_nonempty(name: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > if name == "process_id" { 1025 } else { 512 } {
        Err(Error::Protocol(format!(
            "{name} must be nonempty and within the protocol identifier limit"
        )))
    } else {
        Ok(())
    }
}

fn deadline(timeout: Duration) -> Result<Instant> {
    Instant::now()
        .checked_add(timeout)
        .filter(|_| !timeout.is_zero())
        .ok_or_else(|| {
            Error::Protocol("timeout must be positive and fit a monotonic deadline".to_owned())
        })
}

fn request_timeout(request_id: &str, timeout: Duration) -> Error {
    Error::Timeout {
        request_id: request_id.to_owned(),
        timeout,
    }
}

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn parse_dsn(dsn: &str) -> Result<String> {
    let Some(authority) = dsn.strip_prefix("latzero://") else {
        return Err(Error::InvalidDsn);
    };
    let client_id = authority.split(['/', '?', '#']).next().unwrap_or_default();
    if client_id.is_empty() || client_id.chars().any(char::is_whitespace) {
        return Err(Error::InvalidDsn);
    }
    require_nonempty("client_id", client_id)?;
    Ok(client_id.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_python_compatible_dsn() {
        assert_eq!(parse_dsn("latzero://Worker-1/path").unwrap(), "Worker-1");
        assert!(matches!(parse_dsn("http://worker"), Err(Error::InvalidDsn)));
        assert!(matches!(parse_dsn("latzero://"), Err(Error::InvalidDsn)));
    }

    #[test]
    fn event_data_must_be_an_object() {
        assert!(to_object(&json!({ "x": 1 })).is_ok());
        assert!(matches!(to_object(&[1, 2]), Err(Error::Protocol(_))));
    }

    #[test]
    fn remote_entry_cannot_redirect_to_numeric_loopback() {
        let message = Message::new(
            MessageType::Redirect,
            Some("join".to_owned()),
            Some("worker".to_owned()),
            Some("alpha".to_owned()),
            json!({
                "protocol": REDIRECT_PROTOCOL, "host": "127.0.0.1", "port": 1234,
                "pool": "alpha", "pod_index": 0, "pod_count": 4,
                "router_host": "127.0.0.1", "router_port": 1235,
                "cluster_id": "cluster",
            }),
        );
        for host in ["192.0.2.1", "example.invalid", "localhost.evil", "0.0.0.0"] {
            assert!(matches!(
                RedirectChain::new(host, 4).follow(&message, "worker", "alpha"),
                Err(Error::Protocol(_))
            ));
        }
        assert!(RedirectChain::new("localhost", 4).follow(&message, "worker", "alpha").is_ok());
    }

    #[test]
    fn partial_delivery_keeps_destination_and_child_correlation_metadata() {
        let message = Message::new(
            MessageType::Error,
            Some("broadcast".to_owned()),
            None,
            None,
            json!({"code": "partial_delivery", "message": "not atomic",
                   "accepted": ["worker:job"], "failed": ["slow:job"], "request_ids": ["opaque-child"]}),
        );
        assert!(
            matches!(check_response(message), Err(Error::PartialDelivery {
            accepted, failed, request_ids, ..
        }) if accepted == ["worker:job"] && failed == ["slow:job"] && request_ids == ["opaque-child"])
        );
    }

    #[test]
    fn invalid_deadlines_and_identifier_lengths_fail_before_socket_work() {
        assert!(deadline(Duration::ZERO).is_err());
        assert!(deadline(Duration::MAX).is_err());
        assert!(parse_dsn(&format!("latzero://{}", "x".repeat(513))).is_err());
        assert!(require_nonempty("process_id", &"x".repeat(1025)).is_ok());
        assert!(require_nonempty("process_id", &"x".repeat(1026)).is_err());
    }

    #[test]
    fn byte_reservations_bound_and_release_on_drop() {
        let used = Arc::new(AtomicUsize::new(0));
        let reservation = ByteReservation::acquire(&used, 8, 10, "test").unwrap();
        assert!(matches!(
            ByteReservation::acquire(&used, 3, 10, "test"),
            Err(Error::Overloaded { .. })
        ));
        assert_eq!(used.load(Ordering::Acquire), 8);
        drop(reservation);
        assert_eq!(used.load(Ordering::Acquire), 0);
        assert!(ByteReservation::acquire(&used, usize::MAX, 10, "test").is_err());
    }

    #[tokio::test]
    async fn invalid_builder_limits_and_timeouts_are_rejected_before_connect() {
        for builder in [
            ClientBuilder::new("latzero://invalid", "pool").writer_capacity(0),
            ClientBuilder::new("latzero://invalid", "pool").max_pending_requests(0),
            ClientBuilder::new("latzero://invalid", "pool").max_frame_bytes(0),
            ClientBuilder::new("latzero://invalid", "pool").max_handler_tasks(0),
            ClientBuilder::new("latzero://invalid", "pool").control_reserve(0),
            ClientBuilder::new("latzero://invalid", "pool").timeout(Duration::ZERO),
            ClientBuilder::new("latzero://invalid", "pool").write_timeout(Duration::MAX),
            ClientBuilder::new("latzero://invalid", "pool").shutdown_timeout(Duration::MAX),
            ClientBuilder::new("latzero://invalid", "pool").max_redirects(17),
        ] {
            assert!(matches!(builder.connect().await, Err(Error::Protocol(_))));
        }
    }

    #[tokio::test]
    async fn retired_connection_errors_cannot_close_replacement_and_tasks_are_reaped() {
        let router = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let owner = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let router_port = router.local_addr().unwrap().port();
        let owner_port = owner.local_addr().unwrap().port();
        let (client, (mut old_reader, mut old_writer)) = tokio::join!(
            ClientBuilder::new("latzero://worker", "alpha").port(router_port).connect(),
            async {
                let (socket, _) = router.accept().await.unwrap();
                let (reader, mut writer) = socket.into_split();
                let mut reader = BufReader::new(reader);
                for _ in 0..2 {
                    let frame = read_frame(&mut reader, 4096).await.unwrap().unwrap();
                    let request: Message = serde_json::from_slice(&frame).unwrap();
                    let ack = Message::new(MessageType::Ack, request.request_id, request.client_id, request.pool, json!({}));
                    let mut bytes = serde_json::to_vec(&ack).unwrap();
                    bytes.push(b'\n');
                    writer.write_all(&bytes).await.unwrap();
                }
                (reader, writer)
            }
        );
        let client = client.unwrap();
        let original_inner = Arc::clone(&client.inner);
        let old_generation = client.inner.connection_generation.load(Ordering::Acquire);
        let old_work_generation = client.inner.generation.load(Ordering::Acquire);
        let metrics_id = lock(&client.inner.metrics_task).as_ref().unwrap().id();
        let switched = async {
            client.switch_pool("beta", Some("token")).await.unwrap();
            assert!(Arc::ptr_eq(&original_inner, &client.inner));
            assert_eq!(lock(&client.inner.metrics_task).as_ref().unwrap().id(), metrics_id);
            assert_eq!(client.inner.entry_endpoint.port, router_port);
            assert_eq!(lock(&client.inner.transport).endpoint.port, owner_port);
            assert!(lock(&client.inner.pending).is_empty());
            assert_eq!(client.inner.queued_bytes.load(Ordering::Acquire), 0);
            assert_eq!(client.inner.handler_bytes.load(Ordering::Acquire), 0);
            client.inner.close_connection(old_generation);
            assert!(client.is_connected());
            client.reply_call("obsolete-hop".to_owned(), "old-event", Ok(json!(42)), old_work_generation, "alpha".to_owned());
            assert!(client.is_connected());
            client.clients().await.unwrap();
            client.force_close().await;
            assert!(lock(&client.inner.reader_task).is_none());
            assert!(lock(&client.inner.writer_task).is_none());
            assert!(lock(&client.inner.metrics_task).is_none());
        };
        let peer = async {
            let frame = read_frame(&mut old_reader, 4096).await.unwrap().unwrap();
            let request: Message = serde_json::from_slice(&frame).unwrap();
            let redirect = Message::new(MessageType::Redirect, request.request_id, request.client_id, request.pool, json!({
                "protocol": REDIRECT_PROTOCOL, "host": "127.0.0.1", "port": owner_port,
                "pool": "beta", "pod_index": 1, "pod_count": 4,
                "router_host": "127.0.0.1", "router_port": router_port,
                "cluster_id": "cluster",
            }));
            let mut bytes = serde_json::to_vec(&redirect).unwrap();
            bytes.push(b'\n');
            old_writer.write_all(&bytes).await.unwrap();
            assert!(read_frame(&mut old_reader, 4096).await.unwrap().is_none());
            let (socket, _) = owner.accept().await.unwrap();
            let (reader, mut writer) = socket.into_split();
            let mut reader = BufReader::new(reader);
            for expected in ["hello", "join_pool", "list_clients"] {
                let frame = read_frame(&mut reader, 4096).await.unwrap().unwrap();
                let request: Message = serde_json::from_slice(&frame).unwrap();
                assert_eq!(request.kind, expected);
                let ack = Message::new(MessageType::Ack, request.request_id, request.client_id, request.pool, json!({"clients": ["worker"]}));
                let mut bytes = serde_json::to_vec(&ack).unwrap();
                bytes.push(b'\n');
                writer.write_all(&bytes).await.unwrap();
            }
            assert!(read_frame(&mut reader, 4096).await.unwrap().is_none());
        };
        time::timeout(Duration::from_secs(4), async { tokio::join!(switched, peer); }).await.unwrap();
    }

    #[test]
    fn concurrent_close_cannot_insert_handlers_after_cancellation_barrier() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (client, mut peer_reader, peer_writer) = runtime.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let peer = async {
                let (socket, _) = listener.accept().await.unwrap();
                let (reader, mut writer) = socket.into_split();
                let mut reader = BufReader::new(reader);
                for _ in 0..2 {
                    let mut raw = String::new();
                    reader.read_line(&mut raw).await.unwrap();
                    let request: Message = serde_json::from_str(&raw).unwrap();
                    let ack = Message::new(
                        MessageType::Ack,
                        request.request_id,
                        request.client_id,
                        request.pool,
                        json!({"pool": "test"}),
                    );
                    let mut raw = serde_json::to_vec(&ack).unwrap();
                    raw.push(b'\n');
                    writer.write_all(&raw).await.unwrap();
                }
                (reader, writer)
            };
            let (client, (reader, writer)) = tokio::join!(
                ClientBuilder::new("latzero://worker", "test")
                    .port(port)
                    .connect(),
                peer
            );
            (client.unwrap(), reader, writer)
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let handler_effects = Arc::clone(&effects);
        let handler = adapt_handler(move |_| {
            handler_effects.fetch_add(1, Ordering::AcqRel);
            std::future::ready(Ok::<_, String>(42))
        });
        // Both admission and close must serialize on this exact collection
        // lock; the closer seals connection state before waiting for it.
        let barrier = lock(&client.inner.handler_tasks);
        let admission_client = client.clone();
        let handle = runtime.handle().clone();
        let admission = std::thread::spawn(move || {
            handle.block_on(admission_client.spawn_handler(
                "effect".to_owned(),
                Map::new(),
                vec![(EventHandlerId(Uuid::new_v4()), handler)],
                None,
                Some("hop".to_owned()),
            ))
        });
        runtime.block_on(async {
            time::timeout(Duration::from_secs(2), async {
                while client.inner.handler_bytes.load(Ordering::Acquire) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
        let close_inner = Arc::clone(&client.inner);
        let closer = std::thread::spawn(move || close_inner.close());
        runtime.block_on(async {
            time::timeout(Duration::from_secs(2), async {
                while client.is_connected() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
        drop(barrier);
        admission.join().unwrap();
        closer.join().unwrap();
        runtime.block_on(client.force_close());
        assert_eq!(effects.load(Ordering::Acquire), 0);
        assert_eq!(client.inner.handler_bytes.load(Ordering::Acquire), 0);
        assert!(lock(&client.inner.handler_tasks).is_empty());
        runtime.block_on(async {
            let mut raw = String::new();
            assert_eq!(
                time::timeout(Duration::from_secs(2), peer_reader.read_line(&mut raw))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        drop(peer_writer);
    }
}
