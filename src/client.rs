use std::{
    collections::{HashMap, VecDeque},
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedReadHalf, tcp::OwnedWriteHalf},
    sync::{Notify, RwLock, broadcast, mpsc},
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

    /// Open the TCP connection, perform `hello`, and join the configured pool.
    pub async fn connect(self) -> Result<Client> {
        let client_id = parse_dsn(&self.dsn)?;
        if self.pool.is_empty() {
            return Err(Error::Protocol("pool must not be empty".to_owned()));
        }

        let endpoint = format!("{}:{}", self.host, self.port);
        let stream = match time::timeout(self.timeout, TcpStream::connect(&endpoint)).await {
            Ok(Ok(stream)) => stream,
            Ok(Err(source)) => {
                return Err(Error::Connection { endpoint, source });
            }
            Err(_) => {
                return Err(Error::Connection {
                    endpoint,
                    source: std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "connection timed out",
                    ),
                });
            }
        };
        let _ = stream.set_nodelay(true);
        let (reader, writer) = stream.into_split();
        let (writer_sender, writer_receiver) = mpsc::channel(256);
        let (event_sender, _) = broadcast::channel(self.event_capacity.max(1));

        let client = Client {
            inner: Arc::new(Inner {
                client_id,
                pool: RwLock::new(self.pool.clone()),
                auth_token: RwLock::new(self.auth_token.clone()),
                timeout: self.timeout,
                writer_sender,
                pending: StdMutex::new(HashMap::new()),
                event_handlers: RwLock::new(HashMap::new()),
                processes: RwLock::new(HashMap::new()),
                events: event_sender,
                connected: AtomicBool::new(true),
                operation_gate: RwLock::new(()),
                reader_task: StdMutex::new(None),
                writer_task: StdMutex::new(None),
                metrics_task: StdMutex::new(None),
            }),
        };

        let writer_task = tokio::spawn(writer_loop(writer, writer_receiver));
        *lock(&client.inner.writer_task) = Some(writer_task.abort_handle());
        let reader_inner = Arc::downgrade(&client.inner);
        let reader_task = tokio::spawn(read_loop(reader_inner, reader));
        *lock(&client.inner.reader_task) = Some(reader_task.abort_handle());

        if let Err(error) = client
            .request_in_pool(
                MessageType::Hello,
                json!({ "client_id": client.inner.client_id }),
                None,
                self.timeout,
            )
            .await
        {
            client.force_close().await;
            return Err(error);
        }

        if let Err(error) = client
            .join_pool(&self.pool, self.auth_token.as_deref(), self.timeout)
            .await
        {
            client.force_close().await;
            return Err(error);
        }

        let metrics_inner = Arc::downgrade(&client.inner);
        let metrics_task = tokio::spawn(metrics_loop(metrics_inner));
        *lock(&client.inner.metrics_task) = Some(metrics_task.abort_handle());
        Ok(client)
    }
}

/// Cloneable, asynchronous client for one `latzero-server` connection.
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

struct Inner {
    client_id: String,
    pool: RwLock<String>,
    auth_token: RwLock<Option<String>>,
    timeout: Duration,
    writer_sender: mpsc::Sender<WriterCommand>,
    pending: StdMutex<HashMap<String, mpsc::UnboundedSender<Message>>>,
    event_handlers: RwLock<HashMap<String, Vec<(EventHandlerId, Handler)>>>,
    processes: RwLock<HashMap<String, Arc<ProcessRuntime>>>,
    events: broadcast::Sender<ClientEvent>,
    connected: AtomicBool,
    operation_gate: RwLock<()>,
    reader_task: StdMutex<Option<tokio::task::AbortHandle>>,
    writer_task: StdMutex<Option<tokio::task::AbortHandle>>,
    metrics_task: StdMutex<Option<tokio::task::AbortHandle>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for task in [&self.reader_task, &self.writer_task, &self.metrics_task] {
            if let Some(handle) = lock(task).take() {
                handle.abort();
            }
        }
    }
}

enum WriterCommand {
    Frame(Vec<u8>),
    Shutdown,
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
        self.inner.pool.read().await.clone()
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

    /// Switch the existing connection to another isolated pool.
    pub async fn switch_pool(
        &self,
        pool: impl Into<String>,
        auth_token: Option<&str>,
    ) -> Result<()> {
        let _transition = self.inner.operation_gate.write().await;
        let pool = pool.into();
        if pool.is_empty() {
            return Err(Error::Protocol("pool must not be empty".to_owned()));
        }
        self.request_in_pool(
            MessageType::SwitchPool,
            json!({
                "client_id": self.inner.client_id,
                "pool": pool,
                "auth_token": auth_token,
            }),
            Some(pool.clone()),
            self.inner.timeout,
        )
        .await?;
        *self.inner.pool.write().await = pool;
        *self.inner.auth_token.write().await = auth_token.map(str::to_owned);
        self.inner.processes.write().await.clear();
        Ok(())
    }

    /// Leave the pool and close the connection. Calling this more than once is safe.
    pub async fn disconnect(&self) -> Result<()> {
        if !self.is_connected() {
            return Ok(());
        }
        let _transition = self.inner.operation_gate.write().await;
        let leave_result = self
            .request_in_pool(
                MessageType::LeavePool,
                json!({}),
                Some(self.pool_name().await),
                Duration::from_secs(1),
            )
            .await
            .map(|_| ());
        self.force_close().await;
        leave_result
    }

    async fn force_close(&self) {
        if !self.inner.connected.swap(false, Ordering::AcqRel) {
            return;
        }
        if let Some(task) = lock(&self.inner.metrics_task).take() {
            task.abort();
        }
        let _ = self.inner.writer_sender.try_send(WriterCommand::Shutdown);
        if let Some(task) = lock(&self.inner.writer_task).take() {
            task.abort();
        }
        if let Some(task) = lock(&self.inner.reader_task).take() {
            task.abort();
        }
        self.fail_pending();
        let _ = self.inner.events.send(ClientEvent::Disconnected);
    }

    async fn join_pool(
        &self,
        pool: &str,
        auth_token: Option<&str>,
        timeout: Duration,
    ) -> Result<()> {
        self.request_in_pool(
            MessageType::JoinPool,
            json!({
                "client_id": self.inner.client_id,
                "pool": pool,
                "auth_token": auth_token,
            }),
            Some(pool.to_owned()),
            timeout,
        )
        .await?;
        *self.inner.pool.write().await = pool.to_owned();
        *self.inner.auth_token.write().await = auth_token.map(str::to_owned);
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
        for (key, value) in values {
            self.set_with_options(key, value, ttl, persistent).await?;
        }
        Ok(())
    }

    pub async fn mget<T: DeserializeOwned>(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<T>>> {
        let mut values = HashMap::with_capacity(keys.len());
        for key in keys {
            values.insert(key.clone(), self.get(key).await?);
        }
        Ok(values)
    }

    pub async fn delete_many(&self, keys: &[String]) -> Result<usize> {
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
        let _operation = self.inner.operation_gate.read().await;
        require_nonempty("target_client_id", target_client_id)?;
        require_nonempty("event", event)?;
        let request_id = Uuid::new_v4().to_string();
        let deadline = Instant::now() + timeout;
        let mut pending = self
            .open_request(
                Message::new(
                    MessageType::CallApp,
                    Some(request_id.clone()),
                    Some(self.client_id().to_owned()),
                    Some(self.pool_name().await),
                    json!({
                        "target_client_id": target_client_id,
                        "event": event,
                        "data": to_object(data)?,
                        "response_to": response_to,
                        "timeout": timeout.as_secs_f64(),
                    }),
                ),
                request_id.clone(),
            )
            .await?;
        async {
            self.wait_for(
                &request_id,
                &mut pending,
                deadline,
                timeout,
                &[MessageType::Ack],
            )
            .await?;
            if response_to.is_some_and(|target| target != self.client_id()) {
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
        let id = EventHandlerId(Uuid::new_v4());
        let handler = adapt_handler(handler);
        self.inner
            .event_handlers
            .write()
            .await
            .entry(event.into())
            .or_default()
            .push((id, handler));
        id
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
        let _operation = self.inner.operation_gate.read().await;
        let name = name.into();
        require_nonempty("process_name", &name)?;
        if options.min_workers == 0 || options.min_workers > options.max_workers {
            return Err(Error::Protocol(
                "min_workers must be between 1 and max_workers".to_owned(),
            ));
        }
        let runtime = Arc::new(ProcessRuntime::new(
            name.clone(),
            options.clone(),
            adapt_handler(handler),
        ));
        self.inner
            .processes
            .write()
            .await
            .insert(name.clone(), runtime);

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
                self.inner.timeout,
            )
            .await
        {
            Ok(message) => match serde_json::from_value(message.payload) {
                Ok(registration) => Ok(registration),
                Err(error) => {
                    self.inner.processes.write().await.remove(&name);
                    Err(error.into())
                }
            },
            Err(error) => {
                self.inner.processes.write().await.remove(&name);
                Err(error)
            }
        }
    }

    pub async fn unregister_process(&self, name: &str) -> Result<()> {
        let _operation = self.inner.operation_gate.read().await;
        require_nonempty("process_name", name)?;
        self.request_in_pool(
            MessageType::UnregisterProcess,
            json!({ "process_name": name }),
            Some(self.pool_name().await),
            self.inner.timeout,
        )
        .await?;
        self.inner.processes.write().await.remove(name);
        Ok(())
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
        let _operation = self.inner.operation_gate.read().await;
        require_nonempty("process_id", process_id)?;
        if response_to.is_some() {
            let request_id = Uuid::new_v4().to_string();
            let mut pending = self
                .open_request(
                    Message::new(
                        MessageType::CallProcess,
                        Some(request_id.clone()),
                        Some(self.client_id().to_owned()),
                        Some(self.pool_name().await),
                        json!({
                            "process_id": process_id,
                            "data": to_object(data)?,
                            "response_to": response_to,
                            "timeout": timeout.as_secs_f64(),
                        }),
                    ),
                    request_id.clone(),
                )
                .await?;
            self.wait_for(
                &request_id,
                &mut pending,
                Instant::now() + timeout,
                timeout,
                &[MessageType::Ack],
            )
            .await?;
            return Ok(CallOutcome::Routed { request_id });
        }

        let request_id = Uuid::new_v4().to_string();
        let deadline = Instant::now() + timeout;
        let mut pending = self
            .open_request(
                Message::new(
                    MessageType::CallProcess,
                    Some(request_id.clone()),
                    Some(self.client_id().to_owned()),
                    Some(self.pool_name().await),
                    json!({
                        "process_id": process_id,
                        "data": to_object(data)?,
                        "response_to": null,
                        "timeout": timeout.as_secs_f64(),
                    }),
                ),
                request_id.clone(),
            )
            .await?;
        async {
            self.wait_for(
                &request_id,
                &mut pending,
                deadline,
                timeout,
                &[MessageType::Ack],
            )
            .await?;
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
        .await
    }

    pub async fn broadcast_process<T: Serialize + ?Sized>(
        &self,
        process_name: &str,
        data: &T,
        response_to: Option<&str>,
    ) -> Result<Vec<String>> {
        require_nonempty("process_name", process_name)?;
        let reply = self
            .request(
                MessageType::BroadcastProcess,
                json!({
                    "process_name": process_name,
                    "data": to_object(data)?,
                    "response_to": response_to,
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
        self.request(
            MessageType::WorkerMetrics,
            json!({ "metrics": metrics }),
            self.inner.timeout,
        )
        .await?;
        Ok(())
    }

    // Transport ---------------------------------------------------------

    async fn request(
        &self,
        kind: MessageType,
        payload: Value,
        timeout: Duration,
    ) -> Result<Message> {
        let _operation = self.inner.operation_gate.read().await;
        self.request_in_pool(kind, payload, Some(self.pool_name().await), timeout)
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
            )
            .await?;
        self.wait_for(
            &request_id,
            &mut pending,
            Instant::now() + timeout,
            timeout,
            &[MessageType::Ack],
        )
        .await
    }

    async fn open_request(&self, message: Message, request_id: String) -> Result<PendingResponse> {
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        lock(&self.inner.pending).insert(request_id.clone(), sender);
        let pending = PendingResponse {
            receiver,
            buffered: VecDeque::new(),
            request_id,
            inner: Arc::downgrade(&self.inner),
        };
        self.send_message(&message).await?;
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
            if let Some(index) = pending.buffered.iter().position(|message| {
                message.kind == MessageType::Error.as_str()
                    || expected.iter().any(|kind| message.kind == kind.as_str())
            }) {
                let message = pending.buffered.remove(index).expect("index was found");
                return check_response(message);
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
            if message.kind == MessageType::Error.as_str()
                || expected.iter().any(|kind| message.kind == kind.as_str())
            {
                return check_response(message);
            }
            pending.buffered.push_back(message);
        }
    }

    async fn send_message(&self, message: &Message) -> Result<()> {
        if !self.is_connected() {
            return Err(Error::Disconnected);
        }
        let mut encoded = serde_json::to_vec(message)?;
        encoded.push(b'\n');
        self.inner
            .writer_sender
            .send(WriterCommand::Frame(encoded))
            .await
            .map_err(|_| Error::Disconnected)
    }

    fn fail_pending(&self) {
        let message = Message::new(
            MessageType::Error,
            None,
            None,
            None,
            json!({
                "code": "connection_closed",
                "message": "Connection to latzero server was closed",
            }),
        );
        let mut pending = lock(&self.inner.pending);
        for sender in pending.values() {
            let _ = sender.send(message.clone());
        }
        pending.clear();
    }

    async fn dispatch_message(&self, message: Message) {
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
                    let client = self.clone();
                    tokio::spawn(async move {
                        client.invoke_emitted_event(event).await;
                    });
                }
                Err(error) => self.publish_decode_error("emit_event", error),
            },
            "call_app" => {
                let client = self.clone();
                tokio::spawn(async move {
                    client.handle_incoming_call(message).await;
                });
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
                    if let Some(runtime) = self
                        .inner
                        .processes
                        .read()
                        .await
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
                let _ = self.inner.events.send(ClientEvent::Unknown(message));
            }
        }
    }

    fn publish_decode_error(&self, event: &str, error: serde_json::Error) {
        let _ = self.inner.events.send(ClientEvent::HandlerFailed {
            event: event.to_owned(),
            error: error.to_string(),
        });
    }

    async fn invoke_emitted_event(&self, event: EmittedEvent) {
        let handlers = self
            .inner
            .event_handlers
            .read()
            .await
            .get(&event.event)
            .cloned()
            .unwrap_or_default();
        for (_, handler) in handlers {
            if let Err(error) = handler(event.data.clone()).await {
                let _ = self.inner.events.send(ClientEvent::HandlerFailed {
                    event: event.event.clone(),
                    error,
                });
            }
        }
    }

    async fn handle_incoming_call(&self, message: Message) {
        let _operation = self.inner.operation_gate.read().await;
        let request_id = match message.request_id {
            Some(request_id) => request_id,
            None => return,
        };
        let event = message
            .payload
            .get("event")
            .and_then(Value::as_str)
            .filter(|event| !event.is_empty())
            .map(str::to_owned);
        let data = message
            .payload
            .get("data")
            .and_then(Value::as_object)
            .cloned();

        let event_for_error = event.clone().unwrap_or_default();
        let result = if let (Some(event), Some(data)) = (event.as_ref(), data) {
            let process_name = event.strip_prefix(&format!("{}:", self.client_id()));
            let process = if let Some(process_name) = process_name {
                self.inner.processes.read().await.get(process_name).cloned()
            } else {
                None
            };
            if let Some(process) = process {
                process.invoke(data).await
            } else {
                let handlers = self
                    .inner
                    .event_handlers
                    .read()
                    .await
                    .get(event)
                    .cloned()
                    .unwrap_or_default();
                let mut result = Ok(Value::Null);
                for (_, handler) in handlers {
                    result = handler(data.clone()).await;
                    if result.is_err() {
                        break;
                    }
                }
                result
            }
        } else {
            Err("incoming call requires a non-empty event and object data".to_owned())
        };

        let payload = match result {
            Ok(value) => json!({ "value": value, "error": null }),
            Err(error) => json!({
                "value": null,
                "error": { "type": "HandlerError", "message": error },
            }),
        };
        let response = Message::new(
            MessageType::AppResult,
            Some(request_id),
            Some(self.client_id().to_owned()),
            Some(self.pool_name().await),
            payload,
        );
        if let Err(error) = self.send_message(&response).await {
            let _ = self.inner.events.send(ClientEvent::HandlerFailed {
                event: event_for_error,
                error: error.to_string(),
            });
        }
    }
}

async fn writer_loop(mut writer: OwnedWriteHalf, mut receiver: mpsc::Receiver<WriterCommand>) {
    while let Some(command) = receiver.recv().await {
        match command {
            WriterCommand::Frame(frame) => {
                if writer.write_all(&frame).await.is_err() {
                    break;
                }
            }
            WriterCommand::Shutdown => break,
        }
    }
    let _ = writer.shutdown().await;
}

async fn read_loop(inner: Weak<Inner>, reader: OwnedReadHalf) {
    let mut lines = BufReader::new(reader).lines();
    loop {
        let Some(state) = inner.upgrade() else {
            return;
        };
        if !state.connected.load(Ordering::Acquire) {
            return;
        }
        drop(state);

        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) | Err(_) => break,
        };
        let Some(state) = inner.upgrade() else {
            return;
        };
        let client = Client {
            inner: Arc::clone(&state),
        };
        let message: Message = match serde_json::from_str(&line) {
            Ok(message) => message,
            Err(error) => {
                let _ = state.events.send(ClientEvent::HandlerFailed {
                    event: "protocol".to_owned(),
                    error: error.to_string(),
                });
                continue;
            }
        };
        let pending = if matches!(message.kind.as_str(), "ack" | "error" | "app_result") {
            message
                .request_id
                .as_ref()
                .and_then(|request_id| lock(&state.pending).get(request_id).cloned())
        } else {
            None
        };
        if let Some(sender) = pending {
            let _ = sender.send(message);
        } else {
            client.dispatch_message(message).await;
        }
    }

    if let Some(state) = inner.upgrade() {
        let client = Client { inner: state };
        if client.inner.connected.swap(false, Ordering::AcqRel) {
            client.fail_pending();
            let _ = client.inner.events.send(ClientEvent::Disconnected);
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
        let client = Client { inner: state };
        if !client.is_connected() {
            return;
        }
        let processes: Vec<_> = client
            .inner
            .processes
            .read()
            .await
            .values()
            .cloned()
            .collect();
        if !processes.is_empty() {
            let metrics: Vec<_> = processes.iter().map(|runtime| runtime.metrics()).collect();
            let _ = client.report_worker_metrics(&metrics).await;
        }
    }
}

struct PendingResponse {
    receiver: mpsc::UnboundedReceiver<Message>,
    buffered: VecDeque<Message>,
    request_id: String,
    inner: Weak<Inner>,
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            lock(&inner.pending).remove(&self.request_id);
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
        }
    }

    async fn invoke(&self, data: Map<String, Value>) -> std::result::Result<Value, String> {
        let _permit = self.acquire().await;
        let started = Instant::now();
        let result = (self.handler)(data).await;
        let micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.total_latency_micros
            .fetch_add(micros, Ordering::Relaxed);
        self.completed.fetch_add(1, Ordering::Relaxed);
        result
    }

    async fn acquire(&self) -> ProcessPermit<'_> {
        self.queued.fetch_add(1, Ordering::Relaxed);
        loop {
            let notified = self.notify.notified();
            let active = self.active.load(Ordering::Acquire);
            let capacity = self.capacity.load(Ordering::Acquire);
            if active < capacity
                && self
                    .active
                    .compare_exchange(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                self.queued.fetch_sub(1, Ordering::Relaxed);
                return ProcessPermit { runtime: self };
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
        for (key, value) in values {
            self.set_with_options(key, value, ttl, persistent).await?;
        }
        Ok(())
    }

    pub async fn mget<T: DeserializeOwned>(
        &self,
        keys: &[String],
    ) -> Result<HashMap<String, Option<T>>> {
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
        let future = handler(data);
        Box::pin(async move {
            let value = future.await.map_err(|error| error.to_string())?;
            serde_json::to_value(value).map_err(|error| error.to_string())
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
        "timeout" | "connection_closed" => Err(Error::Timeout {
            request_id: message.request_id.unwrap_or_default(),
            timeout: Duration::ZERO,
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
    if value.is_empty() {
        Err(Error::Protocol(format!("{name} must not be empty")))
    } else {
        Ok(())
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
}
