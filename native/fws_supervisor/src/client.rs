//! Experimental asynchronous client. The actor owns framing and acknowledgements;
//! application bytes are never parsed. No reconnect or input retry is performed.
use crate::{
    framed,
    replay::Cursor,
    session::{Wire, authenticate_client, receive, send},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{fmt, path::Path, sync::Arc};
use tokio::{
    net::UnixStream,
    sync::{Mutex, mpsc, oneshot, watch},
    task::JoinHandle,
    time::{Duration, timeout},
};
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = 256;
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct ClientError {
    pub kind: &'static str,
    pub message: String,
}
impl ClientError {
    fn new(kind: &'static str, message: impl ToString) -> Self {
        Self {
            kind,
            message: message.to_string(),
        }
    }
}
impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.kind, self.message)
    }
}
impl std::error::Error for ClientError {}
pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Output {
        stream: String,
        offset: u64,
        data: Vec<u8>,
    },
    Exited {
        code: Option<i64>,
    },
}
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Status {
    pub pid: u32,
    pub instance: String,
    pub exited: bool,
    pub code: Option<i64>,
    pub failure: Option<String>,
}

enum Op {
    Acquire(bool),
    Write(Vec<u8>),
    Eof,
    Detach,
    Status,
    Stop,
}
struct Command {
    op: Op,
    reply: oneshot::Sender<Result<Value>>,
}
struct Inner {
    commands: mpsc::Sender<Command>,
    events: Mutex<mpsc::Receiver<Event>>,
    failure: watch::Receiver<Option<ClientError>>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
impl Client {
    pub async fn connect(path: impl AsRef<Path>, secret: &[u8], instance: &str) -> Result<Self> {
        Self::connect_inner(path, secret, instance, None).await
    }
    pub async fn connect_from(
        path: impl AsRef<Path>,
        secret: &[u8],
        instance: &str,
        cursor: Cursor,
    ) -> Result<Self> {
        Self::connect_inner(path, secret, instance, Some(cursor)).await
    }
    async fn connect_inner(
        path: impl AsRef<Path>,
        secret: &[u8],
        instance: &str,
        cursor: Option<Cursor>,
    ) -> Result<Self> {
        let (mut wire, replay_supported) = timeout(DEADLINE, async {
            let mut wire = framed(UnixStream::connect(path).await?);
            authenticate_client(&mut wire, secret, instance).await?;
            let attached: Value = receive(&mut wire).await?;
            anyhow::ensure!(
                attached["event"] == "attached" && attached["instance"] == instance,
                "invalid attachment"
            );
            Ok::<_, anyhow::Error>((wire, attached["replay"] == true))
        })
        .await
        .map_err(|_| ClientError::new("connect", "connection deadline"))?
        .map_err(|e| ClientError::new("connect", e))?;
        let (commands, rx) = mpsc::channel(64);
        let (events, output) = mpsc::channel(CAPACITY);
        let mut offsets = [None, None];
        if let Some(cursor) = cursor {
            if !replay_supported {
                return Err(ClientError::new(
                    "connect",
                    "supervisor does not support replay",
                ));
            }
            timeout(DEADLINE, resume(&mut wire, cursor, &events, &mut offsets))
                .await
                .map_err(|_| ClientError::new("connect", "replay deadline"))?
                .map_err(|e| ClientError::new("connect", e))?;
        }
        let (failure_tx, failure) = watch::channel(None);
        let cancel = CancellationToken::new();
        let shutdown = cancel.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                _ = shutdown.cancelled() => Err(ClientError::new("closed", "client closed")),
                result = run(&mut wire, rx, events, offsets) => result,
            };
            let error = result
                .err()
                .unwrap_or_else(|| ClientError::new("closed", "client stopped"));
            failure_tx.send_replace(Some(error));
            // Dropping the wire detaches the writer lease; it never means stdin EOF.
        });
        Ok(Self {
            inner: Arc::new(Inner {
                commands,
                events: Mutex::new(output),
                failure,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        })
    }
    async fn failure(&self) -> ClientError {
        // Channel closure can wake another executor thread before the actor has
        // published its reason. Do not turn a reported gap into a generic close.
        let mut failure = self.inner.failure.clone();
        loop {
            if let Some(error) = failure.borrow().clone() {
                return error;
            }
            if failure.changed().await.is_err() {
                return ClientError::new("closed", "client disconnected");
            }
        }
    }
    async fn request(&self, op: Op) -> Result<Value> {
        if let Some(error) = self.inner.failure.borrow().clone() {
            return Err(error);
        }
        let (reply, result) = oneshot::channel();
        match self.inner.commands.try_send(Command { op, reply }) {
            Ok(()) => (),
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(ClientError::new(
                    "busy",
                    "control queue full; command not sent",
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(self.failure().await),
        }
        match result.await {
            Ok(result) => result,
            Err(_) => Err(self.failure().await),
        }
    }
    pub async fn acquire(&self, takeover: bool) -> Result<u64> {
        let value = self.request(Op::Acquire(takeover)).await?;
        value["generation"]
            .as_u64()
            .ok_or_else(|| ClientError::new("protocol", "missing generation"))
    }
    pub async fn write(&self, data: &[u8]) -> Result<usize> {
        if data.len() > 32768 {
            return Err(ClientError::new(
                "invalid",
                "write exceeds 32768 bytes; not sent",
            ));
        }
        let value = self.request(Op::Write(data.to_vec())).await?;
        match value["written"].as_u64() {
            Some(n) if n == data.len() as u64 => Ok(n as usize),
            _ => Err(ClientError::new(
                "delivery_uncertain",
                "short/invalid write acknowledgement; do not replay",
            )),
        }
    }
    pub async fn eof(&self) -> Result<()> {
        self.request(Op::Eof).await.map(|_| ())
    }
    pub async fn detach(&self) -> Result<()> {
        self.request(Op::Detach).await.map(|_| ())
    }
    pub async fn stop(&self) -> Result<()> {
        self.request(Op::Stop).await.map(|_| ())
    }
    pub async fn status(&self) -> Result<Status> {
        serde_json::from_value(self.request(Op::Status).await?)
            .map_err(|e| ClientError::new("protocol", e))
    }
    pub async fn read(&self) -> Result<Event> {
        let mut events = self.inner.events.lock().await;
        // Deliver previously accepted events before a normal close, never after a gap.
        if let Some(error) = self.inner.failure.borrow().clone()
            && error.kind != "closed"
        {
            return Err(error);
        }
        match events.recv().await {
            Some(event) => Ok(event),
            None => Err(self.failure().await),
        }
    }
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        if let Some(task) = self.inner.task.lock().await.take() {
            let _ = task.await;
        }
    }
}

async fn resume(
    wire: &mut Wire,
    cursor: Cursor,
    events: &mpsc::Sender<Event>,
    offsets: &mut [Option<u64>; 2],
) -> anyhow::Result<()> {
    send(wire, &json!({"op":"resume","cursor":cursor})).await?;
    let mut end: Option<Cursor> = None;
    let mut replayed = 0u64;
    loop {
        let value: Value = receive(wire).await?;
        if value.get("ok").is_some() {
            anyhow::ensure!(value["ok"] == true, "replay rejected: {}", value["error"]);
            let end = end.ok_or_else(|| anyhow::anyhow!("replay ACK before boundary"))?;
            anyhow::ensure!(value["result"]["resumed"] == true, "invalid replay ACK");
            anyhow::ensure!(
                serde_json::from_value::<Cursor>(value["result"]["end"].clone())? == end,
                "replay boundary mismatch"
            );
            anyhow::ensure!(
                value["result"]["replayed_bytes"].as_u64() == Some(replayed),
                "replay byte count mismatch"
            );
            anyhow::ensure!(
                *offsets == [Some(end.stdout), Some(end.stderr)],
                "incomplete replay"
            );
            return Ok(());
        }
        if value["event"] == "replay_begin" {
            anyhow::ensure!(end.is_none(), "duplicate replay boundary");
            anyhow::ensure!(
                serde_json::from_value::<Cursor>(value["cursor"].clone())? == cursor,
                "requested cursor mismatch"
            );
            let boundary: Cursor = serde_json::from_value(value["end"].clone())?;
            replayed = cursor.replay_bytes(boundary)?;
            end = Some(boundary);
            *offsets = [Some(cursor.stdout), Some(cursor.stderr)];
        } else if let Some(end) = end {
            route(&value, events, offsets)?;
            anyhow::ensure!(
                offsets[0] <= Some(end.stdout) && offsets[1] <= Some(end.stderr),
                "output beyond replay boundary"
            );
        } else {
            // The existing live-only server may already have emitted output
            // before handling Resume. The marker establishes the new epoch.
            anyhow::ensure!(
                value["event"] == "output" || value["event"] == "exited",
                "unexpected pre-replay event"
            );
        }
    }
}

fn route(
    value: &Value,
    events: &mpsc::Sender<Event>,
    offsets: &mut [Option<u64>; 2],
) -> Result<()> {
    let event = match value["event"].as_str() {
        Some("output") => {
            let stream = value["stream"]
                .as_str()
                .filter(|s| *s == "stdout" || *s == "stderr")
                .ok_or_else(|| ClientError::new("protocol", "invalid output stream"))?;
            let offset = value["offset"]
                .as_u64()
                .ok_or_else(|| ClientError::new("protocol", "invalid offset"))?;
            let data = STANDARD
                .decode(value["data"].as_str().unwrap_or("!"))
                .map_err(|e| ClientError::new("protocol", e))?;
            if data.len() > 8192 {
                return Err(ClientError::new("protocol", "oversized output chunk"));
            }
            let index = usize::from(stream == "stderr");
            if offsets[index].is_some_and(|expected| expected != offset) {
                return Err(ClientError::new("lagged", "output offset gap"));
            }
            offsets[index] = Some(
                offset
                    .checked_add(data.len() as u64)
                    .ok_or_else(|| ClientError::new("protocol", "offset overflow"))?,
            );
            Event::Output {
                stream: stream.to_owned(),
                offset,
                data,
            }
        }
        Some("exited") => Event::Exited {
            code: value["code"].as_i64(),
        },
        Some("gap") => return Err(ClientError::new("lagged", "supervisor output gap")),
        Some("failure") => return Err(ClientError::new("remote", value["message"].to_string())),
        _ => {
            return Err(ClientError::new(
                "protocol",
                "unexpected supervisor message",
            ));
        }
    };
    events.try_send(event).map_err(|_| {
        ClientError::new(
            "lagged",
            "output consumer full or closed; reconnect explicitly",
        )
    })
}

async fn exchange(
    wire: &mut Wire,
    value: &Value,
    events: &mpsc::Sender<Event>,
    offsets: &mut [Option<u64>; 2],
) -> Result<Value> {
    send(wire, value)
        .await
        .map_err(|e| ClientError::new("closed", e))?;
    loop {
        let value: Value = receive(wire)
            .await
            .map_err(|e| ClientError::new("closed", e))?;
        if value.get("ok").is_some() {
            return match value["ok"].as_bool() {
                Some(true) if value["result"].is_object() => Ok(value["result"].clone()),
                Some(false) if value["error"].is_string() => {
                    let message = value["error"].as_str().unwrap();
                    Err(ClientError::new(
                        if message.starts_with("delivery_uncertain:") {
                            "delivery_uncertain"
                        } else {
                            "remote"
                        },
                        message,
                    ))
                }
                _ => Err(ClientError::new("protocol", "invalid acknowledgement")),
            };
        }
        route(&value, events, offsets)?;
    }
}
async fn run(
    wire: &mut Wire,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
    mut offsets: [Option<u64>; 2],
) -> Result<()> {
    let mut generation: Option<u64> = None;
    loop {
        tokio::select! {
            value = receive::<Value>(wire) => {
                route(&value.map_err(|e| ClientError::new("closed", e))?, &events, &mut offsets)?;
            }
            command = commands.recv() => {
                let Some(command) = command else { return Ok(()); };
                if command.reply.is_closed() { continue; }
                let mutating = matches!(command.op, Op::Write(_) | Op::Eof);
                let value = match &command.op {
                    Op::Acquire(takeover) => json!({"op":"acquire", "takeover":takeover}),
                    Op::Status => json!({"op":"status"}),
                    Op::Stop => json!({"op":"stop"}),
                    other => {
                        let Some(lease) = generation else {
                            let _ = command.reply.send(Err(ClientError::new("invalid", "writer lease required; not sent")));
                            continue;
                        };
                        match other {
                            Op::Write(data) => json!({"op":"write","generation":lease,"data":STANDARD.encode(data)}),
                            Op::Eof => json!({"op":"eof","generation":lease}),
                            Op::Detach => json!({"op":"detach","generation":lease}),
                            _ => unreachable!(),
                        }
                    }
                };
                let result = timeout(DEADLINE, exchange(wire, &value, &events, &mut offsets)).await
                    .unwrap_or_else(|_| Err(ClientError::new("closed", "acknowledgement deadline")));
                let result = result.and_then(|reply| {
                    let valid = match &command.op {
                        Op::Acquire(_) => reply["generation"].as_u64().is_some(),
                        Op::Write(data) => reply["written"].as_u64() == Some(data.len() as u64),
                        Op::Eof => reply["stdin_closed"] == true,
                        Op::Detach => reply["detached"] == true,
                        Op::Stop => reply["stopping"] == true,
                        Op::Status => serde_json::from_value::<Status>(reply.clone()).is_ok(),
                    };
                    if valid { Ok(reply) } else { Err(ClientError::new("protocol", "invalid acknowledgement result")) }
                }).map_err(|error| {
                    if mutating && error.kind != "remote" { ClientError::new("delivery_uncertain", format!("{error}; do not replay")) }
                    else { error }
                });
                let fatal = result.as_ref().err().filter(|e| e.kind != "remote").cloned();
                if let Ok(reply) = &result {
                    if matches!(command.op, Op::Acquire(_)) { generation = reply["generation"].as_u64(); }
                    if matches!(command.op, Op::Detach) { generation = None; }
                }
                let stopping = matches!(command.op, Op::Stop) && result.is_ok();
                let _ = command.reply.send(result);
                if let Some(error) = fatal { return Err(error); }
                if stopping { return Ok(()); }
            }
        }
    }
}
