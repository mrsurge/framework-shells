//! Isolated experimental service. FWS manager integration is deliberately absent.
use crate::{
    framed,
    policy::{AttachmentState, WriterLease},
    replay::{Cursor, Source},
    session::{self, DEADLINE, Wire},
};
use anyhow::{Result, anyhow, ensure};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex as SyncMutex},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::UnixListener,
    process::{ChildStdin, Command},
    sync::{Mutex, Semaphore, broadcast, watch},
    task::JoinSet,
    time::timeout,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub socket: PathBuf,
    pub stdout_log: PathBuf,
    pub stderr_log: PathBuf,
    pub command: Vec<String>,
    pub cwd: Option<PathBuf>,
    pub attachment: Option<crate::attachment::ShellIdentity>,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Resume {
        cursor: Cursor,
    },
    Status,
    Acquire {
        #[serde(default)]
        takeover: bool,
    },
    Write {
        generation: u64,
        data: String,
    },
    Eof {
        generation: u64,
    },
    Detach {
        generation: u64,
    },
    Stop,
}
#[derive(Clone, Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
enum Event {
    Output {
        stream: String,
        offset: u64,
        data: String,
        #[serde(skip_serializing)]
        len: usize,
    },
    Exited {
        code: Option<i32>,
    },
    Failure {
        message: String,
    },
    Gap {
        skipped: u64,
    },
}
struct State {
    policy: AttachmentState,
    stdin: Option<ChildStdin>,
    exited: bool,
    code: Option<i32>,
    failure: Option<String>,
}
struct Shared {
    state: Mutex<State>,
    instance: String,
    pid: u32,
    events: broadcast::Sender<Event>,
    stop: watch::Sender<bool>,
    published: SyncMutex<Published>,
    stdout_source: Source,
    stderr_source: Source,
}

#[derive(Clone, Default)]
struct Published {
    end: Cursor,
    exit: Option<Option<i32>>,
    failure: Option<String>,
}

struct Endpoint {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        if let Ok(meta) = std::fs::symlink_metadata(&self.path)
            && (meta.dev(), meta.ino()) == (self.dev, self.ino)
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn bind(path: &Path) -> Result<(UnixListener, Endpoint)> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("socket requires private parent directory"))?;
    let metadata = std::fs::symlink_metadata(parent)?;
    ensure!(
        metadata.is_dir() && metadata.mode() & 0o077 == 0,
        "socket directory must be private (0700)"
    );
    // Unix euid comparison avoids trusting a private directory owned by somebody else.
    ensure!(
        metadata.uid() == unsafe { libc::geteuid() },
        "socket directory owner mismatch"
    );
    let listener = UnixListener::bind(path)?; // Never unlink somebody else's/stale socket.
    let metadata = std::fs::symlink_metadata(path)?;
    let guard = Endpoint {
        path: path.to_owned(),
        dev: metadata.dev(),
        ino: metadata.ino(),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok((listener, guard))
}

async fn new_log(path: &Path) -> Result<File> {
    Ok(tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await?)
}

fn signal_group(pid: u32) -> Result<()> {
    ensure!(pid > 1 && pid <= i32::MAX as u32, "invalid child group");
    // Child owns its process group; never signal the supervisor/launcher's group.
    if unsafe { libc::kill(-(pid as i32), libc::SIGKILL) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error.into());
        }
    }
    Ok(())
}

async fn capture<R: AsyncRead + Unpin>(
    mut reader: R,
    mut log: File,
    stream: &str,
    shared: Arc<Shared>,
) -> Result<()> {
    let mut buffer = [0u8; 8192];
    let mut offset = 0u64;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        log.write_all(&buffer[..count]).await?;
        log.flush().await?; // Observation is published only after bytes are visible to raw readers.
        let mut published = shared.published.lock().unwrap();
        published.end.set(stream, offset + count as u64)?;
        let _ = shared.events.send(Event::Output {
            stream: stream.into(),
            offset,
            data: B64.encode(&buffer[..count]),
            len: count,
        });
        offset += count as u64;
    }
}

async fn handle(request: Request, connection: &str, shared: &Shared) -> Result<Value> {
    let mut state = shared.state.lock().await;
    let lease = |generation| WriterLease {
        instance: shared.instance.clone(),
        generation,
        connection: connection.to_owned(),
    };
    match request {
        Request::Resume { .. } => Err(anyhow!("resume requires a fresh observer connection")),
        Request::Status => Ok(
            json!({"pid":shared.pid, "instance":shared.instance, "exited":state.exited, "code":state.code, "failure":state.failure}),
        ),
        Request::Acquire { takeover } => {
            let lease = state.policy.acquire(connection.to_owned(), takeover)?;
            Ok(json!({"generation":lease.generation}))
        }
        Request::Write { generation, data } => {
            ensure!(
                state.policy.can_write(&lease(generation)),
                "stale or unavailable writer"
            );
            let bytes = B64.decode(data)?;
            ensure!(bytes.len() <= 32768, "write exceeds 32768 bytes");
            let stdin = state
                .stdin
                .as_mut()
                .ok_or_else(|| anyhow!("stdin closed"))?;
            // The lock fences takeover until this write completes or times out.
            match timeout(DEADLINE, stdin.write_all(&bytes)).await {
                Ok(Ok(())) => Ok(json!({"written":bytes.len()})),
                _ => Err(anyhow!(
                    "delivery_uncertain: write failed or timed out; do not replay"
                )),
            }
        }
        Request::Eof { generation } => {
            state.policy.eof(&lease(generation))?;
            state.stdin.take();
            Ok(json!({"stdin_closed":true}))
        }
        Request::Detach { generation } => {
            state.policy.detach(&lease(generation));
            Ok(json!({"detached":true}))
        }
        Request::Stop => {
            if !state.exited {
                signal_group(shared.pid)?;
            }
            Ok(json!({"stopping":true}))
        }
    }
}

async fn replay(
    wire: &mut Wire,
    cursor: Cursor,
    shared: &Shared,
) -> Result<(broadcast::Receiver<Event>, Cursor)> {
    // Publication and subscription/snapshot share this short lock. File I/O and
    // socket sends never hold it, so an observer cannot block child capture.
    let (output, boundary) = {
        let published = shared.published.lock().unwrap();
        (shared.events.subscribe(), published.clone())
    };
    let count = cursor.replay_bytes(boundary.end)?;
    let mut stdout = shared
        .stdout_source
        .open(cursor.stdout, boundary.end.stdout)
        .await?;
    let mut stderr = shared
        .stderr_source
        .open(cursor.stderr, boundary.end.stderr)
        .await?;
    session::send(
        wire,
        &json!({"event":"replay_begin", "cursor":cursor, "end":boundary.end}),
    )
    .await?;
    let mut buffer = [0u8; 8192];
    for (stream, file, start, end) in [
        ("stdout", &mut stdout, cursor.stdout, boundary.end.stdout),
        ("stderr", &mut stderr, cursor.stderr, boundary.end.stderr),
    ] {
        let mut offset = start;
        while offset < end {
            let count = (end - offset).min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..count]).await?;
            session::send(
                wire,
                &Event::Output {
                    stream: stream.into(),
                    offset,
                    data: B64.encode(&buffer[..count]),
                    len: count,
                },
            )
            .await?;
            offset += count as u64;
        }
    }
    shared
        .stdout_source
        .verify(&stdout, boundary.end.stdout)
        .await?;
    shared
        .stderr_source
        .verify(&stderr, boundary.end.stderr)
        .await?;
    // Lifecycle that predates the snapshot is sticky; later lifecycle stays in
    // the subscribed lane. Cross-stream output ordering is not reconstructed.
    if let Some(code) = boundary.exit {
        session::send(wire, &Event::Exited { code }).await?;
    }
    if let Some(message) = boundary.failure {
        session::send(wire, &Event::Failure { message }).await?;
    }
    session::send(
        wire,
        &json!({"ok":true,"result":{"resumed":true,"end":boundary.end,"replayed_bytes":count}}),
    )
    .await?;
    Ok((output, boundary.end))
}

fn after_boundary(event: Event, cursor: &mut Option<Cursor>) -> Result<Option<Event>> {
    if let Event::Output {
        stream,
        offset,
        data,
        len,
    } = event
    {
        if let Some(cursor) = cursor {
            let Some(skip) = cursor.accept(&stream, offset, len)? else {
                return Ok(None);
            };
            if skip == 0 {
                return Ok(Some(Event::Output {
                    stream,
                    offset,
                    data,
                    len,
                }));
            }
            let bytes = B64.decode(&data)?;
            return Ok(Some(Event::Output {
                stream,
                offset: offset + skip as u64,
                data: B64.encode(&bytes[skip..]),
                len: len - skip,
            }));
        }
        return Ok(Some(Event::Output {
            stream,
            offset,
            data,
            len,
        }));
    }
    Ok(Some(event))
}

async fn serve(mut wire: Wire, secret: Arc<Vec<u8>>, shared: Arc<Shared>) -> Result<()> {
    session::authenticate_server(&mut wire, &secret, &shared.instance).await?;
    let connection = session::nonce()?;
    let mut output = shared.events.subscribe();
    session::send(
        &mut wire,
        &json!({"event":"attached", "instance":shared.instance, "pid":shared.pid, "replay":true}),
    )
    .await?;
    let mut first_request = true;
    let mut cursor = None;
    let result = async {
        loop {
            tokio::select! {
                request = session::receive::<Request>(&mut wire) => {
                    let request = request?;
                    if let Request::Resume { cursor: requested } = request {
                        if !first_request { return Err(anyhow!("resume requires a fresh observer connection")); }
                        first_request = false;
                        let replayed = timeout(DEADLINE, replay(&mut wire, requested, &shared))
                            .await.unwrap_or_else(|_| Err(anyhow!("replay deadline exceeded")));
                        match replayed {
                            Ok((receiver, end)) => { output = receiver; cursor = Some(end); }
                            Err(error) => {
                                let _ = session::send(&mut wire, &json!({"ok":false,"error":format!("replay_unavailable: {error}")})).await;
                                return Err(error);
                            }
                        }
                        continue;
                    }
                    first_request = false;
                    let stopping = matches!(request, Request::Stop);
                    let response = match handle(request, &connection, &shared).await {
                        Ok(value) => json!({"ok":true,"result":value}),
                        Err(error) => json!({"ok":false,"error":error.to_string()}),
                    };
                    let stop_ok = stopping && response["ok"] == true;
                    let sent = session::send(&mut wire, &response).await;
                    // Stop still takes effect if its ACK is lost.
                    if stop_ok { let _ = shared.stop.send(true); }
                    sent?;
                    if stop_ok { return Ok(()); }
                }
                event = output.recv() => match event {
                    Ok(event) => {
                        match after_boundary(event, &mut cursor) {
                            Ok(Some(event)) => session::send(&mut wire, &event).await?,
                            Ok(None) => (),
                            Err(error) => {
                                session::send(&mut wire, &Event::Gap { skipped: 0 }).await?;
                                return Err(error);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        session::send(&mut wire, &Event::Gap { skipped }).await?;
                        return Err(anyhow!("output gap; reconnect and inspect raw logs"));
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    .await;
    // Disconnect is detach, not child EOF. A stale disconnect cannot revoke takeover.
    let mut state = shared.state.lock().await;
    state.policy.detach_connection(&connection);
    result
}

pub async fn run(config: Config, secret: Vec<u8>) -> Result<()> {
    ensure!(
        !secret.is_empty() && !config.command.is_empty(),
        "secret and command required"
    );
    let (listener, _endpoint) = bind(&config.socket)?;
    let instance = session::nonce()?;
    let cwd = std::fs::canonicalize(config.cwd.as_deref().unwrap_or(Path::new(".")))?;
    let attachment_path = crate::attachment::record_path(&config.socket);
    let attachment_enabled = config.attachment.is_some();
    if let Some(identity) = &config.attachment {
        identity.validate()?;
        ensure!(
            config.socket.is_absolute()
                && config.stdout_log.is_absolute()
                && config.stderr_log.is_absolute(),
            "attachment requires absolute paths"
        );
        match std::fs::symlink_metadata(&attachment_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(error.into()),
            Ok(_) => anyhow::bail!("attachment already exists; explicit recovery required"),
        }
    }
    let stdout_log = new_log(&config.stdout_log).await?;
    let stderr_log = new_log(&config.stderr_log).await?;
    let stdout_source = Source::new(config.stdout_log.clone(), &stdout_log).await?;
    let stderr_source = Source::new(config.stderr_log.clone(), &stderr_log).await?;
    let mut command = Command::new(&config.command[0]);
    if let Some(cwd) = &config.cwd {
        command.current_dir(cwd);
    }
    command
        .args(&config.command[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .process_group(0);
    let mut child = command.spawn()?;
    let pid = child.id().ok_or_else(|| anyhow!("missing child pid"))?;
    if let Some(identity) = config.attachment {
        let descriptor = crate::attachment::Descriptor {
            version: 1,
            runtime_id: crate::attachment::runtime_id(&secret),
            shell: identity,
            instance: instance.clone(),
            pid,
            supervisor_pid: std::process::id(),
            socket: config.socket.clone(),
            stdout_log: config.stdout_log.clone(),
            stderr_log: config.stderr_log.clone(),
            command: config.command.clone(),
            cwd,
        };
        if let Err(error) = descriptor.publish(&attachment_path, &secret) {
            let _ = signal_group(pid);
            let _ = child.wait().await;
            return Err(error);
        }
    }
    let (events, _) = broadcast::channel(64);
    let (stop, mut stopped) = watch::channel(false);
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            policy: AttachmentState::new(instance.clone())?,
            stdin: child.stdin.take(),
            exited: false,
            code: None,
            failure: None,
        }),
        instance,
        pid,
        events,
        stop,
        published: SyncMutex::new(Published::default()),
        stdout_source,
        stderr_source,
    });
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow!("missing stdout"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("missing stderr"))?;
    let mut capture_tasks = JoinSet::new();
    capture_tasks.spawn(capture(stdout, stdout_log, "stdout", shared.clone()));
    capture_tasks.spawn(capture(stderr, stderr_log, "stderr", shared.clone()));
    let capacity = Arc::new(Semaphore::new(16));
    let secret = Arc::new(secret);
    let mut sessions = JoinSet::new();
    let mut child_exited = false;
    println!(
        "{}",
        json!({"ready":true,"instance":shared.instance,"pid":pid,"socket":config.socket,
            "attachment_file":attachment_enabled.then_some(&attachment_path)})
    );
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (socket, _) = accepted?;
                if let Ok(permit) = capacity.clone().try_acquire_owned() {
                    let (secret, shared) = (secret.clone(), shared.clone());
                    sessions.spawn(async move { let _permit = permit; let _ = serve(framed(socket), secret, shared).await; });
                }
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {}
            completed = capture_tasks.join_next(), if !capture_tasks.is_empty() => {
                if let Some(result) = completed {
                    let failure = match result { Ok(Ok(())) => None, Ok(Err(e)) => Some(e.to_string()), Err(e) => Some(e.to_string()) };
                    if let Some(message) = failure {
                        let mut state = shared.state.lock().await;
                        state.failure = Some(message.clone());
                        state.policy.child_exited();
                        state.stdin.take();
                        let mut published = shared.published.lock().unwrap();
                        published.failure = Some(message.clone());
                        let _ = shared.events.send(Event::Failure { message });
                        if !state.exited { signal_group(pid)?; }
                    }
                }
            }
            status = child.wait(), if !child_exited => {
                let status = status?;
                child_exited = true;
                let mut state = shared.state.lock().await;
                state.exited = true;
                state.code = status.code();
                state.policy.child_exited();
                state.stdin.take();
                let mut published = shared.published.lock().unwrap();
                published.exit = Some(status.code());
                let _ = shared.events.send(Event::Exited { code:status.code() });
            }
            _ = stopped.changed() => break,
        }
    }
    sessions.abort_all();
    while sessions.join_next().await.is_some() {}
    if !child_exited {
        signal_group(pid)?;
        let _ = timeout(DEADLINE, child.wait()).await??;
    }
    timeout(DEADLINE, async {
        while let Some(result) = capture_tasks.join_next().await {
            result??;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_endpoint_does_not_replace_existing_paths() {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let directory = tempfile::tempdir().unwrap();
                std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700))
                    .unwrap();
                let path = directory.path().join("socket");
                let (listener, endpoint) = bind(&path).unwrap();
                assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
                assert!(bind(&path).is_err());
                drop(listener);
                drop(endpoint);
                assert!(!path.exists());
                std::fs::write(&path, "do not replace").unwrap();
                assert!(bind(&path).is_err());
                assert_eq!(std::fs::read_to_string(&path).unwrap(), "do not replace");
                std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755))
                    .unwrap();
                assert!(bind(&directory.path().join("another")).is_err());
            });
    }
}
