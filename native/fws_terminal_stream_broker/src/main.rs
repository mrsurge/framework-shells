use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use nix::pty::{openpty, Winsize};
use nix::sys::signal::{kill as nix_kill, Signal};
use nix::unistd::Pid;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use signal_hook::consts::signal::{SIGINT, SIGTERM};
use signal_hook::iterator::Signals;
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_COLS: u16 = 80;
const DEFAULT_ROWS: u16 = 24;
const DEFAULT_TERM: &str = "xterm-256color";
const READ_CHUNK_BYTES: usize = 65536;
const POLL_TIMEOUT_MS: i32 = 50;

#[derive(Debug)]
enum BrokerCommand {
    Connect {
        cols: Option<u16>,
        rows: Option<u16>,
    },
    Input(Vec<u8>),
    Resize {
        cols: u16,
        rows: u16,
    },
    Destroy,
    Ping {
        nonce: Option<Value>,
    },
    StdinClosed,
    ShutdownSignal {
        signal_name: &'static str,
    },
}

#[derive(Debug, Deserialize)]
struct JsonRpcNotification {
    jsonrpc: Option<String>,
    method: Option<String>,
    #[serde(default)]
    params: Option<Map<String, Value>>,
}

#[derive(Debug)]
struct BrokerConfig {
    shell_cmd: Vec<String>,
    cwd: String,
    cols: u16,
    rows: u16,
    term: String,
}

fn log_error(message: &str) {
    eprintln!("[terminal_stream_broker_rs] {message}");
}

fn log_error_with<E: std::fmt::Display>(message: &str, error: E) {
    eprintln!("[terminal_stream_broker_rs] {message}: {error}");
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis() as u64
}

fn parse_positive_u16(raw: Option<&str>, fallback: u16) -> u16 {
    raw.and_then(|value| value.trim().parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
}

fn parse_positive_value(raw: Option<&Value>, fallback: u16) -> u16 {
    match raw {
        Some(Value::Number(num)) => num
            .as_u64()
            .and_then(|value| u16::try_from(value).ok())
            .filter(|value| *value > 0)
            .unwrap_or(fallback),
        Some(Value::String(text)) => parse_positive_u16(Some(text), fallback),
        _ => fallback,
    }
}

fn resolve_shell_command() -> Vec<String> {
    if let Ok(env_json) = env::var("TERMINAL_STREAM_SHELL_CMD_JSON") {
        match serde_json::from_str::<Value>(&env_json) {
            Ok(Value::Array(parts)) => {
                let resolved: Vec<String> = parts
                    .into_iter()
                    .filter_map(|part| match part {
                        Value::String(text) if !text.trim().is_empty() => Some(text),
                        other => {
                            let text = other.to_string();
                            if text.trim().is_empty() {
                                None
                            } else {
                                Some(text)
                            }
                        }
                    })
                    .collect();
                if !resolved.is_empty() {
                    return resolved;
                }
            }
            Ok(_) => {
                log_error("TERMINAL_STREAM_SHELL_CMD_JSON must decode to a non-empty array");
            }
            Err(error) => {
                log_error_with("failed to parse TERMINAL_STREAM_SHELL_CMD_JSON", error);
            }
        }
    }

    let args: Vec<String> = env::args().collect();
    if let Some(sep_idx) = args.iter().position(|part| part == "--") {
        let resolved: Vec<String> = args.into_iter().skip(sep_idx + 1).collect();
        if !resolved.is_empty() {
            return resolved;
        }
    }

    vec!["sh".to_string(), "-i".to_string()]
}

fn load_config() -> BrokerConfig {
    let cwd = env::var("TERMINAL_STREAM_CWD").unwrap_or_else(|_| {
        env::current_dir()
            .ok()
            .and_then(|path| path.into_os_string().into_string().ok())
            .unwrap_or_else(|| ".".to_string())
    });
    let cols = parse_positive_u16(
        env::var("TERMINAL_STREAM_COLS").ok().as_deref(),
        DEFAULT_COLS,
    );
    let rows = parse_positive_u16(
        env::var("TERMINAL_STREAM_ROWS").ok().as_deref(),
        DEFAULT_ROWS,
    );
    let term = env::var("TERM").unwrap_or_else(|_| DEFAULT_TERM.to_string());

    BrokerConfig {
        shell_cmd: resolve_shell_command(),
        cwd,
        cols,
        rows,
        term,
    }
}

fn write_json_line(value: &Value, stdout: &mut dyn Write) -> io::Result<()> {
    serde_json::to_writer(&mut *stdout, value)?;
    stdout.write_all(b"\n")?;
    stdout.flush()
}

fn emit_ready(
    stdout: &mut dyn Write,
    child_pid: u32,
    shell_cmd: &[String],
    cwd: &str,
) -> io::Result<()> {
    write_json_line(
        &json!({
            "type": "ready",
            "ts": now_ms(),
            "pid": child_pid,
            "shell": shell_cmd,
            "cwd": cwd,
        }),
        stdout,
    )
}

fn emit_data(stdout: &mut dyn Write, seq: u64, bytes: &[u8]) -> io::Result<()> {
    write_json_line(
        &json!({
            "type": "data",
            "seq": seq,
            "ts": now_ms(),
            "data_b64": BASE64.encode(bytes),
        }),
        stdout,
    )
}

fn emit_pong(stdout: &mut dyn Write, nonce: Option<Value>) -> io::Result<()> {
    write_json_line(
        &json!({
            "type": "pong",
            "nonce": nonce.unwrap_or(Value::Null),
        }),
        stdout,
    )
}

fn emit_closed(
    stdout: &mut dyn Write,
    seq: u64,
    exit_code: Option<i32>,
    reason: &str,
) -> io::Result<()> {
    write_json_line(
        &json!({
            "type": "closed",
            "seq": seq,
            "ts": now_ms(),
            "exit_code": exit_code,
            "reason": reason,
        }),
        stdout,
    )
}

fn parse_notification(line: &str) -> Option<BrokerCommand> {
    let parsed = match serde_json::from_str::<JsonRpcNotification>(line) {
        Ok(value) => value,
        Err(error) => {
            log_error_with("bad JSON command", error);
            return None;
        }
    };

    if parsed.jsonrpc.as_deref() != Some("2.0") {
        log_error("unexpected stdin payload without jsonrpc envelope");
        return None;
    }

    let method = parsed.method.unwrap_or_default();
    if method.is_empty() {
        log_error("stdin payload missing method");
        return None;
    }

    let params = parsed.params.unwrap_or_default();
    match method.as_str() {
        "terminal.connect" => Some(BrokerCommand::Connect {
            cols: params
                .get("cols")
                .map(|value| parse_positive_value(Some(value), DEFAULT_COLS)),
            rows: params
                .get("rows")
                .map(|value| parse_positive_value(Some(value), DEFAULT_ROWS)),
        }),
        "terminal.input" => {
            let Some(data_b64) = params.get("data_b64").and_then(Value::as_str) else {
                return None;
            };
            match BASE64.decode(data_b64.as_bytes()) {
                Ok(bytes) if !bytes.is_empty() => Some(BrokerCommand::Input(bytes)),
                Ok(_) => None,
                Err(error) => {
                    log_error_with("failed to decode input frame", error);
                    None
                }
            }
        }
        "terminal.resize" => Some(BrokerCommand::Resize {
            cols: parse_positive_value(params.get("cols"), DEFAULT_COLS),
            rows: parse_positive_value(params.get("rows"), DEFAULT_ROWS),
        }),
        "terminal.destroy" => Some(BrokerCommand::Destroy),
        "terminal.ping" => Some(BrokerCommand::Ping {
            nonce: params.get("nonce").cloned(),
        }),
        other => {
            log_error(&format!("unsupported JSON-RPC method: {other}"));
            None
        }
    }
}

fn spawn_stdin_reader(tx: Sender<BrokerCommand>) {
    thread::Builder::new()
        .name("terminal-stream-stdin-reader".to_string())
        .spawn(move || {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin.lock());
            for line in reader.lines() {
                match line {
                    Ok(line) => {
                        if line.trim().is_empty() {
                            continue;
                        }
                        if let Some(command) = parse_notification(&line) {
                            if tx.send(command).is_err() {
                                return;
                            }
                        }
                    }
                    Err(error) => {
                        log_error_with("stdin read failed", error);
                        break;
                    }
                }
            }
            let _ = tx.send(BrokerCommand::StdinClosed);
        })
        .expect("failed to spawn stdin reader thread");
}

fn spawn_signal_reader(tx: Sender<BrokerCommand>) {
    thread::Builder::new()
        .name("terminal-stream-signal-reader".to_string())
        .spawn(move || {
            let mut signals = match Signals::new([SIGINT, SIGTERM]) {
                Ok(signals) => signals,
                Err(error) => {
                    log_error_with("failed to register signal handlers", error);
                    return;
                }
            };
            for signal in signals.forever() {
                let signal_name = match signal {
                    SIGINT => "SIGINT",
                    SIGTERM => "SIGTERM",
                    _ => "signal",
                };
                if tx
                    .send(BrokerCommand::ShutdownSignal { signal_name })
                    .is_err()
                {
                    return;
                }
            }
        })
        .expect("failed to spawn signal reader thread");
}

fn apply_resize(master_fd: i32, cols: u16, rows: u16) -> io::Result<()> {
    let winsize = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let rc = unsafe { libc::ioctl(master_fd, libc::TIOCSWINSZ, &winsize) };
    if rc < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn write_all_fd(fd: i32, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        let written = unsafe { libc::write(fd, buf.as_ptr().cast(), buf.len()) };
        if written < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        let written = usize::try_from(written).unwrap_or(0);
        if written == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "short write to PTY",
            ));
        }
        buf = &buf[written..];
    }
    Ok(())
}

fn signal_child(child: &Child, signal: Signal) -> io::Result<()> {
    let pid = Pid::from_raw(i32::try_from(child.id()).unwrap_or_default());
    nix_kill(pid, signal).map_err(io::Error::other)
}

fn dup_file(fd: i32) -> io::Result<File> {
    let duplicated = unsafe { libc::dup(fd) };
    if duplicated < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(unsafe { File::from_raw_fd(duplicated) })
    }
}

fn spawn_pty_child(config: &BrokerConfig) -> io::Result<(Child, File)> {
    let winsize = Winsize {
        ws_row: config.rows,
        ws_col: config.cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty = openpty(Some(&winsize), None).map_err(io::Error::other)?;
    let master_fd = pty.master;
    let slave_fd = pty.slave;

    let slave_raw_fd = slave_fd.as_raw_fd();
    let stdin_file = dup_file(slave_raw_fd)?;
    let stdout_file = dup_file(slave_raw_fd)?;
    let stderr_file = dup_file(slave_raw_fd)?;

    let mut command = Command::new(&config.shell_cmd[0]);
    if config.shell_cmd.len() > 1 {
        command.args(&config.shell_cmd[1..]);
    }
    command.current_dir(&config.cwd);
    command.stdin(Stdio::from(stdin_file));
    command.stdout(Stdio::from(stdout_file));
    command.stderr(Stdio::from(stderr_file));
    command.env("TERM", &config.term);

    let slave_for_child = slave_raw_fd;
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::ioctl(slave_for_child, libc::TIOCSCTTY, 0) < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let child = command.spawn()?;
    let master_file = File::from(master_fd);
    drop(slave_fd);
    Ok((child, master_file))
}

fn drain_commands(
    rx: &Receiver<BrokerCommand>,
    child: &mut Child,
    master_fd: i32,
    stdout: &mut dyn Write,
    shutting_down: &mut bool,
) -> io::Result<()> {
    loop {
        match rx.try_recv() {
            Ok(command) => match command {
                BrokerCommand::Connect { cols, rows } => {
                    if let (Some(cols), Some(rows)) = (cols, rows) {
                        if let Err(error) = apply_resize(master_fd, cols, rows) {
                            log_error_with("connect resize failed", error);
                        }
                    }
                }
                BrokerCommand::Input(bytes) => {
                    if let Err(error) = write_all_fd(master_fd, &bytes) {
                        log_error_with("failed to write input into PTY", error);
                    }
                }
                BrokerCommand::Resize { cols, rows } => {
                    if let Err(error) = apply_resize(master_fd, cols, rows) {
                        log_error_with("resize failed", error);
                    }
                }
                BrokerCommand::Destroy => {
                    *shutting_down = true;
                    if let Err(error) = signal_child(child, Signal::SIGTERM) {
                        log_error_with("destroy failed", error);
                    }
                }
                BrokerCommand::Ping { nonce } => {
                    emit_pong(stdout, nonce)?;
                }
                BrokerCommand::StdinClosed => {
                    *shutting_down = true;
                    if let Err(error) = signal_child(child, Signal::SIGTERM) {
                        log_error_with("stdin closed while killing PTY", error);
                    }
                }
                BrokerCommand::ShutdownSignal { signal_name } => {
                    *shutting_down = true;
                    if let Err(error) = signal_child(child, Signal::SIGTERM) {
                        log_error_with(&format!("failed to kill PTY on {signal_name}"), error);
                    }
                }
            },
            Err(TryRecvError::Empty) => return Ok(()),
            Err(TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn closed_reason(status: std::process::ExitStatus, shutting_down: bool) -> (Option<i32>, String) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return (status.code(), format!("signal:{signal}"));
        }
    }
    if shutting_down {
        return (status.code(), "terminated".to_string());
    }
    (status.code(), "exited".to_string())
}

fn main_loop(
    mut child: Child,
    mut master_file: File,
    shell_cmd: &[String],
    cwd: &str,
    rx: Receiver<BrokerCommand>,
) -> io::Result<i32> {
    let mut stdout = io::stdout().lock();
    emit_ready(&mut stdout, child.id(), shell_cmd, cwd)?;

    let master_fd = master_file.as_raw_fd();
    let mut seq: u64 = 0;
    let mut shutting_down = false;
    let mut saw_pty_eof = false;
    let mut child_status: Option<std::process::ExitStatus> = None;
    let mut buffer = vec![0_u8; READ_CHUNK_BYTES];

    loop {
        drain_commands(&rx, &mut child, master_fd, &mut stdout, &mut shutting_down)?;

        if child_status.is_none() {
            child_status = child.try_wait()?;
        }

        if child_status.is_some() && saw_pty_eof {
            let status = child_status.expect("child_status checked above");
            seq += 1;
            let (exit_code, reason) = closed_reason(status, shutting_down);
            emit_closed(&mut stdout, seq, exit_code, &reason)?;
            return Ok(status.code().unwrap_or_default());
        }

        let mut poll_fd = libc::pollfd {
            fd: master_fd,
            events: libc::POLLIN | libc::POLLHUP,
            revents: 0,
        };
        let poll_result = unsafe { libc::poll(&mut poll_fd, 1, POLL_TIMEOUT_MS) };
        if poll_result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if poll_result == 0 {
            continue;
        }
        if (poll_fd.revents & libc::POLLNVAL) != 0 {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "PTY poll returned POLLNVAL",
            ));
        }
        if (poll_fd.revents & libc::POLLERR) != 0 {
            log_error("PTY poll returned POLLERR");
        }
        if (poll_fd.revents & (libc::POLLIN | libc::POLLHUP)) == 0 {
            continue;
        }

        match master_file.read(&mut buffer) {
            Ok(0) => {
                saw_pty_eof = true;
            }
            Ok(read_count) => {
                seq += 1;
                emit_data(&mut stdout, seq, &buffer[..read_count])?;
            }
            Err(error) => {
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if error.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                if error.raw_os_error() == Some(libc::EIO) {
                    saw_pty_eof = true;
                    continue;
                }
                return Err(error);
            }
        }

        if saw_pty_eof {
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn main() {
    let config = load_config();
    if config.shell_cmd.is_empty() {
        log_error("empty shell command");
        std::process::exit(1);
    }

    let (tx, rx) = mpsc::channel::<BrokerCommand>();
    spawn_stdin_reader(tx.clone());
    spawn_signal_reader(tx);

    let (child, master_file) = match spawn_pty_child(&config) {
        Ok(parts) => parts,
        Err(error) => {
            log_error_with("failed to spawn PTY child", error);
            std::process::exit(1);
        }
    };

    match main_loop(child, master_file, &config.shell_cmd, &config.cwd, rx) {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            log_error_with("broker main loop failed", error);
            std::process::exit(1);
        }
    }
}
