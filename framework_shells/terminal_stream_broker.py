from __future__ import annotations

import base64
import errno
import fcntl
import json
import os
import pty
import queue
import select
import signal
import struct
import subprocess
import sys
import termios
import threading
import time
from dataclasses import dataclass
from typing import Any

DEFAULT_COLS = 80
DEFAULT_ROWS = 24
DEFAULT_TERM = "xterm-256color"
READ_CHUNK_BYTES = 65536
POLL_TIMEOUT_MS = 50


@dataclass(frozen=True)
class BrokerConfig:
    shell_cmd: list[str]
    cwd: str
    cols: int
    rows: int
    term: str


def log_error(message: str, error: BaseException | None = None) -> None:
    if error is None:
        print(f"[terminal_stream_broker_py] {message}", file=sys.stderr, flush=True)
        return
    print(f"[terminal_stream_broker_py] {message}: {error}", file=sys.stderr, flush=True)


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_positive_int(raw: object, fallback: int) -> int:
    if raw is None:
        return fallback
    try:
        value = int(str(raw).strip())
    except Exception:
        return fallback
    return value if value > 0 else fallback


def resolve_shell_command() -> list[str]:
    env_json = os.environ.get("TERMINAL_STREAM_SHELL_CMD_JSON", "").strip()
    if env_json:
        try:
            parsed = json.loads(env_json)
            if isinstance(parsed, list):
                resolved = [str(part) for part in parsed if str(part).strip()]
                if resolved:
                    return resolved
        except Exception as error:
            log_error("failed to parse TERMINAL_STREAM_SHELL_CMD_JSON", error)

    args = sys.argv[1:]
    if "--" in args:
        sep_idx = args.index("--")
        resolved = [str(part) for part in args[sep_idx + 1:] if str(part).strip()]
        if resolved:
            return resolved

    return ["sh", "-i"]


def load_config() -> BrokerConfig:
    cwd = os.environ.get("TERMINAL_STREAM_CWD") or os.getcwd()
    cols = parse_positive_int(os.environ.get("TERMINAL_STREAM_COLS"), DEFAULT_COLS)
    rows = parse_positive_int(os.environ.get("TERMINAL_STREAM_ROWS"), DEFAULT_ROWS)
    term = os.environ.get("TERM") or DEFAULT_TERM
    return BrokerConfig(
        shell_cmd=resolve_shell_command(),
        cwd=cwd,
        cols=cols,
        rows=rows,
        term=term,
    )


def write_json_line(frame: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(frame, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def emit_ready(child_pid: int, shell_cmd: list[str], cwd: str) -> None:
    write_json_line(
        {
            "type": "ready",
            "ts": now_ms(),
            "pid": child_pid,
            "shell": shell_cmd,
            "cwd": cwd,
        }
    )


def emit_data(seq: int, payload: bytes) -> None:
    write_json_line(
        {
            "type": "data",
            "seq": seq,
            "ts": now_ms(),
            "data_b64": base64.b64encode(payload).decode("ascii"),
        }
    )


def emit_pong(nonce: Any) -> None:
    write_json_line(
        {
            "type": "pong",
            "nonce": nonce,
        }
    )


def emit_closed(seq: int, exit_code: int | None, reason: str) -> None:
    write_json_line(
        {
            "type": "closed",
            "seq": seq,
            "ts": now_ms(),
            "exit_code": exit_code,
            "reason": reason,
        }
    )


def parse_notification(line: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(line)
    except Exception as error:
        log_error("bad JSON command", error)
        return None

    if not isinstance(payload, dict):
        return None
    if str(payload.get("jsonrpc") or "") != "2.0":
        log_error("unexpected stdin payload without jsonrpc envelope")
        return None

    method = str(payload.get("method") or "")
    if not method:
        log_error("stdin payload missing method")
        return None

    params = payload.get("params")
    return {
        "method": method,
        "params": params if isinstance(params, dict) else {},
    }


def spawn_stdin_reader(command_queue: queue.Queue[dict[str, Any]]) -> None:
    def _reader() -> None:
        for line in sys.stdin:
            if not line.strip():
                continue
            command = parse_notification(line)
            if command is not None:
                command_queue.put(command)
        command_queue.put({"type": "stdin_closed"})

    thread = threading.Thread(
        target=_reader,
        name="terminal-stream-stdin-reader",
        daemon=True,
    )
    thread.start()


def set_fd_nonblocking(fd: int) -> None:
    flags = int(fcntl.fcntl(fd, fcntl.F_GETFL))
    if flags & os.O_NONBLOCK:
        return
    _ = fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)


def apply_resize(master_fd: int, cols: int, rows: int) -> None:
    winsz = struct.pack("HHHH", max(1, rows), max(1, cols), 0, 0)
    _ = fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsz)


def write_all_fd(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        try:
            written = os.write(fd, view)
        except InterruptedError:
            continue
        except BlockingIOError:
            _ = select.select([], [fd], [], POLL_TIMEOUT_MS / 1000.0)
            continue
        if written <= 0:
            raise OSError(errno.EIO, "short write to PTY")
        view = view[written:]


def _spawn_preexec(slave_fd: int) -> Any:
    def _preexec() -> None:
        os.setsid()
        _ = fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)

    return _preexec


def spawn_pty_child(config: BrokerConfig) -> tuple[subprocess.Popen[bytes], int]:
    master_fd, slave_fd = pty.openpty()
    apply_resize(master_fd, config.cols, config.rows)

    stdin_fd = os.dup(slave_fd)
    stdout_fd = os.dup(slave_fd)
    stderr_fd = os.dup(slave_fd)
    env = os.environ.copy()
    env["TERM"] = config.term

    try:
        child = subprocess.Popen(
            config.shell_cmd,
            cwd=config.cwd,
            env=env,
            stdin=stdin_fd,
            stdout=stdout_fd,
            stderr=stderr_fd,
            close_fds=True,
            preexec_fn=_spawn_preexec(slave_fd),
        )
    finally:
        for fd in (stdin_fd, stdout_fd, stderr_fd, slave_fd):
            try:
                os.close(fd)
            except OSError:
                pass

    set_fd_nonblocking(master_fd)
    return child, master_fd


def signal_child(child: subprocess.Popen[bytes]) -> None:
    try:
        os.kill(child.pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def drain_commands(
    command_queue: queue.Queue[dict[str, Any]],
    *,
    child: subprocess.Popen[bytes],
    master_fd: int,
) -> tuple[bool, bool]:
    shutting_down = False
    emitted_output = False
    while True:
        try:
            item = command_queue.get_nowait()
        except queue.Empty:
            return shutting_down, emitted_output

        item_type = str(item.get("type") or "")
        if item_type == "stdin_closed":
            shutting_down = True
            signal_child(child)
            continue
        if item_type == "signal":
            shutting_down = True
            signal_child(child)
            continue

        method = str(item.get("method") or "")
        params = item.get("params")
        params_map = params if isinstance(params, dict) else {}

        if method == "terminal.connect":
            cols_value = params_map.get("cols")
            rows_value = params_map.get("rows")
            if cols_value is not None and rows_value is not None:
                try:
                    apply_resize(
                        master_fd,
                        parse_positive_int(cols_value, DEFAULT_COLS),
                        parse_positive_int(rows_value, DEFAULT_ROWS),
                    )
                except Exception as error:
                    log_error("connect resize failed", error)
            continue

        if method == "terminal.input":
            data_b64 = params_map.get("data_b64")
            if not isinstance(data_b64, str) or not data_b64:
                continue
            try:
                payload = base64.b64decode(data_b64)
                if payload:
                    write_all_fd(master_fd, payload)
            except Exception as error:
                log_error("failed to decode input frame", error)
            continue

        if method == "terminal.resize":
            try:
                apply_resize(
                    master_fd,
                    parse_positive_int(params_map.get("cols"), DEFAULT_COLS),
                    parse_positive_int(params_map.get("rows"), DEFAULT_ROWS),
                )
            except Exception as error:
                log_error("resize failed", error)
            continue

        if method == "terminal.destroy":
            shutting_down = True
            signal_child(child)
            continue

        if method == "terminal.ping":
            emit_pong(params_map.get("nonce"))
            emitted_output = True
            continue

        log_error(f"unsupported JSON-RPC method: {method}")


def closed_reason(returncode: int, shutting_down: bool) -> tuple[int | None, str]:
    if returncode < 0:
        return None, f"signal:{-returncode}"
    if shutting_down:
        return returncode, "terminated"
    return returncode, "exited"


def main() -> int:
    config = load_config()
    if not config.shell_cmd:
        log_error("empty shell command")
        return 1

    command_queue: queue.Queue[dict[str, Any]] = queue.Queue()
    spawn_stdin_reader(command_queue)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(
            sig,
            lambda signum, _frame, q=command_queue: q.put(
                {
                    "type": "signal",
                    "signal": signum,
                }
            ),
        )

    try:
        child, master_fd = spawn_pty_child(config)
    except Exception as error:
        log_error("failed to spawn PTY child", error)
        return 1

    emit_ready(child.pid, config.shell_cmd, config.cwd)
    seq = 0
    shutting_down = False
    saw_pty_eof = False
    poller = select.poll()
    poller.register(master_fd, select.POLLIN | select.POLLHUP)

    try:
        while True:
            drained_shutdown, _ = drain_commands(
                command_queue,
                child=child,
                master_fd=master_fd,
            )
            shutting_down = shutting_down or drained_shutdown

            returncode = child.poll()
            if returncode is not None and saw_pty_eof:
                seq += 1
                exit_code, reason = closed_reason(returncode, shutting_down)
                emit_closed(seq, exit_code, reason)
                return returncode if returncode >= 0 else 0

            events = poller.poll(POLL_TIMEOUT_MS)
            if not events:
                continue

            readable = False
            for _fd, event_mask in events:
                if event_mask & select.POLLNVAL:
                    raise OSError(errno.EBADF, "PTY poll returned POLLNVAL")
                if event_mask & (select.POLLIN | select.POLLHUP):
                    readable = True
            if not readable:
                continue

            try:
                data = os.read(master_fd, READ_CHUNK_BYTES)
            except InterruptedError:
                continue
            except BlockingIOError:
                continue
            except OSError as error:
                if error.errno in {errno.EAGAIN, errno.EWOULDBLOCK}:
                    continue
                if error.errno == errno.EIO:
                    saw_pty_eof = True
                    continue
                raise

            if not data:
                saw_pty_eof = True
            else:
                seq += 1
                emit_data(seq, data)

            if saw_pty_eof:
                time.sleep(0.01)
    except Exception as error:
        log_error("broker main loop failed", error)
        signal_child(child)
        return 1
    finally:
        try:
            os.close(master_fd)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
