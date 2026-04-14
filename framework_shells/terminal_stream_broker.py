from __future__ import annotations

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
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, TypeGuard, TypedDict, cast

from .protocols.jsonrpc import dump_json_line
from .protocols.terminal_stream import (
    JsonValue,
    TERMINAL_CONNECT_METHOD,
    TERMINAL_DESTROY_METHOD,
    TERMINAL_INPUT_METHOD,
    TERMINAL_PING_METHOD,
    TERMINAL_RESIZE_METHOD,
    TerminalClientNotification,
    TerminalConnectNotification,
    TerminalDestroyNotification,
    TerminalInputNotification,
    TerminalPingNotification,
    TerminalResizeNotification,
    TerminalServerEventFrame,
    build_terminal_closed_event,
    build_terminal_data_event,
    build_terminal_pong_event,
    build_terminal_ready_event,
    decode_terminal_input_bytes,
    parse_terminal_client_notification,
)

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


class StdinClosedCommand(TypedDict):
    type: Literal["stdin_closed"]


class SignalCommand(TypedDict):
    type: Literal["signal"]
    signal: int


BrokerCommand = TerminalClientNotification | StdinClosedCommand | SignalCommand


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
            parsed_obj = cast(object, json.loads(env_json))
            if isinstance(parsed_obj, list):
                parsed = cast(list[object], parsed_obj)
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


def write_json_line(frame: TerminalServerEventFrame) -> None:
    _ = sys.stdout.write(dump_json_line(cast(Mapping[str, object], frame)))
    _ = sys.stdout.flush()


def emit_ready(child_pid: int, shell_cmd: list[str], cwd: str) -> None:
    write_json_line(build_terminal_ready_event(ts=now_ms(), pid=child_pid, shell=shell_cmd, cwd=cwd))


def emit_data(seq: int, payload: bytes) -> None:
    write_json_line(build_terminal_data_event(seq=seq, ts=now_ms(), payload=payload))


def emit_pong(nonce: JsonValue | None) -> None:
    write_json_line(build_terminal_pong_event(nonce))


def emit_closed(seq: int, exit_code: int | None, reason: str) -> None:
    write_json_line(build_terminal_closed_event(seq=seq, ts=now_ms(), exit_code=exit_code, reason=reason))


def parse_notification(line: str) -> TerminalClientNotification | None:
    notification = parse_terminal_client_notification(line)
    if notification is None:
        log_error("bad JSON-RPC terminal notification")
    return notification


def spawn_stdin_reader(command_queue: queue.Queue[BrokerCommand]) -> None:
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


def _spawn_preexec(slave_fd: int) -> Callable[[], None]:
    def _preexec() -> None:
        os.setsid()
        _ = fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)

    return _preexec


def _is_stdin_closed_command(command: BrokerCommand) -> TypeGuard[StdinClosedCommand]:
    return command.get("type") == "stdin_closed"


def _is_signal_command(command: BrokerCommand) -> TypeGuard[SignalCommand]:
    return command.get("type") == "signal"


def _is_terminal_client_notification(command: BrokerCommand) -> TypeGuard[TerminalClientNotification]:
    return "method" in command and "params" in command


def _is_terminal_connect_notification(
    notification: TerminalClientNotification,
) -> TypeGuard[TerminalConnectNotification]:
    return notification["method"] == TERMINAL_CONNECT_METHOD


def _is_terminal_input_notification(
    notification: TerminalClientNotification,
) -> TypeGuard[TerminalInputNotification]:
    return notification["method"] == TERMINAL_INPUT_METHOD


def _is_terminal_resize_notification(
    notification: TerminalClientNotification,
) -> TypeGuard[TerminalResizeNotification]:
    return notification["method"] == TERMINAL_RESIZE_METHOD


def _is_terminal_destroy_notification(
    notification: TerminalClientNotification,
) -> TypeGuard[TerminalDestroyNotification]:
    return notification["method"] == TERMINAL_DESTROY_METHOD


def _is_terminal_ping_notification(
    notification: TerminalClientNotification,
) -> TypeGuard[TerminalPingNotification]:
    return notification["method"] == TERMINAL_PING_METHOD


def _enqueue_signal(command_queue: queue.Queue[BrokerCommand], signum: int) -> None:
    command_queue.put({"type": "signal", "signal": signum})


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
    command_queue: queue.Queue[BrokerCommand],
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

        if _is_stdin_closed_command(item):
            shutting_down = True
            signal_child(child)
            continue
        if _is_signal_command(item):
            shutting_down = True
            signal_child(child)
            continue

        if not _is_terminal_client_notification(item):
            log_error("unexpected broker control command")
            continue

        notification = item

        if _is_terminal_connect_notification(notification):
            cols_value = notification["params"].get("cols")
            rows_value = notification["params"].get("rows")
            if cols_value is not None and rows_value is not None:
                try:
                    apply_resize(master_fd, cols_value, rows_value)
                except Exception as error:
                    log_error("connect resize failed", error)
            continue

        if _is_terminal_input_notification(notification):
            try:
                payload = decode_terminal_input_bytes(notification["params"]["data_b64"])
                if payload:
                    write_all_fd(master_fd, payload)
            except Exception as error:
                log_error("failed to decode input frame", error)
            continue

        if _is_terminal_resize_notification(notification):
            try:
                apply_resize(
                    master_fd,
                    notification["params"]["cols"],
                    notification["params"]["rows"],
                )
            except Exception as error:
                log_error("resize failed", error)
            continue

        if _is_terminal_destroy_notification(notification):
            shutting_down = True
            signal_child(child)
            continue

        if _is_terminal_ping_notification(notification):
            emit_pong(notification["params"].get("nonce"))
            emitted_output = True
            continue

        log_error(f"unsupported JSON-RPC method: {notification['method']}")


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

    command_queue: queue.Queue[BrokerCommand] = queue.Queue()
    spawn_stdin_reader(command_queue)
    for sig in (signal.SIGINT, signal.SIGTERM):
        _ = signal.signal(
            sig,
            lambda signum, _frame, q=command_queue: _enqueue_signal(q, signum),
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
