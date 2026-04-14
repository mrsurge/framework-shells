# FWS Backend And API Contract

## Purpose

This document defines the current `framework-shells` contract for:

1. canonical shell descriptors
2. backend-specific primitives and capabilities
3. current REST endpoint request/response shapes

This is a **current-state contract doc**, not a future-design wishlist.

## Canonical Shell Descriptor

The canonical backend field is now:

```json
{
  "backend": "proc" | "pty" | "pipe" | "dtach"
}
```

`dtach` is now a deprecated compatibility value for new launch requests. New
shellspecs or CLI launches that request `dtach` are routed to `pty`. Legacy
persisted dtach records may still surface as `backend: "dtach"`.

Legacy compatibility fields remain present:

```json
{
  "uses_pty": true | false,
  "uses_pipes": true | false,
  "uses_dtach": true | false
}
```

Those booleans are compatibility descriptors only. `backend` is the source of truth going forward.

## Canonical Shell Payload

Baseline shell payload shape:

```json
{
  "id": "fs_123",
  "spec_id": null,
  "command": ["python", "-u", "-c", "print('ok')"],
  "label": "example",
  "subgroups": [],
  "ui": {},
  "cwd": "/path",
  "pid": 12345,
  "status": "running",
  "created_at": 1770000000.0,
  "updated_at": 1770000001.0,
  "autostart": true,
  "stdout_log": "/.../fs_123.stdout.log",
  "stderr_log": "/.../fs_123.stderr.log",
  "exit_code": null,
  "env_keys": ["PATH"],
  "run_id": "framework-run",
  "launcher_pid": 22222,
  "adopted": false,
  "backend": "pipe",
  "uses_pty": false,
  "uses_pipes": true,
  "uses_dtach": false,
  "pty_mode": "raw",
  "runtime_id": "abcd1234",
  "app_id": null,
  "parent_shell_id": null,
  "is_app_worker": false,
  "capabilities": {
    "backend": "pipe",
    "stdin_write": true,
    "stdin_eof": true,
    "stdout_subscribe": true,
    "stdout_subscribe_bytes": true,
    "stderr_subscribe": false,
    "resize": false,
    "reattach": false
  }
}
```

Notes:

- `pty_mode` is meaningful for PTY-backed terminals; legacy dtach records may still report it
- `capabilities` are live-state-sensitive and may change while the shell is running or after it exits
- In this document, `reattach` means manager/runtime-level resumed communication with an adopted shell session.

## Backend Contracts

### `proc`

Identity:

```json
{
  "backend": "proc",
  "uses_pty": false,
  "uses_pipes": false,
  "uses_dtach": false
}
```

Behavior:

- plain process ownership
- stdout/stderr file logging
- no live stdin transport
- no live output subscription transport
- no resize
- no reattach

Capabilities:

```json
{
  "stdin_write": false,
  "stdin_eof": false,
  "stdout_subscribe": false,
  "stdout_subscribe_bytes": false,
  "stderr_subscribe": false,
  "resize": false,
  "reattach": false
}
```

Relevant manager primitives:

- `spawn_shell(...)`
- `terminate_shell(...)`
- `remove_shell(...)`
- `describe(...)`
- `get_log_tail(...)`
- `search_logs(...)`
- `inspect_logs(...)`

### `pty`

Identity:

```json
{
  "backend": "pty",
  "uses_pty": true,
  "uses_pipes": false,
  "uses_dtach": false
}
```

Behavior:

- live PTY-backed terminal
- stdout/stderr merged into PTY stream and persisted in `stdout_log`
- live input via PTY write
- live output subscription supported
- resize supported
- no reconnect after manager restart

Capabilities:

```json
{
  "stdin_write": true,
  "stdin_eof": false,
  "stdout_subscribe": true,
  "stdout_subscribe_bytes": true,
  "stderr_subscribe": false,
  "resize": true,
  "reattach": false
}
```

Relevant manager primitives:

- `spawn_shell_pty(...)`
- `subscribe_output(...)`
- `subscribe_output_bytes(...)`
- `write_to_pty(...)`
- `write_to_shell(...)`
- `resize_pty(...)`

### `pipe`

Identity:

```json
{
  "backend": "pipe",
  "uses_pty": false,
  "uses_pipes": true,
  "uses_dtach": false
}
```

Behavior:

- raw stdio process
- live stdin through process pipe
- live stdout tee into `stdout_log`
- live stdout subscriptions supported
- stderr remains file-logged
- no resize
- no current manager-adoption path for resuming live raw-pipe I/O after a manager restart
- output subscriptions are raw stream chunks, not line-framed records

Capabilities while live:

```json
{
  "stdin_write": true,
  "stdin_eof": true,
  "stdout_subscribe": true,
  "stdout_subscribe_bytes": true,
  "stderr_subscribe": false,
  "resize": false,
  "reattach": false
}
```

Capabilities after natural exit:

```json
{
  "stdin_write": false,
  "stdin_eof": false,
  "stdout_subscribe": false,
  "stdout_subscribe_bytes": false,
  "stderr_subscribe": false,
  "resize": false,
  "reattach": false
}
```

Relevant manager primitives:

- `spawn_shell_pipe(...)`
- `get_pipe_state(...)`
- `subscribe_output(...)`
- `subscribe_output_bytes(...)`
- `write_to_pipe(...)`
- `send_pipe_eof(...)`
- `write_to_shell(...)`
- `send_shell_eof(...)`

Migration notes:

- Existing `backend: pipe` shellspecs do not require a schema change.
- The compatibility change is mostly behavioral: FWS now owns live stdout observation and stdin/EOF control for pipe shells while the current manager process remains alive.
- If a wrapper previously fanned stdout into stderr only so FWS could observe it, remove that workaround and let protocol/data stdout remain on stdout.
- Review wrappers that use `exec 1>&2`, `2>&1`, or `tee /dev/stderr`; they may now duplicate output or corrupt stdio protocol boundaries.
- For stdio protocol services, keep protocol traffic on stdout and human diagnostics on stderr.
- Pipe subscriptions are stream-chunk oriented, not line-framed; downstream consumers must reassemble their own line or message boundaries when needed.
- A client can reconnect through the live FWS manager and the shell's `shell_id` while that manager still owns the pipe state.
- A new manager process still cannot reconstruct an old raw pipe session after the original owner dies or restarts.
- Experimental `pipe.mode` values now include:
  - `native_pipe_testing` for the raw native stdout pump
  - `native_terminal_pipe_testing` for the PTY-backed native terminal broker over `pipe`
  - `python_terminal_pipe_testing` to force the Python PTY terminal-stream broker
- `pipe.mode: native_terminal_pipe_testing` may be declared without a shellspec `command`.
  - In that broker-resolved shape, the shellspec parser injects an internal placeholder command.
  - The manager replaces it with the native broker binary at launch time when one is available.
  - `pipe.terminal_fallback` controls the non-native path:
    - `python_pty` (default): use `python -m framework_shells.terminal_stream_broker`
    - `command`: use the shellspec `command` as the fallback broker path
    - `error` / `native_only`: fail if the native broker binary is unavailable
  - If `pipe.terminal_fallback: command` is selected, a shellspec `command` is required.
- The native terminal broker preserves the current asymmetric wire contract:
  - stdin uses JSON-RPC notifications
  - stdout uses framed JSONL records
- The Python PTY fallback preserves that same asymmetric wire contract so consumers do not need a separate terminal-stream protocol branch.
- The maintained typed contract helpers for these surfaces now live in:
  - `framework_shells.protocols.fws_ui` for the self-hosted dashboard/log websocket JSON-RPC notifications
  - `framework_shells.protocols.terminal_stream` for the PTY terminal broker JSON-RPC stdin and JSONL stdout frames
- `pipe.mode: python_terminal_pipe_testing` is the explicit escape hatch for always using the Python PTY broker.
  - It may also omit a shellspec `command`; the shellspec parser injects the same internal placeholder command.
  - The manager always replaces that placeholder with `python -m framework_shells.terminal_stream_broker`, regardless of whether a native broker binary is installed.
- Git source installs now attempt to bundle the native terminal broker during wheel build by default.
  - The same source-build path also attempts to bundle the `fws_pipe_pump` extension for the current host Python.
  - `FRAMEWORK_SHELLS_INSTALL_MODE=auto` is the default behavior: try to build the broker, then fall back to a pure-Python wheel if the build is unavailable.
  - `FRAMEWORK_SHELLS_INSTALL_MODE=build` requires that broker build to succeed.
  - `FRAMEWORK_SHELLS_INSTALL_MODE=python-only` disables native broker build and forces a pure-Python wheel.
  - The broker-first release scripts set this mode explicitly so pure-wheel and native-wheel builds stay deterministic.

### `dtach`

This backend is now legacy-only in practice. New launch requests that specify
`dtach` are routed to `pty`, but previously persisted dtach records can still
surface and retain their legacy capability shape.

Identity:

```json
{
  "backend": "dtach",
  "uses_pty": true,
  "uses_pipes": false,
  "uses_dtach": true
}
```

Behavior:

- terminal-like process wrapped by `dtach`
- local attach proxy provides PTY interaction
- `reattach: true` refers to manager/runtime-level resumed communication with an adopted dtach-backed session
- current FWS product/API flow still does not provide a polished terminal-UI rebind workflow around that adoption path
- live output subscriptions are through the local attach proxy state
- resize supported while attached

Capabilities while attached:

```json
{
  "stdin_write": true,
  "stdin_eof": false,
  "stdout_subscribe": true,
  "stdout_subscribe_bytes": true,
  "stderr_subscribe": false,
  "resize": true,
  "reattach": true
}
```

Relevant manager primitives:

- `spawn_shell_dtach(...)`
- `write_to_shell(...)`
- `write_to_pty(...)`
- `resize_pty(...)`
- attach/adoption behavior via manager internals

## Generic Manager Primitives

These primitives are intended to be backend-dispatching:

- `write_to_shell(shell_id, data, append_newline=False)`
- `send_shell_eof(shell_id)`
- `get_shell_capabilities(shell_id_or_record)`

Current dispatch behavior:

- `pty` -> supported
- `pipe` -> supported
- `dtach` -> supported for write, not EOF
- `proc` -> unsupported

## REST API Contract

All REST responses use:

```json
{
  "ok": true,
  "data": ...
}
```

Mutating routes require auth using either:

- `X-Framework-Key: <token>`
- `Authorization: Bearer <token>`

The token is derived from `FRAMEWORK_SHELLS_SECRET`.

### `GET /api/framework_shells`

Query params:

- `include_stats=false|true`

Response:

- `include_stats=false`: list of shell payloads with `capabilities`
- `include_stats=true`: list of `describe(...)` payloads, with best-effort stats

### `GET /api/framework_shells/{shell_id}`

Query params:

- `include_stats=false|true`

Response:

- `include_stats=false`: single shell payload including `env_overrides` and `capabilities`
- `include_stats=true`: `describe(...)` payload with `stats`; falls back to shell payload if describe fails

### `POST /api/framework_shells`

Current request body:

```json
{
  "command": ["bash", "-l", "-i"],
  "cwd": "/path",
  "env": {"KEY": "VALUE"},
  "label": "terminal",
  "subgroups": ["app", "terminal"],
  "ui": {},
  "pty_mode": "interactive",
  "autostart": true
}
```

Important current behavior:

- this endpoint currently creates a **PTY** shell only
- there is no backend selector in this route today

Response:

```json
{
  "ok": true,
  "data": { "...shell payload..." }
}
```

If `label` matches an existing running shell:

```json
{
  "ok": true,
  "data": { "...existing shell payload..." },
  "reused": true
}
```

### `POST /api/framework_shells/{shell_id}/terminate`

Response:

```json
{ "ok": true }
```

### `POST /api/framework_shells/{shell_id}/action`

Current supported action only:

```json
{
  "action": "terminate",
  "force": false
}
```

Response:

```json
{ "ok": true }
```

### `POST /api/framework_shells/{shell_id}/input`

Purpose:

- generic live stdin write / EOF route

Write request:

```json
{
  "data": "status",
  "append_newline": true
}
```

EOF request:

```json
{
  "eof": true
}
```

Write response:

```json
{
  "ok": true,
  "data": {
    "shell_id": "fs_123",
    "backend": "pipe",
    "accepted": true,
    "bytes_written": 7,
    "newline_appended": true,
    "eof_sent": false
  }
}
```

EOF response:

```json
{
  "ok": true,
  "data": {
    "shell_id": "fs_123",
    "backend": "pipe",
    "accepted": true,
    "bytes_written": 0,
    "newline_appended": false,
    "eof_sent": true
  }
}
```

Error cases:

- `404` shell not found
- `409` live input unavailable or unsupported backend
- `400` malformed request body

### `DELETE /api/framework_shells/{shell_id}`

Query params:

- `force=false|true`

Response:

```json
{ "ok": true }
```

### `POST /api/framework_shells/purge_exited`

Response:

```json
{
  "ok": true,
  "data": {
    "purged": 12,
    "errors": []
  }
}
```

### `POST /api/framework_shells/app/{app_id}/shutdown`

Response:

```json
{
  "ok": true,
  "data": {
    "root_pids": [123, 456],
    "stats": { }
  }
}
```

Or, when nothing matches:

```json
{
  "ok": true,
  "data": {
    "root_pids": [],
    "stats": {},
    "note": "no matching running shells"
  }
}
```

### `GET /api/framework_shells/logs/{shell_id}/tail`

Query params:

- `stream=stdout|stderr|both`
- `lines=0..5000`

Response:

- shell metadata
- per-stream file metadata
- `lines`
- boundary metadata such as:
  - `byte_window_start`
  - `byte_window_end`
  - `partial_head`
  - `truncated`
  - `event_count`

### `GET /api/framework_shells/logs/{shell_id}/search`

Query params:

- `query`
- `stream=stdout|stderr|both`
- `limit`
- `regex`
- `ignore_case`

Response:

- shell metadata
- per-stream file metadata
- matching lines with line numbers when available

### `GET /api/framework_shells/logs/{shell_id}/inspect`

Query params:

- `stream=stdout|stderr|both`
- `lines`
- `query`
- `exclude_query`
- `regex`
- `ignore_case`
- `format=plain|json|jsonrpc`
- `signature`
- `exclude_signature`

Response:

- shell metadata
- per-stream file metadata
- event-window metadata
- inspected event records
- summary/counts

Supported parser/classifier scope is intentionally narrow:

- `plain`
- `json`
- `jsonrpc`

## WebSocket Contract

### `WS /ws/events`

Behavior:

- streams shell lifecycle events from the in-process event bus

Payload shape:

```json
{
  "type": "shell.created",
  "shell_id": "fs_123",
  "timestamp": 1770000000.0,
  "data": { "...event payload..." },
  "app_id": null,
  "parent_shell_id": null,
  "is_app_worker": false
}
```

Known event types currently include:

- `shell.created`
- `shell.spawned`
- `shell.ready`
- `shell.updated`
- `shell.exited`
- `shell.removed`
- `shell.pty_chunk`
- `shell.log_chunk`

## Current Contract Gaps

These are real current gaps, not hidden assumptions:

1. `POST /api/framework_shells` does not yet expose backend selection
- it currently creates PTY shells only

2. live stderr subscription is not symmetrical across backends
- stderr remains file-oriented in the current design

3. `pipe` output subscriptions are stream-chunk oriented
- there is no line-framing guarantee

4. `dtach` capability truth depends on a live local attach proxy state
- the backend itself is reconnectable
- the live subscription/write surface still depends on the current manager attaching locally

## Reserved Future Backend

`uds_pipe` is intentionally **not** part of the current live contract.

It is a future backend direction, not a current backend.

When added, it should fit this same model:

- canonical `backend`
- explicit capabilities
- generic input/output primitives
- normal FWS logging surfaces

## Bottom Line

The current repo contract is now organized around:

1. canonical `backend`
2. compatibility booleans for older consumers
3. live `capabilities` for current-process truth
4. generic shell input primitives where supported
5. shared logging/inspection surfaces across backends

That is the foundation the next backend (`uds_pipe`) should build on, not bypass.
