# FWS Runtime Spec

FWS is a portable process-runtime contract. `framework-shells` is the Python implementation; `ferrous-framework` is the Rust implementation path.

The spec goal is not Python compatibility. The goal is that independently written implementations can launch, observe, inspect, and control the same kinds of long-lived shells using the same runtime concepts.

## Core Model

```text
runtime manager
  -> shellspec render
  -> shell launch
  -> record persistence
  -> log/metadata persistence
  -> capability reporting
  -> lifecycle/shutdown control
  -> dashboard/API/tool consumers
```

An implementation may be written in any language. It must preserve the externally visible contract for shellspec rendering, records, logs, capabilities, environment inheritance, and lifecycle behavior.

## Shell Backends

The portable backend set is:

- `proc`: process lifecycle plus stdout/stderr log capture; no stdin control by default.
- `pipe`: process lifecycle plus stdin writes, stdout reads/subscriptions, stdout/stderr logs.
- `pty`: process lifecycle plus PTY input/output, terminal-oriented output log, resize support when implemented.

Deprecated backend aliases are implementation-specific migration concerns. New portable runtime work should target `proc`, `pipe`, and `pty`.

## Shellspec

Shellspec is the portable runtime configuration format.

Required shell identity:

- shell id/spec id
- command

Common optional fields:

- `cwd`
- `env`
- `subgroups`
- `labels`
- `ui`
- `debug`
- `backend`
- `pipe`
- `pty_mode`
- `readiness`
- `restart`
- `autostart`

Template rendering must support:

- `${free_port}`
- `${env:NAME}`
- `${ctx:NAME}`
- `${NAME}` resolving from ctx first, then env

`${free_port}` is stable within one rendered shellspec and independent across separately rendered shellspecs.

## Environment Contract

Managers that launch child workers must preserve the FWS environment contract:

- `FRAMEWORK_SHELLS_SECRET`
- `FRAMEWORK_SHELLS_RUN_ID`
- `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`
- `TE_FRAMEWORK_URL`

If no secret or run id exists, the manager may generate implementation-native defaults. URL values are optional unless the manager hosts or peers with a dashboard/control plane.

The manager environment is the base overlay. Shell-specific env from shellspec/caller input is applied after the manager overlay so deliberate shell overrides win.

## Store Layout

Implementations should use the canonical FWS store layout unless explicitly configured otherwise:

```text
base_dir = FRAMEWORK_SHELLS_BASE_DIR or ~/.cache/framework_shells
repo_fingerprint = FRAMEWORK_SHELLS_REPO_FINGERPRINT
  or standalone_debug when FRAMEWORK_SHELLS_ALLOW_NO_FINGERPRINT is truthy
  or sha256(resolve(cwd))[:16]
runtime_id = sha256(FRAMEWORK_SHELLS_SECRET)[:16]
root = <base_dir>/runtimes/<repo_fingerprint>/<runtime_id>
metadata_dir = <root>/meta
logs_dir = <root>/logs
sockets_dir = <root>/sockets
secret_file = <base_dir>/runtimes/<repo_fingerprint>/secret
```

If a process has no `FRAMEWORK_SHELLS_SECRET`, CLI-style implementations may load `secret_file`, otherwise generate and persist a new temporary secret. Implementations must avoid persisting full environment values in shell records by default.

## Records

Each launched shell has a persisted record. The record is the durable runtime metadata that tool, dashboard, and cross-manager consumers can inspect without owning the live process handle.

Portable record fields:

- `id`
- `spec_id`
- `backend`
- `command`
- `cwd`
- `pid`
- `status`
- `exit_code`
- `label`
- `subgroups`
- `record_path`
- `stdout_log`
- `stderr_log`
- `io_metadata_log`
- `created_at`
- `updated_at`
- `run_id`
- `launcher_pid`
- `capabilities`

Records must not require raw stdout/stderr parsing to understand shell identity, status, backend, log paths, or capabilities.

Environment values should not be persisted by default. Persisted records may expose env keys for observability, but secrets and full env values belong in live runtime memory only unless an implementation explicitly opts into unsafe debug output.

Persisted records loaded by a manager that does not own the live process handle must be explicit about that stale/adopted state. Such records may remain inspectable, but live-only capabilities such as stdin writes, raw output reads, resize, and terminate must not be reported as available unless a live owner/peer path exists.

## Logs

Raw stdout/stderr logs remain raw process output.

Per-event metadata is sidecar data, not a mutation of raw logs. Implementations may provide sidecars for:

- timestamps
- stream side
- stdin writes
- stdin EOF
- chunk sizes
- runtime/debug metadata

Stdin data must not be written into raw stdout/stderr logs.

## Capabilities

Capabilities are explicit runtime facts, not assumptions from backend name alone.

Common capabilities:

- `stdin_write`
- `stdin_eof`
- `stdout_log`
- `stderr_log`
- `stdout_subscribe`
- `stderr_subscribe`
- `output_read`
- `terminate`
- `resize`
- `reattach`

Consumers should check capabilities before presenting controls, but write/control APIs should still return explicit runtime errors when a capability is unavailable.

## Lifecycle

Every implementation must expose:

- list shells
- get shell
- launch shell
- terminate shell
- wait for shell exit
- shutdown tree
- shutdown group

Shutdown behavior should be observable through structured results:

- success/failure
- target kind
- target id
- timing
- root pids
- stats
- event log

## Control Plane

The control plane is implementation-defined, but the semantics should be portable:

- request/response operations for user-initiated commands
- notifications for lifecycle/log facts
- authenticated access through the shared FWS secret when crossing process boundaries

Current Python FWS uses HTTP/WebSocket/Socket.IO surfaces. Future Rust-native implementations may use HTTP, Socket.IO, UDS, or another local transport as long as the same runtime semantics remain available.

## Implementation Status

Python `framework-shells` is the reference implementation for the current dashboard/API/tooling surface.

Rust `ferrous-framework` currently targets:

- native `proc`
- native direct-fd `pipe`
- native direct-fd `pty`
- shellspec rendering parity fixtures
- native environment inheritance
- native record/log persistence in progress
