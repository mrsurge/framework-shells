# Framework Shells Module

A standalone Python package for process orchestration with PTY and pipe backends, plus legacy dtach compatibility.
PTY-backed shells support `pty_mode="raw"` (legacy default) and `pty_mode="interactive"` (normal cooked/echoing terminal behavior).

## Install

```bash
pip install "framework-shells @ git+https://github.com/mrsurge/framework-shells@main"
```

## Dependencies

- Python 3.9+
- `fastapi`, `uvicorn` (for API)
- `pyyaml` (for spec files)
- `dtach` (optional, legacy-only for old dtach sessions)

## Overview

`framework_shells/` is a self-contained module that manages long-running background processes ("shells") with:

- **Multiple backends**: PTY (interactive terminals), pipes (stdin/stdout), legacy dtach compatibility
- **Configurable PTY discipline**: `raw` for legacy byte-oriented behavior, `interactive` for normal terminal echo/canonical input
- **Runtime isolation**: Shells are namespaced by repo fingerprint + secret-derived runtime ID
- **Event bus**: Real-time notifications for shell lifecycle events
- **Singleton manager**: One manager instance per process, thread-safe
- **Integration hooks (optional)**: Host apps can observe shell lifecycle events (e.g., for external process registries)

Texbook use case:

[https://github.com/mrsurge/termux-extensions-2](https://github.com/mrsurge/termux-extensions-2)

(Termux-Extensions-2 is a "Mobile IDE" environment... LSPs, MCP servers, and Agents... on Linux/Termux/macOS ... no user permission orchestration required. All powered by this module)

## Directory Structure

```
framework_shells/
├── __init__.py          # Package exports and get_manager() singleton
├── manager.py           # FrameworkShellManager - core orchestration
├── record.py            # ShellRecord dataclass
├── store.py             # RuntimeStore - namespaced storage paths
├── auth.py              # Secret handling and token derivation
├── events.py            # EventBus for shell lifecycle events
├── hooks.py             # Optional lifecycle hook dataclasses (host integration)
├── pty.py               # PTYState and PipeState dataclasses
├── process_snapshot.py  # Host-agnostic process snapshot types
├── shutdown.py          # Shutdown planner/executor helpers
├── shellspec.py         # YAML shellspec loader + template renderer
├── orchestrator.py      # Shellspec-based orchestration
├── ui/
│   ├── index.html          # Dashboard page
│   ├── fws.css             # Dashboard styles
│   ├── fws.js              # Minimal dashboard websocket client
│   └── logs.html           # Legacy standalone log template (dashboard now uses a log drawer)
├── cli/
│   └── main.py          # CLI tool (fws list/up/down/tree/shutdown-group/inspect/attach)
└── api/
    ├── fastapi_router.py   # REST API endpoints
    ├── fws_ui.py           # Self-hosted dashboard + logs (/fws, /ws/fws)
    └── websocket.py        # WebSocket endpoints for shell events
```

## Core Concepts

### ShellRecord

Metadata for a managed process:

```python
@dataclass
class ShellRecord:
    id: str                    # Unique ID (fs_<timestamp>_<random>)
    command: List[str]         # Command and arguments
    label: Optional[str]       # Human-readable label
    subgroups: List[str]       # Grouping hierarchy (e.g., ["app", "terminal"])
    cwd: str                   # Working directory
    pid: Optional[int]         # Process ID (None if not started)
    status: str                # "pending", "running", "exited"
    created_at: float          # Unix timestamp
    backend: str               # Canonical backend: "proc" | "pty" | "pipe" | legacy "dtach"
    uses_pty: bool             # PTY backend
    uses_pipes: bool           # Pipe backend
    uses_dtach: bool           # Legacy dtach record flag
    pty_mode: str              # "raw" or "interactive" for pty-backed terminals
    stdout_log: str            # Path to stdout log
    stderr_log: str            # Path to stderr log
    exit_code: Optional[int]   # Exit code (if exited)
    runtime_id: str            # Namespace for this runtime
```

### Backends

**PTY** (`spawn_shell_pty`):
- Full terminal emulation
- Supports resize, input/output streaming
- Good for interactive shells
- `pty_mode="raw"` preserves the legacy raw-ish termios behavior
- `pty_mode="interactive"` keeps a normal cooked tty (echo/canonical input/signals)
- No current manager-adoption path for resuming live PTY I/O after a manager restart

The compatibility booleans remain in payloads, but `backend` is the canonical backend descriptor going forward.

**Pipes** (`spawn_shell_pipe`):
- Stdin/stdout/stderr as separate streams
- Good for LSP servers, daemons
- FWS now tees pipe `stdout` into the shell's `stdout_log`
- Supports live stdin write / EOF while the current manager process owns the live pipe state
- Supports experimental native modes under `pipe.mode`, including:
  - `native_pipe_testing` for the raw high-traffic pipe pump
  - `native_terminal_pipe_testing` for the PTY-backed terminal stream broker
  - `python_terminal_pipe_testing` to force the Python PTY terminal-stream broker
- No current manager-adoption path for resuming live raw-pipe I/O after a manager restart

### Pipe Migration Notes

- Existing `backend: pipe` shellspecs do not require a schema change.
- The main change is behavioral: FWS now owns live pipe stdout observability and stdin write/EOF while the current manager process is alive.
- If an old wrapper mirrored stdout to stderr only so FWS could see it, remove that workaround and let stdout stay on stdout.
- Review wrappers that use `exec 1>&2`, `2>&1`, or `tee /dev/stderr`; they may now duplicate output or pollute protocol stdout.
- For stdio protocol servers, keep protocol/data traffic on stdout and human diagnostics on stderr.
- Pipe output subscriptions are raw stream chunks, not line-framed records. Downstream consumers that assume one callback per line need to reassemble lines or messages themselves.
- In this repo, `reattach` means manager/runtime-level resumed communication with an adopted shell session.
- Raw `pipe` currently has no supported adoption path for resuming live I/O in a successor manager process.
- `pipe.mode: native_terminal_pipe_testing` is now native-first with a built-in Python PTY fallback.
- `pipe.mode: python_terminal_pipe_testing` explicitly forces the Python PTY broker even if the native broker binary is available.
- `pipe.terminal_fallback` controls what happens if the native broker binary is unavailable:
  - `python_pty` (default): launch `python -m framework_shells.terminal_stream_broker`
  - `command`: use the shellspec `command` as the fallback broker path
  - `error` / `native_only`: fail instead of falling back

**Dtach** (`spawn_shell_dtach`):
- Deprecated compatibility alias for `pty` on new launches
- Legacy dtach-backed records may still exist and can still be recognized
- CLI attach remains legacy-only for those existing dtach sessions

### Runtime Isolation

Shells are stored under:
```
~/.cache/framework_shells/runtimes/<repo_fingerprint>/<runtime_id>/
├── meta/<shell_id>/meta.json
├── logs/<shell_id>.stdout.log
├── logs/<shell_id>.stderr.log
└── sockets/<shell_id>.sock  (dtach only)
```

- `repo_fingerprint`: SHA256 of repo root path (first 16 chars)
- `runtime_id`: Derived from `FRAMEWORK_SHELLS_SECRET`

This ensures different repos and different secrets don't see each other's shells.
Two instances with different secrets won't see each other's shells, even if running from the same repo. This enables running multiple clones on different ports without interference.

## API

### Manager Methods

```python
from framework_shells import get_manager

mgr = await get_manager()

# Advanced: configure the singleton once (must be consistent per-process)
# mgr = await get_manager(process_hooks=..., enable_dtach_proxy=False, default_pty_mode="interactive")

# Spawn shells
record = await mgr.spawn_shell_pty(["bash", "-l", "-i"], label="terminal", cwd="/home/user", pty_mode="interactive")
record = await mgr.spawn_shell_pipe(["pyright-langserver", "--stdio"], label="lsp:python")
record = await mgr.spawn_shell_dtach(["bash", "-l", "-i"], label="legacy-alias", pty_mode="interactive")  # launches as pty

# List and find
shells = await mgr.list_shells()
shell = await mgr.get_shell(shell_id)
shell = await mgr.find_shell_by_label("terminal", status="running")

# Describe (with stats + capabilities)
info = await mgr.describe(record, include_logs=True, tail_lines=100)

# Live shell I/O
queue = await mgr.subscribe_output(shell_id)
bytes_queue = await mgr.subscribe_output_bytes(shell_id)  # Lossless raw bytes
result = await mgr.write_to_shell(shell_id, "status", append_newline=True)
eof = await mgr.send_shell_eof(shell_id)  # Supported for live pipe shells

# PTY-only terminal behavior
await mgr.write_to_pty(shell_id, "ls -la\n")
await mgr.resize_pty(shell_id, cols=120, rows=40)
await mgr.unsubscribe_output(shell_id, queue)
await mgr.unsubscribe_output_bytes(shell_id, bytes_queue)

# Pipe I/O (in-memory only)
pipe_state = mgr.get_pipe_state(shell_id)
caps = await mgr.get_shell_capabilities(shell_id)

# Lifecycle
await mgr.terminate_shell(shell_id, force=True)
await mgr.remove_shell(shell_id, force=True)  # Also removes logs/metadata
result = await mgr.shutdown_app_group("demo-app")  # UI-equivalent group shutdown

# Log helpers / retention
tail = await mgr.get_log_tail(shell_id, stream="both", lines=50)
matches = await mgr.search_logs(shell_id, stream="stdout", query="ready", limit=20)
inspection = await mgr.inspect_logs(shell_id, stream="stdout", lines=100, format="jsonrpc")
inspection = await mgr.inspect_logs(shell_id, stream="stderr", exclude_signature="plain:ipc_chunk")
purged = await mgr.prune_exited_shells(max_count=50)

# Optional: enumerate running PIDs for external monitoring
pids = await mgr.list_active_pids()

# Optional: provide lightweight aggregated stats (requires psutil for per-process CPU/RSS)
stats = await mgr.aggregate_resource_stats()
```

### SIGWINCH on resize (optional)

Some interactive programs (readline, shells, TUIs) cache terminal width and rely
on `SIGWINCH` to refresh after a PTY resize. Legacy dtach sessions may still
need the attach proxy to receive the signal.

You can enable best-effort `SIGWINCH` delivery after `resize_pty()` by either:

- Passing `signal_winch_on_resize=True` when creating the singleton manager (must be consistent per-process), or
- Setting `FRAMEWORK_SHELLS_SIGWINCH_ON_RESIZE=1` in the environment.

### PTY mode

PTY-backed shells support two terminal modes:

- `raw`: legacy default; disables canonical input, echo, and signal-generating terminal keys
- `interactive`: leaves the PTY in a normal cooked terminal mode

You can select the default mode for new PTY shells by either:

- Passing `default_pty_mode="interactive"` when creating the singleton manager, or
- Setting `FRAMEWORK_SHELLS_PTY_MODE=interactive` in the environment.

### REST API

```
GET    /api/framework_shells                 # List all shells
POST   /api/framework_shells                 # Create shell
GET    /api/framework_shells/{id}            # Get shell details
POST   /api/framework_shells/{id}/terminate  # Terminate shell
POST   /api/framework_shells/{id}/action     # Terminate, etc.
POST   /api/framework_shells/{id}/input      # Generic live stdin write / EOF
DELETE /api/framework_shells/{id}            # Purge metadata/logs (Exited-shell cleanup)
POST   /api/framework_shells/purge_exited    # Purge metadata/logs for all exited shells
POST   /api/framework_shells/app/{app_id}/shutdown      # UI-equivalent group shutdown
GET    /api/framework_shells/logs/{id}/tail             # Structured stdout/stderr tail + boundary metadata
GET    /api/framework_shells/logs/{id}/search           # Structured log search + metadata
GET    /api/framework_shells/logs/{id}/inspect          # Event-first log inspection (`plain`, `json`, `jsonrpc`)
GET    /api/framework_shells/{id}/replay     # Get stdout log
```

Shell payloads returned by the REST API include a canonical `backend` field plus compatibility booleans (`uses_pty`, `uses_pipes`, `uses_dtach`). Payloads also include a `capabilities` block describing live input/output support for that shell in the current manager process. `pty_mode` (`raw` or `interactive`) remains relevant for PTY-backed terminals; legacy dtach records may still report it as well.

The inspection surface is intentionally narrow in v1:

- parser/classifier support is limited to `plain`, `json`, and `jsonrpc`
- raw event text remains primary and parsed fragments are annotations
- `tail` now includes boundary metadata such as `partial_head`, byte-window offsets, and event count
- stable bracketed plain-text prefixes are promoted into signatures such as `plain:ipc_chunk`
- the only negative filters are `exclude_query` and `exclude_signature`

Example input body for the new live input route:

```json
{
  "data": "status",
  "append_newline": true
}
```

Or, for live pipe shells, send EOF:

```json
{
  "eof": true
}
```

## Self-hosted UI (FWS)

When mounted in a FastAPI app, `framework_shells` can self-host a simple dashboard:

- `GET /fws/` dashboard (live-updating via `WS /ws/fws`)
- shell logs open in a full-page in-dashboard drawer backed by `WS /ws/fws/logs/{shell_id}`
- `GET /fws/logs/{shell_id}` redirects into the dashboard drawer for compatibility

The dashboard toolbar includes **Truncate Logs**, which truncates all `.stdout.log`/`.stderr.log` files in the current runtime (it does not delete shell records). Exited shells can be fully removed via **Purge Exited** in the Exited section (deletes metadata + logs), and automatic exited-shell retention keeps only the newest 50 exited shell records.

UI styling and grouping metadata is carried on each shell record via `ShellSpec.ui` / `ShellRecord.ui` (see Shellspec below).

## Shellspec Convention (Recommended)

`framework_shells` is framework-agnostic, but the intended integration pattern is:

- Describe host-run processes as `ShellSpec` (YAML).
- Start shells via `Orchestrator` (from a spec or spec ref).
- Keep optional UI hints in the shellspec under `ui` (not host-specific code).

### Shellspec Format

A shellspec YAML file is a mapping of **shell type id → shell definition**:

```yaml
version: "1"
shells:
  <shell_type_id>:
    command: ["bash", "-lc", "echo hello"]
```

### Shellspec Examples

Minimal “proc” service:

```yaml
version: "1"
shells:
  api:
    backend: proc
    command: ["python", "-m", "http.server", "${free_port}"]
    env:
      PORT: ${free_port}
      LOG_LEVEL: info
```

Deprecated dtach alias (new launches route to `pty`):

```yaml
version: "1"
shells:
  terminal:
    backend: dtach
    pty_mode: interactive
    cwd: ${ctx:PROJECT_ROOT}
    subgroups: ["terminal", "project:${ctx:APP_ID}"]
    command: ["bash", "-l", "-i"]
```

Experimental native terminal stream over `pipe`:

```yaml
version: "1"
shells:
  terminal-stream:
    backend: pipe
    pipe:
      mode: native_terminal_pipe_testing
      terminal_fallback: python_pty
    cwd: ${ctx:PROJECT_ROOT}
    env:
      TERMINAL_STREAM_CWD: ${ctx:PROJECT_ROOT}
      TERMINAL_STREAM_COLS: ${ctx:COLS}
      TERMINAL_STREAM_ROWS: ${ctx:ROWS}
      TERMINAL_STREAM_SHELL_CMD_JSON: ${ctx:SHELL_CMD_JSON}
```

Notes:

- This mode runs a native PTY broker under an outer `pipe` shell.
- If the native broker binary is unavailable, FWS falls back to the Python PTY broker by default.
- The terminal stream contract stays asymmetric:
  - stdin uses JSON-RPC notifications
  - stdout uses framed JSONL records
- If no `command` is provided, FWS injects an internal placeholder and resolves either the native broker or the configured fallback automatically.
- Set `pipe.terminal_fallback: command` if you want the shellspec `command` to be the fallback broker path.
- Set `pipe.terminal_fallback: error` (or `native_only`) if you want launch to fail when the native broker is unavailable.

Explicit Python PTY terminal stream over `pipe`:

```yaml
version: "1"
shells:
  terminal-stream:
    backend: pipe
    pipe:
      mode: python_terminal_pipe_testing
    cwd: ${ctx:PROJECT_ROOT}
    env:
      TERMINAL_STREAM_CWD: ${ctx:PROJECT_ROOT}
      TERMINAL_STREAM_COLS: ${ctx:COLS}
      TERMINAL_STREAM_ROWS: ${ctx:ROWS}
      TERMINAL_STREAM_SHELL_CMD_JSON: ${ctx:SHELL_CMD_JSON}
```

Notes:

- This mode always launches `python -m framework_shells.terminal_stream_broker`.
- It uses the same stdin/stdout terminal-stream contract as the native broker.

### UI Hints (`shellspec.ui`)

Shells can carry optional UI metadata via `ShellSpec.ui` / `ShellRecord.ui`.

The dashboard currently supports `ui.subgroup_styles`: a mapping from subgroup name (or a glob pattern like `project:*` / `lsp:*`) to simple style properties for the subgroup “card”.

Notes:
- Patterns use `fnmatch` wildcards (`*`, `?`, `[]`).
- If multiple patterns match a subgroup, the most-specific (longest) pattern wins.

### Per-app Shellspec Layout

A common layout is to keep shellspecs next to an app/module:

```
app/apps/<app_id>/
└── shellspec/
    └── app_worker.yaml
```

Example shellspec with env, subgroups, and UI styling:

```yaml
version: "1"
shells:
  worker:
    backend: proc
    cwd: ${ctx:PROJECT_ROOT}
    subgroups: ["worker", "project:${ctx:APP_ID}"]
    ui:
      subgroup_styles:
        worker:
          bg: rgba(68, 45, 47, 0.80)
          border: rgba(168, 85, 247, 0.60)
        project:*:
          bg: rgba(0, 0, 0, 0.88)
          border: rgba(29, 70, 126, 0.88)
    command:
      - python
      - -m
      - your_module.worker
      - --project
      - ${ctx:PROJECT_ROOT}
      - --port
      - ${free_port}
    env:
      APP_ID: ${ctx:APP_ID}
      PORT: ${free_port}
      LOG_LEVEL: info
      FEATURE_FLAG_X: "1"
      DATABASE_URL: ${env:DATABASE_URL}
```

Then start it from a shellspec ref (`<path>#<id>`) with a render context:

```python
shell = await Orchestrator(mgr).start_from_ref(
    "shellspec/app_worker.yaml#worker",
    base_dir=app_dir,
    ctx={"APP_ID": app_id, "PROJECT_ROOT": project_root},
    label=f"worker:{app_id}",
)
```

### Events

```python
from framework_shells.events import get_event_bus, EventType

bus = get_event_bus()
queue = bus.subscribe()

while True:
    event = await queue.get()
    # event.type: shell.created, shell.spawned, shell.pty_chunk, shell.exited, ...
    # event.shell_id, event.data, event.timestamp
```

## CLI

```bash
# List shells
fws list

# Include exited shells in the list
fws list --all

# Apply spec file
fws up shells.yaml

# Terminate one shell (ID, label, or unique ID prefix)
fws terminate <shell_id>

# Remove one shell's metadata/logs (terminates if still running)
fws rm <shell_id>

# Shutdown all running shells in an app/group (same semantics as UI "Shutdown Group")
fws shutdown-group <app_id>
fws shutdown-group <app_id> --json

# Inspect recent log events with optional raw/structured filters
fws inspect <shell_id>
fws inspect <shell_id> --stream stdout --format jsonrpc --json
fws inspect <shell_id> --stream stderr --exclude-signature plain:ipc_chunk

# Spawn a one-off shell without a spec
fws run --backend pty --pty-mode interactive --label demo --env FOO=bar --env PORT=1234 -- bash -l -i

# Terminate all shells
fws down
fws down --tree

# Attach to legacy dtach shell
fws attach <shell_id>

# Show process trees (managed shells + procfs descendants)
fws tree --depth 4

# Include exited shells in the tree if they still have a PID recorded
fws tree --all
```

The CLI auto-detects the repo fingerprint from cwd and loads the stored secret.

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FRAMEWORK_SHELLS_SECRET` | Secret for runtime ID derivation and API auth |
| `FRAMEWORK_SHELLS_REPO_FINGERPRINT` | Override auto-computed repo fingerprint |
| `FRAMEWORK_SHELLS_BASE_DIR` | Override storage base dir (default `~/.cache/framework_shells`) |
| `FRAMEWORK_SHELLS_PTY_MODE` | Default PTY discipline for new PTY shells (`raw` or `interactive`) |

## Secret & Fingerprint Surface

`framework_shells` has two key inputs that define where it stores metadata/logs and which shells belong to the current runtime:

- `FRAMEWORK_SHELLS_REPO_FINGERPRINT`: repo-scoped namespace (defaults to a SHA256 of `cwd` if unset)
- `FRAMEWORK_SHELLS_SECRET`: secret used to derive the `runtime_id` (and API tokens when auth is enabled)
- `FRAMEWORK_SHELLS_BASE_DIR`: optional override for the on-disk storage root (defaults to `~/.cache/framework_shells`)
- `FRAMEWORK_SHELLS_PTY_MODE`: optional default PTY mode for newly created PTY shells

### Standalone / CLI

The CLI tries to be usable standalone:

- If `FRAMEWORK_SHELLS_REPO_FINGERPRINT` is missing, it computes one from `cwd` (and sets the env var).
- If `FRAMEWORK_SHELLS_SECRET` is missing, it tries to load the stored secret file under the computed fingerprint.
- If no stored secret exists, it creates and stores a new secret for that fingerprint (so subsequent `fws` invocations share the same runtime).


## Integration Hooks (Optional)

`FrameworkShellManager` supports optional host-provided lifecycle hooks via `ShellLifecycleHooks`.

This stays intentionally framework-agnostic: the library does not know about IPC, FastAPI, systemd, etc.
Hooks are best-effort (errors are swallowed) and may be sync or async.

Common uses:
- Register/unregister shell PIDs in an external process registry
- Emit metrics/telemetry for shell start/adopt/exit events
- Maintain parent/child graphs outside of `framework_shells`

Exposed hook points:
- `on_shell_running(record)`
- `on_shell_adopted(record)`
- `on_shell_exited(record, last_pid)`

## Notes on Detach / Process Groups

Shell processes are launched with `start_new_session=True` for isolation. This means:
- Killing the host process does not necessarily kill the shells it spawned.
- Host frameworks should call `terminate_shell()` on shutdown.
- If a host framework uses an external “last resort” killer, it should either:
    - scan `framework_shells` runtime metadata and terminate shells, or
    - ensure shell PIDs are registered with that external supervisor.

## Auth

Mutating API endpoints can require authentication via:
- `X-Framework-Key` header (frontend uses this)
- `Authorization: Bearer <token>` header

Token is derived from `FRAMEWORK_SHELLS_SECRET`.

- If `FRAMEWORK_SHELLS_SECRET` is unset/empty, auth is disabled (dev mode).
- If `FRAMEWORK_SHELLS_SECRET` is set, mutating endpoints require a valid token.

## Screenshots
Although the dashboard can be rendered as a standalone page and has a corresponding url, it can also be embedded within your app, like in these examples via iframe, fastHTML/HTMX or however your platform does it natively:
| Dash screen               | Logs screen               |
|----------------------------|----------------------------|
| <img width="200" height="444" alt="dash.png" src="https://raw.githubusercontent.com/mrsurge/framework-shells/refs/heads/main/pngs/dash.png" /> | <img width="200" height="400" alt="logs.png" src="https://raw.githubusercontent.com/mrsurge/framework-shells/refs/heads/main/pngs/logs.png" /> |
