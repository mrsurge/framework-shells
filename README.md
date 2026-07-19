# framework-shells

`framework-shells` is the Python implementation of FWS, a small process-runtime layer for apps that need to run child processes and keep them observable.

Use it when your application starts long-lived workers or tools and you want one consistent way to:

- launch them
- stop them cleanly
- group them by app/project/purpose
- capture stdout/stderr logs
- write to stdin when the backend supports it
- inspect recent output
- expose a dashboard or API for runtime control
- share runtime state with other FWS-compatible managers

Examples of things FWS can supervise:

- app workers
- language servers
- build workers
- file-system or git helpers
- JSON-RPC stdio services
- terminal brokers
- adapter processes
- any other child process that should not be a hidden `subprocess.Popen(...)`

`framework-shells` is not the application protocol. It does not know what your JSON-RPC methods mean, how your DTOs are shaped, or how your business logic routes requests. It owns process lifecycle and observability. Your application owns protocol semantics.

For Rust-native hosts or paths where Python should not own process I/O, use [`ferrous-framework`](https://github.com/mrsurge/ferrous-framework), the Rust implementation of the same FWS runtime contract.

## Mental Model

An FWS runtime is a process manager plus a small metadata store.

When FWS starts a child process, it creates a shell record. A shell record answers questions like:

- what command was launched?
- what backend owns it?
- what PID is currently running?
- where are stdout/stderr logs?
- what labels and groups does it belong to?
- can this manager write to stdin?
- can this manager stream output live?
- is the process still running or has it exited?

Those records live under a runtime-specific directory. The runtime is keyed by a repo fingerprint and secret, so different projects or manager roots do not accidentally claim each other's processes.

```text
~/.cache/framework_shells/runtimes/<repo_fingerprint>/<runtime_id>/
  meta/<shell_id>/meta.json
  logs/<shell_id>.stdout.log
  logs/<shell_id>.stderr.log
```

## Backends

FWS intentionally keeps the backend model small.

| Backend | Use it for | Shape |
| --- | --- | --- |
| `proc` | app workers, services, build jobs, helpers that do not need stdin | supervised process plus stdout/stderr logs |
| `pipe` | JSON-RPC stdio servers, protocol adapters, language workers, structured backend tools | supervised stdin/stdout/stderr byte streams |
| `pty` | interactive shells, terminal applications, TUI-like processes | supervised terminal byte stream with input and resize |

The `pipe` backend is protocol-neutral. It does not parse JSON-RPC, line protocols, editor control messages, or application DTOs. It owns the child process, stdin writes, stdout/stderr capture, logs, capabilities, and shutdown. The consumer owns framing and semantics.

PTY-backed shells support `pty_mode="raw"` for byte-oriented compatibility and `pty_mode="interactive"` for normal cooked/echoing terminal behavior.

## Shellspecs

A shellspec is a YAML launch file.

It is not a new protocol. It is just a declarative way to say: "these are the runtime processes this app may need, and here is how to launch them."

A shellspec gives FWS a stable process contract:

- shell id inside the spec
- backend (`proc`, `pipe`, or `pty`)
- command and arguments
- working directory
- environment variables
- labels and subgroups
- optional readiness checks
- optional dashboard/UI hints
- optional debug/inspection flags

Minimal example:

```yaml
version: "1"
shells:
  api:
    backend: proc
    cwd: ${ctx:PROJECT_ROOT}
    command: ["python", "-m", "http.server", "${free_port}"]
    env:
      PORT: ${free_port}
      APP_ID: ${ctx:APP_ID}
```

In that example:

- `api` is the shellspec entry id.
- `${ctx:PROJECT_ROOT}` and `${ctx:APP_ID}` come from the render context supplied by the host app.
- `${free_port}` asks FWS to reserve and reuse one free port during rendering.
- FWS launches the rendered command and stores the resulting shell record.

Starting a shellspec entry from Python:

```python
from framework_shells import get_manager
from framework_shells.orchestrator import Orchestrator

mgr = await get_manager()
shell = await Orchestrator(mgr).start_from_ref(
    "shellspec/app_worker.yaml#api",
    base_dir=app_dir,
    ctx={"APP_ID": "demo", "PROJECT_ROOT": str(project_root)},
    label="app-worker:demo",
)
```

A `pipe` shellspec for a stdio JSON-RPC worker:

```yaml
version: "1"
shells:
  rpc_worker:
    backend: pipe
    cwd: ${ctx:PROJECT_ROOT}
    command: ["python", "-m", "my_app.rpc_worker"]
    subgroups: ["demo", "rpc"]
    inspect_hints:
      - json
      - jsonrpc
```

FWS will supervise the process and preserve stdout/stderr observability. Your app still owns the JSON-RPC reader, writer, request routing, and DTO validation.

## Quick Python Usage

```python
from framework_shells import get_manager

mgr = await get_manager()

# A normal supervised process.
proc = await mgr.spawn_shell(
    ["python", "-m", "http.server", "8080"],
    label="demo-server",
)

# A stdio worker with live stdin/stdout while this manager owns it.
pipe = await mgr.spawn_shell_pipe(
    ["python", "-m", "my_app.rpc_worker"],
    label="rpc-worker",
)
await mgr.write_to_shell(pipe.id, '{"jsonrpc":"2.0","id":1,"method":"ping"}', append_newline=True)

# An interactive terminal process.
pty = await mgr.spawn_shell_pty(
    ["bash", "-l", "-i"],
    label="terminal",
    pty_mode="interactive",
)
await mgr.resize_pty(pty.id, cols=120, rows=40)

# Inspect and control.
shells = await mgr.list_shells()
tail = await mgr.get_log_tail(pipe.id, stream="stdout", lines=100)
await mgr.terminate_shell(proc.id, force=False)
```

## CLI

The `fws` CLI uses the same runtime store and secret resolution as the Python manager.

```bash
# List current runtime shells.
fws list
fws list --all

# Start all autostart entries from a shellspec file.
fws up shells.yaml

# Start a one-off process.
fws run --backend proc --label demo -- python -m http.server 8080

# Inspect recent output.
fws inspect <shell_id>
fws inspect <shell_id> --stream stdout --format jsonrpc --json

# Write to live stdin when the owning manager exposes input.
fws write <shell_id> '{"jsonrpc":"2.0","id":"probe","method":"ping","params":{}}' --newline
cat payload.json | fws write <shell_id> - --json --newline

# Stop or remove shells.
fws terminate <shell_id>
fws rm <shell_id>
fws shutdown-group <app_id>
fws down --tree

# Show process trees.
fws tree --depth 4
```

## Dashboard And API

`framework-shells` can be mounted into a FastAPI/ASGI host. The mounted runtime exposes:

- REST endpoints for shell list/detail/control
- live dashboard updates
- live log streaming
- shell stdin injection for capable live shells
- log inspection helpers
- runtime auth derived from `FRAMEWORK_SHELLS_SECRET`

The helper mount is:

```python
from framework_shells.api.socketio_backend import mount_fws_dashboard_runtime

mount_fws_dashboard_runtime(app)
```

Important routes:

```text
GET    /fws/                                      dashboard
GET    /api/framework_shells                     list shells
GET    /api/framework_shells/{id}                shell detail
POST   /api/framework_shells/{id}/terminate      terminate shell
POST   /api/framework_shells/{id}/input          write stdin / send EOF when supported
DELETE /api/framework_shells/{id}                remove metadata and logs
GET    /api/framework_shells/logs/{id}/tail      structured log tail
GET    /api/framework_shells/logs/{id}/inspect   plain/json/jsonrpc inspection
POST   /api/framework_shells/app/{app_id}/shutdown  group shutdown
```

The dashboard is an observability/control surface. It is not required for the supervised process to run.

## Logs And Inspection

Raw stdout and stderr logs stay raw. FWS does not rewrite process output to add timestamps or protocol metadata.

Inspection is a read-side helper over the raw logs. It can classify recent output as:

- `plain`
- `json`
- `jsonrpc`

For deeper debugging, a shell can opt into I/O metadata sidecars:

```yaml
version: "1"
shells:
  rpc_worker:
    backend: pipe
    command: ["python", "-m", "my_app.rpc_worker"]
    debug:
      io_metadata: true
```

When enabled, FWS writes a sibling JSONL sidecar with output chunk metadata and stdin write records. Stdin is never appended to raw stdout/stderr logs.

CLI example:

```bash
fws inspect <shell_id> --io-metadata --stdin --timestamps --json
```

## Runtime Boundaries

FWS owns:

- process launch
- shutdown and group shutdown
- process metadata
- stdout/stderr log paths
- live input/output capability reporting
- dashboard/API/control surfaces
- peer-manager coordination across Python and Ferrous runtimes

Your application owns:

- protocol framing
- request/response correlation
- DTOs
- schema validation
- business logic
- user-facing app behavior

This boundary is the reason `pipe` is a byte stream instead of a JSON-RPC framework. FWS should make protocol workers observable and controllable without becoming part of their protocol.

## Native Packaging

Git source installs default to `auto` native packaging.

If Cargo is available, setup attempts to build and bundle:

- the native terminal broker binary
- the native pipe reader extension

If native build fails in `auto` mode, the install continues with the pure-Python package. To require native artifacts:

```bash
FRAMEWORK_SHELLS_INSTALL_MODE=build \
FRAMEWORK_SHELLS_PIPE_PUMP_MODE=build \
python -m pip install "framework-shells @ git+https://github.com/mrsurge/framework-shells@main"
```

To force Python-only install:

```bash
FRAMEWORK_SHELLS_INSTALL_MODE=python-only \
python -m pip install "framework-shells @ git+https://github.com/mrsurge/framework-shells@main"
```

Free-threaded Termux Python is supported by the source build path. The PyO3 pipe pump is built as a version-specific extension such as:

```text
fws_pipe_pump.cpython-314t-aarch64-linux-android.so
```

## Environment Variables

| Variable | Description |
| --- | --- |
| `FRAMEWORK_SHELLS_SECRET` | Secret for runtime ID derivation and API auth |
| `FRAMEWORK_SHELLS_REPO_FINGERPRINT` | Override auto-computed repo fingerprint |
| `FRAMEWORK_SHELLS_BASE_DIR` | Override storage base dir, default `~/.cache/framework_shells` |
| `FRAMEWORK_SHELLS_PTY_MODE` | Default PTY mode for new PTY shells, `raw` or `interactive` |
| `FRAMEWORK_SHELLS_INSTALL_MODE` | Native packaging mode, `auto`, `build`, or `python-only` |
| `FRAMEWORK_SHELLS_PIPE_PUMP_MODE` | Native pipe extension packaging mode, defaults to install mode |

If `FRAMEWORK_SHELLS_SECRET` is missing, the CLI tries to load or create the runtime secret under the current repo fingerprint. If the secret is set, mutating API endpoints require a derived token via `X-Framework-Key` or `Authorization: Bearer ...`.

## Directory Layout

```text
framework_shells/
  manager.py              core FrameworkShellManager
  record.py               ShellRecord metadata model
  store.py                runtime-scoped storage paths
  shellspec.py            YAML shellspec loader and renderer
  orchestrator.py         shellspec start/apply helpers
  events.py               process-local lifecycle/output event bus
  shutdown.py             shutdown planner/executor helpers
  native_pipe.py          optional native pipe extension loader
  terminal_stream_broker.py  Python terminal-stream fallback broker
  api/                    REST, dashboard, and Socket.IO runtime
  cli/                    fws command
  protocols/              typed JSON-RPC/dashboard/peer contracts
  ui/                     dashboard frontend assets
```

## Screenshots

The dashboard can run as a standalone page or be embedded by a host application.

| Dashboard | Logs |
| --- | --- |
| <img width="200" height="444" alt="dash.png" src="https://raw.githubusercontent.com/mrsurge/framework-shells/refs/heads/main/pngs/dash.png" /> | <img width="200" height="400" alt="logs.png" src="https://raw.githubusercontent.com/mrsurge/framework-shells/refs/heads/main/pngs/logs.png" /> |
