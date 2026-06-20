# Ferrous Framework Adoption

`ferrous-framework` is the Rust implementation path for framework-shells-style process management. The target is a Rust-compiled FWS-compatible manager/runtime suite with Rust-owned `proc`, `pipe`, and `pty` support.

The crate is developed from an ignored local checkout in this repo:

```text
.external/ferrous-framework
```

That checkout is not a submodule and is not a framework-shells package dependency. It is a local development/reference checkout for pushing changes to `mrsurge/ferrous-framework`.

## Target Shape

```text
Rust application
  -> ferrous-framework crate
  -> Rust-owned proc/pipe/pty runtime
  -> FWS-compatible records, logs, capabilities, metadata, and lifecycle semantics
```

Python is not part of the Ferrous crate runtime path. Downstream consumers that still need the old PyO3 bridge are expected to stay pinned to compatible historical versions until they move to the native API.

## Native Runtime Contract

Current native API:

- `FerrousNativeManager`
- `FerrousNativeHost`
- `FerrousNativeEnv`
- `FerrousNativeProcConfig`
- `FerrousNativePipeConfig`
- `FerrousNativePtyConfig`
- `FerrousFrameworkPipe`
- `FerrousPipeConfig`
- `FerrousNativePipeState`
- `FerrousShellInputResult`
- `FerrousNativeShellRecord`
- `FerrousNativeShellStatus`
- `FerrousNativeShellCapabilities`

Backend coverage:

```text
proc: launch, stdout/stderr logs, list/get, terminate, wait
pipe: launch, direct stdin writes, direct stdout reads, stdout/stderr logs, list/get, terminate, wait
pty: launch, direct PTY writes, direct PTY reads, PTY output log, list/get, terminate, wait
```

The `pipe` and `pty` hot paths are direct fd paths. They do not use a Python bridge, stdout pump queue, or drain worker. Reads are caller-driven and tee output to logs as bytes are read.

PTY launch now supports explicit native terminal modes. `FerrousNativePtyMode::Raw` applies raw termios to the PTY slave before child spawn, `Interactive` preserves the cooked/default behavior, and native shellspec launch honors `pty_mode`.

Passive log capture and exit status persistence now run through a single manager-owned native reactor thread instead of one helper thread per stream or shell. Pipe and PTY stdout remain direct caller-driven reads; the reactor handles proc stdout/stderr, pipe stderr, and child exit status.

Native output subscriptions now exist through a bounded `subscribe_output(shell_id, stream, capacity)` API. Reactor-owned streams publish chunks as they are logged, while pipe/PTY stdout publish when the direct read path drains bytes. Slow subscribers are disconnected on full queues instead of creating unbounded buffering.

Ferrous now also exposes a Python-FWS-shaped compatibility surface over the native runtime. `FerrousNativeManager` has async manager names for `spawn_shell`, `spawn_shell_pipe`, `spawn_shell_pty`, `write_to_pipe`, `write_to_shell`, `send_shell_eof`, `terminate_shell`, and `subscribe_output_bytes`, plus the live `get_pipe_state(...)` DTO used for pipe readiness checks. These are aliases/adapters over native Rust primitives, not a Python bridge and not an app-level JSON-RPC layer.

The ALS-style `FerrousFrameworkPipe` wrapper is retained as a native compatibility adapter. It accepts `FerrousPipeConfig`, can load YAML/JSON shellspecs, renders ctx/env values including `PYTHON` and `CWD`, merges caller env with rendered shellspec env, waits for live stdin readiness before returning, and exposes `shell_id`, `write_line_blocking`, `read_line_blocking`, and `close_blocking`. The wrapper is deliberately pipe-only so protocol framing and request/response matching stay in downstream applications.

Native launches now write a sidecar record at `FerrousNativeShellRecord.record_path`, next to stdout/stderr logs. The sidecar captures command/backend/status/log paths/capabilities/run metadata and env keys, but not env values or secrets. It now includes FWS-compatible metadata keys for `created_at`, `updated_at`, `autostart`, `ui`, `debug`, `io_metadata_log`, backend flags, `runtime_id`, and derived app context. `io_metadata_log` is a stable path only until Ferrous grows IO sidecar writers.

Ferrous now mirrors the Python FWS store and secret bootstrap rules. `FerrousNativeManager::new()` resolves `FRAMEWORK_SHELLS_BASE_DIR` or `~/.cache/framework_shells`, computes/uses `FRAMEWORK_SHELLS_REPO_FINGERPRINT`, derives `runtime_id = sha256(secret)[:16]`, creates `meta`, `logs`, and `sockets`, and uses `runtimes/<repo_fingerprint>/secret` when `FRAMEWORK_SHELLS_SECRET` is absent. Native spawn configs use that canonical `logs` dir when `log_dir` is omitted.

Fresh Ferrous managers now load persisted records from the canonical store logs directory. Loaded sidecars are marked `adopted: true` and clear live-only capabilities such as `stdin_write` and `terminate`, while retaining log paths and inspection metadata.

Ferrous can now launch rendered shellspec entries directly through native `proc`, `pipe`, and `pty` dispatch. The current API accepts a shellspec document value plus entry id, renders ctx/env/free-port templates, parses command/env/subgroups/backend, rejects `autostart: false`, and writes records/logs through the canonical FWS store when no explicit log dir is provided.

Native shellspec launch now waits for supported readiness probes. Current Ferrous support covers `tcp_port` and `stdout_regex`; unsupported probe types fail explicitly.

Ferrous now has a native shellspec apply/reconcile path for multi-entry documents. It starts missing autostart specs, skips already-running live records with the same `spec_id`, and can prune live specs no longer present in the desired document.

Native capability records now distinguish stdin write, stdin EOF, output read, output subscription, log availability, terminate, and resize. Pipe and PTY expose stdin EOF while live; adopted/stale records clear live-only controls.

PTY shells now expose native resize through `resize_pty_blocking(...)`, implemented with `TIOCSWINSZ` on a retained PTY master fd.

Ferrous now has a Rust-owned host/control-plane MVP. `FerrousNativeHost` wraps `FerrousNativeManager` with an Axum/Tokio HTTP server, exposes `/fws`, runtime info, shell list/detail/create, shellspec apply, stdin write/EOF, log tail, terminate, action alias, and group shutdown routes, and uses the same `HMAC(secret, "api")` token shape as Python FWS for mutating routes. This host reports `socketio: false`; no Socket.IO-compatible peer lane is claimed yet.

## FWS Environment Contract

Ferrous owns a native FWS child-env contract so a Rust framework can launch nested workers and extension shells without depending on Python bootstrap code.

Current managed keys:

- `FRAMEWORK_SHELLS_SECRET`
- `FRAMEWORK_SHELLS_RUN_ID`
- `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL`
- `TE_FRAMEWORK_URL`

`FerrousNativeManager::new()` derives those values from the current process. Missing secret/run values are generated natively. URL values remain optional unless a host/control plane is attached. `FerrousNativeHost::spawn(...)` sets `TE_FRAMEWORK_URL` to the bound host URL when absent and intentionally does not set `FRAMEWORK_SHELLS_FWS_SOCKETIO_URL` until a real Socket.IO-compatible lane exists.

`FerrousNativeManager::with_env(FerrousNativeEnv { ... })` is the explicit host path. Native `proc`, `pipe`, and `pty` launches receive the manager overlay first, then shell-specific config env is applied last. That preserves the FWS inheritance contract while still allowing one shellspec or caller to override a value deliberately.

`FerrousNativeManager::try_with_env_map(...)` and `with_env_map(...)` are the explicit prebuilt-environment path for Rust callers that already resolved FWS/TE2 env values outside Ferrous. The env map can supply the FWS base dir, repo fingerprint, secret, run id, FWS Socket.IO URL, and TE framework URL; missing values follow the same native fallback rules as `new()`.

## Shellspec Compatibility

Shellspec compatibility is a core Ferrous requirement. A compiled Rust framework should be able to change runtime parameters without rebuilding the framework binary. Shellspecs are the runtime override layer for command, cwd, environment, backend, labels, subgroups, readiness, debug metadata, and future nesting behavior.

The current Ferrous test fixture covers `proc`, `pipe`, and `pty` render surfaces, ctx/env precedence, missing values, stable `${free_port}` reuse, command/cwd/env/subgroups/pipe/readiness/autostart rendering.

Current fixture locations:

- Python FWS: `tests/fixtures/shellspec_parity_cases.json`
- Ferrous crate: `.external/ferrous-framework/testdata/shellspec_parity_cases.json`

The mirrored fixture is acceptable during the ignored-checkout workflow. Longer term, the fixture should become crate-owned and FWS should consume or mirror it with an explicit drift check.

## Runtime Direction

Pipe is an ideal Rust-owned target. The receiving side of the pipe is part of the Ferrous runtime itself, not a separate broker process and not Python-owned I/O.

PTY is also a strong Rust-owned target. PTY sessions should be part of the crate runtime while preserving FWS management semantics.

The framed terminal-stream protocol is intentionally deferred. Current native `pty` is a raw PTY byte-stream backend, not the JSONL-out / JSON-RPC-in terminal broker shape. If needed later, that should be added as a separate higher-level protocol mode or backend layered over PTY instead of changing base PTY semantics.

Proc/app-worker style shells are lifecycle/log workers: stdout/stderr logging, process lifecycle, metadata, readiness, and dashboard visibility, but no stdin control unless a future capability explicitly adds it.

## Next Slices

1. Host/dashboard runtime follow-through.

   The Rust-owned HTTP host/control-plane MVP exists. Remaining host work is downstream integration, live notification transport, richer dashboard UI, and a real Socket.IO-compatible or UDS peer lane when needed.

2. Finish the reactor cutover.

   Passive log streams and child exit status now use one manager-owned reactor thread. Remaining work is moving readiness polling and future live subscriptions onto the same runtime model without changing pipe/PTY direct-read semantics.

3. Metadata persistence.

   FWS-compatible record fields now exist for core shell/dashboard metadata. Remaining work is IO metadata sidecar writers and any future signing/verification parity.

4. Capability expansion.

   EOF, PTY resize, output subscription, and bounded slow-subscriber disconnection are implemented. Remaining work is raw write naming/API polish and deeper backpressure metrics/policy.

5. PTY terminal semantics.

   PTY resize and raw/interactive terminal mode controls are implemented. Remaining work is deeper terminal semantics only if a concrete consumer needs them.

6. Framed terminal-stream protocol.

   Deferred. Add JSONL-out / JSON-RPC-in terminal broker semantics only as an explicit higher-level PTY protocol mode/backend when a consumer needs it.

## Test Matrix

Ferrous checks:

- `cargo test`
- `cargo test pipe_ -- --nocapture` for timing output
- `cargo test pty_terminal -- --nocapture` for PTY terminal timing output
- shellspec parity fixture test
- native proc/pipe/PTY lifecycle tests

Framework-shells checks while this repo carries the ignored Ferrous checkout:

- `python -m compileall -q framework_shells setup.py scripts/release`
- `basedpyright --project pyrightconfig.json`
