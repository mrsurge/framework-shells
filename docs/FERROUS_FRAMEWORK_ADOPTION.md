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
- `FerrousNativeProcConfig`
- `FerrousNativePipeConfig`
- `FerrousNativePtyConfig`
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

Proc/app-worker style shells are lifecycle/log workers: stdout/stderr logging, process lifecycle, metadata, readiness, and dashboard visibility, but no stdin control unless a future capability explicitly adds it.

## Next Slices

1. Replace thread-per-stream with a reactor.

   The current direct fd path is semantically right but still synchronous/blocking. The next performance step is an evented Rust reactor for pipe/PTY reads and process lifecycle.

2. Metadata persistence.

   Write FWS-compatible records/log metadata from Rust so dashboards and inspectors can consume native Ferrous shells.

3. Capability expansion.

   Add EOF, raw write, resize, output subscription, and backpressure capabilities.

4. PTY terminal semantics.

   Add PTY resize and terminal mode controls.

5. Host/dashboard runtime.

   Add Rust-owned host/dashboard/socket runtime after backend parity is stable.

## Test Matrix

Ferrous checks:

- `cargo test`
- `cargo test pipe_ -- --nocapture` for timing output
- shellspec parity fixture test
- native proc/pipe/PTY lifecycle tests

Framework-shells checks while this repo carries the ignored Ferrous checkout:

- `python -m compileall -q framework_shells setup.py scripts/release`
- `basedpyright --project pyrightconfig.json`
