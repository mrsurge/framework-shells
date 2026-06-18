# Ferrous Framework Adoption

`ferrous-framework` is intended to become the Rust implementation path for framework-shells-style process management. It should let Rust projects run inside an existing FWS environment, share FWS secrets/metadata/dashboard state, and eventually run a Rust-compiled FWS manager suite without requiring Python in the hot runtime path.

The crate is developed from an ignored local checkout in this repo:

```text
.external/ferrous-framework
```

That checkout is not a submodule and is not a framework-shells package dependency. It is a local development/reference checkout for pushing changes to `mrsurge/ferrous-framework`.

## Target Shape

End state:

```text
Rust application
  -> ferrous-framework crate
  -> Rust FWS-compatible manager/runtime
  -> FWS-compatible records, logs, dashboard metadata, shared-secret semantics
```

Optional interop state:

```text
Rust application
  -> ferrous-framework crate
  -> PyO3 bridge
  -> installed framework-shells Python manager
```

The PyO3 bridge is an interoperability path, not the final architecture. It exists so Rust consumers can participate in the current Python FWS environment while Ferrous grows the Rust-owned runtime pieces.

Bridge-hosted root state:

```text
Rust application
  -> ferrous-framework FerrousFrameworkHost
  -> PyO3 bridge
  -> framework-shells FastAPI/FWS Socket.IO runtime
  -> child processes inherit FWS URL/secret/run-id env
```

This is the current transitional answer for Rust programs that are the FWS initializer. It preserves the root/peer split while leaving room for a later Rust-owned Axum/socketioxide host behind the same Ferrous host API.

## Current Contract

- `framework-shells` owns the Python bridge module: `framework_shells.ferrous_framework`.
- `ferrous-framework` defaults its PyO3 bridge import to `framework_shells.ferrous_framework`.
- `FerrousFrameworkHost` starts a bridge-backed FWS dashboard/Socket.IO root and returns child env for peer managers.
- `FerrousFrameworkHost` supports free-port parity through `port = 0`.
- `FerrousFrameworkHost` exposes shutdown group/tree hooks that return typed metrics while delegating to the existing FWS shutdown implementation.
- `FerrousFrameworkPipe` remains the compatibility class name for current ALS-RS usage.
- ALS-RS consumes `ferrous-framework` as a Rust submodule/crate.
- ALS-RS runtime must have `framework-shells >= 0.0.54` installed so the bridge import exists.
- The current bridge supports pipe-backed stdio JSON-RPC use cases first.

## Bridge Version Pinning

The transitional PyO3 bridge must fail fast when the Rust crate and installed Python bridge drift.

Immediate direction:

- Keep `framework_shells.ferrous_framework` importable for current consumers.
- The Python bridge exposes `ferrous_bridge_info()` with bridge API, `framework_shells` version, and support flags.
- `ferrous-framework` checks that bridge metadata before selecting the Python bridge path.
- Preserve explicit `python_module` / `python_class` overrides for debugging and migration.

End-game direction:

- Move the canonical Python compatibility shim into the `ferrous-framework` crate repo.
- Load that embedded shim when the `pyo3-embed` bridge path is selected.
- Keep any `framework_shells.ferrous_framework` module as a compatibility export, not the authoritative bridge implementation.
- Let the Rust crate version own the transitional bridge behavior until the native Rust path no longer needs Python.

This pins adapter behavior to the crate version while still allowing the embedded shim to import installed `framework_shells` runtime APIs during the transition.

## Parity Test Contract

Ferrous and Python FWS must share parity cases for shellspec rendering while the native Rust implementation grows.

Current parity fixture coverage:

- backend rendering surfaces: `proc`, `pipe`, `pty`
- `${ctx:KEY}`
- `${env:KEY}`
- direct `${KEY}` resolution as ctx first, env second
- missing ctx/env values resolving to empty strings
- `${free_port}` stability within one rendered shell
- command, cwd, env, subgroups, pipe config, readiness, and autostart rendering

Current fixture locations:

- Python FWS: `tests/fixtures/shellspec_parity_cases.json`
- Ferrous crate: `.external/ferrous-framework/testdata/shellspec_parity_cases.json`

The mirrored fixture is acceptable during the ignored-checkout workflow. Once the canonical bridge source moves into `ferrous-framework`, the fixture should become crate-owned and FWS should consume that source or mirror it with an explicit drift check.

## Shutdown Hook Contract

Ferrous exposes bridge-host shutdown hooks as management calls, not as a replacement shutdown algorithm:

```text
Rust caller
  -> FerrousFrameworkHost shutdown call
  -> PyO3 bridge
  -> existing framework-shells shutdown_app_group/shutdown_snapshot behavior
  -> typed shutdown result DTO
```

Current host methods:

- `shutdown_group(app_id)`: shuts down the running FWS app/group matching `app_id`.
- `shutdown_tree(root_pids)`: shuts down a process snapshot rooted at the provided root PIDs. An empty root list targets the whole snapshot.

The response shape is stable across both methods:

```json
{
  "ok": true,
  "kind": "shutdown_group",
  "target": "example-app",
  "started_at_ms": 1780000000000,
  "ended_at_ms": 1780000000250,
  "elapsed_ms": 250,
  "root_pids": [1234],
  "stats": {
    "total": 1,
    "terminated": 1,
    "clean_exits": 1,
    "force_killed": 0,
    "errors": []
  },
  "events": []
}
```

The hook may collect shutdown log/event strings from the existing shutdown helper, but it must not change ordering, force-kill behavior, descendant discovery, or group matching semantics.

Dependency direction today:

```text
ALS-RS
  -> ferrous-framework Rust crate/submodule
  -> installed framework-shells Python package

ferrous-framework
  -> PyO3 imports framework_shells.ferrous_framework

framework-shells
  -> owns bridge module
  -> does not depend on ferrous-framework
```

## Primary Consumers

ALS-RS is a compatibility consumer, not the primary target for the full Ferrous design.

ALS-RS currently needs:

- one pipe abstraction for the extension adapter
- line-oriented JSON-RPC request/response over that pipe
- FWS metadata/dashboard visibility for the adapter shell
- compatibility with extension stdio transports spawned underneath that adapter

TE2 is the stronger full-compatibility canary because FWS originally spun out of the TE2 process/runtime model. TE2 exercises the more traditional FWS suite:

- app-worker processes with stdout/stderr logs and no stdin control
- pipe workers for stdio protocol services
- PTY-backed terminal-like sessions
- shared dashboard and metadata management
- nested managers/process trees under a shared FWS environment

Generic Rust frameworks are the long-term target: they should be able to use FWS-style lifecycle, logs, metadata, shellspec/runtime semantics, and dashboard visibility without adopting Python as their runtime manager.

## Shellspec Compatibility

Shellspec compatibility is a core Ferrous requirement. A compiled Rust framework should be able to change runtime parameters without rebuilding the framework binary. Shellspecs are the runtime override layer for command, cwd, environment, backend, labels, subgroups, readiness, debug metadata, and future nesting behavior.

Compatibility path:

```text
compiled Rust framework binary
  + FWS-compatible shellspec
  -> runtime-specific process behavior
  -> no framework rebuild
```

Implementation path:

- The current PyO3 bridge may render shellspecs through `framework_shells.shellspec`.
- Ferrous should grow Rust DTOs for rendered shellspec data before replacing the parser.
- A future Rust parser/renderer must be parity-tested against Python FWS shellspec behavior.
- TE2's eventual Rust framework spike depends on shellspec compatibility because shellspecs keep runtime wiring outside the compiled framework.

## Runtime Direction

Pipe is an ideal Rust-owned target. The receiving side of the pipe should become part of the Ferrous runtime itself, not a separate broker process and not a Python pump. FWS management should remain visible through records, logs, metadata, capabilities, and shared-secret environment semantics.

PTY is also a strong Rust-owned target. `rustix`/PTY-capable Rust code should make the PTY session part of the crate runtime while preserving FWS management semantics.

Proc/app-worker style shells are lifecycle/log workers: stdout/stderr logging, process lifecycle, metadata, readiness, and dashboard visibility, but no stdin control unless a future capability explicitly adds it.

The first native Ferrous baseline is `FerrousNativeManager` with Rust-owned `proc` support. It launches child processes directly from Rust, captures stdout/stderr to log files, tracks typed shell records, lists/gets records, terminates children, and waits for exit status. This path is additive and does not replace the PyO3 bridge path yet.

Backend target set:

```text
pipe
pty
proc
```

## Ownership Boundaries

`framework-shells` owns today:

- Python bridge module API and runtime behavior.
- Current FWS manager calls, shellspec rendering, shared-secret inheritance, records, logs, dashboard metadata, and capabilities.
- Compatibility aliases required by current consumers.

`ferrous-framework` should own over time:

- Rust caller API.
- Rust-side typed config, shell identity, capability, record, and error DTOs.
- Rust-owned pipe runtime.
- Rust-owned PTY runtime.
- Rust-owned proc lifecycle/log runtime.
- Optional PyO3 compatibility bridge into installed Python FWS.
- Eventual Rust FWS-compatible manager suite.

ALS-RS owns:

- Submodule pointer.
- Adapter integration choices.
- ALS-specific shellspecs and extension adapter lifecycle.
- Its pipe-only compatibility usage.

TE2 will likely own:

- The main compatibility validation path for broad FWS runtime behavior.
- Migration of its framework runtime toward Ferrous-backed Rust management.

## Compatibility Rules

- Do not remove `FerrousFrameworkPipe` until ALS-RS no longer depends on it.
- Do not remove the current line-oriented pipe read/write methods until ALS-RS no longer depends on them.
- ALS-RS compatibility should not drive the generic Ferrous API shape beyond preserving its current pipe use case.
- Any breaking `ferrous-framework` crate API change must be paired with an ALS-RS update before the ALS submodule pointer moves.
- Any bridge relocation must preserve an importable compatibility path for at least one transition release.
- `framework-shells` must remain usable without the local `.external/ferrous-framework` checkout.
- Ferrous must preserve FWS shared-secret/environment compatibility so Rust and Python managers can coexist or nest.

## Current State

As of this adoption note:

- FWS bridge module: `framework_shells/ferrous_framework.py`.
- FWS version containing generic bridge, shellspec ctx passthrough, bridge-hosted FWS root, shellspec parity tests, and bridge metadata: `0.0.57`.
- `ferrous-framework` expected head: local checkout with bridge metadata validation, shellspec parity tests, and bridge-host shutdown DTO support.
- Crate default module: `framework_shells.ferrous_framework`.
- Crate default class: `FerrousFrameworkPipe`.
- ALS-RS should pass, or inherit, the same module/class defaults.

## Next Slices

1. Generic naming while preserving pipe aliases.

   Add generic Rust names such as `FerrousShellConfig` and `FerrousFrameworkShell`, while keeping `FerrousPipeConfig` and `FerrousFrameworkPipe` as compatibility aliases.

2. Shellspec-compatible config input.

   Preserve shellspec path/entry input in the generic API, and allow rendered shellspec backend/env/cwd/command/subgroups to override compiled defaults.

3. Backend selection.

   Add a typed backend field with:

   ```text
   pipe
   pty
   proc
   ```

4. Capability reporting.

   Expose FWS-compatible capabilities so Rust callers can decide whether stdin write, EOF, stdout subscription, stderr subscription, resize, or reattach-like behavior is available.

5. Rust-owned pipe runtime.

   Move pipe receiving/writing into the Ferrous runtime. The pipe runtime should be the crate runtime, not a separate broker process and not Python-owned I/O.

6. Rust-owned PTY runtime.

   Add PTY lifecycle and write/read support through Rust PTY primitives. PTY output is terminal stream data, not JSON-RPC line data.

7. Rust-owned proc lifecycle.

   Add proc lifecycle support as lifecycle/log metadata only. Do not pretend proc has live stdin unless Ferrous/FWS explicitly adds that capability.

8. Rust-side DTO cleanup.

   Move toward typed Rust request/response structs for config, capabilities, shell identity, records, metadata, and errors before adding more behavior.

9. Rust manager suite.

   Build toward a Rust-compiled manager that can run standalone or participate in an existing FWS environment through the same shared-secret/runtime contract.

## Test Matrix

Framework-shells checks:

- `python -m compileall -q framework_shells setup.py scripts/release`
- `basedpyright --project pyrightconfig.json`
- wheel smoke confirms `framework_shells.ferrous_framework` is packaged
- installed site-package import confirms `framework_shells.ferrous_framework.FerrousFrameworkPipe`

Ferrous-framework checks:

- `cargo check --features pyo3-embed`
- default constants point to `framework_shells.ferrous_framework` and `FerrousFrameworkPipe`
- generic aliases preserve current pipe behavior
- future Rust-owned pipe/PTY/proc runtimes report FWS-compatible capabilities

ALS-RS checks:

- submodule pointer is on expected ferrous-framework commit
- ALS adapter build with `ferrous-framework-pyo3`
- extension adapter starts through ferrous path
- JSON-RPC request/response still works over the pipe adapter
- fallback path remains explicit and observable if ferrous startup fails

TE2 checks:

- app-worker lifecycle/log behavior remains compatible
- pipe stdio protocol workers remain dashboard-visible and metadata-managed
- PTY sessions remain dashboard-visible and metadata-managed
- nested manager/process-tree behavior preserves shared-secret environment semantics

## Open Questions

- Should generic names land first in the Rust crate or in the Python bridge?
- Should the first Rust-owned runtime target be pipe only, or pipe plus proc lifecycle?
- Should byte-oriented reads be added before PTY support, or is line-oriented compatibility enough for the transition slice?
- What is the minimal record/log metadata contract a Rust manager must write so existing dashboards and inspectors can consume it?
- How should Rust and Python FWS managers discover each other when nested under the same shared secret?
