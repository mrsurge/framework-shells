# Persistent UDS Shells

## Scope And Status

Working branches: `feature/uds-shells` in Python FWS and Ferrous, based on their
merged main snapshots. Implement in parallel; no live TE2/ALS restart for tests.

Implemented slices cover pure policy/attachment state, existing-manager adoption
characterization, and bounded UDS framing with a real Python/Rust round trip.
An experimental standalone supervisor now provides authenticated sessions,
ordinary child stdio, raw capture, writer fencing, disconnect/reconnect and
explicit EOF/Stop. It is not connected to shellspec launch or FWS adoption;
framework shutdown survival is not guaranteed yet.
Signed discovery records and explicit authenticated attachment adapters now
provide the adoption foundation. Ordinary manager launch/shutdown remains unchanged.
Existing proc/pipe/PTY paths and Socket.IO peer transport remain unchanged.
The broader requirements remain in LOG_PROJECTION_PLAN.md.

## Contract Decisions

- Persistent stdio is an opt-in managed transport, not an application protocol.
  The child still reads stdin/writes stdout and stderr, including opaque bytes.
- Replace the prototype JSON/base64 supervisor envelope with a schema-defined
  binary envelope and generated Python/Rust bindings. Application payloads stay
  opaque byte vectors, never JSON-parsed or base64-encoded by the supervisor.
  Cap'n Proto and FlatBuffers are candidates, not a selected dependency yet.
  Benchmark actual bindings and Termux/free-threaded installation before choosing.
- Reserve `lifecycle.survive_shutdown` for launch integration. Missing means false.
  Render ctx/env first; accept booleans and trimmed case-insensitive true/false,
  1/0 strings. Reject null, numeric values, empty/unresolved values and containers.
  Do not accept a flag at launch until its supervisor is available and shutdown
  protection is active. Unsupported platforms/artifacts must fail explicitly.
- Framework teardown preserves each verified survival root and its entire
  descendant tree. A nested manager inside that boundary must also preserve it.
- Explicit shell/group Stop terminates the selected subtree, including nested
  survival roots. An empty selection in the policy primitive means no targets;
  existing manager all-shell aliases must expand their roots explicitly.
- PID selection is not permission to send a broad process-group signal. The
  executor must verify identity and prevent group signals from crossing protected
  boundaries. A PID snapshot alone is not sufficient against PID reuse/races.
- Disconnect/detach releases writer ownership, never closes child stdin. EOF is
  explicit, owner-only and irreversible. Child exit revokes input capability.
- Acquire fails while a writer owns stdin unless takeover is explicit. Every
  acquisition increments a non-wrapping generation. Each write/EOF is bound to
  authenticated connection, instance nonce and current generation. Old detach
  must not revoke a newer owner's lease. Observers never acquire a writer lease.
- Check the lease at actual write execution, not just enqueue time; ownership
  changes and writes must be serialized. Partial/uncertain writes are reported,
  not replayed automatically. These pure state objects are not authentication or
  concurrency primitives and must not be exposed directly to untrusted callers.

## Implementation Order

1. Policy/state primitives and mirrored golden tests. Verified: three Python and
   three Rust tests pass; focused Python strict typing has zero errors/warnings.
2. Existing adoption characterized with disposable Python/Rust managers: a fresh
   manager sees the live shell with original ID/PID/labels but lacks live input
   and output capabilities. Its write fails; original owner can still exchange
   bytes; metadata-based Stop works. This is two managers while the owner lives,
   not a claim that child processes survive owner death. Abrupt-death testing
   remains part of supervisor acceptance.
3. Shared Rust supervisor: Tokio UnixListener/process I/O, bounded versioned
   framing using established libraries; private endpoint, secret authorization,
   nonce handshake, owner fencing, raw logging and explicit detach/write/EOF/stop.
   Canonical standalone crate now lives at Ferrous `crates/fws-supervisor`, with
   a checked snapshot at Python `native/fws_supervisor`. User approved this layout.
   `scripts/sync_supervisor.py --write` refreshes it; `--check` rejects drift.
   FWS sdist includes source; no install-time binary build is enabled yet because
   the prototype is not production-ready. Ferrous re-exports policy primitives.
   Framing uses Tokio LengthDelimitedCodec / Python asyncio readexactly: unsigned
   four-byte big-endian lengths, 64 KiB max payload, opaque bytes, empty frames
   distinct from transport EOF. Malformed connections must be closed, not resynced.
   Experimental session V1 now uses mutual HMAC-SHA256 proofs bound to instance,
   version/domain and fresh server/client nonces. Private directory ownership and
   0600 sockets, 16-session bounds, three-second handshake/send/write deadlines,
   and fenced input are implemented. See the shared crate README for the exact
   request/event contract. Logging continues when clients disconnect; live output
   is event-driven with bounded broadcast and explicit lag gaps. Bounded replay
   is provided by the following slice; manager adoption remains separate.
4. Binary-envelope comparison and control baselines as specified below. Select
   the schema/codec before freezing the production wire contract. The shared-client
   experiment now allows replay semantics to be tested on the existing experimental
   JSON/base64 wire first, without choosing a production codec by implication.
5. Raw byte cursor replay/live attachment with bounded buffering and explicit
   gaps/errors; test disconnected output and lost acknowledgements.

   Active slice after pushed checkpoints FWS `80d81b5` / Ferrous `7db12fd`:
   explicit new-connection stdout/stderr cursors, bound to the authenticated
   supervisor instance. Capture a published-byte watermark and subscribe to live
   output in one critical section; replay only to that watermark, then forward
   live events from there. Suppress pre-resume live events until a replay-begin
   marker. Reject future offsets and replay spans above 1 MiB combined rather
   than clipping, resetting cursors or silently jumping to tail. This initial
   startup replay is bounded, not an arbitrary-history paging API; raw logs remain
   untouched. Live broadcast lag during replay fails explicitly. Validate raw-log
   identity/size; external log mutation is unsupported (not an atomic filesystem
   snapshot). A reconnect is explicit and never replays stdin or reacquires its
   writer lease automatically. Keep the ordinary live-only connect path unchanged.
6. Python and Rust manager adapters, shellspec rendering, persisted identity and
   acquire-before-spawn reconciliation. Integrate typed shutdown intent and
   process-group-safe protection before advertising survival capability.
7. Mixed-runtime nonce/counter/grandchild acceptance, installation, then separately
   authorized live TE2 restart with ALS preserved.

UDS manager-to-manager control remains independent and deferred. Do not replace
Socket.IO or direct pipes as a side effect of persistent shell work.

## Binary Envelope And Benchmark Plan

### Protocol Boundary

Use an established schema compiler/runtime rather than another handwritten wire
layout. One canonical schema generates both languages' bindings. Model typed
operations/events: authenticate, acquire, write, output, detach, EOF, Stop, status,
and later resume/gap/error. Fields such as ownership generation, instance identity,
stream, cursor, request correlation and opaque payload belong to the supervisor.
The enclosed application protocol does not. The envelope remains necessary for
ongoing I/O, not just reconnect. JSON-RPC is a benchmark workload, not a constraint
on managed children; include opaque binary and MessagePack-shaped payloads too.

Serialization does not provide reconnection, authentication or shutdown semantics
by itself. Retain fencing, uncertainty reporting, bounded framing/verification,
schema evolution rules and explicit protocol-version rejection. Test malformed
frames, length limits, unknown variants and incompatible versions in both bindings.
Generated schemas are not a substitute for validating untrusted messages.

Remove text encoding overhead, but do not promise zero-copy or a universally
smallest envelope. Measure buffer construction, verification/access, copies and
Python binding overhead. UDS/pipe I/O and logging still have costs. Generated code
must ship with reproducible generation/version instructions; evaluate compiler
and runtime requirements for Rust, Python 3.13 and Termux free-threaded 3.14t.

### Measurement Layers

| Layer | Control | Comparison | What it isolates |
| --- | --- | --- | --- |
| Codec only | Existing JSON/base64 envelope | Candidate schema envelopes | Encode/build, verify/access, allocations, encoded size; no sockets/logs |
| Transport microbench | Length-framed raw bytes over UDS | Same UDS with JSON/base64 or candidate envelope | Incremental envelope cost on identical socket topology |
| Application workload | Direct stdio echo/application fixture | Same child/workload via Rust supervisor, current then candidate envelope | Total supervisor cost, including extra hop, scheduling and observability |
| Observation comparison | Matched transport-only bench fixture | Matched raw-log capture enabled | Logging/flush cost separated from codec cost |

The transport-only fixtures are benchmark controls, not new production opt-in
flags or a Python supervisor implementation. Existing direct FWS pipe and native
Ferrous paths remain reference baselines. Do not call the whole direct-pipe versus
supervisor difference serialization cost: that comparison changes topology too.
For JSON/base64 versus binary supervisor runs, keep child, logging/flush policy,
authentication scope, scheduling and workload identical. Current capture writes
and flushes raw logs before publishing events; changing that ordering is a separate
experiment with its own correctness/durability contract, not a hidden codec win.

### Language Matrix

Run isolated UDS endpoints in separate OS processes for Python->Python,
Python->Rust, Rust->Python and Rust->Rust. Python->Python is the requested primary
overhead-sensitive/worst-case candidate; do not assume it must be slowest or
confuse separate-process GILs with one shared interpreter lock. Record interpreter
version, GIL/free-threaded state and event loop; asyncio/uvloop results are distinct
where supported. Rust-only runs give a native reference, mixed runs catch schema,
integer-width, byte-vector, stream-boundary and ownership/cursor interpretation gaps.

Separately vary the application child (existing Python and native Rust fixtures)
under the real Rust supervisor. Keep the child language fixed within each control/
candidate comparison. A Python child can cap total RPS; it cannot alone establish
the supervisor's ceiling. Rust debug/release profiles are separate result series.

### Workloads And Existing Assets

Reuse workload generators and assertions from:

- Python FWS `scripts/bench_native_pipe_jsonrpc.py` and
  `scripts/bench_pipe_jsonrpc_server.py`; the runner already selects Python/Rust
  children, loop, request count, concurrency, response sizes and push counts/sizes.
- Python FWS `native/fws_pipe_pump/src/bin/bench_pipe_jsonrpc_server.rs`
  (the `fws-pipe-jsonrpc-bench-server` executable).
- Ferrous `tests/native_manager.rs` RTT, async-concurrent and mixed workloads,
  plus `src/bin/ferrous-jsonrpc-echo.rs`.

Adapt these rather than writing a different application workload per codec.
Separate workload generation, transport adapters and metrics collection. Use the
same deterministic payloads/IDs and seed for every variant; preserve cached binaries.

Cover sequential RTT, concurrent request/response, interleaved notifications,
notification-only bursts, asymmetric request/response sizes and mixed payload
types. Include empty bytes, NUL/non-UTF-8 data, large integer control fields, and
near-limit chunks. Large logical payloads must use the same explicit chunking
policy in every variant; a byte-stream wrapper must not invent message boundaries.
Retain burst-then-drain as a labeled stress case, not the sole workload model.

Sweep concurrency (e.g. 1/8/24/64) and payload sizes (e.g. 128 B/4 KiB/32 KiB).
Distinguish application requests in flight, UDS sessions and outstanding supervisor
requests. Current V1 allows only one outstanding supervisor request per session;
do not imply it supports multiplexed requests or exceed its 16-session cap silently.
Any correlation/multiplexing change must be explicit and shared across comparisons.

### Measurements And Acceptance

- Record completed application requests/s, response/notification rates, useful
  payload bytes/s and wire bytes/s separately, including envelope expansion.
- Record RTT min/median/p95/p99/max, mean and standard deviation; define jitter
  explicitly (e.g. successive RTT differences and notification interarrival
  variation) instead of treating an unspecified "jitter" number as comparable.
- Report CPU time by process, wall time, peak RSS and allocations/copies where
  instrumentation can measure them. Do not invent allocation counts from RSS.
- Report malformed/unknown/duplicate responses, missing notifications, timeouts,
  gaps and payload hash mismatches. Failed/timed-out work must not disappear from
  latency or throughput reporting. Check request IDs and ordering guarantees.
- Separate startup/auth/reconnect latency from warmed steady-state measurements.
  Use monotonic timings at the initiating endpoint, enough samples for tail
  percentiles, repeated paired runs and alternating/randomized variant order.
  Record load/thermal context and run-to-run spread; do not cherry-pick best RPS.
- Use a fixed offered-load case as well as closed-loop concurrency/saturation;
  report scheduler lateness so a stalled sender cannot make tail latency look good.
- Require byte/semantic parity before accepting speed improvements. Both languages
  must pass framing, authentication, ownership and reconnect tests after switching
  codecs. No library selection or claimed speedup until matched results exist.

Store commands, exact source revisions plus dirty diff identity, build profiles,
interpreter/library versions, machine-readable results and summarized comparison
tables under ignored `.external/bench-results/`. Separate any profiling-instrumented
runs from throughput runs. Do not erase cached binaries, restart live workers,
or turn these benchmark controls into a new production tuning-option matrix.

Initial control harness implemented on 2026-09-19 in
`scripts/bench_uds_envelope.py`: separate Python UDS processes, raw length-framed
bytes versus a minimal JSON/base64 data envelope, one request per connection,
byte-equality checks, warmup and randomized variant order. This is transport-only:
no actual supervisor, authentication, logging or application JSON-RPC processing.
It does not yet exercise the existing application fixtures or Rust/mixed peers.

First shakeout: 300 requests per case, 128 B/4 KiB/32 KiB, 1/8/24 connections,
three repeats (54 successful runs) recorded at
`.external/bench-results/uds-python-controls-20260919.jsonl`, with summary and
provenance files alongside it. Three harness tests and focused strict typing pass.
Interpreter probe: Python 3.14.6 free-threaded build, GIL enabled. This preliminary
run did not isolate system load/thermal conditions and had overlapping validation
work near its end; do not use it for codec selection or production ceilings.
Server CPU, allocation profiles, fixed offered load and detailed jitter remain
unimplemented metrics. The harness reports RTT spread, wire expansion and client
CPU only; failures invalidate runs rather than disappearing into successful RPS.

With explicit user approval, installed Termux packages `flatbuffers` and
`capnproto`; compiler versions are flatc 25.12.19 and Cap'n Proto 1.5.0.
The 2026-09-20 continuation installs candidate bindings only into the ignored
`.external/uds-codecs-venv`: flatbuffers 25.12.19 and pycapnp 2.2.4. Both import
from that venv under `python -I`; inherited harness `PYTHONPATH` is not used.
This host now runs regular CPython 3.14.6 with its GIL enabled, unlike the prior
free-threaded-build control, so the old timings are not used as matched baselines.

`scripts/uds_bench/` in the Python FWS checkout now contains matching FlatBuffers
and Cap'n Proto schemas, typed Python adapters, a separately locked Rust peer
crate, isolated setup instructions and a four-direction transport runner. The
Rust code generates both bindings during its build; Python uses generated
FlatBuffers code and pycapnp's schema loader. Production FWS/Ferrous dependencies
and the JSON/base64 supervisor prototype remain unchanged.

Candidate envelopes preserve version, operation, generation, stream, offset and
opaque bytes. JSON uses the same metadata; raw framing is the payload-only
control. Warmup/startup are excluded, bytes and large integer metadata are checked,
and timed payloads carry per-request sequences to detect stale replies. Both
peers decode/re-encode instead of echoing encoded buffers. Current codec-only
controls are Python-only. Cap'n Proto uses unpacked serialization. Generated
Python FlatBuffers bulk reads use a documented pinned runtime vector accessor;
identifier/operation/vector bounds checks are not equivalent to Rust's full
verifier. This validation asymmetry is an explicit selection caveat.

Seven focused Python tests (including mixed/native subprocess pairings), two Rust
codec tests, focused strict Python typing and Rust Clippy with warnings denied
pass. Binary/empty/32 KiB payloads and u64 metadata survive both mixed directions.
The comparison runner covers all four runtime pairings with raw, JSON/base64,
FlatBuffers and Cap'n Proto, with randomized repeated runs and exclusive output
files. Provenance records binding versions, interpreter/GIL, source and binary
hashes, and declared Rust profile. No candidate has been selected.

2026-09-20 results: the broad sweep completed 432 transport trials plus 36 Python
codec-only trials with zero invalid runs. Large timing variation prompted a
longer focused rerun: 4 KiB payloads, eight sessions, 3,000 round trips per trial,
five repetitions (80 transport and 20 codec-only trials), also zero invalid runs.
Median round trips per second in that focused transport run:

| Client -> server | Raw framing | JSON/base64 | FlatBuffers | Cap'n Proto |
| --- | ---: | ---: | ---: | ---: |
| Python -> Python | 5,339 | 2,637 | 2,397 | 2,253 |
| Python -> Rust | 5,656 | 3,027 | 3,226 | 2,767 |
| Rust -> Python | 6,541 | 3,105 | 3,164 | 2,802 |
| Rust -> Rust | 27,898 | 14,303 | 23,435 | 20,647 |

These numbers are not FWS/supervisor/application throughput. Rust is release,
single-thread Tokio; Python is regular CPython 3.14.6 with standard asyncio.
No compilation or test jobs from this task overlapped the transport runs, but
unrelated device load/thermal conditions were not controlled (`/proc/loadavg`
is inaccessible here). Five-repeat Rust/Rust ranges were raw 23.8-31.6k,
JSON 12.5-15.2k, FlatBuffers 20.4-29.7k and Cap'n Proto 17.9-28.9k. Candidate
ranges overlap; do not select one from medians alone. Python/mixed paths do not
show a consistent binary-codec throughput gain. The 4 KiB envelopes are 5,583
bytes JSON, 4,160 FlatBuffers and 4,144 Cap'n Proto, excluding the shared 4-byte
frame prefix. The binary wire-size benefit is clear; total throughput depends on
bindings and scheduling as well as representation.

Artifacts: `.external/bench-results/uds-candidates-20260920.jsonl` and
`uds-candidates-focused-20260920.jsonl`, with per-case min/median/max summaries,
console logs and compiler/interpreter provenance alongside. The benchmark README
documents regeneration, isolation, validation differences and missing metrics.

Remaining measurement work includes the existing JSON-RPC request/notification
workloads, actual supervisor versus direct-pipe controls, logging-separated
measurements, Rust codec-only costs, server CPU/RSS/allocation attribution, fixed
offered load, and malformed/adversarial envelope parity before production use.
Cached artifacts and live workers remain untouched.

### Msgspec And Native-Pipe Follow-Up (2026-09-20)

The next measured slice now adds `json-msgspec` to the envelope controls, keeping
the JSON/base64 wire bytes and manual field validation identical to the stdlib
control. Only Python changes codec implementation; Rust uses serde_json under
both JSON labels. msgspec 0.21.1 and FWS import dependencies are installed only in
the ignored benchmark venv. A repeat at 4 KiB/eight sessions/3,000 round trips/
three repetitions completed 60 transport trials plus 15 codec controls with zero
invalid results. Python/Python median JSON throughput was 3,036 rps with stdlib
and 3,277 with msgspec; ranges overlapped (2,720-3,425 vs 2,643-3,493), while median
client CPU was 0.510 s vs 0.403 s. Do not claim a large, universal speedup.

`scripts/uds_bench/rpc.py` then exercises the actual FWS manager/native extension
against the actual authenticated Rust supervisor, running the same existing
`fws-pipe-jsonrpc-bench-server` child in every lane. Request encoding and final
unwrapped JSON-RPC decoding/routing use msgspec. Startup/authentication, 16 warmup
requests, validation of raw logs and teardown are outside timing. Each trial has
an isolated process/store/secret and disabled peer startup; no live manager is
contacted. Native activation is asserted and fallback fails the run.

Five repetitions of 1,000 requests per workload/lane (60 successful trials):

| Lane | Sequential RTT RPC/s | Concurrent RPC/s | Mixed RPC/s |
| --- | ---: | ---: | ---: |
| FWS native pipe, defaults | 4,682 | 7,137 | 4,538 |
| FWS native pipe, 8 KiB read/flush match | 4,614 | 6,705 | 4,188 |
| Rust supervisor, Python asyncio reader | 1,075 | 1,341 | 1,077 |
| Rust supervisor, Python native FD reader | 1,014 | 1,411 | 1,048 |

Concurrent means eight outstanding RPCs with 4 KiB responses. Mixed adds two
2 KiB notifications per response. Default native pipe concurrent p50/p95 was
1.075/1.309 ms; supervisor asyncio was 5.290/11.482 ms. Default native mixed raw
output throughput was 36.68 MiB/s versus supervisor asyncio 8.71 MiB/s. These are
whole-path results, not an isolated codec or UDS-versus-pipe syscall benchmark.
Device load and thermals remain uncontrolled; per-run ranges and p99 are retained.

The log policy matches semantically: one raw-log owner, raw writes visible before
delivery, no fsync. Raw log lengths and SHA-256 match observed application bytes
before child shutdown; stderr is empty, IDs/payloads and notification counts/order
are checked. Implementations differ: native uses std::fs::File writes, supervisor
uses Tokio file write/flush completion. A matched 8 KiB native lane retains this
difference rather than pretending that flush scheduling and chunking are equal.

Native UDS reader reuse is **benchmark-only**: pause asyncio's receive transport,
preserve its prebuffer, duplicate the socket FD into the existing PyO3 reader,
feed event-driven byte batches into the same frame decoder, and retain asyncio
writes. The mandatory extension log sink is /dev/null, so discard-write overhead
is included without a second retained envelope log. EOF/prebuffer and fragmented
output tests pass. No new reader thread, production option or runtime adapter.

The supervisor still has one outstanding write control command per session and
an ACK per input write. Native pipe has no corresponding ACK. The receiver swap
shows no consistent improvement, so the next attribution work should examine
serialized write/ACK scheduling, JSON/base64 envelope processing and Tokio log
completion. These experiments do not isolate which cost dominates and do not
justify changing those semantics blindly. Binary codecs are still not integrated
into the supervisor; replay/launch/adoption work remains pending.

The native-pipe fixture explicitly closes its test-owned paused asyncio subprocess
transport after manager termination; this eliminates an observed ResourceWarning
outside timing, without editing production manager cleanup. Thirteen focused
Python tests, two Rust codec tests, strict Python checks and warning-denied Rust
Clippy pass. Evidence lives in `.external/bench-results/uds-msgspec-controls-20260920.jsonl`
and `uds-rpc-msgspec-20260920.jsonl`, with summaries, pinned artifact/source hashes
and the isolated dependency inventory. Existing native artifacts/caches preserved.
No production dependencies, supervisor wire format, manager behavior or installs
were changed; no version bump, commit, push or live restart.

## Shared Native Client Experiment (2026-09-20)

Approved direction: keep one asynchronous Python transport API and select a
shared Rust client through an optional PyO3 extension when available; retain a
Python implementation with the same contract for unsupported environments.
Ferrous uses that Rust client directly, without Python. Selection happens before
connecting. Connection/authentication/I/O failures must never trigger a backend
switch, automatic reconnect, or input replay. Expose the selected implementation
for diagnostics, not another shellspec performance flag.

Move the entire outer client into Rust: authentication, framing, envelope codec,
write acknowledgements, writer generations, bounded event delivery and errors.
Python receives opaque application bytes and retains application-level parsing
(msgspec in the RPC benchmark). Do not move only FD reads again. First keep the
existing JSON/base64 wire and unchanged Rust supervisor/log policy so this
experiment isolates client ownership from a simultaneous codec change. Schema
binary-envelope selection remains a separate measured follow-up.

The shared client is asynchronous and protocol-neutral, not a JSON-RPC router.
Preserve one outstanding control request per connection, offset-bearing output,
disconnect distinct from EOF, takeover fencing, explicit stop, and no retries
after uncertain writes. Bounded queues must report overflow instead of silently
dropping bytes or blocking ACK delivery forever. Test both implementations with
the same disposable supervisor/child and forced backend selection, including
authentication, binary bytes, reconnect, EOF, stale writers and cancellation.

Build the extension only into the ignored benchmark area initially. No production
packaging hook, manager integration, installed-package changes or live restart.
Benchmark the common API against existing native-pipe and supervisor controls
with the same application workload and raw-log hash assertions. Prior Rust echo
numbers are not a measured ceiling for the real supervisor: ACK serialization,
logging and Python/Rust scheduling may still dominate. Preserve build caches.

## Tracker

| Slice | Python | Rust | Evidence |
| --- | --- | --- | --- |
| Strict resolved survival policy | Implemented | Implemented | Identical JSON fixtures |
| Shutdown selection/protected trees | Implemented, pure only | Implemented, pure only | Mixed, nested, explicit stop, empty/missing root, cycle fixtures |
| Attachment owner/EOF state | Implemented, pure only | Implemented, pure only | Takeover fencing, stale detach/EOF, reconnect, exit tests |
| Existing adoption characterization | Tested | Tested | Fresh manager cannot use owner descriptors but can Stop |
| Shared crate and bounded UDS framing | Tested | Tested | Actual Python/Rust socket round trip; canonical/vendor crate tests |
| Supervisor process and framing/auth | Prototype client tested | Shared prototype tested | Nonce/counter, raw cat, auth rejection, fencing, disconnect/EOF |
| Schema-defined binary envelope selection | Candidate adapters tested | Candidate adapters tested | Shared schemas and mixed round trips; no production dependency selected |
| Controlled envelope benchmark matrix | Transport/msgspec/RPC runs complete | Native/mixed and real-supervisor paths exercised | Logging visibility matched; codec/log/ACK cost attribution still pending |
| Shared native client / automatic Python selection | Experimental API v3 binding plus reference | Shared async client | API v2 benchmark checkpoint below; v3 adds explicit cursor attachment |
| Bounded replay/live handoff | Implemented, validation below | Implemented in shared core | Instance-bound cursors, atomic watermark/subscription, bounded raw replay; no stdin replay |
| Persisted supervisor identity and explicit attachment | Implemented, common client | Shared Rust descriptor/client | Signed private records; same PID/nonce/counter after disposable launcher death |
| Launch/adoption, packaging | Pending | Pending | Same bytes/identity across manager replacement |
| Shutdown executor integration | Pending | Pending | No broad signals across preserved boundaries |
| Live downstream acceptance | Not run | Not run | Requires separate authorization |

## Required Supervisor Tests

The process slice adds real Python-to-Rust nonce/counter tests and a Rust client
controlling an unmodified cat. Assertions cover unchanged PID/counter after
reconnect and abrupt client death, takeover/stale writes, wrong secret/instance/
version, EOF remaining closed, raw binary logs and explicit Stop cleanup.
These do not simulate death of the launcher or a mixed protected process tree.
Prototype Stop kills only the live child process group, not arbitrary escaped
descendants; it must not be substituted for the final FWS shutdown algorithm.
No installed-package or live framework acceptance has been performed.

Validation for the authenticated process slice: 10 focused Python tests pass
(including three real supervisor process tests), seven standalone Rust tests
pass from canonical and vendored sources, and four Ferrous policy/adoption
regression tests pass. Focused basedpyright is clean; standalone crate Clippy
passes with warnings denied. Session/process-group isolation is asserted.

Use isolated directories and only test-owned processes. Readiness barriers and
bounded deadlines replace arbitrary sleeps. A child counter and boot nonce plus
a grandchild nonce distinguish survival from restart. Include an unmodified
stdio child and binary payloads. Cross-attach Python to Rust and Rust to Python.

Exercise wrong secret/instance/version, stale endpoint, one-writer races, delayed
old writes after takeover, disconnect without EOF, irreversible explicit EOF,
child exit while detached, supervisor failure, lost write ACK without retries,
replay/live exact byte hashes, slow observers, disk errors, repeated reconnect FD
counts, and preserved descendants versus ordinary siblings at teardown. Unit
policy tests alone do not prove any of those process/transport guarantees.

### Native Binding Scheduling Experiment

The first optional binding used `pyo3-async-runtimes` for every output chunk and
write ACK. Its 60-trial actual-supervisor run passed byte/log/protocol assertions
but materially regressed throughput. Preserve this negative result at
`.external/bench-results/uds-shared-client-20260920.jsonl`; the original binding,
Python adapter and binary are retained under `.external/uds-client-future-baseline`.
This is not proof of a specific GIL cost; it motivates a controlled scheduling
change rather than attributing the whole gap to codec decoding.

The revised experimental binding keeps the same shared Rust client and wire.
Hot-path write submissions and batched output/write completions use a nonblocking
readiness socketpair registered through Python's `loop.add_reader`. Tokio workers
never acquire Python to deliver hot-path bytes or ACKs. No polling, extra process
or blocking read thread; Python only drains typed completions and opaque bytes.
Connection and infrequent control operations still use the standard async bridge.
The optional extension's private API version is now 2 to reject stale artifacts.
Native actor, binding output and Python delivery each cap their own output queue
at 256 events (8192 bytes each); they are separate bounded stages, not one shared
2 MiB budget. Binding write permits cover queued and completed-but-undrained
requests (64). Any output overflow fails explicitly and closes the client.

### Shared Client Results (2026-09-20)

Readiness-binding matrix: 60 valid trials, 1000 measured requests each, five
repeats per lane/workload, release Rust binaries, regular CPython 3.14.6 with GIL
and standard asyncio. No builds/tests overlapped timed trials. Same Rust child,
unchanged JSON/base64 supervisor and raw-before-delivery logging; msgspec decodes
application JSON-RPC in every lane. Median requests/sec:

| Client/path | Sequential RTT | Concurrent responses | Mixed responses + notifications |
| --- | ---: | ---: | ---: |
| Direct FWS native pipe | 3345 | 5364 | 3326 |
| Prior Python asyncio/msgspec supervisor client | 793 | 1126 | 906 |
| Common API, Python reference | 696 | 959 | 819 |
| Common API, shared Rust / readiness binding | 586 | 1906 | 1425 |

The native common client is about 1.99x the reference for concurrent requests and
1.74x for mixed traffic; versus the prior msgspec client, about 1.69x and 1.57x.
But sequential RTT throughput regresses about 16% versus the reference and 26%
versus prior msgspec: this is NOT a universal win or direct-pipe parity. Native
concurrent p50/p95 is 3.673/8.527 ms, mixed 4.801/10.429 ms, sequential
1.407/3.881 ms. Native mixed output throughput is 11.52 MiB/s versus reference
6.62 MiB/s and direct pipe 26.89 MiB/s. Native client CPU per 1000 requests is
0.670s concurrent, 0.837s mixed, 1.146s sequential; multithreaded native work is
included in process CPU. Do not claim blanket CPU savings.

The earlier future-per-completion native binding measured only 276 concurrent,
171 mixed and 145 sequential median requests/sec. Both runs' device conditions
varied (even direct-pipe controls shifted), so that before/after is diagnostic,
not an isolated GIL attribution or a reliable exact improvement multiplier.
Uncontrolled device/thermal noise remains; retain all per-trial ranges/tails.

All 120 trials across the two experiments validated IDs, payloads, notification
counts/order and raw-log length/hash before shutdown. Final evidence:
`.external/bench-results/uds-shared-client-readiness-20260920.jsonl` and matching
summary/run files. Negative initial results remain separately preserved. No
binary codec was selected or integrated by this experiment.

Common-client tests run both Python and native against the same disposable
supervisor: binary data, same PID on reconnect, wrong secret/instance, auto
selection and no post-connect fallback, stale writer/takeover, detach versus EOF,
oversized writes, idle-read cancellation, close wakeups, repeated-close FD counts,
lost ACK without retry and bounded slow-consumer failures. This is not exhaustive
fault/cancellation/hostile-wire coverage. Production packaging, free-threaded
binding acceptance, replay, launch/adoption and live TE2/ALS remain pending.

Final focused verification: 33 Python cases across client/session/framing/RPC/
codec controls completed (the initially skipped Rust framing-peer case was then
rerun successfully with its cached artifact); strict Python checks report zero
errors/warnings. Nine standalone supervisor Rust tests and warning-denied Clippy
for the core and PyO3 binding pass; Ferrous's three policy tests plus pipe-adoption
regression pass with the new reexport. Mirrored plans and canonical/vendor source
inventory match. Validation logs are retained beside benchmark evidence. These
are source/disposable-runtime checks, not an installed-package or live-framework
acceptance run. This experiment was subsequently checkpointed as described below;
existing package artifacts were preserved.

## Pushed Checkpoint And Replay Slice (2026-09-20)

Both `feature/uds-shells` snapshots were committed and pushed without version
changes: Python FWS `80d81b5ba6dfd6a1e80281dd54de1ba22e725d94` (0.0.64),
Ferrous `7db12fdbfc7ed18660ddcdcfbcae20144c3ead4f` (0.2.14). Native package
artifacts were excluded; no installation or live restart was performed.

The subsequent uncommitted slice implements explicit bounded raw replay in the
shared Rust service/client, optional PyO3 API v3 binding, and asyncio reference.
Python `SupervisorClient.connect(..., cursor=SupervisorCursor(stdout=..., stderr=...))`
and Rust `Client::connect_from(..., replay::Cursor { stdout, stderr })` take the
next consumed offsets for the authenticated instance. A fresh connection is
required; ordinary `connect` remains live-only. No writer lease is acquired.

The replay subscription and published stdout/stderr watermark are captured under
one short lock. No file/socket await holds that lock. Raw history is delivered
up to the watermark, followed by contiguous live output. Clients discard live
prelude before `replay_begin` and validate the final replay acknowledgement
before exposing history. Independent stream cursors preserve each stream, not
historical cross-stream chronology. Pre-boundary exit state is sticky.

Startup replay is capped at 1 MiB combined and a three-second service deadline;
normal 8192-byte frames and bounded queues remain. Future/oversized cursors,
log replacement/truncation, and output gaps fail rather than silently clipping.
There is no history paging endpoint yet. File identity/minimum-size checks do
not provide a snapshot against same-inode overwrite/truncate-regrow; external
log mutation is unsupported. Interrupted replay returns no successful client,
and reconnect remains the caller's explicit decision. Stdin is never replayed.

Tests use disposable real supervisors and unmodified `cat`, plus a binary
stdout/stderr child. They cover same PID, raw log equality, mid-chunk offsets,
concurrent output during replay, independent stream offsets, empty/equal-tail
replay, exact/over-limit budgets, replaced sources, and historical exit. Existing
live-only authentication/fencing/EOF/cancellation/fault and JSON-RPC lanes are
regression checks. No claim of manager-restart or installed/live acceptance.

Verification: 39 focused Python tests pass with both client implementations and
all real-process/framing/RPC artifacts supplied; no skips. Eleven standalone
supervisor Rust tests and five Ferrous policy/public-API/adoption checks pass.
Focused basedpyright reports zero errors/warnings; core and PyO3 binding Clippy
pass with warnings denied. Shared source inventory and mirrored plans match.
Evidence: `.external/bench-results/uds-replay-validation-final-20260920.log` and
`uds-replay-ferrous-validation-20260920.log`. An earlier broad test invocation
had two nonexistent module names; the corrected 39-case invocation above passed.
Only isolated release artifacts were built; no new performance claim or full
benchmark rerun. New replay changes remain uncommitted after the checkpoint.

Next: manager launch/adoption and persisted cursor ownership, shutdown executor
protection, then packaging/mixed-runtime acceptance. Production binary-envelope
selection and replay fault/saturation stress remain open; no production wire
format is frozen by this experimental JSON/base64 operation.

## Attachment Foundation (2026-09-20)

User approved implementing persisted identity/authenticated attachment before
changing normal manager launch or shutdown. This is a prerequisite, not automatic
adoption or a claim that `lifecycle.survive_shutdown` can now be enabled.

The standalone supervisor optionally accepts an `attachment` identity
(`id`, `label`, `spec_id`, `subgroups`) and `cwd`. Before readiness it publishes
`<socket>.attachment.json`: bounded signed identity, instance nonce, child and
supervisor PIDs, runtime ID, command/cwd/log paths. No environment map or secret
is stored. This is separate from canonical FWS `meta.json`; integration into
that store remains pending. Existing records/endpoints are never replaced or
automatically cleaned up to force a launch. Records remain after Stop as history.

Publication uses the existing ecosystem `tempfile` crate's no-clobber API. A
direct hard-link implementation failed with EACCES on this Android host; the
library's atomic no-replace path passes. Private owned directories and files,
no-follow bounded reads, domain-separated HMAC over exact UTF-8 body bytes,
strict schema/version and expected runtime checks are shared by Python/Rust.
Ancestor paths are trusted; this is not a hardened untrusted multi-user filesystem
or power-loss recovery guarantee. The signed JSON disk envelope is cold-path
discovery only and does not decide the future hot-path binary codec.

Python `supervisor_attachment.attach_supervisor` and Ferrous
`persistent_shell::attachment::attach` return descriptor/client/status after
authenticating the endpoint and matching expected shell ID, instance and PID.
Stale/unavailable endpoints and even correctly signed wrong PID/instance metadata
fail; exited instances remain explicitly exited. No implicit launch, input lease,
takeover, PID signaling or input replay. Optional cursors reuse the existing
bounded replay client. Python filesystem work runs off-loop; optional native I/O
remains selected by the existing API v3 client, without a new shellspec flag.

Shared descriptor fixtures cover types, version/runtime, path/identity validation,
and Unicode metadata. Filesystem checks cover permissions, symlinks, size caps
and no-clobber publication. Real supervisor tests cover labels/groups, read-only
attachment, explicit writer acquisition, replay, stale endpoints, and exit status.
A disposable Python launcher owns a live counter/nonce child, is killed with
SIGKILL, and Python then native clients attach with the same child PID/nonce and
incremented counter. This proves launcher-death reattachment, not graceful
framework teardown survival, nested protected trees or live TE2/ALS acceptance.

Final verification: 50 focused Python tests pass with both drivers and all
process/framing/RPC artifacts supplied, no skips or ResourceWarnings. Thirteen
shared supervisor Rust tests and five Ferrous public-API/policy/adoption checks
pass. Focused basedpyright is clean; core/binding Clippy pass with warnings denied.
Mirrored plan, source inventory and shared descriptor fixtures match. Evidence:
`.external/bench-results/uds-attachment-final-20260920.log` and
`uds-attachment-ferrous-20260920.log`; isolated artifacts are retained under
`.external/uds-native-attachment`. Earlier tests exposed the hard-link denial;
one prematurely launched Python run targeted its not-yet-built binary and failed,
then was rerun successfully after the build. Fixture setup now registers cleanup
immediately so setup failures do not leave temporary-directory warnings.
No performance claims, production install, live restart, version bump or commit;
previous replay edits, package artifacts and cached binaries remain intact.

Next: integrate discovery with canonical manager records and acquire-before-spawn
reconciliation, then identity-safe protected shutdown. Until those are connected
in both runtimes, keep the survival flag unavailable. Packaging/free-threaded
acceptance, live restarts and the production binary-envelope choice remain open.
