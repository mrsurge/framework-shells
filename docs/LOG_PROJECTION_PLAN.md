# FWS Log Projection And Codec Plan

Status: shared projection primitives implemented; consumer integration pending.
Working branch in both repositories: `feature/log-projection-codecs`.

## Scope And Paired Delivery

Python framework-shells and Rust ferrous-framework implement this together.
Every runtime milestone must include both implementations, matching wire DTOs,
shared expected-output fixtures, and tests before it is considered complete.
Neither variant may require the other language's runtime.

This document is mirrored at `docs/LOG_PROJECTION_PLAN.md` in both repositories.
Keep the copies identical. The FWS checkout currently holds Ferrous at
`.external/ferrous-framework`; that is an independent ignored Git checkout.
The older adoption tracker remains historical context; this is the active tracker
for this work. No commits, pushes, version bumps, or live restarts are implied.

## Reference And Unit Of Information

Reference: ALS at
`/data/data/com.termux/files/home/mrselect6/worktrees/agent_log_server`,
especially `rust/crates/als-server/src/conversation_store.rs`,
`transcript_stream.rs`, and the frontend `transcript_loader.ts`.

Reuse its indexed windows, tail/older/newer/current actions, revisions,
snapshot/delta recovery, and scroll-anchor preservation. ALS uses logical cards;
FWS uses source log lines. Pretty-printing one record into multiple display rows
does not change its identity or count. MessagePack uses complete decoded frames
as the corresponding logical records, not newline splitting of binary bytes.
Arbitrary pipe read chunks are never record boundaries.

Starting defaults for implementation review:
- 1,000 logical records per window, shifted by 250.
- 8 KiB maximum rendered content per record, measured as UTF-8 bytes.
- 1 MiB serialized response budget, including metadata; this may reduce the
  returned record count. Return actual bounds, not requested bounds.
- Independent bounded frontend state and DOM, including paused/live views.

These are initial engineering defaults, not benchmark conclusions. Avoid adding
a collection of public tuning flags before measuring representative workloads.

## Raw Data And Projection Boundary

Keep raw stdout/stderr bytes authoritative and unchanged. Stdin remains opt-in
I/O sidecar data only. The projection may omit visible content with explicit
metadata, but the original remains retrievable by generation and byte range.
Window eviction does not delete raw logs. Automatic disk retention/rotation is
a separate policy decision and is outside the first implementation slice.

Use stable record identity derived from shell, stream, generation, and starting
byte offset. Carry end offsets and partial-record state. Increment generation
on explicit truncate/reset/replacement; stale requests get a reset/expired
response rather than reading an unrelated byte range. Specify replacement and
same-size rewrite detection during contract work.

Indexes must also have a bounded memory policy: paged/on-disk offsets or sparse
checkpoints with bounded scans, plus capped caches. An offset vector growing
forever merely moves the memory problem from browser to manager.

Historical rendering must restore needed ANSI state at window boundaries.
I/O overlays use recorded offsets/timestamps where available; do not fabricate
per-line timestamps or ordering across stdout and stderr.

## Oversized Lines And Structured Omission

First measure the raw line size in bytes. Ordinary text/JSON lines within budget
take the inexpensive existing rendering path. For an oversized declared JSON
record, parse the complete frame within explicit parsing resource limits and
compact its display representation. Pretty whitespace alone may explain size.

If it is still too large, remove or shorten the largest contributing values,
preserving useful envelope/context fields where possible. Use a deterministic
policy shared across Python and Rust. Handle nested strings, arrays, objects,
many small fields, giant keys, top-level scalars, and multibyte text.
Do not assume a single giant string caused the excess.

Describe every omission separately from the original payload, with:
- JSON Pointer (escaped correctly), original type, and original byte extent/size
  when known; distinguish source bytes from reserialized bytes.
- Omission reason and any preview boundaries.
- Raw record reference for bounded retrieval/expansion.

Use projection-specific placeholders plus side metadata so a placeholder cannot
be mistaken for child-emitted data. Do not silently delete JSON-RPC id, method,
or error status. If envelope fields themselves exceed budget, show an explicit
summary linked to the original. Never invent plausible replacement values.

A full parse of an enormous frame can allocate far more than 8 KiB. Specify a
separate parser input/depth/resource ceiling. Above that ceiling, malformed JSON,
or unsupported shapes produce a bounded UTF-8-safe preview and diagnostic with
a raw reference. Giant unterminated lines must not accumulate unbounded buffers.
Expansion is paged and byte-limited, never an unrestricted full-record fetch.

Omission is lossy in the visible projection. Observability remains recoverable
because the source is retained and addressable; do not describe the shortened
view itself as lossless.

## Window And Live Protocol

Define typed requests for tail/older/newer/current and bounded raw expansion.
Responses carry generation, revision, record identities, actual cursor bounds,
available tail, at-start/at-tail flags, and explicit omission metadata.

Snapshot/live handoff must have a defined sequence boundary, deduplication, and
gap recovery. While viewing history, preserve the window and report new output
without retaining every incoming payload. Tail followers receive bounded deltas.
Pause, reconnect, slow subscribers, reset, and manager restart all have defined
recovery behavior. Updates stay event-driven; do not add polling.

Historical filter/search must operate on backend source records before display
omission and must expose its search scope. Otherwise an omitted value could
disappear from inspection results. Bound search work/results and pagination.

The owner manager observes bytes once. Projectors never compete with the actual
pipe/PTY consumer for the descriptor. Specify file visibility at publication
boundaries so buffered log writes cannot create a gap in window retrieval.

## MessagePack Observation

Persist an explicit per-stream observation codec/framing declaration through
shellspec rendering, record storage, peer metadata, and inspection. JSON content,
MessagePack encoding, and dashboard transport serialization are distinct choices.
A shell codec does not silently reconfigure Socket.IO for every browser/peer.

Use established MessagePack libraries. Decode incrementally across arbitrary
chunk boundaries and handle multiple frames per chunk. Specify concatenated
object framing first; other framing requires an explicit contract.
Indexed frame boundaries support random window access.

Return a common readable projection to dashboard, CLI, REST, and MCP. Define
representations for binary/extension values, non-string map keys, large integers,
and other values JSON cannot faithfully represent. Invalid/truncated frames
report exact source context where possible; do not guess a resynchronization
boundary. Preserve raw bytes through peer paths before any UTF-8 decoding.

## UDS Control Plane And Persistent Shells

Implementation follows projection/codecs. Planning now has a concrete consumer:
TE2 launches ALS as an app worker; TE2 must be restartable while ALS, its active
conversation, extension adapter, and extension processes remain alive.

### Two Independent Capabilities

UDS manager control transport preserves symmetric Python/Rust operation,
shared-secret authorization, lifecycle events, request/ack semantics, and
subscription recovery. It removes local TCP port/binding requirements but does
not itself guarantee delivery, recovery, or process survival. Specify endpoint
discovery and controller restart independently of shell supervisor sockets.

Persistent shell I/O uses one supervisor process and one UDS endpoint per shell
instance. The supervisor launches an ordinary stdio child and owns its stdin,
stdout, stderr, log capture, and exit observation. Managers connect through that
endpoint using existing manager-shaped operations. The child requires no socket
code or knowledge of the supervisor protocol.

Topology: manager <-> authenticated UDS supervisor <-> child stdio.

Keep ordinary direct proc/pipe/PTY behavior available. Initial persistent support
targets stdio workers; do not imply every shell/session is resumable or extend
PTY semantics without a separately tested contract. FWS resumes communication;
application protocol state recovery remains the caller's responsibility.

### Existing Adoption And Restored Capabilities

Current adoption can recover persisted identity and observe process liveness,
but cannot recreate a departed manager's pipe descriptors. Confirm both variants'
exact capability/error behavior in characterization tests before changing it.

For persistent shells, authenticate the supervisor, verify its instance identity
and actual child state, then acquire a live attachment. Only then advertise live
input/output/termination capabilities. Persisted metadata or socket existence is
insufficient. A dead child yields an exited record, not a successful attachment.

Preserve shell ID, spec/instance identity, labels, subgroups, app-worker metadata,
runtime identity, and original launch provenance. Record the new attachment
separately instead of relaunching or overwriting the old shell's birth identity.
The secret authorizes access; it is not the shell's instance identity.
Multiple instances of the same spec need explicit disambiguation.

Launch reconciliation must acquire the intended live instance before spawning.
Define atomic acquire-or-create behavior so simultaneous managers cannot duplicate
an app worker. FWS returns the recovered shell; TE2 restores its app registry,
proxy destination, and application connections using that identity and metadata.
An old endpoint value is verified before TE2 resumes routing.

### Explicit Survival Policy

Add an opt-in shellspec survival field; proposed spelling:
`lifecycle.survive_shutdown: "${env:FWS_SURVIVE_SHUTDOWN}"`.
Final schema spelling is a contract task, not an implemented option.
It must support the normal ctx/env render pipeline and persisted resolved policy.
Default is false. Parse rendered booleans strictly: string "false" must not enable
survival through generic truthiness. Specify missing/invalid values explicitly.

Normal framework shutdown preserves a survival-enabled shell's supervisor and
entire descendant subtree, including mixed Python/Ferrous managers and unmanaged
grandchildren. The supervisor must be outside the launching framework's shutdown
process group/session. Tree/group selection must honor protected boundaries
before sending broad group signals. Independent owner managers must not undo
that protection by terminating their own children during framework teardown.

An explicit stop targeting the persistent shell terminates its supervised tree
and removes its live endpoint after orderly cleanup. Distinguish framework
teardown from explicit user shell/group termination in typed shutdown intent;
finalize group scope and nested survival-boundary rules before implementation.
Do not infer user intent from an ambiguous old scope string.

Shutdown results report preserved instances, affected targets, and failures.
SIGTERM/Ctrl-C paths and abrupt manager death are separate cases. No promise of
survival across machine reboot, OS-wide termination, or supervisor failure.

### Attachment, Delivery, And Disconnected Output

Multiple authenticated observers may attach. Default to one stdin owner with an
explicit acquisition/release/takeover contract. Fence stale writers using an
attachment generation/token checked at the supervisor, so a delayed write from
an old connection cannot execute after ownership changes.

Manager disconnect does not send child EOF. Explicit stdin EOF retains existing
semantics and cannot be reversed by reattachment. A graceful attachment detach
also differs from explicit shell stop.

The supervisor continuously drains/logs stdout and stderr while no manager is
attached. Use bounded queues and log cursors; do not retain all disconnected
output in RAM. Specify disk-full, write failure, and output pressure behavior,
with visible failure or backpressure rather than silent data loss.
Unchanged raw logs and opt-in stdin sidecars remain the observation contract.

Reattachment exchanges instance generation, current state, and stream cursors.
Define an atomic replay/live handoff and explicit gaps/expired cursors.
Do not automatically replay uncertain stdin: an acknowledgement lost after a
successful write does not prove the command was unexecuted. Report delivery
uncertainty; application-level request IDs/reconciliation handle recovery.

Use established UDS/process/codec libraries. Keep the supervisor small and
protocol-neutral; it still needs versioned framing, authentication, resource
limits, and fault handling. Share one Rust supervisor implementation between
Python FWS and Ferrous where packaging permits. Specify artifact installation and
an explicit unsupported/unavailable error; never silently ignore survival intent.

Validate private endpoint permissions, instance identity, stale socket cleanup,
and compatible protocol versions. A restarted manager may attach to a different
supervisor build, so unsupported capabilities must fail explicitly.

### Persistent-Shell Test Ideas

Use a disposable Rust test child that writes a boot nonce, increments an in-memory
counter for commands, emits sequenced raw output on both streams, and spawns a
grandchild with its own nonce. This makes survival distinguishable from restart:
PID checks alone can be fooled by PID reuse. Include a trivial unmodified stdio
program to prove the child needs no supervisor-specific library.

Use isolated runtime directories/secrets and test-owned process trees only.
Synchronize through explicit readiness/barriers and bounded deadlines, not
arbitrary sleeps. Never kill/restart the live TE2/ALS harness during these tests.

| Scenario | Proof required |
| --- | --- |
| Graceful manager restart | Child/grandchild boot nonces and counter remain; same shell ID/labels; resumed command advances existing counter |
| Abrupt manager death | Kill only the disposable manager; supervisor and descendants survive; fresh manager restores communication |
| Ordinary-shell control | Existing shutdown behavior unchanged with survival false; no new supervisor/socket overhead on the direct path |
| Mixed protected tree | Persistent and ordinary siblings, nested managers, and grandchildren; teardown removes only intended trees |
| Explicit stop/EOF | Stop kills persistent tree and persists exit; EOF remains EOF after reattach; disconnect alone never becomes EOF |
| Two-manager adoption race | Both discover one instance; one launch only; one writer owner; observers both receive output |
| Fenced stale writer | Hold an old connection's input behind a barrier, transfer ownership, release it; stale command is rejected without incrementing counter |
| Lost write acknowledgement | Child executes increment; sever ack delivery; reconnect does not execute it twice and reports uncertainty |
| Detached output saturation | Produce more than pipe and subscriber buffer capacities; bounded memory, continued progress within declared storage policy, exact replay hashes |
| Replay/live boundary | Emit sequence at the attachment barrier; every retained byte delivered once or an explicit detectable gap, no silent loss/duplication |
| Misleading metadata | Wrong secret, stale socket, reused PID, wrong instance nonce, incompatible supervisor; no false live adoption |
| Child exits while detached | Accurate exit status/event on adoption; no false running state or automatic duplicate launch |
| Supervisor fails | Kill disposable supervisor; report lost transport and actual child state; do not claim resumability survived |
| Endpoint rotation | Restart controller; reconnect manager control lane while shell endpoint stays stable; lifecycle/log notifications recover without polling |
| Resource failure | Simulated log write/disk-full errors and slow observers follow declared policy; healthy observers/control remain responsive |
| Repeated cycles | Many attach/detach/restart cycles without FD/socket/task leaks; explicit final stop cleans test-owned resources |

Run the same supervisor scenarios from Python and Rust clients, including
Python->Rust and Rust->Python attachment across manager replacement. Exercise
both ordinary JSONL and opaque/binary payloads; wire bytes must remain exact.
Use recorded event traces and fixture expectations to compare capabilities,
shutdown results, lifecycle notifications, and byte-range replay across variants.

TE2/ALS acceptance proceeds in two stages:
1. Disposable app-worker fixture with ALS-shaped labels, proxy metadata, and a
   mixed descendant tree: restart the framework, rediscover the same worker,
   restore routing and event subscriptions, and verify no duplicate launch.
2. Explicitly scheduled live integration: restart TE2 with survival enabled for
   ALS, keep the ALS conversation and extension descendants alive, then verify
   proxy/dashboard/inspection and continued application operations. This live
   harness restart requires separate user authorization.

## Semantic Test Contract

Before runtime changes, create the same versioned golden fixtures in Python
`tests/fixtures/log_projection_cases.json` and Rust
`testdata/log_projection_cases.json`, with exact expected projection DTOs.
Use typed Python tests under the strict checker and typed Rust tests.
Verify fixture equality and normalized output equality; each implementation
must not merely compare against outputs generated by itself.

Required cases:
- Empty logs, LF/CRLF, no final newline, partial writes, invalid UTF-8, ANSI state.
- Record/count/byte budgets; arbitrary chunk splitting must yield the same result.
- Older/newer/current/tail, stable anchors, independent clients, paused views.
- Truncate/replacement/restart, stale cursors, duplicate events, sequence gaps,
  subscribe/snapshot races, slow-client overflow and bounded resynchronization.
- Small JSON fast path; oversize whitespace; large string/array/object; many
  small fields; giant keys/scalars; multibyte limits; malformed/deep/huge input.
- Omission pointers and exact original-byte retrieval, JSON-RPC context, filter
  matching on hidden values, bounded expansion and no raw-log modification.
- MessagePack chunk splits/coalescing, incomplete/corrupt frames, binary/ext,
  numeric fidelity and frame-index seeking.
- stdin gating and sidecar-only capture; stdout/stderr separation.
- proc/pipe/PTY, Python fallback/native pipe, Rust blocking/async readers;
  projection never steals bytes or changes write/EOF/lifecycle semantics.
- Python controller/Rust peer and reverse, plus same-variant peers; matching
  dashboard, CLI inspect, and MCP-visible results.
- Browser state/DOM bound over sustained output, scroll stability with Pretty
  JSON/filter/ANSI/overlay features and repeated reconnects.

Measure baseline and after-change memory, window seek latency, time to open
large logs, and pipe RTT/concurrent throughput with the existing Rust producer.
Test subscribed and unsubscribed paths. Keep cached artifacts. Establish
measured tolerances before using performance as a release gate.

## Tracker

| Slice | Python FWS | Ferrous | Shared evidence |
| --- | --- | --- | --- |
| Matching branches and mirrored plan | Complete | Complete | Clean baseline; no runtime edits |
| Typed DTOs, defaults, golden fixture contract | In progress | In progress | Raw reference/omission/record DTOs, window actions, shared initial fixtures pass |
| Indexed bounded line windows and raw retrieval | Pending | Pending | Seek/reset/limit tests |
| Oversized structured-value projection | In progress | In progress | Nested field omission, UTF-8 limits, compact JSON and parse-size ceiling tested |
| Event-driven windows and frontend integration | Pending | Pending | Race/gap/DOM/peer tests |
| MessagePack metadata, frame index and decoding | Pending | Pending | Cross-codec golden cases |
| CLI/REST/MCP projection integration | Pending | Pending | Consumer parity |
| Resource/performance and mixed-runtime verification | Pending | Pending | Baseline comparison |
| UDS peer control transport | Planned, after codecs | Planned, after codecs | Symmetric reconnect/auth/event contract |
| Persistent shell/survival contract and fixtures | Planned, after codecs | Planned, after codecs | Typed policy, ownership and shutdown intent |
| Shared supervisor and manager attachment | Planned, after codecs | Planned, after codecs | Nonce/counter, fault and cross-variant tests |
| TE2 app-worker recovery | Planned, after codecs | Planned, after codecs | Disposable fixture first; authorized live ALS acceptance |

## Implementation Checkpoint

Both implementations now expose pure record projection and window-start selection
in `framework_shells/log_projection.py` and Ferrous `src/log_projection.rs`.
The shared fixtures cover small text, compacting oversized whitespace, large
top-level and nested fields, JSON Pointer escaping, multibyte previews, and window
navigation. Focused Python tests and strict typing pass; Rust integration tests
pass against the identical fixtures.

These primitives are not wired into managers, the dashboard, CLI, or MCP yet.
MessagePack is a declared codec with an explicit unsupported-decoder error until
the frame decoder lands; it is not advertised as a working shellspec option.
Raw references currently describe bytes supplied by the caller; generation
validation and original-byte retrieval await the file/index layer.

Structured omission removes the largest candidate nested field values first,
with byte-size ties ordered by JSON Pointer. It preserves root jsonrpc/id/method
and error.code, and records omissions outside the projected JSON. Arrays are
currently whole-value candidates. Input beyond the parser byte ceiling yields an
explicit preview diagnostic. Preview decoding is bounded before JSON parsing.

Still required before integration: align parser depth/numeric behavior across
languages, bound omission metadata as part of the full response budget, expand
golden fixtures for pathological input, implement indexed windows/raw expansion,
and implement MessagePack framing and normalization. The current record budget
limits rendered text; it is not yet a total serialized-response budget.

Next slice: finish strict whole-response/parser contracts, then build matching
indexed line-window and frame-decoder paths in both repositories.
