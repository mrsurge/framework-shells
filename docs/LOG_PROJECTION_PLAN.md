# FWS Log Projection And Codec Plan

Status: paired manager/REST/dashboard integration implemented; acceptance hardening in progress.
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
- 200 logical records per dashboard window, extending by up to 50 per shift.
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
| Indexed bounded line windows and raw retrieval | In progress | In progress | Disk offset index, append/reset/raw/whole-response budget tests pass; bounded manager caches and REST window/raw routes implemented |
| Oversized structured-value projection | In progress | In progress | Nested field omission, UTF-8 limits, compact JSON and parse-size ceiling tested |
| Event-driven windows and frontend integration | In progress | In progress | Shared dashboard uses event-triggered windows, paging and bounded raw pages; DOM/race stress pending |
| MessagePack metadata, frame index and decoding | In progress | In progress | Shared frame fixtures and indexed-frame tests pass; shellspec/record/peer metadata and Python inspect decoding implemented |
| CLI/REST/MCP projection integration | In progress | In progress | Matching window/raw REST routes; Python inspect detects codec from either runtime; external consumer smoke pending |
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
MessagePack has a bounded complete-frame decoder and indexed-frame reader.
It is not yet advertised as a working shellspec option. Both index implementations
support original-byte retrieval with generation checks.

Structured omission removes the largest candidate nested field values first,
with byte-size ties ordered by JSON Pointer. It preserves root jsonrpc/id/method
and error.code, and records omissions outside the projected JSON. Arrays are
currently whole-value candidates. Input beyond the parser byte ceiling yields an
explicit preview diagnostic. Preview decoding is bounded before JSON parsing.

The new window layer caps the entire serialized response, including omission
metadata, and returns actual selected bounds. Tail selection retains the newest
records when the byte budget is tighter than the record budget. A budget too
small for even one projected record produces an explicit error. Omission count
is capped at 128 and individual omission pointers at 1,024 UTF-8 bytes.

### Indexed File Layer

Python `log_window.LineIndex` and Rust `log_window::LineIndex` keep 8-byte record
end offsets in an owned temporary file, scan source growth in 64 KiB chunks,
and read only selected records. They expose bounded raw retrieval (64 KiB per
call), window selection, and a pending-byte count. JSON/text use LF boundaries
and preserve CRLF bytes. MessagePack indexes complete objects, holding incomplete
suffixes under a 1 MiB frame ceiling. Corrupt streams fail explicitly; a failed
scan invalidates partial index state before retry.

These synchronous primitives require an I/O executor in async hosts. Each object
owns its scratch index and generation; a fresh object requires client resync.
The host must cap/evict its index cache and close/drop evicted handles. Truncation,
replacement, and detectable same-size modification reset generation. A truncate
followed by regrowth beyond the prior length between observations cannot be
distinguished from append by file metadata alone: controlled writers must call
invalidate on reset and propagate generation/reset events across managers.
Arbitrary in-place log editing is outside the append-only writer contract.

Python and Rust now also provide a bounded `IndexCache` with least-recently-used
eviction, keyed by absolute source path and codec. Eviction closes/drops the
temporary index, never the source log. Revisiting an evicted entry creates a new
generation; old cursors and raw references require resync. Explicit path reset
invalidates all cached codec views, even when source metadata has not changed.
Python holds a cache lock for the entire context-managed borrow; Rust requires
an exclusive mutable borrow, with host synchronization still to be wired. These
operations remain blocking-executor work, not async reactor work. The Python
default capacity is 32; Rust takes an explicit capacity. Caches are not yet
attached to managers. Focused paired tests cover eviction ordering, retained
references, stale references, path-wide reset, invalid capacity, and preservation
of raw source files.

### MessagePack Representation

Decode concatenated objects with established libraries (Python msgpack; Rust
rmpv plus rmp-serde strict validation). Preserve original byte references even
when rendering JSON. Objects with unique string keys display as JSON objects;
duplicate/non-string keys or a reserved `$fws` key use a tagged entries list.
Binary/extension payloads display as hex, large integers as tagged decimal
strings, nonfinite floats as tagged IEEE-754 hex, and timestamps as seconds
strings plus nanoseconds. These tags are observation data, not child packets.

Shared hex fixtures include every possible split point for complete frames,
concatenated frames, reserved markers nested inside containers, extension/binary
data, duplicate/non-string/reserved keys, large integers and alternate timestamp
encodings. rmpv maps reserved marker 0xc1 to nil by default; strict rmp-serde
validation prevents that semantic corruption without rejecting legitimate 0xc1
bytes inside binary values. Paired file tests cover partial-frame append,
exact original-byte retrieval, and repeated corrupt-stream failure.

Checkpoint pushed without version changes: FWS 7363e34, Ferrous de04789.
Work after that checkpoint remains uncommitted.

Next slice: acceptance hardening for historical ANSI checkpoints, full-source filtering,
index recovery races, explicit peer gap/revision handling, and measured resource/performance
limits. Manager/host/dashboard integration is implemented, not yet live-validated.

### Consumer Integration Checkpoint

- Shellspec/record field: `log_codecs: {stdout: messagepack, stderr: text}`.
  `text`, `json`, `messagepack` are validated per stream after ctx/env rendering.
  Missing fields preserve text behavior; empty metadata does not alter old Python signatures.
- Managers own bounded 32-entry caches. File work runs through Python to_thread or
  Rust host spawn_blocking. Projection reads never drain a child descriptor.
- GET `/api/framework_shells/logs/{shell_id}/window` accepts stream, action,
  current, count, shift, generation. GET `/raw` accepts stream, generation,
  byte_start, byte_end, offset, limit and returns hex, next_offset, eof.
  Stale generations return HTTP 409. Paths resolve only from shell metadata.
  Reserve 64 bytes of the response budget for the REST envelope.
- Explicit resets write an atomic sibling `<log>.fws-reset` nonce, shared by
  both runtimes. Other caches detect it even after truncate-and-regrow. These
  markers are removed with purged logs. Arbitrary external truncation/regrowth
  without the marker remains outside the coordinated-reset contract.
- Dashboard opens a projection subscription before requesting initial windows.
  The internal `projection: true` open parameter suppresses legacy text backlog;
  ordinary event delivery stays compatible. Incoming log events coalesce behind
  at most one in-flight window request per stream, with no polling timer.
  Paused/history views retain their window and mark new output pending.
- Both dashboards ship identical rebuilt assets: source-record navigation,
  bounded record state/DOM, byte-offset anchors, explicit omission diagnostics,
  one visible 64 KiB original-byte page, and bounded recent sidecar overlays.
  Pretty JSON falls back to compact display when formatting exceeds 8 KiB.
- Python fallback output writes are unbuffered before publication, matching native
  File write visibility. A disposable child test checks that a log event's bytes
  are retrievable before the child exits. Native pipe read/write ownership is unchanged.
- Python inspect decodes declared MessagePack objects before its existing filters,
  preserving binary source offsets. Oversized inspection frames/normalized JSON
  return an explicit budget error directing callers to window/raw retrieval.
  CLI and downstream tools using manager inspect inherit this behavior.

Remaining limitations: dashboard filters explicitly cover displayed records, not
hidden/omitted values or all history. Historical ANSI carry-in checkpoints are not
implemented. Sidecar records lack a reliable cross-stream ordering key and are
shown as bounded recent metadata in tail views, not fabricated per-line timestamps.
Window requests have generation/offset identity but not a peer sequence/revision
protocol; dropped-event recovery and full DOM/reconnect stress remain acceptance
work. Python and Rust JSON numeric/depth corner parity and performance baselines
remain open. No live TE2/ALS restart, install, version bump, commit, or push.

Validation for this integration checkpoint: Python unittest discovery 28 passed;
full Ferrous cargo test 76 passed, 2 ignored benchmark probes; focused strict
Python checks and UI typecheck clean; 3 frontend request tests passed. Shared
fixtures, plans, and shipped JS/CSS match. No live browser validation or
performance baseline was performed.

### Paging And Snapshot Hardening

The indexed window API uses actual record boundaries, not the requested count,
which is only a ceiling and can be reduced by the response byte budget:

- `tail`: return the newest bounded records.
- `current`: start at `current`, without clamping back by the requested count.
- `older`: `current` is the previous window's exclusive start boundary; return
  preceding records, nearest first during selection and ascending in the response.
- `newer`: `current` is the previous window's exclusive end boundary; return
  subsequent records in ascending order.
- Older/newer select at most `min(count, shift)` records. They are adjacent pages,
  not overlapping fixed-size shifted windows. Clients must use returned start/end.

Both implementations recheck pathname identity, reset nonce, size and same-size
mtime after indexing, reads, and window assembly (including empty windows).
Replacement/reset is rejected rather than returned under an old generation;
ordinary append beyond the captured size is allowed. This is bounded snapshot
validation, not an atomic filesystem snapshot or arbitrary rewrite detector.
The coordinated reset marker remains required for truncate-and-regrow guarantees.

Paired text/MessagePack tests traverse byte-limited history in both directions
without gaps and retain the current cursor. Mutation coverage checks replacement,
reset, append, and recovery. Frontend Newer uses the returned end cursor. Late
request failures from a previous drawer are ignored, and queued navigation takes
precedence over an in-flight tail response. DOM/deferred-request stress remains
pending; no live worker was restarted or tested for this slice.

### Sliding Dashboard Viewport (2026-09-17)

Implemented after ALS source comparison. The backend's tested adjacent-page
contract stays intact; the shared browser merges adjoining slices into a
200-record window, evicting the opposite edge. Typical shifts add 50 records;
byte-budget-shortened pages use their actual bounds. Newer reads refresh the
previous boundary record before extending it, preserving growing partial text
lines. Text and MessagePack share record identity and navigation semantics.
Python manager/index/REST defaults and Ferrous REST defaults are 200/50. Explicit
API counts up to 1000 remain supported for non-dashboard consumers.

Scroll direction and proximity trigger bounded fetches, with one request per
stream in flight. Overlap retains visible byte-offset anchors; browser measurements
restore pixel offset across replacement, pretty-print changes and viewport resize.
Programmatic scrolls cannot trigger a fetch cascade. This is a bounded sliding
buffer, not a full-history spacer or a global pixel-height estimate. Rows within
that buffer remain mounted; there is no separate offscreen-row parking layer.

Pinned mode follows output. User scroll-away detaches; incoming events mark new
output available without modifying the historical view or accumulating chunks.
Jump to live replaces the window and pins to its end. Reset events invalidate
in-flight work per stream and restore the new generation. There are no polling
timers. Existing explicit pause is still respected.

Record content retains the 8 KiB preview/structured omission contract. This slice
initially introduced a 12rem row height ceiling; the subsequent pane-layout slice
below removes it at the user's request. The source-record buffer is at
most 200; the existing IO overlay separately retains at most 128 recent metadata
records. No spacer represents off-window history. Original-byte buttons and hex
panels are removed; raw retrieval API and its tests remain. Header navigation
uses existing .btn/.btn-small classes and theme variables.

Validation: 31 Python tests, 79 Rust tests (2 benchmark probes ignored), 11 Node
frontend tests; focused Python strict checks and UI typecheck pass. Node tests
cover contiguous overlapping traversal, short pages, partial-line refresh,
generation isolation, live snap and scroll direction/busy guards. These are not
browser pixel-layout tests. Live mobile/desktop anchor/resize/pretty-print and
rapid user-interaction acceptance remains outstanding. No install/restart,
version change, commit or push in this slice. User-confirmed .314t build artifacts
were preserved untouched.

### Collapsible Pane Layout (2026-09-17)

The shared dashboard now has stacked STDIN/STDOUT/STDERR panes. STDOUT alone is
expanded on first use. Clicking a header/title toggles its corresponding content;
filters, checkboxes and navigation buttons do not toggle it. There are no twisties.
Expanded panes share available height equally unless the user has resized that
combination. Horizontal pointer/touch and keyboard-accessible separators persist
sizes per shell and expanded combination in `fws.log.panes.v1.<shell_id>`.
All panes may be collapsed. A one-time hint explains header and divider behavior.

STDIN uses the existing input capability/backend-attempt eligibility, not a new
shellspec flag or debug.io_metadata requirement. Ineligible STDIN forms are
removed from the DOM; the detached element retains its handlers for later
eligible shells. Existing cross-manager write-attempt semantics are unchanged.

The 12rem per-record/IO row cap and nested record scrolling have been removed.
The 200-record window and byte-budget/atomic-value omissions remain, but pixel
height is no longer capped per record. Subtle borders/alternating tint identify
record boundaries. Wrap is globally shared across the drawer streams/composer,
on by default, and saved as `fws.log.wrap`. Navigation now spans the full header:
buttons on the right, record-range information across its bottom row.

Verification: strict UI typecheck, 16 Node projection/layout tests, and an isolated
browser-frame probe through TE2 console eval using current checkout HTML/CSS and
the compiled layout module. The browser probe checks default collapse, equal two/
three-pane geometry, keyboard resize/persistence, input DOM removal, controls not
collapsing headers, uncapped record height, shared wrap, and a 360px-wide layout
with full-width navigation/no document overflow. The probe restores its storage
and removes its frame. `tests/log_pane_layout.browser.js` accepts
`window.__paneLayoutSource = {html, css, script}`; script is an esbuild IIFE of
log_pane_layout.ts with globalName PaneLayoutPreview. It runs without starting
Socket.IO or sending stdin. Real touch-drag and full deployed streaming interaction
remain user acceptance checks. No manager restart, install, version, commit/push;
only a dashboard client refresh was performed for inspection.
