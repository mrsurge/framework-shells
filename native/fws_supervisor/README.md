# Shared FWS Supervisor Prototype

Canonical source lives in Ferrous `crates/fws-supervisor`. Python FWS vendors
this directory at `native/fws_supervisor`; do not independently edit that copy.
Run Python FWS `scripts/sync_supervisor.py --check` to reject drift;
`--write` explicitly refreshes the vendored snapshot.

This is an experimental standalone binary and library, NOT a manager launch
backend. No installed FWS shell changes behavior. Do not enable the planned
shellspec survival flag yet. Packaging includes source but does not install or
select this binary automatically.

## Disposable Launch

Build with `cargo build --manifest-path crates/fws-supervisor/Cargo.toml` in
Ferrous. Run `fws-supervisor /private/path/config.json` with a nonempty
`FRAMEWORK_SHELLS_SECRET` environment variable. Never pass the secret in argv.
Required config fields are `socket`, `stdout_log`, `stderr_log`, and `command`
(argv array). Optional `cwd` sets the child's working directory; environment is
inherited as before. Optional `attachment` enables signed discovery, below.
The socket's parent must already be a private directory owned by the effective
user. The binary makes a new session, and launches its child in a separate
process group with ordinary stdin/stdout/stderr. It prints one readiness JSON
object containing socket, child PID and a fresh instance nonce.

Socket mode is 0600. Existing socket/file paths are never unlinked to make room.
Raw log paths are opened create-new; existing logs are never overwritten. Endpoint
cleanup checks inode/device before removing its own socket. Ancestor directories
and launch config are trusted caller inputs, not remotely supplied paths.

## Experimental Session V1

Framing: four-byte unsigned big-endian payload length, at most 65536 payload bytes.
Use Tokio LengthDelimitedCodec or Python asyncio readexactly. Partial/oversized
frames close the connection; no resynchronization or automatic retries.

Handshake objects:

1. Server sends `{version:1, instance, nonce}` (fresh 32-byte base64 nonce).
2. Client sends `{version:1, instance, nonce:client_nonce, proof}` with its own
   fresh 32-byte nonce.
3. Server verifies and sends the same proof-object shape with a server proof.

Proof is base64 HMAC-SHA256 over UTF-8 fields joined by a NUL byte:
`fws-supervisor-v1`, role (`client` or `server`), instance, server nonce, client
nonce. Each endpoint verifies the expected instance/version; the client also
checks its nonce was retained. The secret itself never travels on the socket.
Handshake deadline is three seconds; at most 16 sockets are served concurrently.
Python's `supervisor_session.connect` and Rust's `session::authenticate_client`
implement the same handshake. This has focused tests, not a security audit.

After authentication, server sends `event: attached` with `instance`, `pid`, and
`replay:true`. Requests are serialized JSON objects with `op`:

- `status`: child PID, instance, exit/code and capture failure information.
- `acquire`: optional `takeover` (default false), returns a writer generation.
- `write`: `generation` and base64 `data`, maximum 32768 decoded bytes.
- `eof`: `generation`; permanently closes child stdin.
- `detach`: `generation`; releases ownership WITHOUT EOF.
- `stop`: signals the live child's process group, then shuts down the supervisor.
- `resume`: first request on a fresh connection only; `cursor` contains next
  consumed byte offsets `stdout` and `stderr` for the authenticated instance.

Responses use `{ok:true,result:{...}}` or `{ok:false,error:...}`. One outstanding
request per client; no request-ID multiplexing in this experimental version.
Output/exit events may interleave with responses. A write ACK means accepted by
the OS pipe, not executed by the application. On failure/timeout, the error says
`delivery_uncertain`; lost ACKs also require application reconciliation, never
blind replay. Actual writes and lease transitions are serialized together.

Authenticated clients receive bounded `output` events with `stream`, raw byte
`offset`, and base64 `data`. Application bytes are untouched in raw log files.
Logging continues without clients. Three-second socket send deadlines prevent a
slow client from blocking child capture. Broadcast history is bounded to 64 chunks
of at most 8192 bytes each; a lagged receiver gets a `gap` event and disconnects
(or a timed-out transport closes). Ordinary reconnection stays live-only;
explicit cursor attachment replays bounded history as described below.
Capture failures publish a failure event and terminate the live child group
instead of silently discarding output. Disk retention/quotas remain future work.

## Bounded Replay

Rust callers use `Client::connect_from(..., replay::Cursor { stdout, stderr })`.
Ferrous reexports these as `persistent_shell::client::Client` and
`persistent_shell::ReplayCursor` without a separate supervisor dependency.
Python callers pass `cursor=SupervisorCursor(stdout=..., stderr=...)` to
`SupervisorClient.connect`. A cursor belongs to the supplied authenticated
instance, not to a pathname reused by a different child. Clients buffer replay
until its completion acknowledgement has been validated.

The supervisor subscribes and snapshots published log offsets under one short
lock. It sends `replay_begin` with `cursor` and `end`, reads the raw logs up to
those ends, then acknowledges `{resumed:true,end,replayed_bytes}` and continues
on the subscribed live lane. It preserves each stream's bytes and offsets;
replay does not reconstruct historical ordering between stdout and stderr.
Bytes arriving before the replay-begin marker are suppressed by resumed clients.
Reads never acquire the writer lease or replay stdin.

Combined startup replay is limited to 1 MiB and a three-second service deadline,
with 8192-byte output frames and the existing bounded client queues. Future offsets, oversized history, source
replacement/truncation, and live broadcast lag fail explicitly, never clip.
This is not an arbitrary-history paging API. Logs are untouched. Source checks
cover file identity and minimum size, not same-inode overwrites or truncate/regrow
races; external mutation is unsupported, not an atomic filesystem snapshot.
Reconnect remains explicit; no automatic retry occurs after failure.

## Experimental Attachment Records

To publish discovery metadata, add this to the standalone supervisor config:

```json
{"attachment":{"id":"fs_example","label":"app-worker:example","spec_id":"example:worker","subgroups":["example","worker"]}}
```

This is NOT a shellspec field or an enabled survival backend. All log/socket paths
must be absolute. The supervisor publishes `<socket>.attachment.json` before
readiness and includes its path in `attachment_file`. Records carry shell identity,
instance nonce, child/supervisor PIDs, command, cwd, log paths and runtime ID
(`sha256(secret)[:16]`), but no environment map or secret. Records survive Stop
as discovery/history, not as a claim that a child remains alive.

The record has a UTF-8 JSON `body` string and base64 `proof`, an HMAC-SHA256 of
`fws-supervisor-attachment-v1\0` plus the exact UTF-8 body bytes. This avoids
cross-language canonical-JSON differences. It is a cold-path disk envelope,
not a new application or hot-path transport. Unknown versions/fields, wrong
runtime/secret, oversized records (64 KiB), symlinks, non-regular files, and
non-private files/directories are rejected. Ancestor paths remain trusted inputs.
`tempfile::persist_noclobber` publishes a complete 0600 file without overwriting
an earlier instance; direct hard-link publication is denied on this Termux host.
This does not promise crash/power-loss durability or auto-recover stale endpoints.

Rust uses `attachment::attach(path, secret, expected_shell_id, cursor)`; Ferrous
reexports that module as `persistent_shell::attachment`. Python uses
`framework_shells.supervisor_attachment.attach_supervisor(...)` with the same
arguments and optional `cursor=SupervisorCursor(...)`. Python metadata parsing
stays on the cold path; the selected native/reference client still owns I/O.

Both return a descriptor, connected client and authenticated status snapshot.
They verify expected shell ID and live instance/PID before returning. A valid
signature or PID alone is insufficient. Exited children are returned as exited,
not silently relaunched; unavailable endpoints fail. Attachment does not acquire
or steal a writer, signal a PID, replay stdin, or change manager state. Close the
returned client in a `finally` block. This is the adoption foundation, not automatic
FWS manager discovery, reconciled launch, persisted consumer cursors or shutdown
protection. Applications must not treat the descriptor as PID-signal authority.

## Explicit Limits

- No per-shell FWS metadata or manager-owned cursor persistence.
- No FWS launch, adoption, framework shutdown policy, install hook, or dashboard
  integration. A disposable launcher-death test proves same child PID/nonce/counter
  with Python/native reattachment; graceful framework restart and mixed-manager
  protected trees are NOT established by that test.
- Stop currently uses SIGKILL for the live child process group. It is not the
  final graceful process-tree algorithm; escaped groups and grandchildren after
  leader exit still require the later lifecycle integration.
- Child exit stays queryable until explicit Stop; stale endpoint recovery after
  supervisor crash is not implemented. A supervisor failure is not resumable.
- Startup/capture fault matrices, slow-client saturation, log-full behavior,
  uncertain-write injection, signal cleanup and resource-leak stress need more
  acceptance coverage before production use.

## Tests

`cargo test --manifest-path crates/fws-supervisor/Cargo.toml` exercises framing,
mutual authentication, private endpoint handling and a real Rust client with an
unmodified `cat`. Python FWS `tests/test_supervisor_service.py` additionally uses
the binary selected by `FWS_SUPERVISOR_BINARY` for nonce/counter, takeover,
wrong-secret/instance/version, explicit EOF, reconnect, abrupt-client death, raw
binary logs and endpoint cleanup. Those Python process tests skip explicitly if
no binary is supplied. No test targets live TE2/ALS processes.

## Shared Async Client (Experimental)

`client::Client` owns outer UDS/auth/framing/JSON-base64 processing and exposes
opaque application bytes with stream offsets. Ferrous reexports it through
`persistent_shell::client`; Python's optional PyO3 binding wraps this same crate.
The Python common API chooses native before connecting when API v3 is available,
otherwise uses a matching asyncio reference implementation. Neither retries or
switches implementation after connection. This is not manager integration.

The client has async connect/acquire/write/read/status/eof/detach/stop/close.
Use close to detach, not EOF; cancellation of a submitted write does not establish
non-delivery. There are 64 queued controls plus one active request and 256 output
events capped at 8192 bytes. Slow consumers fail explicitly instead of silently
losing output. One outstanding control ACK per connection is preserved. Explicit
bounded cursor replay is available, but no automatic reconnect. Tests include
independent stream cursors, concurrent replay/live output, real child reuse and injected lost
ACK/slow-consumer faults. Python binding build/install remains experimental and
outside production packaging hooks.
