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
Config fields are `socket`, `stdout_log`, `stderr_log`, and `command` (argv array).
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
`replay:false`. Requests are serialized JSON objects with `op`:

- `status`: child PID, instance, exit/code and capture failure information.
- `acquire`: optional `takeover` (default false), returns a writer generation.
- `write`: `generation` and base64 `data`, maximum 32768 decoded bytes.
- `eof`: `generation`; permanently closes child stdin.
- `detach`: `generation`; releases ownership WITHOUT EOF.
- `stop`: signals the live child's process group, then shuts down the supervisor.

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
(or a timed-out transport closes). Reconnection DOES NOT replay missed output.
Capture failures publish a failure event and terminate the live child group
instead of silently discarding output. Disk retention/quotas remain future work.

## Explicit Limits

- No raw cursor replay, atomic replay/live boundary, or per-shell FWS metadata.
- No FWS launch, adoption, framework shutdown policy, install hook, or dashboard
  integration. Reconnecting clients is tested; whole framework restart survival
  and mixed-manager protected trees are NOT established by these tests.
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
The Python common API chooses native before connecting when API v2 is available,
otherwise uses a matching asyncio reference implementation. Neither retries or
switches implementation after connection. This is not manager integration.

The client has async connect/acquire/write/read/status/eof/detach/stop/close.
Use close to detach, not EOF; cancellation of a submitted write does not establish
non-delivery. There are 64 queued controls plus one active request and 256 output
events capped at 8192 bytes. Slow consumers fail explicitly instead of silently
losing output. One outstanding control ACK per connection is preserved. No replay
or automatic reconnect exists. Tests include real child reuse and injected lost
ACK/slow-consumer faults. Python binding build/install remains experimental and
outside production packaging hooks.
