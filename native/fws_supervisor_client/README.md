# Experimental Python Binding For The Shared Supervisor Client

This optional extension wraps `fws-supervisor::client::Client`. Ferrous uses that
same asynchronous Rust client directly through `persistent_shell::client`.
Python uses `framework_shells.supervisor_client.SupervisorClient`:

```python
client = await SupervisorClient.connect(socket_path, secret_bytes, instance_id)
try:
    print(client.backend)  # native when API v3 binding is importable, else python
    await client.acquire()
    await client.write(application_bytes)
    event = await client.read()  # stream, offset, raw data; or exited/code
finally:
    await client.close()  # Detach the connection, not stdin EOF or child Stop.
```

The client does not launch children or discover endpoints. Connection details
come from the experimental supervisor startup record. It does not parse the
application payload, including JSON-RPC. API selection happens before connecting;
failed auth, writes or reads never switch implementations or replay input.
An incompatible/unavailable native API selects Python only before connection.
Explicit `backend="native"` or `"python"` exists for parity tests and benchmarks,
not as a new shellspec flag.

The Rust client owns authentication, frame/envelope codecs, ACK processing and
writer leases. PyO3 exposes awaitables using pyo3-async-runtimes, with two runtime
worker threads; it never nests `block_on` in a caller's Tokio runtime. Native I/O
waits do not hold the GIL. The Python reference uses asyncio and stdlib JSON,
without optional codec dependencies. `msgspec` remains application-side in the
benchmark. Current wire is unchanged JSON/base64; binary schema selection is
still separate work.

Both APIs have `acquire(takeover=False)`, `write(bytes)`, `read()`, `status()`,
`eof()`, `detach()`, `stop()` and `close()`. Async cancellation does not promise
that a previously submitted write was not delivered. Reconcile uncertain writes
at application level; never retry blindly. Active cancelled commands consume
their ACK before another is issued; cancelled queued commands may be skipped.
A write ACK means kernel-pipe acceptance, not application execution. Always close
clients in `finally`. Reconnect is explicit. For bounded raw output replay, pass
`cursor=SupervisorCursor(stdout=next_stdout_byte, stderr=next_stderr_byte)` to
`connect`, retaining the same authenticated instance ID. Replay and subsequent
live output are contiguous per stream; historical inter-stream order is not
reconstructed. Startup replay is capped at 1 MiB combined and rejected if too
large, beyond the log end, or unavailable. There is no clipping or stdin replay,
and no writer lease is acquired by resuming. Default connections stay live-only.

There are 64 queued controls plus one active command and 256 output events of at
most 8192 bytes each. Full control queues reject new commands (`busy`, not sent).
Output overflow/gaps close the connection with an explicit error (`lagged`), not
silent dropping or unlimited buffering. One outstanding control request on the
wire preserves the existing supervisor contract. Initial output offsets may be
nonzero on reattachment; subsequent offsets must be contiguous per stream.

## Isolated Build

For a regular CPython interpreter, build without installing or changing FWS:

```sh
PYO3_BUILD_EXTENSION_MODULE=1 PYO3_PYTHON=/path/to/venv/bin/python \
  cargo build --release --manifest-path native/fws_supervisor_client/Cargo.toml
```

Stage `libfws_supervisor_client.so` from Cargo's resolved target directory as
`.external/uds-native/fws_supervisor_client.so`. Test/benchmark loaders accept that
exact path via `FWS_SUPERVISOR_CLIENT_EXTENSION`; no artifact is installed into
the production package. Free-threaded builds need `--no-default-features` and
interpreter-specific naming/configuration; this experiment does not validate
free-threaded packaging or add a setup.py build/install hook.

Use `FWS_SUPERVISOR_BINARY` and `FWS_SUPERVISOR_CLIENT_EXTENSION` for
`python -m unittest tests.test_supervisor_client`. Both backend implementations
run the same cases with disposable test-owned children only. The RPC harness in
`scripts/uds_bench/rpc.py` provides `supervisor-shared-python` and
`supervisor-shared-native` alongside the earlier native-pipe/msgspec controls.

## Hot-Path Scheduling

The initial future-per-chunk/ACK binding is retained only in ignored benchmark
artifacts after a substantial regression. The current API v3 binding registers a
nonblocking readiness socketpair with asyncio `add_reader`; Rust batches typed
output/completions without calling Python from runtime threads. Write submission
returns a local completion ID, not a wire protocol ID. Python resolves its own
awaiters when that FD is readable. This is event driven, not a polling timer.
Connect/rare controls still use pyo3-async-runtimes. Always `close()` before closing
the Python loop so its watcher and both readiness descriptors are released.

The shared client, native binding, and Python delivery buffer have separate
256-event limits, so their combined output budget is up to about 6 MiB plus
metadata, not 2 MiB total. Binding write permits are released only after the
completion batch is drained, bounding both in-flight and undrained completions.
