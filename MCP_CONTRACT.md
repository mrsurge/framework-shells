# FWS MCP Contract

This document defines the practical read-only contract that TE2 MCP can rely on today when working against `framework_shells`.

## Scope

Use this contract for:
- shell listing
- shell detail
- log tail
- log search
- log replay/file access
- coarse age/time-based observability from persisted metadata

Do not use this contract as if it provides:
- per-line timestamps
- live event replay history
- stdin write to active shells
- attach-like interactive streaming guarantees
- access to another process's in-memory subscriber state

The key boundary is:
- persisted runtime-store state: supported
- process-local live state: not part of this contract

## Capability Matrix

| Capability | Surface | Status | Notes |
| --- | --- | --- | --- |
| List shells | HTTP / direct import | Available | `GET /api/framework_shells` or `await mgr.list_shells()` |
| Shell detail | HTTP / direct import | Available | `GET /api/framework_shells/{shell_id}` |
| Group shutdown | HTTP / CLI / direct import | Available | Matches UI shutdown-group behavior |
| Log tail | HTTP / direct import | Available | Includes file metadata and line payload |
| Log search | HTTP / direct import | Available | Exact or regex; line-number matches |
| Raw stdout replay | HTTP | Available | `GET /api/framework_shells/{shell_id}/replay` |
| Whole-log age | HTTP / direct import | Available | Derived from file `mtime` |
| Per-line timestamps | None | Not available | Current logs are plain text |
| Live event replay history | None | Not available | Would require event persistence |
| stdin write to active shell | process-local only | Not part of MCP contract | Needs explicit upstream live control surface |

## Current HTTP Endpoints

### List shells

`GET /api/framework_shells?include_stats=true`

Example:

```sh
curl -s "http://localhost:8089/api/framework_shells?include_stats=true"
```

Response shape:

```json
{
  "ok": true,
  "data": [
    {
      "id": "fs_123",
      "label": "app-worker:demo",
      "pid": 12345,
      "status": "running",
      "pty_mode": "interactive",
      "created_at": 1770000000.123,
      "updated_at": 1770000042.456,
      "stdout_log": "/path/to/fs_123.stdout.log",
      "stderr_log": "/path/to/fs_123.stderr.log",
      "subgroups": ["demo", "app-worker"],
      "stats": {
        "alive": true,
        "uptime": 42.3,
        "cpu_percent": 0.0,
        "memory_rss": 7340032,
        "num_threads": 1
      }
    }
  ]
}
```

### Get shell detail

`GET /api/framework_shells/{shell_id}?include_stats=true`

Example:

```sh
curl -s "http://localhost:8089/api/framework_shells/fs_123?include_stats=true"
```

Shell detail returns the same shell payload shape as list, including `pty_mode`.

### Shutdown group

`POST /api/framework_shells/app/{app_id}/shutdown`

Auth required.

Example:

```sh
curl -s -X POST \
  -H "X-Framework-Key: $FRAMEWORK_KEY" \
  "http://localhost:8089/api/framework_shells/app/demo/shutdown"
```

Response shape:

```json
{
  "ok": true,
  "data": {
    "root_pids": [12345, 12346],
    "stats": {
      "terminated": 2,
      "signaled": 4,
      "skipped": 0
    }
  }
}
```

### Log tail

`GET /api/framework_shells/logs/{shell_id}/tail?stream=stdout|stderr|both&lines=<n>`

Example:

```sh
curl -s "http://localhost:8089/api/framework_shells/logs/fs_123/tail?stream=both&lines=50"
```

Response shape:

```json
{
  "ok": true,
  "data": {
    "shell_id": "fs_123",
    "created_at": 1770000000.123,
    "updated_at": 1770000042.456,
    "status": "running",
    "stdout": {
      "path": "/path/to/fs_123.stdout.log",
      "mtime": 1770000040.111,
      "size": 123456,
      "age_seconds": 2.3,
      "lines": [
        "server ready on :8080",
        "healthcheck ok"
      ]
    },
    "stderr": {
      "path": "/path/to/fs_123.stderr.log",
      "mtime": 1770000038.222,
      "size": 48,
      "age_seconds": 4.2,
      "lines": []
    }
  }
}
```

Notes:
- `mtime`, `size`, and `age_seconds` describe the whole log file.
- `lines` are plain text log lines with no per-line timestamp.
- `stream=stdout` or `stream=stderr` returns only that stream.

### Log search

`GET /api/framework_shells/logs/{shell_id}/search?stream=stdout|stderr|both&query=<text>&limit=<n>&regex=true|false&ignore_case=true|false`

Examples:

Exact/substring search:

```sh
curl -s "http://localhost:8089/api/framework_shells/logs/fs_123/search?stream=stdout&query=ready&limit=20"
```

Regex search:

```sh
curl -s "http://localhost:8089/api/framework_shells/logs/fs_123/search?stream=both&query=ready|healthy&regex=true&ignore_case=true&limit=20"
```

Response shape:

```json
{
  "ok": true,
  "data": {
    "shell_id": "fs_123",
    "created_at": 1770000000.123,
    "updated_at": 1770000042.456,
    "status": "running",
    "stream": "both",
    "query": "ready|healthy",
    "regex": true,
    "ignore_case": true,
    "stdout": {
      "path": "/path/to/fs_123.stdout.log",
      "mtime": 1770000040.111,
      "size": 123456,
      "age_seconds": 2.3,
      "matches": [
        {"line_number": 88, "text": "HTTP server ready"},
        {"line_number": 101, "text": "worker healthy"}
      ]
    },
    "stderr": {
      "path": "/path/to/fs_123.stderr.log",
      "mtime": 1770000038.222,
      "size": 48,
      "age_seconds": 4.2,
      "matches": []
    }
  }
}
```

Notes:
- non-regex mode is substring search, not whole-line equality
- `line_number` is 1-based in the current log file
- invalid regex returns HTTP `400`

### Raw stdout replay

`GET /api/framework_shells/{shell_id}/replay`

Example:

```sh
curl -s "http://localhost:8089/api/framework_shells/fs_123/replay"
```

Current behavior:
- serves stdout log only
- plain text file response
- not structured

## Direct Import Usage

If TE2 MCP is running in the same runtime context and can resolve the same FWS store, direct import is a valid read-only path.

Example:

```python
from framework_shells import get_manager

async def snapshot_for_mcp(shell_id: str) -> dict:
    mgr = await get_manager()
    shell = await mgr.get_shell(shell_id)
    if not shell:
        return {"ok": False, "error": "not found"}

    tail = await mgr.get_log_tail(shell_id, stream="both", lines=50)
    search = await mgr.search_logs(shell_id, stream="stdout", query="ready", limit=20)
    return {
        "ok": True,
        "shell": shell.to_payload(),
        "tail": tail,
        "search": search,
    }
```

## Recommended MCP Output Model

For read-only metrics tools, normalize around this shape:

```json
{
  "shell_id": "fs_123",
  "status": "running",
  "pty_mode": "interactive",
  "created_at": 1770000000.123,
  "updated_at": 1770000042.456,
  "stdout": {
    "path": "/path/to/stdout.log",
    "mtime": 1770000040.111,
    "size": 123456,
    "age_seconds": 2.3
  },
  "stderr": {
    "path": "/path/to/stderr.log",
    "mtime": 1770000038.222,
    "size": 48,
    "age_seconds": 4.2
  }
}
```

That gives MCP enough information to answer:
- whether a shell is stale
- how recently logs changed
- whether a process is still active
- whether certain readiness/error text exists
- whether output volume changed materially

## Gaps To Add Later

If TE2 MCP needs richer observability later, the next contract additions should be:

1. Event replay endpoint
- persisted `ShellEvent` history, not only in-memory pub/sub

2. Structured JSONL log stream
- each chunk stored with timestamp and stream metadata

3. stdin/live control surface
- explicit write endpoint or TE2-native Socket.IO namespace

4. stderr replay parity
- current `replay` endpoint serves stdout only

## Practical Guidance

For TE2 MCP today:
- prefer direct import for internal read-only tooling
- keep HTTP endpoints as the stable external contract
- use Socket.IO only for TE2 UI/live interaction, not as the MCP storage contract
