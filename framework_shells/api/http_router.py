from typing import Literal, cast

from starlette.exceptions import HTTPException
from starlette.responses import FileResponse, Response
from starlette.requests import Request
from starlette.routing import Route, Router

from .http_support import Query, http_route, path_text, read_json_object
import hmac
from pathlib import Path

from ..auth import get_secret, derive_api_token
from ..manager import FrameworkShellManager
from ..record import ShellRecord
from ..log_projection import RawReference, WindowAction
from ..shared_manager import get_manager as get_shared_manager


JsonDict = dict[str, object]


def _json_dict(value: object) -> JsonDict:
    if isinstance(value, dict):
        return cast(JsonDict, value)
    return {}


def _json_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _json_str_list(value: object) -> list[str] | None:
    if not isinstance(value, list):
        return None
    items: list[str] = []
    for item in cast(list[object], value):
        if not isinstance(item, str):
            return None
        items.append(item)
    return items


def _json_str_dict(value: object) -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    result: dict[str, str] = {}
    for key, item in cast(dict[object, object], value).items():
        if not isinstance(key, str) or not isinstance(item, str):
            return None
        result[key] = item
    return result


def _json_bool(value: object, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return default


async def get_manager_dep() -> FrameworkShellManager:
    # Always use the package-level singleton so hosts can configure hooks/providers once.
    return await get_shared_manager()


async def _payload_with_capabilities(
    mgr: FrameworkShellManager,
    record: ShellRecord,
    *,
    include_env: bool = False,
) -> JsonDict:
    payload: JsonDict = record.to_payload(include_env=include_env)
    payload["capabilities"] = await mgr.get_shell_capabilities(record)
    return payload

async def require_auth(
    authorization: str | None = None,
    x_framework_key: str | None = None,
) -> None:
    """Require valid Bearer token or X-Framework-Key for mutating endpoints."""
    secret = get_secret()

    # If no secret configured, skip auth (dev mode)
    if not secret:
        return

    expected = derive_api_token(secret)
    token = None

    # Check X-Framework-Key first (frontend uses this)
    if x_framework_key:
        token = x_framework_key
    # Fall back to Authorization: Bearer
    elif authorization and authorization.startswith("Bearer "):
        token = authorization[7:]

    # No token provided - skip auth if no token required
    if not token:
        raise HTTPException(403, "Missing auth token (X-Framework-Key or Authorization header)")

    if not hmac.compare_digest(token, expected):
        raise HTTPException(403, "Invalid auth token")

async def list_shells(
    mgr: FrameworkShellManager,
    include_stats: bool = False,
) -> JsonDict:
    records = await mgr.list_shells()
    if not include_stats:
        payloads: list[JsonDict] = []
        for rec in records:
            payloads.append(await _payload_with_capabilities(mgr, rec))
        return {"ok": True, "data": payloads}
    described: list[JsonDict] = []
    for rec in records:
        try:
            described.append(await mgr.describe(rec))
        except Exception:
            described.append(await _payload_with_capabilities(mgr, rec))
    return {"ok": True, "data": described}

async def get_shell(
    shell_id: str,
    mgr: FrameworkShellManager,
    include_stats: bool = False,
) -> JsonDict:
    record = await mgr.get_shell(shell_id)
    if not record:
        raise HTTPException(404, "Shell not found")
    if not include_stats:
        return {"ok": True, "data": await _payload_with_capabilities(mgr, record, include_env=True)}
    try:
        data = await mgr.describe(record)
        data["env_overrides"] = record.env_overrides
        return {"ok": True, "data": data}
    except Exception:
        return {"ok": True, "data": await _payload_with_capabilities(mgr, record, include_env=True)}

async def find_or_create_shell(
    payload: JsonDict,
    mgr: FrameworkShellManager,
    _: None = None,
) -> JsonDict:
    command = _json_str_list(payload.get("command"))
    cwd = _json_str(payload.get("cwd"))
    env = _json_str_dict(payload.get("env"))
    label = _json_str(payload.get("label"))
    subgroups = _json_str_list(payload.get("subgroups"))
    ui = _json_dict(payload.get("ui")) if isinstance(payload.get("ui"), dict) else None
    debug = _json_dict(payload.get("debug")) if isinstance(payload.get("debug"), dict) else None
    pty_mode = _json_str(payload.get("pty_mode"))
    autostart = _json_bool(payload.get("autostart"), default=True)

    # Idempotency check
    if label:
        existing = await mgr.find_shell_by_label(label)
        if existing:
             return {"ok": True, "data": await _payload_with_capabilities(mgr, existing), "reused": True}

    if not command:
        raise HTTPException(400, "Command required")

    record = await mgr.spawn_shell_pty(
        command, cwd=cwd, env=env, label=label,
        subgroups=subgroups, ui=ui, debug=debug, pty_mode=pty_mode, autostart=autostart
    )
    return {"ok": True, "data": await _payload_with_capabilities(mgr, record)}

async def terminate_shell(
    shell_id: str,
    mgr: FrameworkShellManager,
    _: None = None,
) -> JsonDict:
    await mgr.terminate_shell(shell_id)
    return {"ok": True}

async def shell_action(
    shell_id: str,
    payload: JsonDict,
    mgr: FrameworkShellManager,
    _: None = None,
) -> JsonDict:
    """Handle shell actions (terminate, etc.)."""
    action = _json_str(payload.get("action"))
    if action == "terminate":
        force = _json_bool(payload.get("force"))
        await mgr.terminate_shell(shell_id, force=force)
        return {"ok": True}
    else:
        raise HTTPException(400, f"Unknown action: {action}")


async def shell_input(
    shell_id: str,
    payload: JsonDict,
    _mgr: FrameworkShellManager,
    _auth: None = None,
) -> JsonDict:
    data = payload.get("data")
    append_newline = _json_bool(payload.get("append_newline"))
    eof = _json_bool(payload.get("eof"))
    source = _json_str(payload.get("source")) or "api"

    if eof and data not in (None, ""):
        raise HTTPException(400, "Provide either data or eof=true, not both")
    if not eof and data is None:
        raise HTTPException(400, "data is required unless eof=true")

    try:
        from .socketio_backend import write_shell_input_control

        result = await write_shell_input_control(
            shell_id,
            str(data) if data is not None else None,
            append_newline=append_newline,
            eof=eof,
            source=source,
        )
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except RuntimeError as exc:
        raise HTTPException(409, str(exc))
    except ValueError as exc:
        raise HTTPException(400, str(exc))

    return {"ok": True, "data": result}


async def remove_shell(
    shell_id: str,
    mgr: FrameworkShellManager,
    _: None = None,
    force: bool = False,
) -> JsonDict:
    """Purge a shell's metadata and logs.

    The Sessions & Shortcuts "Exited shells" UI uses this to delete old logs.
    """
    ok = await mgr.remove_shell(shell_id, force=force)
    if not ok:
        raise HTTPException(404, "Shell not found")
    return {"ok": True}


async def purge_exited_shells(
    mgr: FrameworkShellManager,
    _: None = None,
) -> JsonDict:
    """Purge metadata/logs for all exited shells."""
    records = await mgr.list_shells()
    exited = [r for r in records if r.status == "exited"]
    errors: list[str] = []
    purged = 0
    for rec in exited:
        try:
            purged += int(await mgr.remove_shell(rec.id, force=True))
        except Exception as exc:
            errors.append(f"{rec.id}: {exc}")
    return {"ok": True, "data": {"purged": purged, "errors": errors}}

async def shutdown_app_group(
    app_id: str,
    mgr: FrameworkShellManager,
    _: None = None,
) -> JsonDict:
    """UI-equivalent group shutdown for downstream consumers."""
    result: JsonDict = await mgr.shutdown_app_group(app_id)
    return result


async def get_log_window(
    shell_id: str,
    mgr: FrameworkShellManager,
    stream: Literal["stdout", "stderr"] = "stdout",
    action: WindowAction = "tail",
    current: int = 0,
    count: int = 200,
    shift: int = 50,
    generation: str | None = None,
) -> JsonDict:
    try:
        window = await mgr.get_log_window(shell_id, stream=stream, action=action,
            current=current, count=count, shift=shift, generation=generation)
        return {"ok": True, "data": window.to_dict()}
    except (KeyError, FileNotFoundError):
        raise HTTPException(404, "Shell or log not found")
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(409 if "stale" in str(exc) or "changed" in str(exc) else 400, str(exc))


async def get_log_raw(
    shell_id: str,
    mgr: FrameworkShellManager,
    generation: str,
    byte_start: int,
    byte_end: int,
    stream: Literal["stdout", "stderr"] = "stdout",
    offset: int = 0,
    limit: int = 65536,
) -> JsonDict:
    try:
        data = await mgr.get_log_raw(shell_id, RawReference(generation, byte_start, byte_end),
            stream=stream, offset=offset, limit=limit)
        next_offset = min(offset + len(data), byte_end - byte_start)
        return {"ok": True, "data": {"hex": data.hex(), "next_offset": next_offset,
                                      "eof": next_offset >= byte_end - byte_start}}
    except (KeyError, FileNotFoundError):
        raise HTTPException(404, "Shell or log not found")
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(409 if "stale" in str(exc) or "changed" in str(exc) else 400, str(exc))


async def get_log_tail(
    shell_id: str,
    mgr: FrameworkShellManager,
    stream: str = "both",
    lines: int = 200,
) -> JsonDict:
    try:
        data = await mgr.get_log_tail(shell_id, stream=stream, lines=lines)
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"ok": True, "data": data}


async def search_logs(
    shell_id: str,
    mgr: FrameworkShellManager,
    query: str,
    stream: str = "both",
    limit: int = 100,
    regex: bool = False,
    ignore_case: bool = False,
) -> JsonDict:
    try:
        data = await mgr.search_logs(
            shell_id,
            stream=stream,
            query=query,
            limit=limit,
            regex=regex,
            ignore_case=ignore_case,
        )
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:
        raise HTTPException(400, str(exc))
    return {"ok": True, "data": data}


async def inspect_logs(
    shell_id: str,
    mgr: FrameworkShellManager,
    stream: str = "both",
    lines: int = 200,
    query: str | None = None,
    exclude_query: str | None = None,
    regex: bool = False,
    ignore_case: bool = False,
    format: str | None = None,
    signature: str | None = None,
    exclude_signature: str | None = None,
    include_io_metadata: bool = False,
    include_stdin: bool = False,
    include_timestamps: bool = False,
    include_output_metadata: bool = False,
) -> JsonDict:
    try:
        data = await mgr.inspect_logs(
            shell_id,
            stream=stream,
            lines=lines,
            query=query,
            exclude_query=exclude_query,
            regex=regex,
            ignore_case=ignore_case,
            format=format,
            signature=signature,
            exclude_signature=exclude_signature,
            include_io_metadata=include_io_metadata,
            include_stdin=include_stdin,
            include_timestamps=include_timestamps,
            include_output_metadata=include_output_metadata,
        )
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"ok": True, "data": data}

async def replay_log(
    shell_id: str,
    mgr: FrameworkShellManager,
) -> Response | JsonDict:
    """Serve the stdout log for a shell."""
    record = await mgr.get_shell(shell_id)
    if not record:
        raise HTTPException(404, "Shell not found")

    path = Path(record.stdout_log)
    if not path.exists():
         return {"ok": True, "content": ""}

    # Simple FileResponse for now.
    # Front-end can handle range headers automatically with FileResponse if needed.
    return FileResponse(path, media_type="text/plain")


async def _dispatch(request: Request, operation: str) -> Response | JsonDict:
    query = Query(request)
    shell_id = path_text(request, "shell_id") if "shell_id" in request.path_params else ""
    payload: JsonDict = {}
    if operation in {"create", "action", "input"}:
        payload = await read_json_object(request)
    if operation in {"create", "terminate", "action", "input", "remove", "purge", "shutdown"}:
        await require_auth(request.headers.get("authorization"), request.headers.get("x-framework-key"))

    if operation in {"list", "detail"}:
        include_stats = query.boolean("include_stats")
        query.check()
        manager = await get_manager_dep()
        if operation == "list":
            return await list_shells(manager, include_stats)
        return await get_shell(shell_id, manager, include_stats)
    if operation == "window":
        window_stream = cast(Literal["stdout", "stderr"], query.literal("stream", ("stdout", "stderr"), "stdout"))
        action = cast(WindowAction, query.literal("action", ("tail", "older", "newer", "current"), "tail"))
        current = query.integer("current", 0, minimum=0)
        count = query.integer("count", 200, minimum=1, maximum=1000)
        shift = query.integer("shift", 50, minimum=1)
        generation = query.text("generation")
        query.check()
        return await get_log_window(shell_id, await get_manager_dep(), window_stream, action,
                                    current, count, shift, generation)
    if operation == "raw":
        generation = query.required("generation")
        start = query.integer("byte_start", minimum=0)
        end = query.integer("byte_end", minimum=0)
        raw_stream = cast(Literal["stdout", "stderr"], query.literal("stream", ("stdout", "stderr"), "stdout"))
        offset = query.integer("offset", 0, minimum=0)
        limit = query.integer("limit", 65536, minimum=1, maximum=65536)
        query.check()
        return await get_log_raw(shell_id, await get_manager_dep(), generation, start, end, raw_stream, offset, limit)
    if operation == "tail":
        stream = query.text("stream", "both") or ""
        lines = query.integer("lines", 200, minimum=0, maximum=5000)
        query.check()
        return await get_log_tail(shell_id, await get_manager_dep(), stream, lines)
    if operation == "search":
        text = query.required("query", minimum_length=1)
        stream = query.text("stream", "both") or ""
        limit = query.integer("limit", 100, minimum=1, maximum=1000)
        regex = query.boolean("regex")
        ignore_case = query.boolean("ignore_case")
        query.check()
        return await search_logs(shell_id, await get_manager_dep(), text, stream, limit, regex, ignore_case)
    if operation == "inspect":
        stream = query.text("stream", "both") or ""
        lines = query.integer("lines", 200, minimum=0, maximum=5000)
        regex = query.boolean("regex")
        ignore_case = query.boolean("ignore_case")
        include_io = query.boolean("include_io_metadata")
        include_stdin = query.boolean("include_stdin")
        include_ts = query.boolean("include_timestamps")
        include_output = query.boolean("include_output_metadata")
        query.check()
        return await inspect_logs(shell_id, await get_manager_dep(), stream, lines,
            query.text("query"), query.text("exclude_query"), regex, ignore_case,
            query.text("format"), query.text("signature"), query.text("exclude_signature"),
            include_io, include_stdin, include_ts, include_output)

    force = query.boolean("force") if operation == "remove" else False
    query.check()
    manager = await get_manager_dep()
    if operation == "create":
        return await find_or_create_shell(payload, manager)
    if operation == "terminate":
        return await terminate_shell(shell_id, manager)
    if operation == "action":
        return await shell_action(shell_id, payload, manager)
    if operation == "input":
        return await shell_input(shell_id, payload, manager)
    if operation == "remove":
        return await remove_shell(shell_id, manager, force=force)
    if operation == "purge":
        return await purge_exited_shells(manager)
    if operation == "shutdown":
        return await shutdown_app_group(path_text(request, "app_id"), manager)
    if operation == "replay":
        return await replay_log(shell_id, manager)
    raise RuntimeError(f"Unknown FWS HTTP operation: {operation}")


def _route(path: str, operation: str, method: str = "GET") -> Route:
    async def dispatch(request: Request) -> Response | JsonDict:
        return await _dispatch(request, operation)
    return http_route(path, dispatch, methods=[method])


projection_router = Router(routes=[
    _route("/api/framework_shells/logs/{shell_id}/window", "window"),
    _route("/api/framework_shells/logs/{shell_id}/raw", "raw"),
])

router = Router(routes=[
    _route("/api/framework_shells", "list"),
    _route("/api/framework_shells", "create", "POST"),
    _route("/api/framework_shells/purge_exited", "purge", "POST"),
    _route("/api/framework_shells/app/{app_id}/shutdown", "shutdown", "POST"),
    *projection_router.routes,
    _route("/api/framework_shells/logs/{shell_id}/tail", "tail"),
    _route("/api/framework_shells/logs/{shell_id}/search", "search"),
    _route("/api/framework_shells/logs/{shell_id}/inspect", "inspect"),
    _route("/api/framework_shells/{shell_id}/terminate", "terminate", "POST"),
    _route("/api/framework_shells/{shell_id}/action", "action", "POST"),
    _route("/api/framework_shells/{shell_id}/input", "input", "POST"),
    _route("/api/framework_shells/{shell_id}/replay", "replay"),
    _route("/api/framework_shells/{shell_id}", "detail"),
    _route("/api/framework_shells/{shell_id}", "remove", "DELETE"),
])
