from typing import Annotated, cast

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query
from fastapi.responses import FileResponse
import hmac
from pathlib import Path

from ..auth import get_secret, derive_api_token
from ..manager import FrameworkShellManager
from ..record import ShellRecord
from ..shared_manager import get_manager as get_shared_manager

router = APIRouter()

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
    payload = record.to_payload(include_env=include_env)
    payload["capabilities"] = await mgr.get_shell_capabilities(record)
    return payload

async def require_auth(
    authorization: Annotated[str | None, Header()] = None,
    x_framework_key: Annotated[str | None, Header(alias="X-Framework-Key")] = None,
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

@router.get("/api/framework_shells")
async def list_shells(
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    include_stats: Annotated[bool, Query()] = False,
):
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

@router.get("/api/framework_shells/{shell_id}")
async def get_shell(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    include_stats: Annotated[bool, Query()] = False,
):
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

@router.post("/api/framework_shells")
async def find_or_create_shell(
    payload: Annotated[JsonDict, Body(...)],
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
):
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

@router.post('/api/framework_shells/{shell_id}/terminate')
async def terminate_shell(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
):
    await mgr.terminate_shell(shell_id)
    return {"ok": True}

@router.post('/api/framework_shells/{shell_id}/action')
async def shell_action(
    shell_id: str,
    payload: Annotated[JsonDict, Body(...)],
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
):
    """Handle shell actions (terminate, etc.)."""
    action = _json_str(payload.get("action"))
    if action == "terminate":
        force = _json_bool(payload.get("force"))
        await mgr.terminate_shell(shell_id, force=force)
        return {"ok": True}
    else:
        raise HTTPException(400, f"Unknown action: {action}")


@router.post('/api/framework_shells/{shell_id}/input')
async def shell_input(
    shell_id: str,
    payload: Annotated[JsonDict, Body(...)],
    _mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _auth: Annotated[None, Depends(require_auth)],
):
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


@router.delete('/api/framework_shells/{shell_id}')
async def remove_shell(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
    force: Annotated[bool, Query()] = False,
):
    """Purge a shell's metadata and logs.

    The Sessions & Shortcuts "Exited shells" UI uses this to delete old logs.
    """
    ok = await mgr.remove_shell(shell_id, force=force)
    if not ok:
        raise HTTPException(404, "Shell not found")
    return {"ok": True}


@router.post('/api/framework_shells/purge_exited')
async def purge_exited_shells(
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
):
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

@router.post("/api/framework_shells/app/{app_id}/shutdown")
async def shutdown_app_group(
    app_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    _: Annotated[None, Depends(require_auth)],
):
    """UI-equivalent group shutdown for downstream consumers."""
    return await mgr.shutdown_app_group(app_id)


@router.get("/api/framework_shells/logs/{shell_id}/tail")
async def get_log_tail(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    stream: Annotated[str, Query()] = "both",
    lines: Annotated[int, Query(ge=0, le=5000)] = 200,
):
    try:
        data = await mgr.get_log_tail(shell_id, stream=stream, lines=lines)
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"ok": True, "data": data}


@router.get("/api/framework_shells/logs/{shell_id}/search")
async def search_logs(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    query: Annotated[str, Query(min_length=1)],
    stream: Annotated[str, Query()] = "both",
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    regex: Annotated[bool, Query()] = False,
    ignore_case: Annotated[bool, Query()] = False,
):
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


@router.get("/api/framework_shells/logs/{shell_id}/inspect")
async def inspect_logs(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
    stream: Annotated[str, Query()] = "both",
    lines: Annotated[int, Query(ge=0, le=5000)] = 200,
    query: Annotated[str | None, Query()] = None,
    exclude_query: Annotated[str | None, Query()] = None,
    regex: Annotated[bool, Query()] = False,
    ignore_case: Annotated[bool, Query()] = False,
    format: Annotated[str | None, Query()] = None,
    signature: Annotated[str | None, Query()] = None,
    exclude_signature: Annotated[str | None, Query()] = None,
    include_io_metadata: Annotated[bool, Query()] = False,
    include_stdin: Annotated[bool, Query()] = False,
    include_timestamps: Annotated[bool, Query()] = False,
    include_output_metadata: Annotated[bool, Query()] = False,
):
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

@router.get("/api/framework_shells/{shell_id}/replay")
async def replay_log(
    shell_id: str,
    mgr: Annotated[FrameworkShellManager, Depends(get_manager_dep)],
):
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
