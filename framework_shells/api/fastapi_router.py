from fastapi import APIRouter, Depends, Header, HTTPException, Query, Body
from fastapi.responses import FileResponse
from typing import List, Optional, Any
import hmac
from pathlib import Path

from ..auth import get_secret, derive_api_token
from ..manager import FrameworkShellManager
from ..store import RuntimeStore
from .. import get_manager as get_shared_manager

router = APIRouter()


async def get_manager_dep() -> FrameworkShellManager:
    # Always use the package-level singleton so hosts can configure hooks/providers once.
    return await get_shared_manager()


async def _payload_with_capabilities(
    mgr: FrameworkShellManager,
    record,
    *,
    include_env: bool = False,
):
    payload = record.to_payload(include_env=include_env)
    payload["capabilities"] = await mgr.get_shell_capabilities(record)
    return payload

async def require_auth(
    authorization: str = Header(None),
    x_framework_key: str = Header(None, alias="X-Framework-Key")
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
    include_stats: bool = Query(False),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
):
    records = await mgr.list_shells()
    if not include_stats:
        payloads: List[dict] = []
        for rec in records:
            payloads.append(await _payload_with_capabilities(mgr, rec))
        return {"ok": True, "data": payloads}
    described: List[dict] = []
    for rec in records:
        try:
            described.append(await mgr.describe(rec))
        except Exception:
            described.append(await _payload_with_capabilities(mgr, rec))
    return {"ok": True, "data": described}

@router.get("/api/framework_shells/{shell_id}")
async def get_shell(
    shell_id: str,
    include_stats: bool = Query(False),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
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
    payload: dict = Body(...),
    authorization: str = Header(None), # Verify explicit param vs dependency
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth)
):
    command = payload.get("command")
    cwd = payload.get("cwd")
    env = payload.get("env")
    label = payload.get("label")
    subgroups = payload.get("subgroups")
    ui = payload.get("ui")
    pty_mode = payload.get("pty_mode")
    autostart = payload.get("autostart", True)

    # Idempotency check
    if label:
        existing = await mgr.find_shell_by_label(label)
        if existing:
             return {"ok": True, "data": await _payload_with_capabilities(mgr, existing), "reused": True}

    if not command:
        raise HTTPException(400, "Command required")

    record = await mgr.spawn_shell_pty(
        command, cwd=cwd, env=env, label=label,
        subgroups=subgroups, ui=ui, pty_mode=pty_mode, autostart=autostart
    )
    return {"ok": True, "data": await _payload_with_capabilities(mgr, record)}

@router.post('/api/framework_shells/{shell_id}/terminate')
async def terminate_shell(
    shell_id: str,
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth)
):
    await mgr.terminate_shell(shell_id)
    return {"ok": True}

@router.post('/api/framework_shells/{shell_id}/action')
async def shell_action(
    shell_id: str,
    payload: dict = Body(...),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth)
):
    """Handle shell actions (terminate, etc.)."""
    action = payload.get("action")
    if action == "terminate":
        force = payload.get("force", False)
        await mgr.terminate_shell(shell_id, force=force)
        return {"ok": True}
    else:
        raise HTTPException(400, f"Unknown action: {action}")


@router.post('/api/framework_shells/{shell_id}/input')
async def shell_input(
    shell_id: str,
    payload: dict = Body(...),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth),
):
    data = payload.get("data")
    append_newline = bool(payload.get("append_newline", False))
    eof = bool(payload.get("eof", False))

    if eof and data not in (None, ""):
        raise HTTPException(400, "Provide either data or eof=true, not both")
    if not eof and data is None:
        raise HTTPException(400, "data is required unless eof=true")

    try:
        if eof:
            result = await mgr.send_shell_eof(shell_id)
        else:
            result = await mgr.write_to_shell(
                shell_id,
                str(data),
                append_newline=append_newline,
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
    force: bool = Query(False),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth),
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
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth),
):
    """Purge metadata/logs for all exited shells."""
    records = await mgr.list_shells()
    exited = [r for r in records if (getattr(r, 'status', None) or '') == 'exited']
    errors: list[str] = []
    purged = 0
    for rec in exited:
        try:
            await mgr.remove_shell(rec.id, force=True)
            purged += 1
        except Exception as exc:
            errors.append(f"{rec.id}: {exc}")
    return {"ok": True, "data": {"purged": purged, "errors": errors}}

@router.post("/api/framework_shells/app/{app_id}/shutdown")
async def shutdown_app_group(
    app_id: str,
    mgr: FrameworkShellManager = Depends(get_manager_dep),
    _: None = Depends(require_auth),
):
    """UI-equivalent group shutdown for downstream consumers."""
    return await mgr.shutdown_app_group(app_id)


@router.get("/api/framework_shells/logs/{shell_id}/tail")
async def get_log_tail(
    shell_id: str,
    stream: str = Query("both"),
    lines: int = Query(200, ge=0, le=5000),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
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
    query: str = Query(..., min_length=1),
    stream: str = Query("both"),
    limit: int = Query(100, ge=1, le=1000),
    regex: bool = Query(False),
    ignore_case: bool = Query(False),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
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
    stream: str = Query("both"),
    lines: int = Query(200, ge=0, le=5000),
    query: Optional[str] = Query(None),
    exclude_query: Optional[str] = Query(None),
    regex: bool = Query(False),
    ignore_case: bool = Query(False),
    format: Optional[str] = Query(None),
    signature: Optional[str] = Query(None),
    exclude_signature: Optional[str] = Query(None),
    mgr: FrameworkShellManager = Depends(get_manager_dep),
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
        )
    except KeyError:
        raise HTTPException(404, "Shell not found")
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    return {"ok": True, "data": data}


from fastapi.responses import FileResponse

@router.get("/api/framework_shells/{shell_id}/replay")
async def replay_log(
    shell_id: str,
    mgr: FrameworkShellManager = Depends(get_manager_dep)
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
