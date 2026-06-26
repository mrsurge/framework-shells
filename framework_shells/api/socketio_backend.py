from __future__ import annotations
# pyright: reportPrivateUsage=false

import asyncio
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol, cast

import aiofiles
import socketio
from fastapi import FastAPI

from ..auth import derive_api_token, derive_runtime_id, get_secret
from ..events import EventType, ShellEvent, get_event_bus
from ..fws_socketio_contract import FWS_SOCKETIO_NAMESPACE, FWS_SOCKETIO_SOCKET_PATH
from ..protocols.fws_peer import (
    FWS_BROWSER_ROLE,
    FWS_DASHBOARD_ROOM,
    FWS_NOTIFICATION_EVENT,
    FWS_PEER_REQUEST_EVENT,
    FWS_PEER_ROLE,
    FWS_PEER_ROOM,
    FWS_PEER_SUBSCRIPTIONS_EVENT,
    build_peer_shell_input_request,
    build_peer_subscriptions,
    parse_peer_notification,
    shell_room,
)
from ..io_metadata import read_io_metadata
from ..protocols.fws_ui import (
    APP_SHUTDOWN_METHOD,
    DASHBOARD_OPEN_METHOD,
    DASHBOARD_REFRESH_METHOD,
    DashboardProcessPayload,
    DashboardShellPayload,
    EXITED_PURGE_METHOD,
    FwsNotification,
    FwsRequest,
    IoMetadataPayload,
    LOGS_CHUNK_METHOD,
    LOGS_CLOSE_METHOD,
    LOGS_INITIAL_METHOD,
    LOGS_IO_METADATA_METHOD,
    LOGS_OPEN_METHOD,
    LOGS_RESET_METHOD,
    LOGS_TRUNCATE_METHOD,
    PID_TERMINATE_METHOD,
    SHELL_INPUT_METHOD,
    SHELL_PURGE_METHOD,
    SHELL_REMOVED_NOTIFICATION_METHOD,
    SHELL_TERMINATE_METHOD,
    SHUTDOWN_METHOD,
    build_action_response,
    build_error_notification,
    build_logs_chunk_notification,
    build_logs_initial_notification,
    build_logs_io_metadata_notification,
    build_logs_open_response,
    build_logs_reset_notification,
    build_request_error_response,
    parse_fws_request,
)
from ..shared_manager import get_manager
from .fws_ui import (
    _action_purge_exited,
    _action_purge_shell,
    _action_shutdown,
    _action_shutdown_app,
    _action_terminate_pid,
    _action_terminate_shell,
    _action_truncate_logs,
    _dashboard_notification_for_event,
    _dashboard_state_parts,
)

_LOCAL_REPLAY_MAX_LINES = 2000

_browser_log_subscriptions: dict[str, str] = {}
_peer_sids: set[str] = set()
_local_event_task: asyncio.Task[None] | None = None
_local_event_lock = asyncio.Lock()
_ObjectMapping = Mapping[str, object]


class _SocketIoCallServer(Protocol):
    async def call(
        self,
        event: str,
        data: object = None,
        *,
        to: str | None = None,
        sid: str | None = None,
        namespace: str | None = None,
        timeout: int | float = 60,
        ignore_queue: bool = False,
    ) -> object:
        ...


def _shell_room(shell_id: str) -> str:
    return shell_room(shell_id)


def _as_object_mapping(value: object) -> _ObjectMapping | None:
    if not isinstance(value, Mapping):
        return None
    return cast(_ObjectMapping, value)


def _request_id_from_payload(payload: object) -> str | None:
    mapping = _as_object_mapping(payload)
    if mapping is None:
        return None
    request_id = mapping.get("id")
    return request_id if isinstance(request_id, str) else None


def _int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _coerce_request_payload(payload: object) -> FwsRequest | None:
    mapping = _as_object_mapping(payload)
    if mapping is None:
        return None
    try:
        raw = json.dumps(dict(mapping), separators=(",", ":"))
    except Exception:
        return None
    return parse_fws_request(raw)


def _peer_auth_valid(auth: object) -> bool:
    mapping = _as_object_mapping(auth)
    if mapping is None:
        return False
    try:
        secret = get_secret()
    except Exception:
        return False
    expected_token = derive_api_token(secret)
    expected_runtime = derive_runtime_id(secret)
    api_token = mapping.get("api_token")
    runtime_id = mapping.get("runtime_id")
    return isinstance(api_token, str) and isinstance(runtime_id, str) and api_token == expected_token and runtime_id == expected_runtime


def _active_log_shell_ids() -> list[str]:
    return sorted({shell_id for shell_id in _browser_log_subscriptions.values() if shell_id})


async def _broadcast_peer_subscriptions() -> None:
    await FWS_SOCKETIO_SIO.emit(
        FWS_PEER_SUBSCRIPTIONS_EVENT,
        build_peer_subscriptions(_active_log_shell_ids()),
        namespace=FWS_SOCKETIO_NAMESPACE,
        room=FWS_PEER_ROOM,
    )


async def _set_browser_log_shell(ns: socketio.AsyncNamespace, sid: str, shell_id: str | None) -> None:
    try:
        session = await ns.get_session(sid)
    except Exception:
        session = {}
    previous_shell_id = session.get("log_shell_id")
    if isinstance(previous_shell_id, str) and previous_shell_id:
        try:
            await ns.leave_room(sid, _shell_room(previous_shell_id))
        except Exception:
            pass
        _ = _browser_log_subscriptions.pop(sid, None)
    if shell_id:
        await ns.enter_room(sid, _shell_room(shell_id))
        _browser_log_subscriptions[sid] = shell_id
    session["log_shell_id"] = shell_id
    await ns.save_session(sid, session)
    await _broadcast_peer_subscriptions()


async def _load_log_backlog(shell_id: str) -> tuple[str, str, list[IoMetadataPayload]]:
    if not shell_id:
        raise LookupError("Shell not found: ")
    mgr = await get_manager()
    record = await mgr.load_shell_record(shell_id)
    if record is None:
        if _peer_sids:
            return "", "", []
        raise LookupError(f"Shell not found: {shell_id}")

    async def _read_tail(path: Path) -> str:
        if not path.exists():
            return ""
        async with aiofiles.open(path, "r", encoding="utf-8", errors="replace") as fh:
            lines = (await fh.read()).splitlines()
        return "\n".join(lines[-_LOCAL_REPLAY_MAX_LINES:])

    metadata: list[IoMetadataPayload] = []
    if record.io_metadata_log:
        metadata = cast(
            list[IoMetadataPayload],
            await read_io_metadata(
                Path(record.io_metadata_log),
                limit=_LOCAL_REPLAY_MAX_LINES,
                include_output=False,
                include_stdin=True,
                include_timestamps=True,
            ),
        )
    return await _read_tail(Path(record.stdout_log)), await _read_tail(Path(record.stderr_log)), metadata


def _notification_shell_id(notification: _ObjectMapping) -> str | None:
    params = notification.get("params")
    mapping = _as_object_mapping(params)
    if mapping is None:
        return None
    shell_id = mapping.get("shell_id")
    return shell_id if isinstance(shell_id, str) else None


def _is_live_input_unavailable(exc: BaseException) -> bool:
    return "Live input unavailable" in str(exc)


def _response_error_code(value: object) -> str:
    mapping = _as_object_mapping(value)
    if mapping is None:
        return ""
    code = mapping.get("code")
    return code if isinstance(code, str) else ""


async def _write_shell_input_local(
    shell_id: str,
    data: str | None,
    *,
    append_newline: bool,
    eof: bool,
    source: str,
) -> Mapping[str, object]:
    mgr = await get_manager()
    if eof:
        return await mgr.send_shell_eof(shell_id, source=source)
    return await mgr.write_to_shell(shell_id, data or "", append_newline=append_newline, source=source)


async def _call_peer_shell_input(
    shell_id: str,
    data: str | None,
    *,
    append_newline: bool,
    eof: bool,
    source: str,
) -> Mapping[str, object]:
    peer_sids = list(_peer_sids)
    if not peer_sids:
        raise RuntimeError(f"Live input unavailable for shell {shell_id}: no connected FWS peer owns live input")

    payload: Mapping[str, object] = build_peer_shell_input_request(
        shell_id=shell_id,
        data=data,
        append_newline=append_newline,
        eof=eof,
        source=source,
    )

    async def _call_one(sid: str) -> object:
        try:
            sio = cast(_SocketIoCallServer, FWS_SOCKETIO_SIO)
            response = await sio.call(
                FWS_PEER_REQUEST_EVENT,
                payload,
                to=sid,
                namespace=FWS_SOCKETIO_NAMESPACE,
                timeout=3,
            )
            return response
        except Exception as exc:
            return {"ok": False, "code": "peer_error", "error": str(exc)}

    responses = await asyncio.gather(*(_call_one(sid) for sid in peer_sids))
    fallback_errors: list[str] = []
    for response in responses:
        response_map = _as_object_mapping(response)
        if response_map is None:
            continue
        if response_map.get("ok") is True:
            data_obj = response_map.get("data")
            if isinstance(data_obj, Mapping):
                return cast(Mapping[str, object], data_obj)
            return {"ok": True}
        code = _response_error_code(response)
        error = response_map.get("error")
        if code not in {"not_owner", "not_found"} and isinstance(error, str) and error:
            fallback_errors.append(error)

    if fallback_errors:
        raise RuntimeError(fallback_errors[0])
    raise RuntimeError(f"Live input unavailable for shell {shell_id}: no connected FWS peer accepted the write")


async def write_shell_input_control(
    shell_id: str,
    data: str | None,
    *,
    append_newline: bool = False,
    eof: bool = False,
    source: str = "control",
) -> Mapping[str, object]:
    try:
        return await _write_shell_input_local(
            shell_id,
            data,
            append_newline=append_newline,
            eof=eof,
            source=source,
        )
    except KeyError:
        return await _call_peer_shell_input(
            shell_id,
            data,
            append_newline=append_newline,
            eof=eof,
            source=source,
        )
    except RuntimeError as exc:
        if not _is_live_input_unavailable(exc):
            raise
        return await _call_peer_shell_input(
            shell_id,
            data,
            append_newline=append_newline,
            eof=eof,
            source=source,
        )


async def _emit_notification(notification: FwsNotification) -> None:
    method = notification["method"]
    if method in {"fws.shell.created", "fws.shell.spawned", "fws.shell.updated", "fws.shell.exited", SHELL_REMOVED_NOTIFICATION_METHOD}:
        await FWS_SOCKETIO_SIO.emit(FWS_NOTIFICATION_EVENT, notification, namespace=FWS_SOCKETIO_NAMESPACE, room=FWS_DASHBOARD_ROOM)
        return
    shell_id = _notification_shell_id(notification)
    if method in {LOGS_INITIAL_METHOD, LOGS_CHUNK_METHOD, LOGS_IO_METADATA_METHOD, LOGS_RESET_METHOD} and shell_id:
        await FWS_SOCKETIO_SIO.emit(FWS_NOTIFICATION_EVENT, notification, namespace=FWS_SOCKETIO_NAMESPACE, room=_shell_room(shell_id))
        return
    if method == "fws.error":
        if shell_id:
            await FWS_SOCKETIO_SIO.emit(FWS_NOTIFICATION_EVENT, notification, namespace=FWS_SOCKETIO_NAMESPACE, room=_shell_room(shell_id))
        else:
            await FWS_SOCKETIO_SIO.emit(FWS_NOTIFICATION_EVENT, notification, namespace=FWS_SOCKETIO_NAMESPACE, room=FWS_DASHBOARD_ROOM)


async def _emit_notifications_for_event(event: ShellEvent) -> None:
    lifecycle_notification = await _dashboard_notification_for_event(event)
    if lifecycle_notification is not None:
        await _emit_notification(lifecycle_notification)

    if event.type == EventType.LOG_CHUNK:
        stream_name = str(event.data.get("stream") or "stdout")
        chunk = str(event.data.get("chunk") or "")
        if chunk and stream_name in {"stdout", "stderr"}:
            stream = "stderr" if stream_name == "stderr" else "stdout"
            await _emit_notification(build_logs_chunk_notification(event.shell_id, stream, chunk))
        return

    if event.type == EventType.PTY_CHUNK:
        chunk = str(event.data.get("chunk") or "")
        if chunk:
            await _emit_notification(build_logs_chunk_notification(event.shell_id, "stdout", chunk))
        return

    if event.type == EventType.IO_METADATA:
        record_obj = event.data.get("record")
        if isinstance(record_obj, dict):
            await _emit_notification(
                build_logs_io_metadata_notification(
                    event.shell_id,
                    cast(IoMetadataPayload, dict(cast(dict[object, object], record_obj))),
                )
            )
        return

    if event.type == EventType.LOG_RESET:
        stream_name = str(event.data.get("stream") or "stdout")
        if stream_name in {"stdout", "stderr"}:
            stream = "stderr" if stream_name == "stderr" else "stdout"
            await _emit_notification(build_logs_reset_notification(event.shell_id, stream))
        return

    if event.type == EventType.SHELL_REMOVED:
        await _emit_notification(build_error_notification("Shell removed", code="shell_removed", shell_id=event.shell_id))


async def _local_event_loop() -> None:
    bus = get_event_bus()
    queue = bus.subscribe()
    try:
        while True:
            event = await queue.get()
            await _emit_notifications_for_event(event)
    except asyncio.CancelledError:
        raise
    except Exception:
        pass
    finally:
        bus.unsubscribe(queue)


async def _ensure_local_event_forwarder() -> None:
    global _local_event_task
    if _local_event_task is not None and not _local_event_task.done():
        return
    async with _local_event_lock:
        if _local_event_task is not None and not _local_event_task.done():
            return
        _local_event_task = asyncio.create_task(_local_event_loop())


class FwsSocketIoNamespace(socketio.AsyncNamespace):
    async def on_connect(self, sid: str, environ: Mapping[str, object], auth: object | None = None) -> bool | None:
        await _ensure_local_event_forwarder()
        auth_mapping = _as_object_mapping(auth)
        if auth_mapping is not None and auth_mapping.get("role") == FWS_PEER_ROLE:
            if not _peer_auth_valid(auth):
                return False
            _peer_sids.add(sid)
            await self.enter_room(sid, FWS_PEER_ROOM)
            await self.save_session(sid, {"role": FWS_PEER_ROLE, "log_shell_id": None})
            await self.emit(FWS_PEER_SUBSCRIPTIONS_EVENT, build_peer_subscriptions(_active_log_shell_ids()), to=sid)
            return True
        await self.save_session(sid, {"role": FWS_BROWSER_ROLE, "log_shell_id": None})
        return True

    async def on_disconnect(self, sid: str, reason: object | None = None) -> None:
        _ = reason
        _peer_sids.discard(sid)
        _ = _browser_log_subscriptions.pop(sid, None)
        try:
            session = await self.get_session(sid)
        except Exception:
            session = {}
        current_shell_id = session.get("log_shell_id")
        if isinstance(current_shell_id, str) and current_shell_id:
            try:
                await self.leave_room(sid, _shell_room(current_shell_id))
            except Exception:
                pass
        await _broadcast_peer_subscriptions()

    async def on_fws_request(self, sid: str, payload: object) -> Mapping[str, object]:
        request_id = _request_id_from_payload(payload)
        request = _coerce_request_payload(payload)
        if request is None:
            return build_request_error_response(
                request_id,
                code=-32600,
                message="Invalid request",
                error_code="invalid_request",
            )

        method = request["method"]
        try:
            if method == DASHBOARD_OPEN_METHOD:
                await self.enter_room(sid, FWS_DASHBOARD_ROOM)
                shells, processes = await _dashboard_state_parts()
                return build_action_or_state_response(request["id"], method, shells=shells, processes=processes)

            if method == DASHBOARD_REFRESH_METHOD:
                shells, processes = await _dashboard_state_parts()
                return build_action_or_state_response(request["id"], method, shells=shells, processes=processes)

            if method == LOGS_OPEN_METHOD:
                params = cast(Mapping[str, object], request["params"])
                shell_id = str(params.get("shell_id") or "")
                stdout_text, stderr_text, io_metadata_records = await _load_log_backlog(shell_id)
                await _set_browser_log_shell(self, sid, shell_id)
                await self.emit(
                    FWS_NOTIFICATION_EVENT,
                    build_logs_initial_notification(shell_id, stdout_text, stderr_text, io_metadata=io_metadata_records),
                    to=sid,
                )
                return build_logs_open_response(request["id"], shell_id)

            if method == LOGS_CLOSE_METHOD:
                params = cast(Mapping[str, object], request["params"])
                shell_id = str(params.get("shell_id") or "")
                try:
                    session = await self.get_session(sid)
                except Exception:
                    session = {}
                current_shell_id = session.get("log_shell_id")
                if current_shell_id == shell_id:
                    await _set_browser_log_shell(self, sid, None)
                return build_action_response(request["id"])

            if method == LOGS_TRUNCATE_METHOD:
                await _action_truncate_logs()
                return build_action_response(request["id"])

            if method == EXITED_PURGE_METHOD:
                await _action_purge_exited()
                return build_action_response(request["id"])

            if method == SHELL_TERMINATE_METHOD:
                params = cast(Mapping[str, object], request["params"])
                await _action_terminate_shell(str(params.get("shell_id") or ""))
                return build_action_response(request["id"])

            if method == SHELL_PURGE_METHOD:
                params = cast(Mapping[str, object], request["params"])
                await _action_purge_shell(str(params.get("shell_id") or ""))
                return build_action_response(request["id"])

            if method == SHELL_INPUT_METHOD:
                params = cast(Mapping[str, object], request["params"])
                data = params.get("data")
                _ = await write_shell_input_control(
                    str(params.get("shell_id") or ""),
                    data if isinstance(data, str) else None,
                    append_newline=bool(params.get("append_newline", False)),
                    eof=bool(params.get("eof", False)),
                    source="dashboard",
                )
                return build_action_response(request["id"])

            if method == PID_TERMINATE_METHOD:
                params = cast(Mapping[str, object], request["params"])
                await _action_terminate_pid(_int_or_zero(params.get("pid")))
                return build_action_response(request["id"])

            if method == APP_SHUTDOWN_METHOD:
                params = cast(Mapping[str, object], request["params"])
                await _action_shutdown_app(str(params.get("app_id") or ""))
                return build_action_response(request["id"])

            if method == SHUTDOWN_METHOD:
                params = cast(Mapping[str, object], request["params"])
                await _action_shutdown(str(params.get("scope") or ""))
                return build_action_response(request["id"])
        except LookupError as exc:
            params = cast(Mapping[str, object], request["params"])
            return build_request_error_response(
                request["id"],
                code=-32004,
                message=str(exc),
                error_code="not_found",
                shell_id=str(params.get("shell_id")) if isinstance(params.get("shell_id"), str) else None,
            )
        except Exception as exc:
            params = cast(Mapping[str, object], request["params"])
            return build_request_error_response(
                request["id"],
                code=-32000,
                message=str(exc),
                error_code="action_failed",
                shell_id=str(params.get("shell_id")) if isinstance(params.get("shell_id"), str) else None,
            )

        return build_request_error_response(
            request["id"],
            code=-32601,
            message=f"Method not found: {method}",
            error_code="method_not_found",
        )

    async def on_fws_peer_notification(self, sid: str, payload: object) -> None:
        try:
            session = await self.get_session(sid)
        except Exception:
            session = {}
        if session.get("role") != FWS_PEER_ROLE:
            return
        notification = parse_peer_notification(payload)
        if notification is None:
            return
        await _emit_notification(notification)

def build_action_or_state_response(
    request_id: str,
    method: str,
    *,
    shells: object | None = None,
    processes: object | None = None,
) -> Mapping[str, object]:
    if method == DASHBOARD_OPEN_METHOD:
        assert shells is not None
        assert processes is not None
        from ..protocols.fws_ui import build_dashboard_open_response

        return build_dashboard_open_response(
            request_id,
            cast(list[DashboardShellPayload], shells),
            cast(list[DashboardProcessPayload], processes),
        )
    if method == DASHBOARD_REFRESH_METHOD:
        assert shells is not None
        assert processes is not None
        from ..protocols.fws_ui import build_dashboard_refresh_response

        return build_dashboard_refresh_response(
            request_id,
            cast(list[DashboardShellPayload], shells),
            cast(list[DashboardProcessPayload], processes),
        )
    return build_action_response(request_id)


FWS_SOCKETIO_SIO = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    max_http_buffer_size=8 * 1024 * 1024,
)
FWS_SOCKETIO_SIO.register_namespace(FwsSocketIoNamespace(FWS_SOCKETIO_NAMESPACE))
FWS_SOCKETIO_ASGI_APP = socketio.ASGIApp(FWS_SOCKETIO_SIO, socketio_path="")


def mount_fws_socketio_runtime(app: FastAPI) -> None:
    if getattr(app.state, "_framework_shells_fws_socketio_runtime_mounted", False):
        return
    os.environ["FRAMEWORK_SHELLS_FWS_SOCKETIO_SERVER_PID"] = str(os.getpid())
    app.mount(FWS_SOCKETIO_SOCKET_PATH, FWS_SOCKETIO_ASGI_APP)
    setattr(app.state, "_framework_shells_fws_socketio_runtime_mounted", True)


def mount_fws_dashboard_runtime(app: FastAPI) -> None:
    if getattr(app.state, "_framework_shells_fws_dashboard_runtime_mounted", False):
        return
    from .fws_ui import router as fws_ui_router

    app.include_router(fws_ui_router)
    mount_fws_socketio_runtime(app)
    setattr(app.state, "_framework_shells_fws_dashboard_runtime_mounted", True)
