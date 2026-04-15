from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from typing import cast

import socketio

from .auth import derive_api_token, derive_runtime_id, get_secret
from .events import EventType, ShellEvent, get_event_bus
from .fws_socketio_contract import FWS_SOCKETIO_NAMESPACE, FWS_SOCKETIO_SOCKET_PATH
from .protocols.fws_ui import (
    FwsNotification,
    SHELL_CREATED_NOTIFICATION_METHOD,
    SHELL_EXITED_NOTIFICATION_METHOD,
    SHELL_REMOVED_NOTIFICATION_METHOD,
    SHELL_SPAWNED_NOTIFICATION_METHOD,
    SHELL_UPDATED_NOTIFICATION_METHOD,
    build_error_notification,
    build_logs_chunk_notification,
    build_logs_reset_notification,
)


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _is_socketio_server_process() -> bool:
    server_pid = os.environ.get("FRAMEWORK_SHELLS_FWS_SOCKETIO_SERVER_PID", "").strip()
    if server_pid:
        return server_pid == str(os.getpid())
    return _truthy_env("FRAMEWORK_SHELLS_FWS_SOCKETIO_SERVER")


def _default_framework_url() -> str:
    return (os.environ.get("FRAMEWORK_SHELLS_FWS_SOCKETIO_URL") or os.environ.get("TE_FRAMEWORK_URL") or "http://127.0.0.1:8089").rstrip("/")


def _notification_shell_id(notification: Mapping[str, object]) -> str | None:
    params = notification.get("params")
    if not isinstance(params, Mapping):
        return None
    shell_id = params.get("shell_id")
    return shell_id if isinstance(shell_id, str) else None


def _notification_requires_subscription(notification: Mapping[str, object]) -> bool:
    return notification.get("method") in {"fws.logs.chunk", "fws.logs.reset", "fws.error"}


class FwsSocketIoPeerRelay:
    def __init__(self) -> None:
        self.client = socketio.AsyncClient(
            reconnection=True,
            reconnection_attempts=0,
            reconnection_delay=1,
            reconnection_delay_max=5,
            logger=False,
        )
        self._connect_task: asyncio.Task[None] | None = None
        self._relay_task: asyncio.Task[None] | None = None
        self._subscriptions: set[str] = set()
        self._started = False
        self._connect_logged = False

        self.client.on("fws_peer_subscriptions", self._on_subscriptions, namespace=FWS_SOCKETIO_NAMESPACE)

    async def _on_subscriptions(self, payload: object) -> None:
        if not isinstance(payload, Mapping):
            self._subscriptions = set()
            return
        shell_ids = payload.get("shell_ids")
        if not isinstance(shell_ids, list):
            self._subscriptions = set()
            return
        self._subscriptions = {str(shell_id).strip() for shell_id in shell_ids if str(shell_id).strip()}

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._connect_task = asyncio.create_task(self._connect_loop())
        self._relay_task = asyncio.create_task(self._relay_loop())

    async def _connect_loop(self) -> None:
        socketio_path = FWS_SOCKETIO_SOCKET_PATH.lstrip("/")
        url = _default_framework_url()
        try:
            secret = get_secret()
        except Exception:
            return
        auth = {
            "role": "peer",
            "api_token": derive_api_token(secret),
            "runtime_id": derive_runtime_id(secret),
            "pid": str(os.getpid()),
        }

        while True:
            try:
                if not self.client.connected:
                    await self.client.connect(
                        url,
                        auth=auth,
                        namespaces=[FWS_SOCKETIO_NAMESPACE],
                        socketio_path=socketio_path,
                        transports=["websocket"],
                        wait=True,
                        wait_timeout=5,
                    )
                    self._connect_logged = False
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self._connect_logged:
                    print(f"[framework_shells] fws socketio peer connect failed: {exc}", flush=True)
                    self._connect_logged = True
                await asyncio.sleep(2)

    async def _relay_loop(self) -> None:
        bus = get_event_bus()
        queue = bus.subscribe()
        try:
            while True:
                event = await queue.get()
                notifications = await self._notifications_for_event(event)
                if not notifications or not self.client.connected:
                    continue
                for notification in notifications:
                    shell_id = _notification_shell_id(notification)
                    if _notification_requires_subscription(notification) and (not shell_id or shell_id not in self._subscriptions):
                        continue
                    try:
                        await self.client.emit("fws_peer_notification", notification, namespace=FWS_SOCKETIO_NAMESPACE)
                    except Exception:
                        break
        except asyncio.CancelledError:
            raise
        finally:
            bus.unsubscribe(queue)

    async def _notifications_for_event(self, event: ShellEvent) -> list[FwsNotification]:
        notifications: list[FwsNotification] = []

        lifecycle_notification = _dashboard_notification_for_event(event)
        if lifecycle_notification is not None:
            notifications.append(lifecycle_notification)

        if event.type == EventType.LOG_CHUNK:
            stream_name = str(event.data.get("stream") or "stdout")
            chunk = str(event.data.get("chunk") or "")
            if chunk and stream_name in {"stdout", "stderr"}:
                stream = "stderr" if stream_name == "stderr" else "stdout"
                notifications.append(build_logs_chunk_notification(event.shell_id, stream, chunk))
            return notifications

        if event.type == EventType.PTY_CHUNK:
            chunk = str(event.data.get("chunk") or "")
            if chunk:
                notifications.append(build_logs_chunk_notification(event.shell_id, "stdout", chunk))
            return notifications

        if event.type == EventType.LOG_RESET:
            stream_name = str(event.data.get("stream") or "stdout")
            if stream_name in {"stdout", "stderr"}:
                stream = "stderr" if stream_name == "stderr" else "stdout"
                notifications.append(build_logs_reset_notification(event.shell_id, stream))
            return notifications

        if event.type == EventType.SHELL_REMOVED:
            notifications.append(build_error_notification("Shell removed", code="shell_removed", shell_id=event.shell_id))

        return notifications


def _dashboard_notification_for_event(event: ShellEvent) -> FwsNotification | None:
    method_map = {
        EventType.SHELL_CREATED: SHELL_CREATED_NOTIFICATION_METHOD,
        EventType.SHELL_SPAWNED: SHELL_SPAWNED_NOTIFICATION_METHOD,
        EventType.SHELL_UPDATED: SHELL_UPDATED_NOTIFICATION_METHOD,
        EventType.SHELL_EXITED: SHELL_EXITED_NOTIFICATION_METHOD,
    }
    if event.type == EventType.SHELL_REMOVED:
        return cast(FwsNotification, cast(object, {
            "jsonrpc": "2.0",
            "method": SHELL_REMOVED_NOTIFICATION_METHOD,
            "params": {"shell_id": event.shell_id},
        }))
    method = method_map.get(event.type)
    if method is None:
        return None
    return cast(FwsNotification, cast(object, {
        "jsonrpc": "2.0",
        "method": method,
        "params": {"shell": dict(event.data)},
    }))


_peer_relay: FwsSocketIoPeerRelay | None = None


async def ensure_fws_socketio_peer_started() -> None:
    global _peer_relay
    if _is_socketio_server_process() or _truthy_env("FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER"):
        return
    if _peer_relay is None:
        _peer_relay = FwsSocketIoPeerRelay()
    await _peer_relay.start()
