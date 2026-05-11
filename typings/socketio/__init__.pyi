from collections.abc import Mapping
from typing import Any

class AsyncNamespace:
    namespace: str | None
    def __init__(self, namespace: str | None = None) -> None: ...
    async def emit(
        self,
        event: str,
        data: object | None = None,
        *,
        to: str | None = None,
        room: str | None = None,
        namespace: str | None = None,
        callback: object | None = None,
        **kwargs: object,
    ) -> None: ...
    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None: ...
    async def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None: ...
    async def get_session(self, sid: str, namespace: str | None = None) -> dict[str, object]: ...
    async def save_session(
        self,
        sid: str,
        session: Mapping[str, object],
        namespace: str | None = None,
    ) -> None: ...

class AsyncServer:
    def __init__(self, **kwargs: object) -> None: ...
    async def emit(
        self,
        event: str,
        data: object | None = None,
        *,
        namespace: str | None = None,
        room: str | None = None,
        to: str | None = None,
        callback: object | None = None,
        **kwargs: object,
    ) -> None: ...
    def register_namespace(self, namespace_handler: AsyncNamespace) -> None: ...

class AsyncClient:
    connected: bool
    def __init__(self, **kwargs: object) -> None: ...
    def on(self, event: str, handler: object | None = None, *, namespace: str | None = None) -> object: ...
    async def connect(
        self,
        url: str,
        *,
        auth: object | None = None,
        namespaces: list[str] | None = None,
        socketio_path: str = "socket.io",
        transports: list[str] | None = None,
        wait: bool = True,
        wait_timeout: float | int = 1,
        **kwargs: object,
    ) -> None: ...
    async def emit(
        self,
        event: str,
        data: object | None = None,
        *,
        namespace: str | None = None,
        callback: object | None = None,
        **kwargs: object,
    ) -> None: ...

class ASGIApp:
    def __init__(
        self,
        socketio_server: AsyncServer,
        other_asgi_app: object | None = None,
        socketio_path: str = "socket.io",
        **kwargs: object,
    ) -> None: ...
    async def __call__(self, scope: object, receive: object, send: object) -> None: ...
