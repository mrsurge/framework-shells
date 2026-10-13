from __future__ import annotations

import json
import os
from typing import cast
import unittest
from unittest.mock import AsyncMock, patch

from httpx import ASGITransport, AsyncClient, Client
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import Message, Scope
from starlette.testclient import TestClient

from framework_shells.api.application import create_fws_application, mount_fws_http_api
from framework_shells.api import fws_ui, http_router, socketio_backend
from framework_shells.api.http_support import http_route
from framework_shells.auth import derive_api_token
from framework_shells.manager import FrameworkShellManager
from framework_shells.events import EventType, ShellEvent, get_event_bus

JsonDict = dict[str, object]


class Window:
    def to_dict(self) -> JsonDict:
        return {"generation": "test-generation", "records": [], "total": 0}


class Manager:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []
        self.window_error: str | None = None

    async def list_shells(self) -> list[object]:
        return []

    async def get_shell(self, _shell_id: str) -> None:
        return None

    async def terminate_shell(self, shell_id: str, *, force: bool = False) -> None:
        self.calls.append(("terminate", (shell_id, force)))

    async def remove_shell(self, shell_id: str, *, force: bool = False) -> bool:
        self.calls.append(("remove", (shell_id, force)))
        return True

    async def shutdown_app_group(self, app_id: str) -> JsonDict:
        self.calls.append(("shutdown", app_id))
        return {"ok": True}

    async def get_log_window(self, _shell_id: str, **kwargs: object) -> Window:
        self.calls.append(("window", kwargs))
        if self.window_error:
            raise ValueError(self.window_error)
        return Window()

    async def get_log_raw(self, _shell_id: str, _reference: object, **_kwargs: object) -> bytes:
        return b"{}"

    async def inspect_logs(self, _shell_id: str, **kwargs: object) -> JsonDict:
        self.calls.append(("inspect", kwargs))
        return {"events": []}


class HttpApplicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.environment = patch.dict(os.environ, {"FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER": "1"})
        _ = self.environment.start()
        self.addCleanup(self.environment.stop)
        self.manager = Manager()
        self.provider = patch.object(http_router, "get_manager_dep",
            AsyncMock(return_value=cast(FrameworkShellManager, self.manager)))
        _ = self.provider.start()
        self.addCleanup(self.provider.stop)
        self.secret = patch.object(http_router, "get_secret", return_value="http-test-secret")
        _ = self.secret.start()
        self.addCleanup(self.secret.stop)
        self.app = create_fws_application()
        self.client = AsyncClient(transport=ASGITransport(app=self.app), base_url="http://fixture")
        self.addAsyncCleanup(self.client.aclose)
        self.auth = {"X-Framework-Key": derive_api_token("http-test-secret")}

    async def test_validation_matches_previous_boundary(self) -> None:
        response = await self.client.get("/api/framework_shells?include_stats=bad")
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json(), {"detail": [{"type": "bool_parsing",
            "loc": ["query", "include_stats"], "msg": "Input should be a valid boolean, unable to interpret input",
            "input": "bad"}]})
        response = await self.client.get("/api/framework_shells/logs/x/window?count=0")
        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json(), {"detail": [{"type": "greater_than_equal",
            "loc": ["query", "count"], "msg": "Input should be greater than or equal to 1",
            "input": "0", "ctx": {"ge": 1}}]})
        response = await self.client.get("/api/framework_shells/logs/x/raw")
        self.assertEqual(response.status_code, 422)
        errors = cast(list[JsonDict], cast(JsonDict, response.json())["detail"])
        self.assertEqual([row["loc"] for row in errors],
                         [["query", "generation"], ["query", "byte_start"], ["query", "byte_end"]])
        self.assertEqual(self.manager.calls, [])

    async def test_query_coercion_projection_and_stale_status(self) -> None:
        response = await self.client.get("/api/framework_shells?include_stats=YES")
        self.assertEqual(response.json(), {"ok": True, "data": []})
        response = await self.client.get("/api/framework_shells/logs/x/window?count=1.0")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(cast(JsonDict, self.manager.calls[-1][1])["count"], 1)
        self.manager.window_error = "stale generation"
        self.assertEqual((await self.client.get("/api/framework_shells/logs/x/window")).status_code, 409)
        self.manager.window_error = "invalid window"
        self.assertEqual((await self.client.get("/api/framework_shells/logs/x/window")).status_code, 400)

    async def test_auth_and_control_semantics(self) -> None:
        response = await self.client.post("/api/framework_shells/x/terminate")
        self.assertEqual(response.status_code, 403)
        self.assertEqual(self.manager.calls, [])
        response = await self.client.post("/api/framework_shells/x/terminate", headers=self.auth)
        self.assertEqual(response.json(), {"ok": True})
        self.assertEqual(self.manager.calls[-1], ("terminate", ("x", False)))
        response = await self.client.post("/api/framework_shells/x/action",
            headers={"Authorization": "Bearer " + self.auth["X-Framework-Key"]},
            json={"action": "terminate", "force": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.manager.calls[-1], ("terminate", ("x", True)))
        response = await self.client.delete("/api/framework_shells/x?force=1", headers=self.auth)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.manager.calls[-1], ("remove", ("x", True)))

    async def test_body_validation_and_disconnect_before_mutation(self) -> None:
        for data in (b"", b"null", b"[]", b"{"):
            response = await self.client.post("/api/framework_shells", headers=self.auth,
                content=data)
            self.assertEqual(response.status_code, 422)
        response = await self.client.post("/api/framework_shells", headers=self.auth, json={})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"detail": "Command required"})
        response = await self.client.post("/api/framework_shells", headers={**self.auth,
            "Content-Length": str(8 * 1024 * 1024 + 1)}, content=b"{}")
        self.assertEqual(response.status_code, 413)
        sent: list[Message] = []
        scope: Scope = {"type": "http", "method": "POST", "path": "/api/framework_shells",
            "root_path": "", "query_string": b"", "headers": []}
        async def receive() -> Message:
            return {"type": "http.disconnect"}
        async def send(message: Message) -> None:
            sent.append(message)
        await self.app(scope, receive, send)
        self.assertEqual(sent, [])
        self.assertEqual(self.manager.calls, [])

    async def test_stdin_control_and_inspect_overlay(self) -> None:
        with patch.object(socketio_backend, "write_shell_input_control",
                          AsyncMock(return_value={"written": 3})) as write:
            response = await self.client.post("/api/framework_shells/x/input", headers=self.auth,
                json={"data": "abc", "append_newline": True})
            self.assertEqual(response.json(), {"ok": True, "data": {"written": 3}})
            write.assert_awaited_once_with("x", "abc", append_newline=True, eof=False, source="api")
        response = await self.client.get("/api/framework_shells/logs/x/inspect?include_stdin=on&include_timestamps=1")
        self.assertEqual(response.status_code, 200)
        params = cast(JsonDict, self.manager.calls[-1][1])
        self.assertIs(params["include_stdin"], True)
        self.assertIs(params["include_timestamps"], True)

    async def test_dashboard_static_redirects_and_method_policy(self) -> None:
        response = await self.client.get("/fws")
        self.assertEqual((response.status_code, response.headers["location"]), (308, "/fws/"))
        response = await self.client.get("/fws/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("no-store", response.headers["cache-control"])
        response = await self.client.head("/fws/")
        self.assertEqual(response.status_code, 405)
        self.assertEqual(response.headers["allow"], "GET")
        response = await self.client.get("/fws/logs/x")
        self.assertEqual((response.status_code, response.headers["location"]), (307, "/fws/?log=x"))
        self.assertEqual((await self.client.get("/fws/static/%2e%2e/manager.py")).status_code, 404)
        count = len(self.app.routes)
        mount_fws_http_api(self.app)
        socketio_backend.mount_fws_dashboard_runtime(self.app)
        self.assertEqual(len(self.app.routes), count)

    async def test_ui_form_and_ajax_semantics(self) -> None:
        with patch.object(fws_ui, "_action_shutdown", AsyncMock()) as shutdown:
            response = await self.client.post("/fws/action/shutdown", data={"scope": "shells"})
            self.assertEqual(response.status_code, 303)
            shutdown.assert_awaited_once_with("shells")
        with patch.object(fws_ui, "_action_shutdown", AsyncMock()) as shutdown:
            response = await self.client.post("/fws/action/shutdown",
                files={"scope": (None, "tree")})
            self.assertEqual(response.status_code, 303)
            shutdown.assert_awaited_once_with("tree")
        with patch.object(fws_ui, "_action_shutdown", AsyncMock()) as shutdown:
            response = await self.client.post("/fws/action/shutdown", data={"scope": ""})
            self.assertEqual(response.status_code, 303)
            shutdown.assert_awaited_once_with("tree")
        with patch.object(fws_ui, "_action_shutdown", AsyncMock()) as shutdown:
            response = await self.client.post("/fws/action/shutdown",
                content=b"scope=tree", headers={"Content-Length": "65537"})
            self.assertEqual(response.status_code, 413)
            shutdown.assert_not_awaited()
        with patch.object(fws_ui, "_action_terminate_shell", AsyncMock()) as terminate:
            response = await self.client.post("/fws/action/shell/x/terminate", headers={"X-FWS-Ajax": "1"})
            self.assertEqual(response.status_code, 204)
            terminate.assert_awaited_once_with("x")

    async def test_existing_host_lifespan_and_root_mount(self) -> None:
        from contextlib import asynccontextmanager
        from collections.abc import AsyncIterator
        from starlette.applications import Starlette
        from starlette.routing import Mount

        stages: list[str] = []
        @asynccontextmanager
        async def lifespan(_app: Starlette) -> AsyncIterator[None]:
            stages.append("start")
            try:
                yield
            finally:
                stages.append("stop")

        host = Starlette(lifespan=lifespan, routes=[Mount("/runtime", app=self.app)])
        with TestClient(host) as client:
            typed_client = cast(Client, client)
            self.assertEqual(typed_client.get("/runtime/api/framework_shells").json(),
                             {"ok": True, "data": []})
            self.assertEqual(typed_client.get("/runtime/fws/static/fws.css").status_code, 200)
            self.assertEqual(stages, ["start"])
        self.assertEqual(stages, ["start", "stop"])

    async def test_response_send_failure_does_not_retry_dispatch(self) -> None:
        calls: list[bool] = []
        async def dispatch(_request: Request) -> Response:
            calls.append(True)
            return Response("body")
        route = http_route("/", dispatch, methods=["GET"])
        messages: list[str] = []
        async def send(message: Message) -> None:
            messages.append(str(message["type"]))
            if message["type"] == "http.response.body":
                raise OSError("disconnected during send")
        async def receive() -> Message:
            return {"type": "http.request", "body": b""}
        scope: Scope = {"type": "http", "method": "GET", "path": "/", "root_path": "",
            "query_string": b"", "headers": []}
        with self.assertRaisesRegex(OSError, "disconnected"):
            await route(scope, receive, send)
        self.assertEqual(calls, [True])
        self.assertEqual(messages.count("http.response.start"), 1)

    async def test_dashboard_websocket_open_and_lifecycle_notification(self) -> None:
        with patch.object(fws_ui, "_dashboard_state_parts", AsyncMock(return_value=([], []))):
            with TestClient(self.app) as client:
                with client.websocket_connect("/ws/fws") as socket:
                    socket.send_text(json.dumps({"jsonrpc": "2.0", "id": "open-1",
                        "method": "fws.dashboard.open", "params": {"view": "html"}}) + "\n")
                    response = cast(JsonDict, json.loads(socket.receive_text()))
                    self.assertEqual(response["id"], "open-1")
                    assert client.portal is not None
                    client.portal.call(get_event_bus().publish,
                        ShellEvent(EventType.SHELL_REMOVED, "test-removed"))
                    notification = cast(JsonDict, json.loads(socket.receive_text()))
                    self.assertEqual(notification["method"], "fws.shell.removed")


if __name__ == "__main__":
    _ = unittest.main()
