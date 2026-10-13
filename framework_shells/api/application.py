"""Standalone ASGI host and explicit mounting helpers."""
from __future__ import annotations

from starlette.applications import Starlette
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


async def _http_error(_request: Request, exception: Exception) -> Response:
    if not isinstance(exception, HTTPException):
        raise exception
    return JSONResponse({"detail": exception.detail}, status_code=exception.status_code,
                        headers=exception.headers)


def mount_fws_http_api(app: Starlette) -> None:
    if getattr(app.state, "_framework_shells_http_api_mounted", False):
        return
    from .http_router import router
    from .websocket import router as events_router
    app.router.routes.extend(router.routes)
    app.router.routes.extend(events_router.routes)
    setattr(app.state, "_framework_shells_http_api_mounted", True)


def create_fws_application() -> Starlette:
    from .socketio_backend import mount_fws_dashboard_runtime
    app = Starlette(exception_handlers={HTTPException: _http_error})
    mount_fws_http_api(app)
    mount_fws_dashboard_runtime(app)
    return app
