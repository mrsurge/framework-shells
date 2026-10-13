"""Optional uvloop selection, only for loops explicitly owned by FWS."""
from __future__ import annotations

import asyncio
import importlib
import os
import sys
from collections.abc import Coroutine
from typing import Protocol, TypeVar, cast

Result = TypeVar("Result")


class _LoopModule(Protocol):
    def new_event_loop(self) -> asyncio.AbstractEventLoop: ...


def new_event_loop() -> asyncio.AbstractEventLoop:
    disabled = os.environ.get("FRAMEWORK_SHELLS_DISABLE_UVLOOP", "").strip().lower()
    if disabled in {"1", "true", "yes", "on"}:
        return asyncio.new_event_loop()
    try:
        module = importlib.import_module("uvloop")
    except ModuleNotFoundError as exc:
        if exc.name != "uvloop":
            raise
        return asyncio.new_event_loop()
    return cast(_LoopModule, module).new_event_loop()


def _installed_loop() -> asyncio.AbstractEventLoop:
    loop = new_event_loop()
    asyncio.set_event_loop(loop)
    return loop


def _run_legacy(coroutine: Coroutine[object, object, Result]) -> Result:
    """Python 3.9/3.10 equivalent of owned-runner cleanup, without a policy swap."""
    loop: asyncio.AbstractEventLoop | None = None
    try:
        loop = _installed_loop()
        return loop.run_until_complete(coroutine)
    finally:
        try:
            if loop is not None:
                tasks = asyncio.all_tasks(loop)
                for task in tasks:
                    _ = task.cancel()
                if tasks:
                    _ = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                for task in tasks:
                    if not task.cancelled() and task.exception() is not None:
                        loop.call_exception_handler({
                            "message": "unhandled exception during FWS loop shutdown",
                            "exception": task.exception(), "task": task,
                        })
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            if loop is not None:
                loop.close()
            coroutine.close()


def run(coroutine: Coroutine[object, object, Result]) -> Result:
    """Run and close an owned loop; never replace a caller's running loop."""
    try:
        _ = asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        coroutine.close()
        raise RuntimeError("FWS run() cannot be called from a running event loop")

    if sys.version_info < (3, 11):
        return _run_legacy(coroutine)
    try:
        with asyncio.Runner(loop_factory=_installed_loop) as runner:
            return runner.run(coroutine)
    finally:
        # A factory failure occurs before the runner takes coroutine ownership.
        coroutine.close()
        asyncio.set_event_loop(None)
