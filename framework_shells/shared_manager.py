from __future__ import annotations

import asyncio
from typing import TypedDict, Unpack

from .hooks import ShellLifecycleHooks
from .manager import FrameworkShellManager
from .process_snapshot import ExternalProcessProvider
from .store import RuntimeStore


class FrameworkShellManagerConfig(TypedDict, total=False):
    store: RuntimeStore
    max_app_shells: int
    max_service_shells: int
    run_id: str
    enable_dtach_proxy: bool
    signal_winch_on_resize: bool
    default_pty_mode: str
    process_hooks: ShellLifecycleHooks
    external_process_provider: ExternalProcessProvider
    enable_procfs_process_discovery: bool


_manager_instance: FrameworkShellManager | None = None
_manager_lock: asyncio.Lock | None = None
_manager_kwargs: FrameworkShellManagerConfig | None = None


def _get_lock() -> asyncio.Lock:
    global _manager_lock
    if _manager_lock is None:
        _manager_lock = asyncio.Lock()
    return _manager_lock


async def get_manager(**kwargs: Unpack[FrameworkShellManagerConfig]) -> FrameworkShellManager:
    global _manager_instance
    global _manager_kwargs
    if _manager_instance is not None:
        if kwargs and _manager_kwargs is not None and kwargs != _manager_kwargs:
            raise ValueError("FrameworkShellManager singleton already created with different configuration")
    else:
        async with _get_lock():
            if _manager_instance is None:
                _manager_kwargs = kwargs
                _manager_instance = FrameworkShellManager(**kwargs)
                manager = _manager_instance
                async with manager._get_lock():
                    await manager._adopt_orphaned_shells()

    assert _manager_instance is not None
    try:
        from .fws_socketio_peer import ensure_fws_socketio_peer_started

        await ensure_fws_socketio_peer_started()
    except Exception as exc:
        print(f"[framework_shells] failed to ensure fws socketio peer: {exc}", flush=True)
    return _manager_instance
