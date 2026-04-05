from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from .record import ShellRecord


MaybeAwaitable = Awaitable[object] | object | None


@dataclass(frozen=True)
class ShellLifecycleHooks:
    """Optional callbacks for integrating FrameworkShellManager with host systems.

    This is intentionally generic (no IPC, no FastAPI, no repo-specific imports).
    Callbacks may be sync or async; exceptions are swallowed (best-effort).
    """

    # Called after a shell is confirmed running and persisted.
    on_shell_running: Callable[[ShellRecord], MaybeAwaitable] | None = None

    # Called when a running shell from a previous run is adopted.
    on_shell_adopted: Callable[[ShellRecord], MaybeAwaitable] | None = None

    # Called when a shell is marked exited (often discovered during adoption).
    # `last_pid` is the PID that was previously associated with the shell.
    on_shell_exited: Callable[[ShellRecord, int | None], MaybeAwaitable] | None = None
