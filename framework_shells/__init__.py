"""Framework Shells - Standalone process orchestration library."""

from .manager import FrameworkShellManager
from .record import ShellRecord
from .pty import PTYState, PipeState
from .events import get_event_bus, EventBus, ShellEvent, EventType
from .store import RuntimeStore
from .auth import get_secret, derive_api_token, derive_runtime_id
from .hooks import ShellLifecycleHooks
from .process_snapshot import ProcessRecord, ProcessSnapshot, ExternalProcessProvider, ProcfsProcessProvider
from .shutdown import ShutdownPolicy, plan_shutdown, shutdown_snapshot
from .shellspec import ShellSpec, ReadinessProbe, RestartPolicy, load_shellspec, render_shellspec
from .shared_manager import FrameworkShellManagerConfig, get_manager

__all__ = [
    "FrameworkShellManager",
    "ShellRecord", 
    "PTYState",
    "PipeState",
    "get_event_bus",
    "EventBus",
    "ShellEvent",
    "EventType",
    "RuntimeStore",
    "get_secret",
    "derive_api_token",
    "derive_runtime_id",
    "ShellLifecycleHooks",
    "ProcessRecord",
    "ProcessSnapshot",
    "ExternalProcessProvider",
    "ProcfsProcessProvider",
    "ShutdownPolicy",
    "plan_shutdown",
    "shutdown_snapshot",
    "ShellSpec",
    "ReadinessProbe",
    "RestartPolicy",
    "load_shellspec",
    "render_shellspec",
    "FrameworkShellManagerConfig",
    "get_manager",
]
