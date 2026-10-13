"""Pure persistent-shell policy; not yet wired into launch or signal delivery."""

from collections.abc import Mapping, Set
from dataclasses import dataclass
from enum import StrEnum


class ShutdownIntent(StrEnum):
    FRAMEWORK_TEARDOWN = "framework_teardown"
    EXPLICIT_STOP = "explicit_stop"


def parse_survival(value: object = False) -> bool:
    """Parse an already-rendered flag. Missing fields default to false at callers."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1"):
            return True
        if normalized in ("false", "0"):
            return False
    raise ValueError("survive_shutdown must be a boolean or true/false/1/0 string")


@dataclass(frozen=True)
class ShutdownSelection:
    terminate: frozenset[int]
    preserved: frozenset[int]


def select_shutdown(
    parents: Mapping[int, int | None],
    roots: Set[int],
    survival_roots: Set[int],
    intent: ShutdownIntent,
) -> ShutdownSelection:
    """Select snapshot PIDs, not signal groups; protected descendants win at teardown."""
    def descendants(seeds: Set[int]) -> set[int]:
        children: dict[int, list[int]] = {}
        for pid, parent in parents.items():
            if parent is not None:
                children.setdefault(parent, []).append(pid)
        found: set[int] = set()
        pending = list(seeds)
        while pending:
            pid = pending.pop()
            if pid in found:
                continue
            found.add(pid)
            pending.extend(children.get(pid, ()))
        return found.intersection(parents)

    selected = descendants(roots)
    protected = (
        descendants(survival_roots)
        if intent == ShutdownIntent.FRAMEWORK_TEARDOWN
        else set[int]()
    )
    return ShutdownSelection(frozenset(selected - protected), frozenset(selected & protected))


@dataclass(frozen=True)
class WriterLease:
    instance: str
    generation: int
    connection: str


class AttachmentState:
    """Supervisor-owned state; caller serializes transitions and authenticates first."""

    def __init__(self, instance: str) -> None:
        if not instance:
            raise ValueError("instance identity must not be empty")
        self.instance: str = instance
        self.generation: int = 0
        self.owner: WriterLease | None = None
        self.stdin_closed: bool = False
        self.exited: bool = False

    def acquire(self, connection: str, *, takeover: bool = False) -> WriterLease:
        if not connection or self.exited or self.stdin_closed:
            raise ValueError("stdin unavailable")
        if self.owner is not None and not takeover:
            raise ValueError("stdin already owned")
        if self.generation == (1 << 64) - 1:
            raise ValueError("attachment generation exhausted")
        self.generation += 1
        self.owner = WriterLease(self.instance, self.generation, connection)
        return self.owner

    def can_write(self, lease: WriterLease) -> bool:
        return not self.exited and not self.stdin_closed and self.owner == lease

    def detach(self, lease: WriterLease) -> None:
        if self.owner == lease:
            self.owner = None

    def detach_connection(self, connection: str) -> None:
        if self.owner is not None and self.owner.connection == connection:
            self.owner = None

    def eof(self, lease: WriterLease) -> None:
        if not self.can_write(lease):
            raise ValueError("stale or unavailable writer")
        self.stdin_closed = True
        self.owner = None

    def child_exited(self) -> None:
        self.exited = True
        self.owner = None
