import argparse
import asyncio
import json
import os
import sys
import shutil
import hashlib
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import cast

from ..auth import derive_api_token, get_secret
from ..manager import FrameworkShellManager
from ..process_snapshot import ProcfsProcessProvider, ProcessSnapshot
from ..record import ShellRecord
from ..shutdown import ShutdownPolicy, shutdown_snapshot
from ..shellspec import load_shellspec
from ..orchestrator import Orchestrator

JSONMap = dict[str, object]

def compute_standalone_fingerprint() -> str:
    """Compute fingerprint based on current working directory (assuming repo root)."""
    cwd = Path.cwd().resolve()
    return hashlib.sha256(str(cwd).encode()).hexdigest()[:16]

def _default_base_dir() -> Path:
    return Path.home() / ".cache" / "framework_shells"

def get_base_dir() -> Path:
    base_dir = os.environ.get("FRAMEWORK_SHELLS_BASE_DIR")
    if base_dir:
        return Path(os.path.expanduser(base_dir)).resolve()
    return _default_base_dir()

def load_stored_secret(fingerprint: str) -> str | None:
    """Try to load secret from stored file for this fingerprint."""
    secret_file = get_base_dir() / "runtimes" / fingerprint / "secret"
    if secret_file.exists():
        try:
            return secret_file.read_text().strip()
        except Exception:
            pass
    return None

def setup_environment():
    """Auto-detect fingerprint and secret if not set."""
    had_secret_env = "FRAMEWORK_SHELLS_SECRET" in os.environ
    # Compute fingerprint from cwd if not set
    if "FRAMEWORK_SHELLS_REPO_FINGERPRINT" not in os.environ:
        fp = compute_standalone_fingerprint()
        os.environ["FRAMEWORK_SHELLS_REPO_FINGERPRINT"] = fp
    else:
        fp = os.environ["FRAMEWORK_SHELLS_REPO_FINGERPRINT"]
    
    # Try to load stored secret if not set
    if "FRAMEWORK_SHELLS_SECRET" not in os.environ:
        secret = load_stored_secret(fp)
        if secret:
            os.environ["FRAMEWORK_SHELLS_SECRET"] = secret
        else:
            print(
                "Info: No stored secret found. Creating and storing a new secret for this repo fingerprint."
            )
            os.environ["FRAMEWORK_SHELLS_SECRET"] = "temporary_secret_" + os.urandom(8).hex()

    # Best-effort persistence: make subsequent CLI invocations share the same runtime.
    try:
        secret_file = get_base_dir() / "runtimes" / fp / "secret"
        secret_file.parent.mkdir(parents=True, exist_ok=True)
        secret = os.environ.get("FRAMEWORK_SHELLS_SECRET", "")
        if secret:
            if had_secret_env or not secret_file.exists():
                _ = secret_file.write_text(secret)
                try:
                    os.chmod(secret_file, 0o600)
                except Exception:
                    pass
    except Exception:
        pass

def _parse_env_kv(pairs: list[str] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for item in pairs or []:
        if "=" not in item:
            raise ValueError(f"Invalid --env value {item!r} (expected KEY=VALUE)")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid --env value {item!r} (empty KEY)")
        out[k] = v
    return out

def _print_shell_candidates(cands: list[ShellRecord]) -> None:
    for s in cands:
        try:
            backend = (
                s.backend
                or (
                    "dtach"
                    if s.uses_dtach
                    else ("pipe" if s.uses_pipes else ("pty" if s.uses_pty else "proc"))
                )
            )
            print(f"- {s.id}  label={s.label or '-'}  status={s.status}  pid={s.pid or '-'}  backend={backend}")
        except Exception:
            try:
                print(f"- {s.id}: {s}")
            except Exception:
                pass


def _arg_str(args: argparse.Namespace, name: str, default: str = "") -> str:
    value = getattr(args, name, default)
    return default if value is None else str(value)


def _arg_bool(args: argparse.Namespace, name: str, default: bool = False) -> bool:
    return bool(getattr(args, name, default))


def _arg_int(args: argparse.Namespace, name: str, default: int) -> int:
    value = getattr(args, name, default)
    return _arg_like_int(value, default=default)


def _arg_like_int(value: object, default: int = 0) -> int:
    if not isinstance(value, int | float | str) or isinstance(value, bool):
        return default
    try:
        return int(value)
    except Exception:
        return default


def _arg_float(args: argparse.Namespace, name: str, default: float) -> float:
    value = getattr(args, name, default)
    return _arg_like_float(value, default=default)


def _arg_like_float(value: object, default: float = 0.0) -> float:
    if not isinstance(value, int | float | str) or isinstance(value, bool):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _arg_str_list(args: argparse.Namespace, name: str) -> list[str]:
    value = getattr(args, name, None)
    return [str(item) for item in cast(list[object], value)] if isinstance(value, list) else []


def _read_write_data(args: argparse.Namespace) -> str:
    data_arg = getattr(args, "data", None)
    if data_arg == "-":
        return sys.stdin.read()
    if data_arg is None:
        if not sys.stdin.isatty():
            return sys.stdin.read()
        raise SystemExit("fws write requires DATA or '-' for stdin")
    return str(data_arg)


def _normalize_write_data(data: str, *, compact_json: bool) -> str:
    if not compact_json:
        return data
    try:
        parsed = cast(object, json.loads(data))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid JSON for --json: {exc}") from exc
    return json.dumps(parsed, separators=(",", ":"), ensure_ascii=False)


def _fws_api_base_url(args: argparse.Namespace) -> str | None:
    explicit = _arg_str(args, "api_url").strip()
    if explicit:
        return explicit.rstrip("/")
    for name in ("FRAMEWORK_SHELLS_API_URL", "FRAMEWORK_SHELLS_FWS_SOCKETIO_URL", "TE_FRAMEWORK_URL"):
        value = os.environ.get(name, "").strip()
        if value:
            return value.rstrip("/")
    return None


def _post_shell_input_sync(base_url: str, shell_id: str, payload: JSONMap) -> JSONMap:
    quoted_shell_id = urllib.parse.quote(shell_id, safe="")
    url = f"{base_url}/api/framework_shells/{quoted_shell_id}/input"
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-Framework-Key": derive_api_token(get_secret()),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"FWS API write failed ({exc.code}): {message}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"FWS API write failed: {exc}") from exc

    parsed = cast(object, json.loads(raw or "{}"))
    if not isinstance(parsed, dict):
        raise RuntimeError("FWS API returned a non-object response")
    return cast(JSONMap, parsed)


async def _post_shell_input(base_url: str, shell_id: str, payload: JSONMap) -> JSONMap:
    return await asyncio.to_thread(_post_shell_input_sync, base_url, shell_id, payload)


async def _resolve_shell_target(
    manager: FrameworkShellManager,
    target: str,
    *,
    allow_exited: bool,
) -> ShellRecord:
    target = str(target or "").strip()
    if not target:
        raise SystemExit("Target shell is required.")

    rec = await manager.get_shell(target)
    if rec and (allow_exited or rec.status == "running"):
        return rec

    shells = await manager.list_shells()
    if not allow_exited:
        shells = [s for s in shells if s.status == "running"]

    label_matches = [s for s in shells if (s.label or "") == target]
    if len(label_matches) == 1:
        return label_matches[0]
    if len(label_matches) > 1:
        print(f"Ambiguous label {target!r}; matches multiple shells:")
        _print_shell_candidates(label_matches)
        raise SystemExit(2)

    prefix_matches = [s for s in shells if s.id.startswith(target)]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    if len(prefix_matches) > 1:
        print(f"Ambiguous id prefix {target!r}; matches multiple shells:")
        _print_shell_candidates(prefix_matches)
        raise SystemExit(2)

    raise SystemExit(f"Shell not found: {target!r}")

async def _terminate_one(
    manager: FrameworkShellManager,
    rec: ShellRecord,
    *,
    tree: bool,
    force: bool,
    depth: int,
    grace_s: float,
    sigkill_timeout_s: float,
) -> None:
    if not tree:
        await manager.terminate_shell(rec.id, force=force)
        return

    pid = getattr(rec, "pid", None)
    if not pid:
        await manager.terminate_shell(rec.id, force=force)
        return

    depth = max(1, int(depth or 8))
    try:
        manager._procfs_provider = ProcfsProcessProvider(max_depth=depth)  # type: ignore[attr-defined]
    except Exception:
        pass

    policy = ShutdownPolicy(
        sigterm_timeout_s=0.0 if force else max(0.0, float(grace_s)),
        sigkill_timeout_s=max(0.0, float(sigkill_timeout_s)),
    )
    snapshot = await manager.build_process_snapshot(shells=[rec], include_procfs_descendants=True)
    _ = await shutdown_snapshot(
        snapshot,
        manager=manager,
        policy=policy,
        root_pids=[int(pid)],
        log=print,
    )

def main():
    parser = argparse.ArgumentParser(description="Framework Shells CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # fs up [spec.yaml]
    up_parser = subparsers.add_parser("up", help="Apply a shell specification")
    _ = up_parser.add_argument("spec", nargs="?", default="shells.yaml", help="Path to spec file")
    _ = up_parser.add_argument("--prune", action="store_true", help="Remove shells not in spec")
    
    # fs list
    list_parser = subparsers.add_parser("list", help="List running shells")
    _ = list_parser.add_argument("--stats", action="store_true", help="Include CPU/RSS stats (best-effort)")
    _ = list_parser.add_argument("--all", action="store_true", help="Include exited shells too")
    
    # fs down
    down_parser = subparsers.add_parser("down", help="Terminate shells")
    _ = down_parser.add_argument("spec", nargs="?", help="Optional spec file/dir; if provided, only those specs are terminated")
    _ = down_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    _ = down_parser.add_argument("--tree", action="store_true", help="Also terminate procfs descendants (best-effort)")
    _ = down_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    _ = down_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds for --tree (default: 2.0)")
    _ = down_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds for --tree (default: 2.0)")

    # fs shutdown-group <app_id>
    sg_parser = subparsers.add_parser("shutdown-group", help="Shutdown an app/group (UI-equivalent)")
    _ = sg_parser.add_argument("app_id", help="App/group id (matches derive_app_id())")
    _ = sg_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")

    # fs inspect <shell_id>
    inspect_parser = subparsers.add_parser("inspect", help="Inspect structured log events for a shell")
    _ = inspect_parser.add_argument("shell_id", help="Shell ID")
    _ = inspect_parser.add_argument("--stream", default="both", choices=["stdout", "stderr", "both"], help="Log stream to inspect")
    _ = inspect_parser.add_argument("--lines", type=int, default=200, help="Number of recent event containers to inspect")
    _ = inspect_parser.add_argument("--query", help="Raw text substring or regex filter")
    _ = inspect_parser.add_argument("--exclude-query", help="Exclude records matching this substring or regex")
    _ = inspect_parser.add_argument("--regex", action="store_true", help="Treat --query as a regex")
    _ = inspect_parser.add_argument("--ignore-case", action="store_true", help="Case-insensitive matching")
    _ = inspect_parser.add_argument("--format", choices=["plain", "json", "jsonrpc"], help="Filter by detected format")
    _ = inspect_parser.add_argument("--signature", help="Filter by event signature (supports * wildcards)")
    _ = inspect_parser.add_argument("--exclude-signature", help="Exclude event signatures (supports * wildcards)")
    _ = inspect_parser.add_argument("--io-metadata", action="store_true", help="Include sidecar I/O metadata records")
    _ = inspect_parser.add_argument("--stdin", action="store_true", help="Include stdin sidecar records with --io-metadata")
    _ = inspect_parser.add_argument("--timestamps", action="store_true", help="Include sidecar timestamps with --io-metadata")
    _ = inspect_parser.add_argument("--output-metadata", action="store_true", help="Include stdout/stderr chunk sidecar records with --io-metadata")
    _ = inspect_parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")

    # fs write <id|label> [data|-]
    write_parser = subparsers.add_parser("write", help="Write text to live shell stdin")
    _ = write_parser.add_argument("target", help="Shell ID, label, or unique ID prefix")
    _ = write_parser.add_argument("data", nargs="?", help="Text to write, or '-' to read from stdin")
    _ = write_parser.add_argument("--newline", action="store_true", help="Append a trailing newline")
    _ = write_parser.add_argument("--json", action="store_true", help="Parse and compact DATA as JSON before writing")
    _ = write_parser.add_argument("--json-output", action="store_true", help="Emit machine-readable write result")
    _ = write_parser.add_argument("--api-url", help="FWS API base URL; defaults to FRAMEWORK_SHELLS_API_URL, FRAMEWORK_SHELLS_FWS_SOCKETIO_URL, or TE_FRAMEWORK_URL")

    # fs terminate <id|label>
    term_parser = subparsers.add_parser("terminate", help="Terminate a single shell")
    _ = term_parser.add_argument("target", help="Shell ID, label, or unique ID prefix")
    _ = term_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    term_parser.set_defaults(tree=True)
    _ = term_parser.add_argument("--no-tree", dest="tree", action="store_false", help="Do not scan /proc for descendants")
    _ = term_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    _ = term_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds (default: 2.0)")
    _ = term_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds (default: 2.0)")
    _ = term_parser.add_argument("--purge", action="store_true", help="Also remove metadata/logs after termination")

    # fs rm <id|label>
    rm_parser = subparsers.add_parser("rm", help="Terminate (optional) and remove shell metadata/logs", aliases=["remove"])
    _ = rm_parser.add_argument("target", help="Shell ID, label, or unique ID prefix")
    _ = rm_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    rm_parser.set_defaults(tree=True)
    _ = rm_parser.add_argument("--no-tree", dest="tree", action="store_false", help="Do not scan /proc for descendants")
    _ = rm_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    _ = rm_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds (default: 2.0)")
    _ = rm_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds (default: 2.0)")
    
    # fs attach [id]
    attach_parser = subparsers.add_parser("attach", help="Attach to a legacy dtach shell")
    _ = attach_parser.add_argument("id", help="Shell ID or Label")

    # fs run -- <command...>
    run_parser = subparsers.add_parser("run", help="Spawn a one-off shell without a shellspec")
    _ = run_parser.add_argument("--backend", choices=["proc", "pty", "pipe", "dtach"], default="proc", help="Backend (default: proc; legacy dtach aliases to pty)")
    _ = run_parser.add_argument("--pty-mode", choices=["raw", "interactive"], default=None, help="PTY discipline for pty backends (legacy dtach requests also route to pty)")
    _ = run_parser.add_argument("--label", default=None, help="Optional shell label")
    _ = run_parser.add_argument("--cwd", default=None, help="Working directory")
    _ = run_parser.add_argument("--env", action="append", default=None, help="Environment override KEY=VALUE (repeatable)")
    _ = run_parser.add_argument("--subgroup", action="append", default=None, help="Subgroup tag (repeatable)")
    _ = run_parser.add_argument("--debug-io-metadata", action="store_true", help="Record opt-in stdin/output sidecar metadata for this shell")
    _ = run_parser.add_argument("--no-start", action="store_true", help="Create record only (do not start process)")
    _ = run_parser.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (prefix with --)")

    # fs tree
    tree_parser = subparsers.add_parser("tree", help="Show shell process trees (includes procfs descendants)")
    _ = tree_parser.add_argument("--all", action="store_true", help="Include exited shells (if pid still known)")
    _ = tree_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    setup_environment()

    try:
        asyncio.run(run_async(args))
    except KeyboardInterrupt:
        pass

async def run_async(args: argparse.Namespace) -> None:
    command = _arg_str(args, "command")
    if command != "up":
        os.environ.setdefault("FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER", "1")
    manager = FrameworkShellManager()

    if command == "run":
        cmd = _arg_str_list(args, "cmd")
        if cmd and cmd[0] == "--":
            cmd = cmd[1:]
        if not cmd:
            raise SystemExit("fws run requires a command. Example: fws run --backend pty -- bash -l -i")

        env = _parse_env_kv(_arg_str_list(args, "env"))
        subgroups = [x for x in _arg_str_list(args, "subgroup") if x.strip()]
        autostart = not _arg_bool(args, "no_start", False)
        backend = _arg_str(args, "backend", "proc")
        pty_mode = getattr(args, "pty_mode", None)
        debug: JSONMap = {"io_metadata": True} if _arg_bool(args, "debug_io_metadata", False) else {}

        if backend == "dtach":
            backend = "pty"

        if backend == "pty":
            rec = await manager.spawn_shell_pty(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, debug=debug, pty_mode=pty_mode, autostart=autostart)
        elif backend == "pipe":
            rec = await manager.spawn_shell_pipe(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, debug=debug, autostart=autostart)
        else:
            rec = await manager.spawn_shell(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, debug=debug, autostart=autostart)

        print(rec.id)
        return

    if command == "tree":
        depth = _arg_int(args, "depth", 8)
        if depth < 1:
            depth = 1
        # CLI convenience: allow deeper/shallower procfs scanning.
        try:
            manager._procfs_provider = ProcfsProcessProvider(max_depth=depth)  # type: ignore[attr-defined]
        except Exception:
            pass

        shells = await manager.list_shells()
        if not _arg_bool(args, "all", False):
            live: list[ShellRecord] = []
            for s in shells:
                if s.status != "running":
                    continue
                pid = s.pid
                if not pid:
                    continue
                if not await manager._is_pid_alive(pid):  # type: ignore[attr-defined]
                    continue
                live.append(s)
            shells = live

        described: list[dict[str, object]] = []
        for rec in shells:
            try:
                described.append(await manager.describe(rec))
            except Exception:
                described.append(rec.to_payload())

        snapshot: ProcessSnapshot = await manager.build_process_snapshot(shells=shells, include_procfs_descendants=True)
        processes = snapshot.processes

        children_by_parent: dict[int, list[int]] = {}
        for pid, proc in processes.items():
            if proc.parent_pid is None:
                continue
            try:
                ppid = int(proc.parent_pid)
                cpid = int(pid)
                if ppid == cpid:
                    continue
                children_by_parent.setdefault(ppid, []).append(cpid)
            except Exception:
                continue

        def backend_for(info: dict[str, object]) -> str:
            if info.get("backend"):
                return str(info.get("backend"))
            if info.get("uses_dtach"):
                return "dtach"
            if info.get("uses_pipes"):
                return "pipe"
            if info.get("uses_pty"):
                return "pty"
            return "proc"

        def render_node(pid: int, *, indent: str, shell_pid_set: set[int], visited: set[int]) -> None:
            if pid in visited:
                return
            visited.add(pid)
            proc = processes.get(pid)
            if not proc:
                return
            kind = proc.type or "process"
            label = proc.label or str(pid)
            marker = "[shell]" if pid in shell_pid_set else "[proc] "
            print(f"{indent}{marker} {pid:<6} {kind:<7} {label}")

            kids = children_by_parent.get(pid, [])
            for child_pid in sorted(kids):
                if child_pid == pid:
                    continue
                # Avoid duplicating shell roots under other shells in the listing.
                if child_pid in shell_pid_set and child_pid != pid:
                    continue
                render_node(child_pid, indent=indent + "  ", shell_pid_set=shell_pid_set, visited=visited)

        shell_pid_set: set[int] = set()
        for x in described:
            pid = x.get("pid")
            if not pid:
                continue
            try:
                shell_pid_set.add(_arg_like_int(pid))
            except Exception:
                continue
        for info in sorted(described, key=lambda x: (str(x.get("label") or ""), str(x.get("id") or ""))):
            sid = str(info.get("id") or "")
            label = str(info.get("label") or sid)
            status = str(info.get("status") or "")
            pid = info.get("pid")
            if not pid:
                print(f"{sid}  {label}  status={status}  pid=-  backend={backend_for(info)}")
                continue
            print(f"{sid}  {label}  status={status}  pid={pid}  backend={backend_for(info)}")
            render_node(_arg_like_int(pid), indent="  ", shell_pid_set=shell_pid_set, visited=set())
        return

    if command == "terminate":
        rec = await _resolve_shell_target(manager, _arg_str(args, "target"), allow_exited=False)
        print(f"Terminating {rec.id}...")
        await _terminate_one(
            manager,
            rec,
            tree=_arg_bool(args, "tree", True),
            force=_arg_bool(args, "force", False),
            depth=_arg_int(args, "depth", 8),
            grace_s=_arg_float(args, "grace", 2.0),
            sigkill_timeout_s=_arg_float(args, "kill_wait", 2.0),
        )
        if _arg_bool(args, "purge", False):
            await manager.remove_shell(rec.id, force=_arg_bool(args, "force", False))
        return

    if command in {"rm", "remove"}:
        rec = await _resolve_shell_target(manager, _arg_str(args, "target"), allow_exited=True)
        if rec.status == "running":
            print(f"Terminating {rec.id}...")
            await _terminate_one(
                manager,
                rec,
                tree=_arg_bool(args, "tree", True),
                force=_arg_bool(args, "force", False),
                depth=_arg_int(args, "depth", 8),
                grace_s=_arg_float(args, "grace", 2.0),
                sigkill_timeout_s=_arg_float(args, "kill_wait", 2.0),
            )
        print(f"Removing {rec.id}...")
        await manager.remove_shell(rec.id, force=_arg_bool(args, "force", False))
        return
    
    if command == "up":
        spec_path = Path(_arg_str(args, "spec", "shells.yaml"))
        if not spec_path.exists():
            print(f"Spec file not found: {spec_path}")
            sys.exit(1)
            
        print(f"Loading specs from {spec_path}...")
        specs_map = load_shellspec(spec_path)
        specs = list(specs_map.values())
        orch = Orchestrator(manager)
        await orch.apply(specs, prune=_arg_bool(args, "prune", False))
        print(f"Applied {len(specs)} specs.")
        
        # Keep alive for managing PTYs?
        # If we exit, the manager exits, PTYs die (unless dtach).
        # If backend=dtach, we can exit.
        # If backend=pty, we must stay running.
        # Check backend of shells.
        # For now, simplistic: wait forever if any non-dtach?
        # Or just wait forever to act as the daemon.
        print("Manager running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)

    elif command == "list":
        shells = await manager.list_shells()
        if not _arg_bool(args, "all", False):
            shells = [s for s in shells if s.status == "running" and s.pid]
        show_stats = _arg_bool(args, "stats", False)
        if show_stats:
            print(f"{'ID':<20} {'SPEC':<14} {'LABEL':<15} {'STATUS':<10} {'PID':<6} {'CPU':>6} {'RSS':>9} {'BACKEND'}")
        else:
            print(f"{'ID':<20} {'SPEC':<14} {'LABEL':<15} {'STATUS':<10} {'PID':<6} {'BACKEND'}")
        for s in shells:
            backend = (
                getattr(s, "backend", None)
                or (
                    "dtach"
                    if getattr(s, "uses_dtach", False)
                    else ("pipe" if getattr(s, "uses_pipes", False) else ("pty" if getattr(s, "uses_pty", False) else "proc"))
                )
            )
            if not show_stats:
                print(f"{s.id:<20} {(getattr(s, 'spec_id', None) or '-'): <14} {s.label or '-':<15} {s.status:<10} {s.pid or '-':<6} {backend}")
                continue
            try:
                info = await manager.describe(s)
                stats_obj = info.get("stats")
                stats = cast(dict[str, object], stats_obj) if isinstance(stats_obj, dict) else {}
                cpu = stats.get("cpu_percent")
                rss = stats.get("memory_rss")
                cpu_s = "-" if cpu is None else f"{_arg_like_float(cpu):.1f}%"
                rss_s = "-" if rss is None else f"{_arg_like_int(rss) // (1024 * 1024)}MiB"
            except Exception:
                cpu_s = "-"
                rss_s = "-"
            print(f"{s.id:<20} {(getattr(s, 'spec_id', None) or '-'): <14} {s.label or '-':<15} {s.status:<10} {s.pid or '-':<6} {cpu_s:>6} {rss_s:>9} {backend}")

    elif command == "down":
        spec_ids = None
        spec_arg = _arg_str(args, "spec")
        if spec_arg:
            spec_path = Path(spec_arg)
            specs_map = load_shellspec(spec_path)
            spec_ids = set(specs_map.keys())

        shells = await manager.list_shells()
        selected: list[ShellRecord] = []
        for s in shells:
            if spec_ids is not None and s.spec_id not in spec_ids:
                continue
            if s.status != "running":
                continue
            pid = s.pid
            if not pid or not await manager._is_pid_alive(pid):  # type: ignore[attr-defined]
                continue
            selected.append(s)

        if not selected:
            return

        if _arg_bool(args, "tree", False):
            depth = max(1, _arg_int(args, "depth", 8))
            try:
                manager._procfs_provider = ProcfsProcessProvider(max_depth=depth)  # type: ignore[attr-defined]
            except Exception:
                pass

            policy = ShutdownPolicy(
                sigterm_timeout_s=0.0 if _arg_bool(args, "force", False) else max(0.0, _arg_float(args, "grace", 2.0)),
                sigkill_timeout_s=max(0.0, _arg_float(args, "kill_wait", 2.0)),
            )
            snapshot = await manager.build_process_snapshot(shells=selected, include_procfs_descendants=True)
            _ = await shutdown_snapshot(snapshot, manager=manager, policy=policy, log=print)
            return

        for s in selected:
            print(f"Terminating {s.id}...")
            await manager.terminate_shell(s.id, force=_arg_bool(args, "force", False))
            
    elif command == "shutdown-group":
        app_id = _arg_str(args, "app_id").strip()
        if not app_id:
            print("Missing app_id")
            sys.exit(1)
        result = await manager.shutdown_app_group(app_id)
        if _arg_bool(args, "json", False):
            print(json.dumps(result, sort_keys=True))
            return
        data = result.get("data")
        data_map = cast(dict[str, object], data) if isinstance(data, dict) else {}
        root_pids = data_map.get("root_pids")
        print(f"Shutdown group {app_id} (root_pids={root_pids or []})")
        return

    elif command == "inspect":
        try:
            result = await manager.inspect_logs(
                _arg_str(args, "shell_id").strip(),
                stream=_arg_str(args, "stream", "both"),
                lines=max(0, _arg_int(args, "lines", 200)),
                query=getattr(args, "query", None),
                exclude_query=getattr(args, "exclude_query", None),
                regex=_arg_bool(args, "regex", False),
                ignore_case=_arg_bool(args, "ignore_case", False),
                format=getattr(args, "format", None),
                signature=getattr(args, "signature", None),
                exclude_signature=getattr(args, "exclude_signature", None),
                include_io_metadata=_arg_bool(args, "io_metadata", False),
                include_stdin=_arg_bool(args, "stdin", False),
                include_timestamps=_arg_bool(args, "timestamps", False),
                include_output_metadata=_arg_bool(args, "output_metadata", False),
            )
        except KeyError:
            print("Shell not found")
            sys.exit(1)
        except ValueError as exc:
            print(str(exc))
            sys.exit(1)
        if _arg_bool(args, "json", False):
            print(json.dumps(result, sort_keys=True))
            return

        print(
            f"Inspect {result.get('shell_id')} stream={result.get('stream')} "
            f"format={result.get('format') or '-'} signature={result.get('signature') or '-'}"
        )
        for stream_name in ("stdout", "stderr"):
            inspect_payload = cast(object, result.get(stream_name))
            if not isinstance(inspect_payload, dict):
                continue
            payload_map = cast(dict[str, object], inspect_payload)
            records_obj = payload_map.get("records")
            records = cast(list[object], records_obj) if isinstance(records_obj, list) else []
            print(
                f"{stream_name}: events={payload_map.get('event_count', 0)} "
                f"matched={len(records)} partial_head={payload_map.get('partial_head', False)}"
            )
            summary = payload_map.get("summary")
            summary_map = cast(dict[str, object], summary) if isinstance(summary, dict) else {}
            top_signatures_obj = summary_map.get("top_signatures")
            top_signatures = cast(list[object], top_signatures_obj) if isinstance(top_signatures_obj, list) else []
            for item_obj in top_signatures[:5]:
                item = cast(dict[str, object], item_obj) if isinstance(item_obj, dict) else {}
                try:
                    print(f"  {item.get('signature')}: {item.get('count')}")
                except Exception:
                    continue
        return

    elif command == "write":
        target = _arg_str(args, "target").strip()
        if not target:
            raise SystemExit("Target shell is required.")
        rec = await _resolve_shell_target(manager, target, allow_exited=False)
        shell_id = rec.id
        data = _normalize_write_data(_read_write_data(args), compact_json=_arg_bool(args, "json", False))
        payload: JSONMap = {
            "data": data,
            "append_newline": _arg_bool(args, "newline", False),
            "source": "cli",
        }

        explicit_api_base_url = _fws_api_base_url(args)
        api_base_url = explicit_api_base_url or "http://127.0.0.1:8089"
        api_error: Exception | None = None
        try:
            result = await _post_shell_input(api_base_url, shell_id, payload)
        except Exception as exc:
            api_error = exc
            if explicit_api_base_url:
                print(str(exc))
                sys.exit(1)
            try:
                result = await manager.write_to_shell(
                    shell_id,
                    str(payload.get("data") or ""),
                    append_newline=bool(payload.get("append_newline", False)),
                    source="cli",
                )
            except Exception as direct_exc:
                if "Live input unavailable" in str(direct_exc):
                    print(str(api_error))
                else:
                    print(str(direct_exc))
                sys.exit(1)

        if _arg_bool(args, "json_output", False):
            print(json.dumps(result, sort_keys=True))
            return
        data_obj = result.get("data")
        result_data = cast(JSONMap, data_obj) if isinstance(data_obj, dict) else result
        bytes_written = result_data.get("bytes_written")
        print(f"Wrote {bytes_written if bytes_written is not None else '?'} byte(s) to {shell_id}")
        return

    elif command == "attach":
        # Check specific shell
        target_id = _arg_str(args, "id")
        record = await manager.find_shell_by_label(target_id) or await manager.get_shell(target_id)
        if not record:
             print("Shell not found")
             sys.exit(1)
        
        if not getattr(record, "uses_dtach", False):
             print("Shell is not a legacy dtach shell. Attach is only available for legacy dtach sessions.")
             sys.exit(1)
             
        socket_path = manager.sockets_dir / f"{record.id}.sock"
        if not socket_path.exists():
             print("Socket not found")
             sys.exit(1)
             
        # Exec dtach -a
        # This replaces the CLI process with dtach
        dtach_bin = shutil.which("dtach") or "dtach"
        os.execvp(dtach_bin, [dtach_bin, "-a", str(socket_path)])

if __name__ == "__main__":
    main()
