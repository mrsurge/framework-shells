import argparse
import asyncio
import os
import sys
import shutil
import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..manager import FrameworkShellManager
from ..process_snapshot import ProcfsProcessProvider, ProcessSnapshot
from ..shutdown import ShutdownPolicy, shutdown_snapshot
from ..shellspec import load_shellspec
from ..orchestrator import Orchestrator

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
                secret_file.write_text(secret)
                try:
                    os.chmod(secret_file, 0o600)
                except Exception:
                    pass
    except Exception:
        pass

def _parse_env_kv(pairs: Optional[List[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in pairs or []:
        if "=" not in item:
            raise ValueError(f"Invalid --env value {item!r} (expected KEY=VALUE)")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise ValueError(f"Invalid --env value {item!r} (empty KEY)")
        out[k] = v
    return out

def _print_shell_candidates(cands: List[Any]) -> None:
    for s in cands:
        try:
            backend = (
                "dtach"
                if getattr(s, "uses_dtach", False)
                else ("pipe" if getattr(s, "uses_pipes", False) else ("pty" if getattr(s, "uses_pty", False) else "proc"))
            )
            print(f"- {s.id}  label={s.label or '-'}  status={s.status}  pid={s.pid or '-'}  backend={backend}")
        except Exception:
            try:
                print(f"- {getattr(s, 'id', '-')}: {s}")
            except Exception:
                pass

async def _resolve_shell_target(
    manager: FrameworkShellManager,
    target: str,
    *,
    allow_exited: bool,
) -> Any:
    target = str(target or "").strip()
    if not target:
        raise SystemExit("Target shell is required.")

    rec = await manager.get_shell(target)
    if rec and (allow_exited or getattr(rec, "status", None) == "running"):
        return rec

    shells = await manager.list_shells()
    if not allow_exited:
        shells = [s for s in shells if getattr(s, "status", None) == "running"]

    label_matches = [s for s in shells if (getattr(s, "label", None) or "") == target]
    if len(label_matches) == 1:
        return label_matches[0]
    if len(label_matches) > 1:
        print(f"Ambiguous label {target!r}; matches multiple shells:")
        _print_shell_candidates(label_matches)
        raise SystemExit(2)

    prefix_matches = [s for s in shells if str(getattr(s, "id", "")).startswith(target)]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    if len(prefix_matches) > 1:
        print(f"Ambiguous id prefix {target!r}; matches multiple shells:")
        _print_shell_candidates(prefix_matches)
        raise SystemExit(2)

    raise SystemExit(f"Shell not found: {target!r}")

async def _terminate_one(
    manager: FrameworkShellManager,
    rec: Any,
    *,
    tree: bool,
    force: bool,
    depth: int,
    grace_s: float,
    sigkill_timeout_s: float,
) -> None:
    if not getattr(rec, "id", None):
        return

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
    await shutdown_snapshot(
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
    up_parser.add_argument("spec", nargs="?", default="shells.yaml", help="Path to spec file")
    up_parser.add_argument("--prune", action="store_true", help="Remove shells not in spec")
    
    # fs list
    list_parser = subparsers.add_parser("list", help="List running shells")
    list_parser.add_argument("--stats", action="store_true", help="Include CPU/RSS stats (best-effort)")
    
    # fs down
    down_parser = subparsers.add_parser("down", help="Terminate shells")
    down_parser.add_argument("spec", nargs="?", help="Optional spec file/dir; if provided, only those specs are terminated")
    down_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    down_parser.add_argument("--tree", action="store_true", help="Also terminate procfs descendants (best-effort)")
    down_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    down_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds for --tree (default: 2.0)")
    down_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds for --tree (default: 2.0)")

    # fs terminate <id|label>
    term_parser = subparsers.add_parser("terminate", help="Terminate a single shell")
    term_parser.add_argument("target", help="Shell ID, label, or unique ID prefix")
    term_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    term_parser.set_defaults(tree=True)
    term_parser.add_argument("--no-tree", dest="tree", action="store_false", help="Do not scan /proc for descendants")
    term_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    term_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds (default: 2.0)")
    term_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds (default: 2.0)")
    term_parser.add_argument("--purge", action="store_true", help="Also remove metadata/logs after termination")

    # fs rm <id|label>
    rm_parser = subparsers.add_parser("rm", help="Terminate (optional) and remove shell metadata/logs", aliases=["remove"])
    rm_parser.add_argument("target", help="Shell ID, label, or unique ID prefix")
    rm_parser.add_argument("--force", action="store_true", help="Force kill (SIGKILL)")
    rm_parser.set_defaults(tree=True)
    rm_parser.add_argument("--no-tree", dest="tree", action="store_false", help="Do not scan /proc for descendants")
    rm_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")
    rm_parser.add_argument("--grace", type=float, default=2.0, help="SIGTERM wait time in seconds (default: 2.0)")
    rm_parser.add_argument("--kill-wait", type=float, default=2.0, help="SIGKILL wait time in seconds (default: 2.0)")
    
    # fs attach [id]
    attach_parser = subparsers.add_parser("attach", help="Attach to a shell (dtach)")
    attach_parser.add_argument("id", help="Shell ID or Label")

    # fs run -- <command...>
    run_parser = subparsers.add_parser("run", help="Spawn a one-off shell without a shellspec")
    run_parser.add_argument("--backend", choices=["proc", "pty", "pipe", "dtach"], default="proc", help="Backend (default: proc)")
    run_parser.add_argument("--label", default=None, help="Optional shell label")
    run_parser.add_argument("--cwd", default=None, help="Working directory")
    run_parser.add_argument("--env", action="append", default=None, help="Environment override KEY=VALUE (repeatable)")
    run_parser.add_argument("--subgroup", action="append", default=None, help="Subgroup tag (repeatable)")
    run_parser.add_argument("--no-start", action="store_true", help="Create record only (do not start process)")
    run_parser.add_argument("cmd", nargs=argparse.REMAINDER, help="Command to run (prefix with --)")

    # fs tree
    tree_parser = subparsers.add_parser("tree", help="Show shell process trees (includes procfs descendants)")
    tree_parser.add_argument("--all", action="store_true", help="Include exited shells (if pid still known)")
    tree_parser.add_argument("--depth", type=int, default=8, help="Max procfs discovery depth (default: 8)")

    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    setup_environment()

    try:
        asyncio.run(run_async(args))
    except KeyboardInterrupt:
        pass

async def run_async(args):
    manager = FrameworkShellManager()

    if getattr(args, "command", None) == "run":
        cmd = list(getattr(args, "cmd", []) or [])
        if cmd and cmd[0] == "--":
            cmd = cmd[1:]
        if not cmd:
            raise SystemExit("fws run requires a command. Example: fws run --backend pty -- bash -l -i")

        env = _parse_env_kv(getattr(args, "env", None))
        subgroups = [str(x) for x in (getattr(args, "subgroup", None) or []) if str(x).strip()]
        autostart = not bool(getattr(args, "no_start", False))
        backend = getattr(args, "backend", "proc")

        if backend == "pty":
            rec = await manager.spawn_shell_pty(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, autostart=autostart)
        elif backend == "pipe":
            rec = await manager.spawn_shell_pipe(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, autostart=autostart)
        elif backend == "dtach":
            rec = await manager.spawn_shell_dtach(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, autostart=autostart)
        else:
            rec = await manager.spawn_shell(cmd, cwd=getattr(args, "cwd", None), env=env, label=getattr(args, "label", None), subgroups=subgroups, autostart=autostart)

        print(rec.id)
        return

    if getattr(args, "command", None) == "tree":
        depth = int(getattr(args, "depth", 8) or 8)
        if depth < 1:
            depth = 1
        # CLI convenience: allow deeper/shallower procfs scanning.
        try:
            manager._procfs_provider = ProcfsProcessProvider(max_depth=depth)  # type: ignore[attr-defined]
        except Exception:
            pass

        shells = await manager.list_shells()
        if not getattr(args, "all", False):
            shells = [s for s in shells if (getattr(s, "status", None) or "") == "running"]

        described: List[Dict[str, Any]] = []
        for rec in shells:
            try:
                described.append(await manager.describe(rec))
            except Exception:
                described.append(rec.to_payload())

        snapshot: ProcessSnapshot = await manager.build_process_snapshot(shells=shells, include_procfs_descendants=True)
        processes = snapshot.processes

        children_by_parent: Dict[int, List[int]] = {}
        for pid, proc in processes.items():
            if proc.parent_pid is None:
                continue
            try:
                children_by_parent.setdefault(int(proc.parent_pid), []).append(int(pid))
            except Exception:
                continue

        def backend_for(info: Dict[str, Any]) -> str:
            if info.get("uses_dtach"):
                return "dtach"
            if info.get("uses_pipes"):
                return "pipe"
            if info.get("uses_pty"):
                return "pty"
            return "proc"

        def render_node(pid: int, *, indent: str, shell_pid_set: set[int]) -> None:
            proc = processes.get(pid)
            if not proc:
                return
            kind = proc.type or "process"
            label = proc.label or str(pid)
            marker = "[shell]" if pid in shell_pid_set else "[proc] "
            print(f"{indent}{marker} {pid:<6} {kind:<7} {label}")

            kids = children_by_parent.get(pid, [])
            for child_pid in sorted(kids):
                # Avoid duplicating shell roots under other shells in the listing.
                if child_pid in shell_pid_set and child_pid != pid:
                    continue
                render_node(child_pid, indent=indent + "  ", shell_pid_set=shell_pid_set)

        shell_pid_set = {int(x.get("pid")) for x in described if x.get("pid")}
        for info in sorted(described, key=lambda x: (str(x.get("label") or ""), str(x.get("id") or ""))):
            sid = str(info.get("id") or "")
            label = str(info.get("label") or sid)
            status = str(info.get("status") or "")
            pid = info.get("pid")
            if not pid:
                print(f"{sid}  {label}  status={status}  pid=-  backend={backend_for(info)}")
                continue
            print(f"{sid}  {label}  status={status}  pid={pid}  backend={backend_for(info)}")
            render_node(int(pid), indent="  ", shell_pid_set=shell_pid_set)
        return

    if getattr(args, "command", None) == "terminate":
        rec = await _resolve_shell_target(manager, getattr(args, "target", ""), allow_exited=False)
        print(f"Terminating {rec.id}...")
        await _terminate_one(
            manager,
            rec,
            tree=bool(getattr(args, "tree", True)),
            force=bool(getattr(args, "force", False)),
            depth=int(getattr(args, "depth", 8) or 8),
            grace_s=float(getattr(args, "grace", 2.0) or 2.0),
            sigkill_timeout_s=float(getattr(args, "kill_wait", 2.0) or 2.0),
        )
        if bool(getattr(args, "purge", False)):
            await manager.remove_shell(rec.id, force=bool(getattr(args, "force", False)))
        return

    if getattr(args, "command", None) in {"rm", "remove"}:
        rec = await _resolve_shell_target(manager, getattr(args, "target", ""), allow_exited=True)
        if getattr(rec, "status", None) == "running":
            print(f"Terminating {rec.id}...")
            await _terminate_one(
                manager,
                rec,
                tree=bool(getattr(args, "tree", True)),
                force=bool(getattr(args, "force", False)),
                depth=int(getattr(args, "depth", 8) or 8),
                grace_s=float(getattr(args, "grace", 2.0) or 2.0),
                sigkill_timeout_s=float(getattr(args, "kill_wait", 2.0) or 2.0),
            )
        print(f"Removing {rec.id}...")
        await manager.remove_shell(rec.id, force=bool(getattr(args, "force", False)))
        return
    
    if args.command == "up":
        spec_path = Path(args.spec)
        if not spec_path.exists():
            print(f"Spec file not found: {spec_path}")
            sys.exit(1)
            
        print(f"Loading specs from {spec_path}...")
        specs_map = load_shellspec(spec_path)
        specs = list(specs_map.values())
        orch = Orchestrator(manager)
        await orch.apply(specs, prune=args.prune)
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

    elif args.command == "list":
        shells = await manager.list_shells()
        show_stats = bool(getattr(args, "stats", False))
        if show_stats:
            print(f"{'ID':<20} {'SPEC':<14} {'LABEL':<15} {'STATUS':<10} {'PID':<6} {'CPU':>6} {'RSS':>9} {'BACKEND'}")
        else:
            print(f"{'ID':<20} {'SPEC':<14} {'LABEL':<15} {'STATUS':<10} {'PID':<6} {'BACKEND'}")
        for s in shells:
            backend = (
                "dtach"
                if getattr(s, "uses_dtach", False)
                else ("pipe" if getattr(s, "uses_pipes", False) else ("pty" if s.uses_pty else "proc"))
            )
            if not show_stats:
                print(f"{s.id:<20} {(getattr(s, 'spec_id', None) or '-'): <14} {s.label or '-':<15} {s.status:<10} {s.pid or '-':<6} {backend}")
                continue
            try:
                info = await manager.describe(s)
                stats = info.get("stats") if isinstance(info.get("stats"), dict) else {}
                cpu = stats.get("cpu_percent")
                rss = stats.get("memory_rss")
                cpu_s = "-" if cpu is None else f"{float(cpu):.1f}%"
                rss_s = "-" if rss is None else f"{int(rss) // (1024 * 1024)}MiB"
            except Exception:
                cpu_s = "-"
                rss_s = "-"
            print(f"{s.id:<20} {(getattr(s, 'spec_id', None) or '-'): <14} {s.label or '-':<15} {s.status:<10} {s.pid or '-':<6} {cpu_s:>6} {rss_s:>9} {backend}")

    elif args.command == "down":
        spec_ids = None
        if getattr(args, "spec", None):
            spec_path = Path(args.spec)
            specs_map = load_shellspec(spec_path)
            spec_ids = set(specs_map.keys())

        shells = await manager.list_shells()
        selected = []
        for s in shells:
            if spec_ids is not None and getattr(s, "spec_id", None) not in spec_ids:
                continue
            selected.append(s)

        if not selected:
            return

        if bool(getattr(args, "tree", False)):
            depth = max(1, int(getattr(args, "depth", 8) or 8))
            try:
                manager._procfs_provider = ProcfsProcessProvider(max_depth=depth)  # type: ignore[attr-defined]
            except Exception:
                pass

            policy = ShutdownPolicy(
                sigterm_timeout_s=0.0 if bool(getattr(args, "force", False)) else max(0.0, float(getattr(args, "grace", 2.0) or 2.0)),
                sigkill_timeout_s=max(0.0, float(getattr(args, "kill_wait", 2.0) or 2.0)),
            )
            snapshot = await manager.build_process_snapshot(shells=selected, include_procfs_descendants=True)
            await shutdown_snapshot(snapshot, manager=manager, policy=policy, log=print)
            return

        for s in selected:
            print(f"Terminating {s.id}...")
            await manager.terminate_shell(s.id, force=bool(getattr(args, "force", False)))
            
    elif args.command == "attach":
        # Check specific shell
        record = await manager.find_shell_by_label(args.id) or await manager.get_shell(args.id)
        if not record:
             print("Shell not found")
             sys.exit(1)
        
        if not getattr(record, "uses_dtach", False):
             print("Shell is not using dtach backend. Cannot attach client.")
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
