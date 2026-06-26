#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import statistics
import subprocess
import sys
import time
from typing import Any


def _percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1.0 - weight) + sorted_values[upper] * weight


def _stats(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)
    if not ordered:
        return {
            "count": 0.0,
            "min_ms": 0.0,
            "p50_ms": 0.0,
            "p95_ms": 0.0,
            "p99_ms": 0.0,
            "mean_ms": 0.0,
            "max_ms": 0.0,
        }
    return {
        "count": float(len(ordered)),
        "min_ms": ordered[0],
        "p50_ms": _percentile(ordered, 0.50),
        "p95_ms": _percentile(ordered, 0.95),
        "p99_ms": _percentile(ordered, 0.99),
        "mean_ms": statistics.mean(ordered),
        "max_ms": ordered[-1],
    }


class JsonRpcRouter:
    def __init__(self, queue: "asyncio.Queue[bytes]") -> None:
        self._queue = queue
        self._buffer = b""
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._task: asyncio.Task[None] | None = None
        self.notifications = 0
        self.malformed_frames = 0
        self.unknown_responses = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="bench-jsonrpc-router")

    async def stop(self) -> None:
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    def register(self, request_id: int) -> "asyncio.Future[dict[str, Any]]":
        future: "asyncio.Future[dict[str, Any]]" = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        return future

    async def _run(self) -> None:
        while True:
            chunk = await self._queue.get()
            if not chunk:
                continue
            self._buffer += bytes(chunk)
            while b"\n" in self._buffer:
                raw_line, self._buffer = self._buffer.split(b"\n", 1)
                if raw_line.endswith(b"\r"):
                    raw_line = raw_line[:-1]
                if not raw_line:
                    continue
                try:
                    message = json.loads(raw_line.decode("utf-8", errors="strict"))
                except Exception:
                    self.malformed_frames += 1
                    continue
                if not isinstance(message, dict):
                    self.malformed_frames += 1
                    continue
                response_id = message.get("id")
                if isinstance(response_id, int):
                    future = self._pending.pop(response_id, None)
                    if future is None:
                        self.unknown_responses += 1
                        continue
                    if not future.done():
                        future.set_result(message)
                    continue
                self.notifications += 1


async def _run_case(
    *,
    label: str,
    force_python_pump: bool,
    manager_module: Any,
    native_pipe_module: Any,
    server_command: list[str],
    server_cwd: str,
    request_count: int,
    concurrency: int,
    request_bytes: int,
    response_bytes: int,
    push_count: int,
    push_bytes: int,
) -> dict[str, Any]:
    manager = await manager_module.get_manager()

    original_native_module = native_pipe_module._NATIVE_MODULE
    if force_python_pump:
        native_pipe_module._NATIVE_MODULE = None

    shell = None
    subscription = None
    router: JsonRpcRouter | None = None
    try:
        shell = await manager.spawn_shell_pipe(
            server_command,
            cwd=server_cwd,
            label=f"bench:{label}",
            pipe_config={"mode": "native_pipe_testing"},
        )
        subscription = await manager.subscribe_output_bytes(shell.id)
        router = JsonRpcRouter(subscription)
        router.start()

        request_body = "q" * max(0, request_bytes)
        outstanding = 0
        start_times: dict[int, float] = {}
        latencies_ms: list[float] = []
        errors = 0
        request_id = 0
        completed = 0

        async def issue_one(ordinal: int) -> None:
            nonlocal outstanding
            nonlocal errors
            nonlocal completed
            request_id_local = ordinal + 1
            params = {
                "ordinal": ordinal,
                "request_body": request_body,
                "response_bytes": response_bytes,
                "push_count": push_count,
                "push_bytes": push_bytes,
            }
            future = router.register(request_id_local)
            start_times[request_id_local] = time.perf_counter()
            payload = {
                "jsonrpc": "2.0",
                "id": request_id_local,
                "method": "bench.echo",
                "params": params,
            }
            try:
                await manager.write_to_pipe(shell.id, json.dumps(payload, separators=(",", ":")) + "\n")
                message = await asyncio.wait_for(future, timeout=5.0)
            except Exception:
                errors += 1
            else:
                result = message.get("result")
                if not isinstance(result, dict) or result.get("ok") is not True or result.get("ordinal") != ordinal:
                    errors += 1
                else:
                    latencies_ms.append((time.perf_counter() - start_times[request_id_local]) * 1000.0)
            finally:
                completed += 1
                outstanding -= 1

        started_at = time.perf_counter()
        tasks: set[asyncio.Task[None]] = set()
        while request_id < request_count or tasks:
            while request_id < request_count and outstanding < concurrency:
                task = asyncio.create_task(issue_one(request_id))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
                request_id += 1
                outstanding += 1
            if tasks:
                await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0

        pipe_state = manager.get_pipe_state(shell.id)
        return {
            "label": label,
            "engine": getattr(pipe_state, "native_engine", None) if pipe_state is not None else None,
            "phase": getattr(pipe_state, "native_phase", None) if pipe_state is not None else None,
            "request_count": request_count,
            "completed": completed,
            "errors": errors,
            "notifications": router.notifications,
            "malformed_frames": router.malformed_frames,
            "unknown_responses": router.unknown_responses,
            "elapsed_ms": elapsed_ms,
            "throughput_rps": (completed / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else 0.0,
            "latency_ms": _stats(latencies_ms),
        }
    finally:
        native_pipe_module._NATIVE_MODULE = original_native_module
        if router is not None:
            await router.stop()
        if subscription is not None and shell is not None:
            try:
                await manager.unsubscribe_output_bytes(shell.id, subscription)
            except Exception:
                pass
        if shell is not None:
            try:
                await manager.terminate_shell(shell.id, force=True)
            except Exception:
                pass
        await asyncio.sleep(0.1)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _rust_bench_server_path(*, release: bool) -> Path:
    suffix = ".exe" if sys.platform.startswith("win") else ""
    profile = "release" if release else "debug"
    return (
        _repo_root()
        / "native"
        / "fws_pipe_pump"
        / "target"
        / profile
        / f"fws-pipe-jsonrpc-bench-server{suffix}"
    )


def _build_rust_bench_server(*, release: bool) -> Path:
    manifest = _repo_root() / "native" / "fws_pipe_pump" / "Cargo.toml"
    command = [
        "cargo",
        "build",
        "--manifest-path",
        str(manifest),
        "--bin",
        "fws-pipe-jsonrpc-bench-server",
    ]
    if release:
        command.append("--release")
    subprocess.run(command, cwd=str(_repo_root()), check=True)
    binary = _rust_bench_server_path(release=release)
    if not binary.is_file():
        raise FileNotFoundError(f"Rust benchmark server did not build: {binary}")
    return binary


def _server_command(args: argparse.Namespace) -> tuple[list[str], str]:
    if args.server == "python":
        script_path = Path(__file__).resolve().parent / "bench_pipe_jsonrpc_server.py"
        return [sys.executable, str(script_path)], str(script_path.parent)

    release = args.rust_server_profile == "release"
    binary = _rust_bench_server_path(release=release)
    if args.build_rust_server or not binary.is_file():
        binary = _build_rust_bench_server(release=release)
    if not binary.is_file():
        raise FileNotFoundError(
            f"Rust benchmark server not found: {binary}. "
            "Run with --build-rust-server or build it manually."
        )
    return [str(binary)], str(binary.parent)


async def _main_async(args: argparse.Namespace) -> int:
    import framework_shells as framework_shells_module
    import framework_shells.native_pipe as native_pipe_module

    server_command, server_cwd = _server_command(args)

    python_case = await _run_case(
        label="python-reader",
        force_python_pump=True,
        manager_module=framework_shells_module,
        native_pipe_module=native_pipe_module,
        server_command=server_command,
        server_cwd=server_cwd,
        request_count=args.requests,
        concurrency=args.concurrency,
        request_bytes=args.request_bytes,
        response_bytes=args.response_bytes,
        push_count=args.push_count,
        push_bytes=args.push_bytes,
    )
    native_case = await _run_case(
        label="native-reader",
        force_python_pump=False,
        manager_module=framework_shells_module,
        native_pipe_module=native_pipe_module,
        server_command=server_command,
        server_cwd=server_cwd,
        request_count=args.requests,
        concurrency=args.concurrency,
        request_bytes=args.request_bytes,
        response_bytes=args.response_bytes,
        push_count=args.push_count,
        push_bytes=args.push_bytes,
    )

    output = {
        "loop": args.loop,
        "server": args.server,
        "server_command": server_command,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "request_bytes": args.request_bytes,
        "response_bytes": args.response_bytes,
        "push_count": args.push_count,
        "push_bytes": args.push_bytes,
        "python": python_case,
        "native": native_case,
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Python vs native pipe for deterministic stdio JSON-RPC.")
    parser.add_argument("--loop", choices=("uvloop", "asyncio"), default="uvloop")
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--request-bytes", type=int, default=128)
    parser.add_argument("--response-bytes", type=int, default=4096)
    parser.add_argument("--push-count", type=int, default=2)
    parser.add_argument("--push-bytes", type=int, default=2048)
    parser.add_argument("--server", choices=("python", "rust"), default="python")
    parser.add_argument("--rust-server-profile", choices=("release", "debug"), default="release")
    parser.add_argument("--build-rust-server", action="store_true")
    args = parser.parse_args()
    if args.loop == "uvloop":
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
