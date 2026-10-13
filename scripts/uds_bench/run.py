#!/usr/bin/env python3
"""Isolated codec/UDS comparison. No FWS logs, authentication or supervisor."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
import hashlib
from importlib.metadata import version
import json
import os
from pathlib import Path
import random
import statistics
import sys
import sysconfig
import tempfile
import time
from typing import cast

# Permit `python -I scripts/uds_bench/run.py` without inherited harness imports.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.bench_uds_envelope import read, send
from scripts.uds_bench.codecs import CODECS, Codec, Envelope


def metrics(times: list[float], seconds: float, cpu: float, size: int, wire: int) -> dict[str, object]:
    ordered = sorted(times)
    def percentile(p: float) -> float:
        return ordered[int((len(ordered) - 1) * p)]
    return {"completed": len(times), "errors": 0, "seconds": seconds,
        "rps": len(times) / seconds, "client_cpu_seconds": cpu,
        "useful_bytes_per_second": len(times) * size * 2 / seconds, "wire_bytes": wire,
        "rtt_ms": {"min": ordered[0], "p50": percentile(.5), "p95": percentile(.95),
            "p99": percentile(.99), "max": ordered[-1], "mean": statistics.mean(times),
            "stddev": statistics.pstdev(times)}}


async def server(path: str, name: str) -> None:
    codec = Codec(name)
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                await send(writer, codec.encode(codec.decode(await read(reader))))
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
    listener = await asyncio.start_unix_server(echo, path)
    print("ready", flush=True)
    async with listener:
        await listener.serve_forever()


async def python_client(path: str, name: str, size: int, sessions: int, requests: int) -> dict[str, object]:
    codec = Codec(name)
    value = Envelope(bytes(i % 256 for i in range(size)))
    connections: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
    try:
        for _ in range(sessions):
            reader, writer = await asyncio.open_unix_connection(path)
            connections.append((reader, writer))
            await send(writer, codec.encode(value))
            if codec.decode(await read(reader)) != value:
                raise AssertionError("warmup mismatch")
        times: list[float] = []
        wire = 0
        async def lane(i: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            nonlocal wire
            for sequence in range(i, requests, sessions):
                start = time.perf_counter_ns()
                # Sequence is opaque application data, including for the raw control.
                expected = replace(value, payload=sequence.to_bytes(8, "big") + value.payload[8:]) if size >= 8 else value
                frame = codec.encode(expected)
                await send(writer, frame)
                response = await read(reader)
                if codec.decode(response) != expected:
                    raise AssertionError("payload or metadata mismatch")
                times.append((time.perf_counter_ns() - start) / 1e6)
                wire += len(frame) + len(response) + 8
        cpu = time.process_time()
        start = time.perf_counter()
        await asyncio.gather(*(lane(i, r, w) for i, (r, w) in enumerate(connections)))
        return metrics(times, time.perf_counter() - start, time.process_time() - cpu, size, wire)
    finally:
        for _, writer in connections:
            writer.close()
        for _, writer in connections:
            await writer.wait_closed()


async def trial(name: str, size: int, sessions: int, requests: int,
                client: str = "python", peer: str = "python", rust: Path | None = None) -> dict[str, object]:
    if sessions < 1 or requests < 1 or not 0 <= size <= 32768:
        raise ValueError("invalid workload")
    if client not in ("python", "rust") or peer not in ("python", "rust"):
        raise ValueError("invalid runtime")
    if "rust" in (client, peer) and rust is None:
        raise ValueError("Rust peer needs --rust-binary")
    with tempfile.TemporaryDirectory(prefix="udsc-") as directory:
        path = str(Path(directory) / "s")
        command = ([str(rust), "server", path, name] if peer == "rust" else
                   [sys.executable, "-I", __file__, "--server", path, "--codec", name])
        child = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE)
        probe: asyncio.subprocess.Process | None = None
        try:
            assert child.stdout is not None
            async with asyncio.timeout(10):
                if await child.stdout.readline() != b"ready\n":
                    raise RuntimeError("server failed before ready")
            async with asyncio.timeout(35):
                if client == "python":
                    result = await python_client(path, name, size, sessions, requests)
                else:
                    probe = await asyncio.create_subprocess_exec(str(rust), "client", path, name,
                        str(size), str(sessions), str(requests), stdout=asyncio.subprocess.PIPE)
                    stdout, _ = await probe.communicate()
                    if probe.returncode != 0:
                        raise RuntimeError(f"Rust client exit {probe.returncode}")
                    result = cast(dict[str, object], json.loads(stdout))
            if result.get("completed") != requests or result.get("errors") != 0:
                raise AssertionError("incomplete or invalid trial")
            result.update(codec=name, payload_bytes=size, sessions=sessions, client=client,
                          server=peer, outstanding_per_session=1, valid=True)
            return result
        finally:
            # Only processes launched by this trial are terminated.
            for process in (probe, child):
                if process is not None:
                    if process.returncode is None:
                        try:
                            process.terminate()
                        except ProcessLookupError:
                            pass
                    try:
                        async with asyncio.timeout(5):
                            await process.wait()
                    except TimeoutError:
                        process.kill()
                        await process.wait()


def codec_trial(name: str, size: int, requests: int) -> dict[str, object]:
    codec = Codec(name)
    value = Envelope(bytes(i % 256 for i in range(size)))
    if codec.decode(codec.encode(value)) != value:
        raise AssertionError("codec warmup mismatch")
    start = time.perf_counter()
    cpu = time.process_time()
    for _ in range(requests):
        if codec.decode(codec.encode(value)) != value:
            raise AssertionError("codec mismatch")
    elapsed = time.perf_counter() - start
    return {"kind": "codec", "client": "python", "codec": name, "payload_bytes": size,
            "completed": requests, "seconds": elapsed, "roundtrips_per_second": requests / elapsed,
            "client_cpu_seconds": time.process_time() - cpu, "frame_bytes": len(codec.encode(value)),
            "valid": True, "scope": "encode+decode+equality, no socket"}


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server")
    parser.add_argument("--codec", choices=CODECS, default="raw")
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--sizes", type=int, nargs="+", default=[128, 4096, 32768])
    parser.add_argument("--sessions", type=int, nargs="+", default=[1, 8, 24])
    parser.add_argument("--rust-binary", type=Path)
    parser.add_argument("--rust-profile", choices=("debug", "release", "unspecified"), default="unspecified")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.server:
        await server(str(args.server), str(args.codec))
        return
    if args.requests < 1 or args.repeats < 1 or args.output is None:
        parser.error("positive requests/repeats and --output required")
    sizes = cast(list[int], args.sizes)
    sessions = cast(list[int], args.sessions)
    if any(size < 0 or size > 32768 for size in sizes) or any(n < 1 for n in sessions):
        parser.error("sizes must be 0..32768, sessions must be positive")
    rust = Path(str(args.rust_binary)).resolve() if args.rust_binary else None
    runtimes = ("python", "rust") if rust else ("python",)
    cases = [(codec, size, count, client, peer, repeat)
             for repeat in range(args.repeats) for size in sizes for count in sessions
             for codec in CODECS for client in runtimes for peer in runtimes]
    random.Random(12345).shuffle(cases)
    with Path(str(args.output)).open("x") as output:
        metadata: dict[str, object] = {"python": sys.version, "executable": sys.executable,
            "prefix": sys.prefix, "free_threaded_build": sysconfig.get_config_var("Py_GIL_DISABLED"),
            "gil_enabled": sys._is_gil_enabled(),  # pyright: ignore[reportPrivateUsage]
            "pid": os.getpid(),
            "bindings": {package: version(package) for package in ("flatbuffers", "pycapnp", "msgspec")},
            "json_msgspec": "Python uses msgspec; Rust uses the same serde_json JSON wire codec",
            "python_loop": "asyncio", "rust_loop": "Tokio current_thread",
            "rust_binary": str(rust) if rust else None,
            "rust_profile": str(args.rust_profile),
            "rust_binary_sha256": hashlib.sha256(rust.read_bytes()).hexdigest() if rust else None,
            "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((ROOT / "scripts/uds_bench").rglob("*"))
                if path.is_file() and path.suffix in (".py", ".rs", ".capnp", ".fbs", ".toml", ".lock")
                and "target" not in path.parts},
            "scope": "transport-only, separate processes, no logs/auth; not supervisor/RPC throughput",
            "validation": "Python FlatBuffers bounds/accessor checks are not a full verifier"}
        output.write(json.dumps({"metadata": metadata}) + "\n")
        # These short controls are separate from transport results, not subtracted.
        for repeat in range(args.repeats):
            for size in sizes:
                for codec in CODECS:
                    result = codec_trial(codec, size, args.requests)
                    result["repeat"] = repeat
                    output.write(json.dumps(result) + "\n")
        output.flush()
        for codec, size, count, client, peer, repeat in cases:
            try:
                result = await trial(codec, size, count, args.requests, client, peer, rust)
            except Exception as exc:
                result = {"codec": codec, "payload_bytes": size, "sessions": count,
                          "client": client, "server": peer, "valid": False, "error": repr(exc)}
            result.update(kind="transport", repeat=repeat)
            output.write(json.dumps(result) + "\n")
            output.flush()
            print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
