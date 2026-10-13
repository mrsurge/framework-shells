#!/usr/bin/env python3
"""Disposable separate-process UDS envelope controls; no production transport edits."""
from __future__ import annotations
import argparse
import asyncio
import base64
import json
import os
import random
import statistics
import struct
import sys
import tempfile
import time
from pathlib import Path
from typing import cast

LIMIT = 65536


def encode(payload: bytes, codec: str) -> bytes:
    if codec == "raw":
        return payload
    return json.dumps({"op": "data", "data": base64.b64encode(payload).decode()}, separators=(",", ":")).encode()


def decode(payload: bytes, codec: str) -> bytes:
    if codec == "raw":
        return payload
    obj = cast(dict[str, object], json.loads(payload))
    if obj.get("op") != "data" or not isinstance(obj.get("data"), str):
        raise ValueError("invalid envelope")
    return base64.b64decode(cast(str, obj["data"]), validate=True)


async def read(reader: asyncio.StreamReader) -> bytes:
    size = int.from_bytes(await reader.readexactly(4), "big")
    if size > LIMIT:
        raise ValueError("frame exceeds limit")
    return await reader.readexactly(size)


async def send(writer: asyncio.StreamWriter, payload: bytes) -> None:
    if len(payload) > LIMIT:
        raise ValueError("frame exceeds limit")
    writer.write(struct.pack("!I", len(payload)) + payload)
    await writer.drain()


async def server(path: str, codec: str) -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await read(reader)
                await send(writer, encode(decode(data, codec), codec))
        except asyncio.IncompleteReadError:
            pass
        finally:
            writer.close()
            await writer.wait_closed()
    listener = await asyncio.start_unix_server(echo, path)
    print("ready", flush=True)
    async with listener:
        await listener.serve_forever()


async def trial(codec: str, size: int, concurrency: int, requests: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory(prefix="udsbench-") as directory:
        path = str(Path(directory) / "s")
        child = await asyncio.create_subprocess_exec(sys.executable, __file__, "--server", path, "--codec", codec,
            stdout=asyncio.subprocess.PIPE)
        connections: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []
        try:
            assert child.stdout is not None
            async with asyncio.timeout(10):
                if await child.stdout.readline() != b"ready\n":
                    raise RuntimeError("server startup failed")
                for _ in range(concurrency):
                    connections.append(await asyncio.open_unix_connection(path))
            payload = bytes(index % 256 for index in range(size))
            # Warm each connection before timing; construction/encoding is timed below.
            for reader, writer in connections:
                await send(writer, encode(payload, codec))
                if decode(await read(reader), codec) != payload:
                    raise AssertionError("warmup mismatch")
            latencies: list[float] = []
            wire_bytes = 0
            async def lane(index: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
                nonlocal wire_bytes
                for _ in range(index, requests, concurrency):
                    started = time.perf_counter_ns()
                    frame = encode(payload, codec)
                    await send(writer, frame)
                    returned = await read(reader)
                    if decode(returned, codec) != payload:
                        raise AssertionError("payload mismatch")
                    latencies.append((time.perf_counter_ns() - started) / 1e6)
                    wire_bytes += len(frame) + len(returned) + 8
            cpu = time.process_time()
            start = time.perf_counter()
            async with asyncio.timeout(30):
                await asyncio.gather(*(lane(i, r, w) for i, (r, w) in enumerate(connections)))
            elapsed = time.perf_counter() - start
            cpu = time.process_time() - cpu
            ordered = sorted(latencies)
            def percentile(p: float) -> float:
                return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))]
            return {"codec": codec, "payload_bytes": size, "sessions": concurrency,
                "outstanding_per_session": 1, "completed": len(latencies), "errors": 0,
                "seconds": elapsed, "rps": requests / elapsed, "client_cpu_seconds": cpu,
                "useful_bytes_per_second": requests * size * 2 / elapsed,
                "wire_bytes": wire_bytes, "rtt_ms": {"min": ordered[0], "p50": percentile(.5),
                "p95": percentile(.95), "p99": percentile(.99), "max": ordered[-1],
                "mean": statistics.mean(ordered), "stddev": statistics.pstdev(ordered)}}
        finally:
            for _, writer in connections:
                writer.close()
                await writer.wait_closed()
            if child.returncode is None:
                child.terminate()
            await child.wait()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server")
    parser.add_argument("--codec", choices=["raw", "json"], default="raw")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.server:
        await server(str(args.server), str(args.codec))
        return
    if args.requests < 1 or args.repeats < 1 or args.output is None:
        parser.error("positive requests/repeats and --output required")
    cases = [(codec, size, concurrency, repeat) for repeat in range(args.repeats)
             for size in (128, 4096, 32768) for concurrency in (1, 8, 24) for codec in ("raw", "json")]
    random.Random(12345).shuffle(cases)
    with Path(str(args.output)).open("x") as output:
        metadata = {"python": sys.version, "executable": sys.executable, "loop": "asyncio",
                    "pid": os.getpid(), "topology": "separate-process Python UDS echo; no logs/auth",
                    "scope": "transport-only controls, not actual supervisor envelope or JSON-RPC workload"}
        output.write(json.dumps({"metadata": metadata}) + "\n")
        output.flush()
        for codec, size, concurrency, repeat in cases:
            try:
                result = await trial(codec, size, concurrency, args.requests)
            except Exception as exc:
                result = {"codec": codec, "payload_bytes": size, "sessions": concurrency,
                          "error": repr(exc), "valid": False}
            result["repeat"] = repeat
            output.write(json.dumps(result) + "\n")
            output.flush()
            print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
