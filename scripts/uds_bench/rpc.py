#!/usr/bin/env python3
"""Disposable native-pipe vs actual supervisor RPC bench; never a runtime adapter."""
from __future__ import annotations

import argparse
import asyncio
import base64
from contextlib import suppress
from dataclasses import dataclass
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import random
import signal
import sys
import tempfile
import time
from typing import Protocol, cast

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import msgspec
from framework_shells.manager import FrameworkShellManager
from framework_shells.native_pipe import NativePipePumpFactory, NativePipePumpHandle
from framework_shells.store import RuntimeStore
from framework_shells.supervisor_session import connect, receive_message
from framework_shells.supervisor_client import SupervisorClient
from scripts.uds_bench.run import metrics

Message = dict[str, object]
DECODER = msgspec.json.Decoder(dict[str, object])
ENCODER = msgspec.json.Encoder()
LANES = ("native-default", "native-matched", "supervisor-asyncio", "supervisor-native-reader",
         "supervisor-shared-python", "supervisor-shared-native")


def load_native(path: Path) -> NativePipePumpFactory:
    spec = importlib.util.spec_from_file_location("fws_pipe_pump", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load native extension")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Select the explicitly built artifact only in this disposable benchmark process.
    import framework_shells.native_pipe as native
    setattr(native, "_NATIVE_MODULE", module)
    return cast(NativePipePumpFactory, cast(object, module.NativePipePump))


class FrameReader(Protocol):
    async def readexactly(self, n: int) -> bytes: ...


class NativeSocketReader:
    """Benchmark-only handoff from asyncio to the existing native FD reader.

    Outer wire bytes go to /dev/null, not a second log. Supervisor owns raw logs.
    The existing extension still pays for its discard writes; report that cost.
    """
    def __init__(self, original: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 factory: NativePipePumpFactory) -> None:
        self.loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self.reader: asyncio.StreamReader = asyncio.StreamReader()
        self.queued: int = 0
        cast(asyncio.Transport, writer.transport).pause_reading()
        pending: object = getattr(original, "_buffer", None)
        if not isinstance(pending, bytearray):
            raise RuntimeError("benchmark handoff requires standard asyncio bytearray buffer")
        sock: object = writer.get_extra_info("socket")
        class HasFd(Protocol):
            def fileno(self) -> int: ...
        self.pump: NativePipePumpHandle = factory(cast(HasFd, sock).fileno(), os.devnull, 8192, 1, 1)
        self.fd: int = self.pump.reader_fd()
        if pending:
            self.queued += len(pending)
            self.reader.feed_data(bytes(pending))
            pending.clear()
        self.loop.add_reader(self.fd, self.ready)

    def ready(self) -> None:
        try:
            for chunk in self.pump.read_available(64):
                self.queued += len(chunk)
                if self.queued > 2 * 1024 * 1024:
                    raise BufferError("native UDS reader overflow")
                self.reader.feed_data(chunk)
            if self.pump.is_finished():
                self.loop.remove_reader(self.fd)
                self.reader.feed_eof()
        except Exception as exc:
            self.loop.remove_reader(self.fd)
            self.reader.set_exception(exc)

    async def readexactly(self, n: int) -> bytes:
        data = await self.reader.readexactly(n)
        self.queued -= len(data)
        return data

    def close(self) -> None:
        self.loop.remove_reader(self.fd)
        self.pump.stop()


class Transport(Protocol):
    stdout_log: Path
    stderr_log: Path
    async def send(self, data: bytes) -> None: ...
    async def read(self) -> bytes: ...
    async def close(self) -> None: ...


class NativePipe:
    def __init__(self, root: Path) -> None:
        self.manager: FrameworkShellManager = FrameworkShellManager(
            store=RuntimeStore(base_dir=root), enable_procfs_process_discovery=False)
        self.shell_id: str = ""
        self.stdout_log: Path = root / "unused-out"
        self.stderr_log: Path = root / "unused-err"
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()

    async def start(self, child: Path, matched: bool) -> None:
        config: Message = {"mode": "native_pipe_testing"}
        if matched:
            config.update(read_chunk_bytes=8192, log_flush_bytes=1, log_flush_interval_ms=1)
        record = await self.manager.spawn_shell_pipe([str(child)], pipe_config=config, label="isolated-rpc-bench")
        self.shell_id = record.id
        self.stdout_log, self.stderr_log = Path(record.stdout_log), Path(record.stderr_log)
        state = self.manager.get_pipe_state(record.id)
        if state is None or state.native_engine != "native-pipe" or state.native_pump is None:
            raise RuntimeError("native activation required; refusing Python fallback")
        self.queue = await self.manager.subscribe_output_bytes(record.id)

    async def send(self, data: bytes) -> None:
        # Public Python pipe API accepts text; include that conversion in its cost.
        await self.manager.write_to_pipe(self.shell_id, data.decode("utf-8"))

    async def read(self) -> bytes:
        return await self.queue.get()

    async def close(self) -> None:
        if self.shell_id:
            state = self.manager.get_pipe_state(self.shell_id)
            await self.manager.unsubscribe_output_bytes(self.shell_id, self.queue)
            await self.manager.terminate_shell(self.shell_id, force=True)
            if state is not None:
                # Native FD handoff leaves asyncio's original stdout transport paused.
                # Close this test-owned subprocess transport outside the timed region.
                transport: object = getattr(state.process, "_transport", None)
                if isinstance(transport, asyncio.BaseTransport):
                    transport.close()
                async with asyncio.timeout(5):
                    await state.process.wait()


class Supervisor:
    def __init__(self, root: Path) -> None:
        self.root: Path = root
        self.stdout_log: Path = root / "out"
        self.stderr_log: Path = root / "err"
        self.process: asyncio.subprocess.Process | None = None
        self.child_pid: int | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.native: NativeSocketReader | None = None
        self.receiver: asyncio.Task[None] | None = None
        self.queue: asyncio.Queue[bytes | Exception] = asyncio.Queue(maxsize=256)
        self.ack: asyncio.Future[Message] | None = None
        self.lock: asyncio.Lock = asyncio.Lock()
        self.generation: int = 0
        self.offset: int = 0

    async def start(self, binary: Path, child: Path, factory: NativePipePumpFactory | None) -> None:
        config = self.root / "config.json"
        config.write_bytes(ENCODER.encode({"socket": str(self.root / "s"),
            "stdout_log": str(self.stdout_log), "stderr_log": str(self.stderr_log), "command": [str(child)]}))
        self.process = await asyncio.create_subprocess_exec(str(binary), str(config), stdout=asyncio.subprocess.PIPE)
        assert self.process.stdout is not None
        async with asyncio.timeout(10):
            ready = DECODER.decode(await self.process.stdout.readline())
            self.child_pid = int(str(ready["pid"]))
            reader, self.writer = await connect(str(self.root / "s"),
                os.environ["FRAMEWORK_SHELLS_SECRET"].encode(), str(ready["instance"]))
            attached = await receive_message(reader)
            if attached.get("event") != "attached":
                raise ValueError("missing attachment")
        source: FrameReader = reader
        if factory is not None:
            self.native = NativeSocketReader(reader, self.writer, factory)
            source = self.native
        self.receiver = asyncio.create_task(self.receive(source))
        result = await self.request({"op": "acquire"})
        self.generation = int(str(result["generation"]))

    async def receive(self, reader: FrameReader) -> None:
        try:
            while True:
                size = int.from_bytes(await reader.readexactly(4), "big")
                if size > 65536:
                    raise ValueError("oversized supervisor frame")
                value = DECODER.decode(await reader.readexactly(size))
                if "ok" in value:
                    if self.ack is None or self.ack.done():
                        raise ValueError("unexpected/duplicate supervisor ACK")
                    self.ack.set_result(value)
                elif value.get("event") == "output":
                    if value.get("stream") != "stdout" or value.get("offset") != self.offset:
                        raise ValueError("unexpected stream or offset gap")
                    data = base64.b64decode(str(value["data"]), validate=True)
                    self.offset += len(data)
                    self.queue.put_nowait(data)
                else:
                    raise ValueError(f"unexpected supervisor event: {value}")
        except Exception as exc:
            if self.ack is not None and not self.ack.done():
                self.ack.set_exception(exc)
            # A full queue is already a failed run, never silently drop output.
            if self.queue.full():
                _ = self.queue.get_nowait()
            self.queue.put_nowait(exc)

    async def request(self, message: Message) -> Message:
        async with self.lock:
            assert self.writer is not None
            self.ack = asyncio.get_running_loop().create_future()
            data = ENCODER.encode(message)
            if len(data) > 65536:
                raise ValueError("oversized request")
            self.writer.write(len(data).to_bytes(4, "big") + data)
            async with asyncio.timeout(5):
                await self.writer.drain()
                reply = await self.ack
            if reply.get("ok") is not True:
                raise ValueError(f"supervisor rejected request: {reply}")
            return cast(Message, reply["result"])

    async def send(self, data: bytes) -> None:
        result = await self.request({"op": "write", "generation": self.generation,
                                     "data": base64.b64encode(data).decode("ascii")})
        if result.get("written") != len(data):
            raise ValueError("short supervisor write")

    async def read(self) -> bytes:
        data = await self.queue.get()
        if isinstance(data, Exception):
            raise data
        return data

    async def close(self) -> None:
        try:
            if self.process is not None and self.process.returncode is None and self.receiver is not None:
                with suppress(Exception):
                    await self.request({"op": "stop"})
                    async with asyncio.timeout(5):
                        await self.process.wait()
        finally:
            if self.receiver is not None:
                self.receiver.cancel()
                with suppress(asyncio.CancelledError):
                    await self.receiver
            if self.native is not None:
                self.native.close()
            if self.writer is not None:
                self.writer.close()
                with suppress(OSError):
                    await self.writer.wait_closed()
            if self.process is not None and self.process.returncode is None:
                self.process.kill()
                await self.process.wait()
                if self.child_pid is not None:
                    with suppress(ProcessLookupError):
                        os.killpg(self.child_pid, signal.SIGKILL)


class SharedSupervisor(Supervisor):
    """Actual common API, same supervisor/wire/logging as the older controls."""
    def __init__(self, root: Path, use_native: bool) -> None:
        super().__init__(root)
        self.use_native: bool = use_native
        self.client: SupervisorClient | None = None

    async def start(self, binary: Path, child: Path, factory: NativePipePumpFactory | None) -> None:
        if self.use_native:
            path = os.environ.get("FWS_SUPERVISOR_CLIENT_EXTENSION")
            if not path:
                raise RuntimeError("FWS_SUPERVISOR_CLIENT_EXTENSION required; no fallback")
            spec = importlib.util.spec_from_file_location("fws_supervisor_client", path)
            assert spec is not None and spec.loader is not None
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            sys.modules["fws_supervisor_client"] = module
        config = self.root / "config.json"
        _ = config.write_bytes(ENCODER.encode({"socket": str(self.root / "s"),
            "stdout_log": str(self.stdout_log), "stderr_log": str(self.stderr_log), "command": [str(child)]}))
        self.process = await asyncio.create_subprocess_exec(str(binary), str(config), stdout=asyncio.subprocess.PIPE)
        assert self.process.stdout is not None
        async with asyncio.timeout(10):
            ready = DECODER.decode(await self.process.stdout.readline())
            self.child_pid = int(str(ready["pid"]))
            self.client = await SupervisorClient.connect(str(self.root / "s"),
                os.environ["FRAMEWORK_SHELLS_SECRET"].encode(), str(ready["instance"]),
                backend="native" if self.use_native else "python")
            _ = await self.client.acquire()

    async def send(self, data: bytes) -> None:
        assert self.client is not None
        _ = await self.client.write(data)

    async def read(self) -> bytes:
        assert self.client is not None
        event = await self.client.read()
        if event.kind != "output" or event.stream != "stdout" or event.offset != self.offset:
            raise ValueError(f"unexpected common-client event: {event.kind}/{event.stream}/{event.offset}")
        self.offset += len(event.data)
        return event.data

    async def close(self) -> None:
        try:
            if self.client is not None:
                try:
                    with suppress(Exception):
                        await self.client.stop()
                        if self.process is not None:
                            async with asyncio.timeout(5):
                                _ = await self.process.wait()
                finally:
                    await self.client.close()
        finally:
            await super().close()


@dataclass(frozen=True)
class Workload:
    name: str
    concurrency: int
    response_bytes: int
    pushes: int = 0
    push_bytes: int = 0


WORKLOADS = (Workload("rtt", 1, 128), Workload("concurrent", 8, 4096), Workload("mixed", 8, 4096, 2, 2048))


class Router:
    def __init__(self, transport: Transport, workload: Workload) -> None:
        self.transport: Transport = transport
        self.workload: Workload = workload
        self.pending: dict[int, asyncio.Future[None]] = {}
        self.pushes: dict[int, int] = {}
        self.notifications: int = 0
        self.total_bytes: int = 0
        self.raw_hash = hashlib.sha256()
        self.expected_response: str = ("resp_" * (workload.response_bytes // 5 + 1))[:workload.response_bytes]
        self.expected_push: str = ("push_" * (workload.push_bytes // 5 + 1))[:workload.push_bytes]
        self.failure: Exception | None = None

    def dispatch(self, value: Message) -> None:
        if value.get("jsonrpc") != "2.0":
            raise ValueError("invalid JSON-RPC version")
        if "id" in value:
            request_id = value["id"]
            if type(request_id) is not int or request_id not in self.pending:
                raise ValueError("unknown or duplicate response id")
            expected: Message = {"ok": True, "ordinal": request_id, "payload": self.expected_response}
            if value.get("result") != expected or "error" in value:
                raise ValueError("wrong result payload")
            if self.pushes.pop(request_id, 0) != self.workload.pushes:
                raise ValueError("notification count mismatch")
            self.pending.pop(request_id).set_result(None)
        else:
            params = value.get("params")
            if value.get("method") != "bench.push" or not isinstance(params, dict):
                raise ValueError("unexpected notification")
            params = cast(Message, params)
            request_id = params.get("request_id")
            if type(request_id) is not int or request_id not in self.pending:
                raise ValueError("notification for unknown request")
            expected = {"request_id": request_id, "ordinal": request_id,
                        "index": self.pushes.get(request_id, 0), "payload": self.expected_push}
            if params != expected or int(str(params["index"])) >= self.workload.pushes:
                raise ValueError("wrong/duplicate notification")
            self.pushes[request_id] = self.pushes.get(request_id, 0) + 1
            self.notifications += 1

    async def receive(self) -> None:
        buffer = bytearray()
        try:
            while True:
                chunk = await self.transport.read()
                if not chunk:
                    raise EOFError("unexpected child EOF")
                self.raw_hash.update(chunk)
                self.total_bytes += len(chunk)
                buffer.extend(chunk)
                if len(buffer) > 1024 * 1024:
                    raise BufferError("application framing overflow")
                while (end := buffer.find(b"\n")) >= 0:
                    line = bytes(buffer[:end])
                    del buffer[:end + 1]
                    self.dispatch(DECODER.decode(line))
        except Exception as exc:
            self.failure = exc
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(exc)

    async def request(self, request_id: int) -> None:
        if self.failure is not None:
            raise self.failure
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.pending[request_id] = future
        value = {"jsonrpc": "2.0", "id": request_id, "method": "bench.echo", "params": {
            "ordinal": request_id, "request_body": "q" * 128, "response_bytes": self.workload.response_bytes,
            "push_count": self.workload.pushes, "push_bytes": self.workload.push_bytes}}
        await self.transport.send(ENCODER.encode(value) + b"\n")
        await future


async def trial(lane: str, workload: Workload, requests: int, child: Path,
                supervisor: Path, native: Path) -> Message:
    if lane not in LANES or requests < 1:
        raise ValueError("invalid trial")
    factory = load_native(native)
    with tempfile.TemporaryDirectory(prefix="fwsrpc-") as directory:
        root = Path(directory)
        os.environ.update(FRAMEWORK_SHELLS_SECRET="disposable-rpc-benchmark", FRAMEWORK_SHELLS_BASE_DIR=directory,
            FRAMEWORK_SHELLS_DISABLE_FWS_SOCKETIO_PEER="1", FRAMEWORK_SHELLS_REPO_FINGERPRINT="rpc-benchmark")
        transport: NativePipe | Supervisor = NativePipe(root) if lane.startswith("native-") else Supervisor(root)
        if lane.startswith("supervisor-shared-"):
            transport = SharedSupervisor(root, lane == "supervisor-shared-native")
        receiver: asyncio.Task[None] | None = None
        try:
            if isinstance(transport, NativePipe):
                await transport.start(child, lane == "native-matched")
            else:
                await transport.start(supervisor, child, factory if lane == "supervisor-native-reader" else None)
            router = Router(transport, workload)
            receiver = asyncio.create_task(router.receive())
            async with asyncio.timeout(10):
                for ordinal in range(requests + 1, requests + 17):
                    await router.request(ordinal)
            # Check visibility before close/EOF, not merely final log flushing.
            warm_log = transport.stdout_log.read_bytes()
            if len(warm_log) != router.total_bytes or hashlib.sha256(warm_log).digest() != router.raw_hash.digest():
                raise AssertionError("warmup log differs from observed bytes")
            warm_bytes = router.total_bytes
            warm_notifications = router.notifications
            times: list[float] = []
            async def worker(index: int) -> None:
                for ordinal in range(index + 1, requests + 1, workload.concurrency):
                    started = time.perf_counter_ns()
                    await router.request(ordinal)
                    times.append((time.perf_counter_ns() - started) / 1e6)
            cpu, started = time.process_time(), time.perf_counter()
            async with asyncio.timeout(60):
                async with asyncio.TaskGroup() as group:
                    for i in range(workload.concurrency):
                        _ = group.create_task(worker(i))
            seconds, cpu = time.perf_counter() - started, time.process_time() - cpu
            raw = transport.stdout_log.read_bytes()
            if len(raw) != router.total_bytes or hashlib.sha256(raw).digest() != router.raw_hash.digest():
                raise AssertionError("raw log missing/corrupt bytes before shutdown")
            if transport.stderr_log.read_bytes():
                raise AssertionError("unexpected child stderr")
            if router.failure is not None:
                raise router.failure
            if router.notifications - warm_notifications != requests * workload.pushes or router.pending:
                raise AssertionError("incomplete RPC workload")
            result = metrics(times, seconds, cpu, 0, 0)
            del result["wire_bytes"], result["useful_bytes_per_second"]
            result.update(lane=lane, workload=workload.name, concurrency=workload.concurrency,
                notifications=router.notifications - warm_notifications, output_bytes=router.total_bytes - warm_bytes,
                output_bytes_per_second=(router.total_bytes - warm_bytes) / seconds,
                raw_log_verified=True, valid=True, application_decoder="msgspec.json",
                supervisor_envelope="JSON/base64 (unchanged wire)",
                client_implementation=lane,
                log_policy="raw writes before delivery; no fsync; one raw log owner",
                native_pipe_activation_verified=lane.startswith("native-"),
                native_uds_reader=lane == "supervisor-native-reader")
            return result
        finally:
            if receiver is not None:
                receiver.cancel()
                with suppress(asyncio.CancelledError):
                    await receiver
            await transport.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", type=Path, required=True)
    parser.add_argument("--supervisor", type=Path, required=True)
    parser.add_argument("--native", type=Path, required=True)
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--trial", choices=LANES)
    parser.add_argument("--lanes", nargs="+", choices=LANES, default=list(LANES))
    parser.add_argument("--workload", choices=[w.name for w in WORKLOADS], default="rtt")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    child, supervisor, native = (Path(str(p)).resolve() for p in (args.child, args.supervisor, args.native))
    if args.requests < 1 or args.repeats < 1:
        parser.error("positive requests and repeats required")
    if args.trial:
        current = asyncio.current_task()
        assert current is not None
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, current.cancel)
        workload = next(w for w in WORKLOADS if w.name == args.workload)
        result: Message = await trial(str(args.trial), workload, args.requests, child, supervisor, native)
        print("RESULT " + json.dumps(result), flush=True)
        return
    if args.output is None:
        parser.error("--output required")
    rng = random.Random(12345)
    cases: list[tuple[str, Workload, int]] = []
    for repeat in range(args.repeats):
        workloads = list(WORKLOADS)
        rng.shuffle(workloads)
        for workload in workloads:
            lanes = [str(lane) for lane in args.lanes]
            rng.shuffle(lanes)
            cases.extend((lane, workload, repeat) for lane in lanes)
    failed = False
    with Path(str(args.output)).open("x") as output:
        provenance = {"python": sys.version, "executable": sys.executable, "msgspec": msgspec.__version__,
            "warmup_requests": 16, "order": "randomized lanes within repeat/workload blocks",
            "scope": "real native FWS pipe versus Rust supervisor; identical native child; Python asyncio",
            "artifacts": {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in (child, supervisor, native)},
            "sources": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in
                (Path(__file__), ROOT / "framework_shells/manager.py", ROOT / "native/fws_pipe_pump/src/lib.rs",
                 ROOT / ".external/ferrous-framework/crates/fws-supervisor/src/service.rs")}}
        extension_path = os.environ.get("FWS_SUPERVISOR_CLIENT_EXTENSION")
        if extension_path:
            provenance["native_client"] = {"path": extension_path,
                "sha256": hashlib.sha256(Path(extension_path).read_bytes()).hexdigest(),
                "client_source_sha256": hashlib.sha256((ROOT / "native/fws_supervisor/src/client.rs").read_bytes()).hexdigest(),
                "wrapper_source_sha256": hashlib.sha256((ROOT / "framework_shells/supervisor_client.py").read_bytes()).hexdigest(),
                "binding_source_sha256": hashlib.sha256((ROOT / "native/fws_supervisor_client/src/lib.rs").read_bytes()).hexdigest()}
        output.write(json.dumps({"metadata": provenance}) + "\n")
        output.flush()
        for lane, workload, repeat in cases:
            process = await asyncio.create_subprocess_exec(sys.executable, "-I", __file__, "--trial", lane,
                "--workload", workload.name, "--requests", str(args.requests), "--child", str(child),
                "--supervisor", str(supervisor), "--native", str(native), stdout=asyncio.subprocess.PIPE)
            try:
                # Inner operation deadlines perform cleanup; leave enough room for it.
                async with asyncio.timeout(100):
                    stdout, _ = await process.communicate()
                if process.returncode != 0:
                    raise RuntimeError(f"trial exit {process.returncode}: {stdout.decode()}")
                lines = [line[7:] for line in stdout.splitlines() if line.startswith(b"RESULT ")]
                if len(lines) != 1:
                    raise RuntimeError("trial did not report exactly one result")
                result = DECODER.decode(lines[0])
            except Exception as exc:
                failed = True
                result = {"lane": lane, "workload": workload.name, "valid": False, "error": repr(exc)}
            finally:
                if process.returncode is None:
                    process.terminate()  # Trial cancellation runs its owned-child cleanup.
                    try:
                        async with asyncio.timeout(20):
                            await process.wait()
                    except TimeoutError:
                        process.kill()
                        await process.wait()
            result["repeat"] = repeat
            output.write(json.dumps(result) + "\n")
            output.flush()
            print(json.dumps(result), flush=True)
    if failed:
        raise SystemExit("one or more RPC trials failed; see results")


if __name__ == "__main__":
    asyncio.run(main())
