"""Experimental common UDS client; not integrated with manager launch/adoption.

The optional native driver owns outer transport I/O and codecs. Selection occurs
only before connecting. Neither driver reconnects or retries a write.
"""
from __future__ import annotations

import asyncio
import base64
from contextlib import suppress
from collections import deque
from dataclasses import dataclass
import importlib
from typing import Awaitable, Callable, Literal, Protocol, TypeVar, cast

from .supervisor_session import Message, connect, receive_message, send_message

EventTuple = tuple[str, str, int, bytes, int | None]
StatusTuple = tuple[int, str, bool, int | None, str | None]
T = TypeVar("T")


class SupervisorError(RuntimeError):
    def __init__(self, kind: str, message: str) -> None:
        self.kind: str = kind
        super().__init__(f"{kind}: {message}")


@dataclass(frozen=True)
class SupervisorEvent:
    kind: str
    stream: str
    offset: int
    data: bytes
    code: int | None


@dataclass(frozen=True)
class SupervisorStatus:
    pid: int
    instance: str
    exited: bool
    code: int | None
    failure: str | None


class _Driver(Protocol):
    def acquire(self, takeover: bool) -> Awaitable[int]: ...
    def write(self, data: bytes) -> Awaitable[int]: ...
    def read(self) -> Awaitable[EventTuple]: ...
    def status(self) -> Awaitable[StatusTuple]: ...
    def eof(self) -> Awaitable[None]: ...
    def detach(self) -> Awaitable[None]: ...
    def stop(self) -> Awaitable[None]: ...
    def close(self) -> Awaitable[None]: ...


class _NativeClass(Protocol):
    def connect(self, path: str, secret: bytes, instance: str) -> Awaitable[_NativeHandle]: ...


class _NativeHandle(Protocol):
    def reader_fd(self) -> int: ...
    def close_fd(self) -> None: ...
    def write_submit(self, data: bytes) -> int: ...
    def drain(self) -> tuple[list[EventTuple], list[tuple[int, int | None, str | None]], str | None]: ...
    def acquire(self, takeover: bool) -> Awaitable[int]: ...
    def status(self) -> Awaitable[StatusTuple]: ...
    def eof(self) -> Awaitable[None]: ...
    def detach(self) -> Awaitable[None]: ...
    def stop(self) -> Awaitable[None]: ...
    def close(self) -> Awaitable[None]: ...


class _NativeModule(Protocol):
    API_VERSION: int
    NativeClient: _NativeClass


def _native() -> _NativeModule | None:
    try:
        module = cast(_NativeModule, importlib.import_module("fws_supervisor_client"))
    except (ImportError, OSError):
        return None
    return module if getattr(module, "API_VERSION", None) == 2 else None


async def _call(operation: Callable[[], Awaitable[T]]) -> T:
    try:
        return await operation()
    except SupervisorError:
        raise
    except RuntimeError as exc:
        kind, separator, message = str(exc).partition(": ")
        raise SupervisorError(kind if separator else "native", message if separator else str(exc)) from exc


class SupervisorClient:
    def __init__(self, driver: _Driver, backend: str) -> None:
        self._driver: _Driver = driver
        self.backend: str = backend
        self._read_lock: asyncio.Lock = asyncio.Lock()

    @classmethod
    async def connect(cls, path: str, secret: bytes, instance: str, *,
                      backend: Literal["auto", "python", "native"] = "auto") -> SupervisorClient:
        """Backend override is for parity testing; normal callers leave auto selected."""
        if backend not in ("auto", "python", "native"):
            raise ValueError("invalid backend")
        module = _native() if backend != "python" else None
        if backend == "native" and module is None:
            raise SupervisorError("unavailable", "native API v2 extension unavailable")
        if module is not None:
            driver = await _call(lambda: module.NativeClient.connect(path, secret, instance))
            try:
                return cls(_NativeDriver(driver), "native")
            except BaseException:
                await driver.close()
                raise
        return cls(await _PythonClient.connect(path, secret, instance), "python")

    async def acquire(self, takeover: bool = False) -> int:
        return await _call(lambda: self._driver.acquire(takeover))

    async def write(self, data: bytes) -> int:
        return await _call(lambda: self._driver.write(data))

    async def read(self) -> SupervisorEvent:
        async with self._read_lock:
            return SupervisorEvent(*await _call(self._driver.read))

    async def status(self) -> SupervisorStatus:
        return SupervisorStatus(*await _call(self._driver.status))

    async def eof(self) -> None:
        await _call(self._driver.eof)

    async def detach(self) -> None:
        await _call(self._driver.detach)

    async def stop(self) -> None:
        await _call(self._driver.stop)

    async def close(self) -> None:
        """Close the connection, not the child or its stdin. Always call in finally."""
        await _call(self._driver.close)


class _NativeDriver:
    """Readiness notifications, not polling or Python calls from Rust workers."""
    def __init__(self, handle: _NativeHandle) -> None:
        self.handle: _NativeHandle = handle
        self.loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self.fd: int = handle.reader_fd()
        self.events: deque[EventTuple] = deque()
        self.reader: asyncio.Future[EventTuple] | None = None
        self.writes: dict[int, asyncio.Future[int]] = {}
        self.failure: SupervisorError | None = None
        self.closed: bool = False
        self.loop.add_reader(self.fd, self._ready)

    @staticmethod
    def _error(message: str) -> SupervisorError:
        kind, _, detail = message.partition(": ")
        return SupervisorError(kind, detail)

    def _fail(self, error: SupervisorError) -> None:
        if self.failure is None or self.failure.kind == "closed":
            self.failure = error
        if error.kind != "closed":
            self.events.clear()
        if self.reader is not None and not self.reader.done():
            self.reader.set_exception(error)

    def _ready(self) -> None:
        try:
            events, replies, failure = self.handle.drain()
            for request_id, count, detail in replies:
                future = self.writes.pop(request_id, None)
                if future is None or future.done():
                    continue  # Cancelled caller; ACK was still consumed in Rust.
                if detail is not None:
                    future.set_exception(self._error(detail))
                elif count is not None:
                    future.set_result(count)
                else:
                    future.set_exception(SupervisorError("protocol", "invalid native completion"))
            for event in events if failure is None or failure.startswith("closed:") else []:
                if self.reader is not None and not self.reader.done():
                    self.reader.set_result(event)
                elif len(self.events) < 256:
                    self.events.append(event)
                else:
                    self._fail(SupervisorError("lagged", "Python delivery queue full; reconnect explicitly"))
                    self.loop.remove_reader(self.fd)
                    # Fatal local delivery overflow must stop native capture too.
                    _ = self.loop.create_task(self.close())
                    break
            if failure is not None:
                self._fail(self._error(failure))
        except Exception as exc:
            self._fail(SupervisorError("native", str(exc)))
            self.loop.remove_reader(self.fd)
            _ = self.loop.create_task(self.close())

    async def acquire(self, takeover: bool) -> int:
        if self.failure is not None:
            raise self.failure
        return await self.handle.acquire(takeover)

    async def write(self, data: bytes) -> int:
        if self.failure is not None:
            raise self.failure
        request_id = self.handle.write_submit(data)
        future: asyncio.Future[int] = self.loop.create_future()
        self.writes[request_id] = future
        try:
            return await future
        finally:
            self.writes.pop(request_id, None)

    async def read(self) -> EventTuple:
        if self.failure is not None and self.failure.kind != "closed":
            raise self.failure
        if self.events:
            return self.events.popleft()
        if self.failure is not None:
            raise self.failure
        if self.reader is not None:
            raise SupervisorError("busy", "only one pending read supported")
        self.reader = self.loop.create_future()
        try:
            return await self.reader
        finally:
            self.reader = None

    async def status(self) -> StatusTuple:
        if self.failure is not None:
            raise self.failure
        try:
            return await self.handle.status()
        except RuntimeError:
            # A control completion can race the readiness callback carrying a
            # more specific local delivery error. Drain before reporting close.
            self._ready()
            if self.failure is not None:
                raise self.failure
            raise

    async def eof(self) -> None:
        if self.failure is not None:
            raise self.failure
        await self.handle.eof()

    async def detach(self) -> None:
        if self.failure is not None:
            raise self.failure
        await self.handle.detach()

    async def stop(self) -> None:
        if self.failure is not None:
            raise self.failure
        await self.handle.stop()

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            await self.handle.close()
            self._ready()
        finally:
            self.loop.remove_reader(self.fd)
            self.handle.close_fd()
            self._fail(SupervisorError("closed", "client closed"))
            for future in self.writes.values():
                if not future.done():
                    future.set_exception(SupervisorError("delivery_uncertain", "client closed; do not replay"))
            self.writes.clear()


class _PythonClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.writer: asyncio.StreamWriter = writer
        self.events: asyncio.Queue[EventTuple] = asyncio.Queue(maxsize=256)
        self.commands: asyncio.Queue[tuple[str, object, asyncio.Future[Message]]] = asyncio.Queue(maxsize=64)
        self.failure: asyncio.Future[SupervisorError] = asyncio.get_running_loop().create_future()
        self.ack: asyncio.Future[Message] | None = None
        self.generation: int | None = None
        self.offsets: dict[str, int] = {}
        self.receiver: asyncio.Task[None] = asyncio.create_task(self._receive(reader))
        self.actor: asyncio.Task[None] = asyncio.create_task(self._run())

    @classmethod
    async def connect(cls, path: str, secret: bytes, instance: str) -> _PythonClient:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout(5):
                reader, writer = await connect(path, secret, instance)
                attached = await receive_message(reader)
                if attached.get("event") != "attached" or attached.get("instance") != instance:
                    raise ValueError("invalid attachment")
                return cls(reader, writer)
        except Exception as exc:
            if writer is not None:
                writer.close()
                with suppress(OSError):
                    await writer.wait_closed()
            raise SupervisorError("connect", str(exc)) from exc
        except BaseException:
            if writer is not None:
                writer.close()
            raise

    def _fail(self, error: SupervisorError) -> None:
        if not self.failure.done():
            self.failure.set_result(error)
        self.writer.close()
        if self.ack is not None and not self.ack.done():
            self.ack.set_exception(error)

    async def _receive(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                value = await receive_message(reader)
                if "ok" in value:
                    if self.ack is None or self.ack.done():
                        raise SupervisorError("protocol", "unexpected acknowledgement")
                    self.ack.set_result(value)
                    continue
                kind = value.get("event")
                if kind == "output":
                    stream, offset, encoded = value.get("stream"), value.get("offset"), value.get("data")
                    if stream not in ("stdout", "stderr") or type(offset) is not int or not isinstance(encoded, str):
                        raise SupervisorError("protocol", "invalid output")
                    stream = cast(str, stream)
                    data = base64.b64decode(encoded, validate=True)
                    if offset < 0 or offset > 2**64 - 1 - len(data) or len(data) > 8192:
                        raise SupervisorError("protocol", "invalid offset/chunk size")
                    if self.offsets.get(stream, offset) != offset:
                        raise SupervisorError("lagged", "output offset gap")
                    self.offsets[stream] = offset + len(data)
                    self.events.put_nowait(("output", stream, offset, data, None))
                elif kind == "exited":
                    code = value.get("code")
                    self.events.put_nowait(("exited", "", 0, b"", code if type(code) is int else None))
                elif kind == "gap":
                    raise SupervisorError("lagged", "supervisor output gap")
                elif kind == "failure":
                    raise SupervisorError("remote", str(value.get("message")))
                else:
                    raise SupervisorError("protocol", "unexpected supervisor event")
        except asyncio.QueueFull:
            self._fail(SupervisorError("lagged", "output consumer full; reconnect explicitly"))
        except SupervisorError as exc:
            self._fail(exc)
        except ConnectionError as exc:
            self._fail(SupervisorError("closed", str(exc)))
        except Exception as exc:
            self._fail(SupervisorError("protocol", str(exc)))

    async def _exchange(self, message: Message) -> Message:
        self.ack = asyncio.get_running_loop().create_future()
        try:
            async with asyncio.timeout(5):
                await send_message(self.writer, message)
                value = await self.ack
            if value.get("ok") is False and isinstance(value.get("error"), str):
                detail = str(value["error"])
                raise SupervisorError("delivery_uncertain" if detail.startswith("delivery_uncertain:") else "remote", detail)
            if value.get("ok") is not True or not isinstance(value.get("result"), dict):
                raise SupervisorError("protocol", "invalid acknowledgement")
            return cast(Message, value["result"])
        finally:
            if self.ack.done() and not self.ack.cancelled():
                _ = self.ack.exception()
            self.ack = None

    async def _run(self) -> None:
        reply: asyncio.Future[Message] | None = None
        try:
            while True:
                op, data, reply = await self.commands.get()
                if reply.done():
                    continue
                try:
                    if self.failure.done():
                        raise self.failure.result()
                    message: Message = {"op": op}
                    if op == "acquire":
                        message["takeover"] = data
                    elif op in ("write", "eof", "detach"):
                        if self.generation is None:
                            raise SupervisorError("invalid", "writer lease required; not sent")
                        message["generation"] = self.generation
                        if op == "write":
                            message["data"] = base64.b64encode(cast(bytes, data)).decode("ascii")
                    try:
                        result = await self._exchange(message)
                        if op == "acquire":
                            lease = result.get("generation")
                            valid = type(lease) is int and 0 <= lease < 2**64
                        elif op == "write":
                            count = result.get("written")
                            valid = type(count) is int and count == len(cast(bytes, data))
                        elif op in ("eof", "detach", "stop"):
                            valid = result.get({"eof": "stdin_closed", "detach": "detached", "stop": "stopping"}[op]) is True
                        else:
                            pid, instance, exited = result.get("pid"), result.get("instance"), result.get("exited")
                            code, failure = result.get("code"), result.get("failure")
                            valid = (type(pid) is int and 0 <= pid < 2**32 and isinstance(instance, str)
                                     and type(exited) is bool and (code is None or type(code) is int)
                                     and (failure is None or isinstance(failure, str)))
                        if not valid:
                            raise SupervisorError("protocol", "invalid acknowledgement result")
                    except Exception as exc:
                        error = exc if isinstance(exc, SupervisorError) else SupervisorError("closed", str(exc))
                        if error.kind != "remote":
                            if op in ("write", "eof"):
                                error = SupervisorError("delivery_uncertain", f"{error}; do not replay")
                            self._fail(error)
                        raise error
                    if op == "acquire":
                        generation = result.get("generation")
                        if type(generation) is not int:
                            raise SupervisorError("protocol", "invalid generation")
                        self.generation = generation
                    elif op == "detach":
                        self.generation = None
                    if not reply.done():
                        reply.set_result(result)
                    if op == "stop":
                        self._fail(SupervisorError("closed", "client stopped"))
                except SupervisorError as exc:
                    if not reply.done():
                        reply.set_exception(exc)
        finally:
            if reply is not None and not reply.done():
                reply.set_exception(SupervisorError("closed", "client closed; any in-flight write is uncertain"))
            while not self.commands.empty():
                _, _, reply = self.commands.get_nowait()
                if not reply.done():
                    reply.set_exception(SupervisorError("closed", "client closed"))

    async def _request(self, op: str, data: object = None) -> Message:
        if self.failure.done():
            raise self.failure.result()
        reply: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        try:
            self.commands.put_nowait((op, data, reply))
        except asyncio.QueueFull as exc:
            raise SupervisorError("busy", "control queue full; command not sent") from exc
        return await reply

    async def acquire(self, takeover: bool) -> int:
        return cast(int, (await self._request("acquire", takeover))["generation"])

    async def write(self, data: bytes) -> int:
        if len(data) > 32768:
            raise SupervisorError("invalid", "write exceeds 32768 bytes; not sent")
        result = await self._request("write", data)
        if result.get("written") != len(data):
            raise SupervisorError("delivery_uncertain", "invalid write acknowledgement; do not replay")
        return len(data)

    async def read(self) -> EventTuple:
        if self.failure.done() and self.failure.result().kind != "closed":
            raise self.failure.result()
        if not self.events.empty():
            return self.events.get_nowait()
        task = asyncio.create_task(self.events.get())
        try:
            _ = await asyncio.wait((task, self.failure), return_when=asyncio.FIRST_COMPLETED)
            if self.failure.done() and self.failure.result().kind != "closed":
                raise self.failure.result()
            if task.done():
                return task.result()
            raise self.failure.result()
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def status(self) -> StatusTuple:
        value = await self._request("status")
        pid, instance, exited, code, failure = (value.get(k) for k in ("pid", "instance", "exited", "code", "failure"))
        if type(pid) is not int or not isinstance(instance, str) or type(exited) is not bool:
            raise SupervisorError("protocol", "invalid status")
        if (code is not None and type(code) is not int) or (failure is not None and not isinstance(failure, str)):
            raise SupervisorError("protocol", "invalid exit status")
        return pid, instance, exited, code, failure

    async def eof(self) -> None:
        _ = await self._request("eof")

    async def detach(self) -> None:
        _ = await self._request("detach")

    async def stop(self) -> None:
        _ = await self._request("stop")

    async def close(self) -> None:
        self._fail(SupervisorError("closed", "client closed"))
        # Let an active request consume the close error before cancelling idle tasks.
        await asyncio.sleep(0)
        for task in (self.receiver, self.actor):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        with suppress(OSError):
            await self.writer.wait_closed()
