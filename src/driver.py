from __future__ import annotations

import asyncio
import errno
import logging
import os
from pathlib import Path

from . import events
from .actions import Action, ActionKind
from .events import Event, EventKind, Timer
from .follower import Follower
from .leader import Leader
from .messages import Message
from .statemachine import StateMachine
from .wire import ProtocolError, read_message, write_message

log = logging.getLogger(__name__)

DEFAULT_RETRY_DELAY_SECONDS = 1.0
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_MAX_BLOB_BYTES = 1 << 30


def errno_of(error: BaseException) -> int:
    if isinstance(error, ProtocolError):
        return errno.EPROTO
    if isinstance(error, OSError) and error.errno:
        return error.errno
    if isinstance(error, TimeoutError):
        return errno.ETIMEDOUT
    # Orderly close by the peer has no errno of its own.
    if isinstance(error, (ConnectionError, asyncio.IncompleteReadError)):
        return errno.ECONNRESET
    return errno.EIO


def write_atomically(path: Path, payload: bytes) -> None:
    # Write then rename, so a crash never leaves a partial file at path.
    partial = path.with_name(path.name + ".partial")
    with open(partial, "wb") as file:
        file.write(payload)
        file.flush()
        os.fsync(file.fileno())
    os.replace(partial, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class Connection:
    def __init__(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.peer = writer.get_extra_info("peername")
        self.read_task: asyncio.Task | None = None
        # Makes drain() return only once send(2) has accepted every byte.
        writer.transport.set_write_buffer_limits(high=0)

    @property
    def usable(self) -> bool:
        return not self.writer.is_closing()

    def close(self) -> None:
        self.writer.close()


class Driver:
    def __init__(
        self,
        machine: StateMachine,
        retry_delay_seconds: float,
        max_blob_bytes: int,
    ) -> None:
        self._machine = machine
        self._retry_delay_seconds = retry_delay_seconds
        self._max_blob_bytes = max_blob_bytes
        self._events: asyncio.Queue[tuple[Event, int | None]] = asyncio.Queue()
        self._timers: dict[Timer, tuple[asyncio.TimerHandle, int]] = {}
        self._timer_generation = 0
        self._tasks: set[asyncio.Task] = set()
        self._send_lock = asyncio.Lock()
        self._connection: Connection | None = None
        self._logged_records = 0

    async def run(self) -> None:
        try:
            await self._open()
            self._execute(self._machine.start())
            while not self._machine.is_final:
                event, generation = await self._events.get()
                if self._is_stale(event, generation):
                    log.debug("stale %s dropped", event)
                    continue
                log.debug("event  %s", event)
                self._execute(self._machine.handle(event))
        finally:
            await self._shutdown()

    async def _open(self) -> None:
        pass

    def _post(self, event: Event, generation: int | None = None) -> None:
        self._events.put_nowait((event, generation))

    def _execute(self, actions: list[Action]) -> None:
        self._log_history()
        for action in actions:
            log.debug("action %s", action)
            match action.kind:
                case ActionKind.SEND_MESSAGE:
                    assert action.message is not None
                    self._spawn(self._send(action.message))
                case ActionKind.ARM_TIMEOUT:
                    assert action.timer is not None and action.seconds is not None
                    self._arm(action.timer, action.seconds)
                case ActionKind.CANCEL_TIMEOUT:
                    assert action.timer is not None
                    self._cancel(action.timer)
                case ActionKind.WRITE_PAYLOAD:
                    assert action.payload is not None
                    self._spawn(self._write(action.payload))

    def _log_history(self) -> None:
        records = self._machine.history.records
        for record in records[self._logged_records :]:
            log.info("%s", record)
        self._logged_records = len(records)

    def _spawn(self, coroutine) -> None:
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _arm(self, timer: Timer, seconds: float) -> None:
        self._cancel(timer)
        self._timer_generation += 1
        generation = self._timer_generation
        handle = asyncio.get_running_loop().call_later(
            seconds, self._post, events.timeout_expired(timer), generation
        )
        self._timers[timer] = (handle, generation)

    def _cancel(self, timer: Timer) -> None:
        armed = self._timers.pop(timer, None)
        if armed is not None:
            armed[0].cancel()

    def _is_stale(self, event: Event, generation: int | None) -> bool:
        # A timeout queued just before it was cancelled or re-armed must not fire.
        if event.kind is not EventKind.TIMEOUT_EXPIRED:
            return False
        assert event.timer is not None
        armed = self._timers.get(event.timer)
        if armed is None or armed[1] != generation:
            return True
        del self._timers[event.timer]
        return False

    async def _usable_connection(self) -> Connection | None:
        raise NotImplementedError

    async def _send(self, message: Message) -> None:
        async with self._send_lock:
            try:
                connection = await self._usable_connection()
            except OSError as error:
                log.warning("connect failed: %s", error)
                connection = None
                failure = errno_of(error)
            else:
                failure = errno.ENOTCONN
            if connection is None:
                # Without this delay the send(2) failed self-loop would spin.
                await asyncio.sleep(self._retry_delay_seconds)
                self._post(events.send_failed(failure))
                return
            try:
                write_message(connection.writer, message)
                await connection.writer.drain()
            except OSError as error:
                log.warning("send to %s failed: %s", connection.peer, error)
                self._drop(connection)
                self._post(events.send_failed(errno_of(error)))
                return
            self._post(events.send_completed())

    def _attach(self, connection: Connection) -> None:
        if self._connection is not None:
            self._drop(self._connection)
        self._connection = connection
        log.info("connected to %s", connection.peer)

    def _drop(self, connection: Connection) -> None:
        if self._connection is connection:
            self._connection = None
            log.info("disconnected from %s", connection.peer)
        connection.close()
        if connection.read_task is not None and connection.read_task is not asyncio.current_task():
            connection.read_task.cancel()

    async def _read_loop(self, connection: Connection) -> None:
        try:
            while True:
                message = await read_message(connection.reader, self._max_blob_bytes)
                self._post(events.recv_completed(message))
        except asyncio.CancelledError:
            raise
        except (OSError, asyncio.IncompleteReadError, ProtocolError) as error:
            current = self._connection is connection
            self._drop(connection)
            # The death of a superseded connection is not news to the state machine.
            if current:
                log.warning("recv from %s failed: %r", connection.peer, error)
                self._post(events.recv_failed(errno_of(error)))

    async def _write(self, payload: bytes) -> None:
        raise NotImplementedError

    async def _shutdown(self) -> None:
        for timer in list(self._timers):
            self._cancel(timer)
        if self._connection is not None:
            self._drop(self._connection)
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._log_history()


class LeaderDriver(Driver):
    def __init__(
        self,
        leader: Leader,
        host: str,
        port: int,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    ) -> None:
        # The Leader never receives a Blob, so any Blob is a protocol error.
        super().__init__(leader, retry_delay_seconds, max_blob_bytes=0)
        self._host = host
        self._port = port
        self._connect_timeout_seconds = connect_timeout_seconds

    async def _usable_connection(self) -> Connection | None:
        if self._connection is not None and self._connection.usable:
            return self._connection
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port),
            self._connect_timeout_seconds,
        )
        connection = Connection(reader, writer)
        self._attach(connection)
        connection.read_task = asyncio.create_task(self._read_loop(connection))
        self._tasks.add(connection.read_task)
        connection.read_task.add_done_callback(self._tasks.discard)
        return connection


class FollowerDriver(Driver):
    def __init__(
        self,
        follower: Follower,
        host: str,
        port: int,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        max_blob_bytes: int = DEFAULT_MAX_BLOB_BYTES,
    ) -> None:
        super().__init__(follower, retry_delay_seconds, max_blob_bytes)
        self._follower = follower
        self._host = host
        self._port = port
        self._server: asyncio.Server | None = None

    async def _open(self) -> None:
        self._server = await asyncio.start_server(
            self._accept, self._host, self._port, reuse_address=True
        )
        for sock in self._server.sockets:
            log.info("listening on %s", sock.getsockname())

    async def _accept(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        # There is one Leader, so a new connection supersedes the old one.
        connection = Connection(reader, writer)
        connection.read_task = asyncio.current_task()
        self._attach(connection)
        await self._read_loop(connection)

    async def _usable_connection(self) -> Connection | None:
        if self._connection is not None and self._connection.usable:
            return self._connection
        return None

    async def _write(self, payload: bytes) -> None:
        try:
            await asyncio.to_thread(write_atomically, self._follower.file_path, payload)
        except OSError as error:
            log.warning("write to %s failed: %s", self._follower.file_path, error)
            self._post(events.write_failed(errno_of(error)))
            return
        self._post(events.write_completed())

    async def _shutdown(self) -> None:
        if self._server is not None:
            self._server.close()
        await super()._shutdown()
