from __future__ import annotations

import errno
from enum import Enum, auto
from pathlib import Path

from . import actions
from .actions import Action
from .events import Event, EventKind
from .messages import SUCCESS, Blob, BlobReply, Message, RestartedQuery, RestartedReply
from .statemachine import History, StateConf, StateMachine


class FollowerState(Enum):
    INITIAL = auto()
    WAITING_FOR_MESSAGE = auto()
    RESTARTED_REPLY_LOOP = auto()
    WRITING = auto()
    WRITTEN = auto()
    WRITE_FAILED = auto()
    BLOB_REPLY_LOOP = auto()


# The Follower is a server loop, so its set of final states is empty.
FOLLOWER_CONF = {
    FollowerState.INITIAL: StateConf(
        name="INITIAL",
        allowed=frozenset({FollowerState.WAITING_FOR_MESSAGE}),
        initial=True,
    ),
    FollowerState.WAITING_FOR_MESSAGE: StateConf(
        name="WAITING_FOR_MESSAGE",
        allowed=frozenset(
            {
                FollowerState.WAITING_FOR_MESSAGE,
                FollowerState.RESTARTED_REPLY_LOOP,
                FollowerState.WRITING,
            }
        ),
    ),
    FollowerState.RESTARTED_REPLY_LOOP: StateConf(
        name="RESTARTED_REPLY_LOOP",
        allowed=frozenset(
            {FollowerState.RESTARTED_REPLY_LOOP, FollowerState.WAITING_FOR_MESSAGE}
        ),
    ),
    FollowerState.WRITING: StateConf(
        name="WRITING",
        allowed=frozenset({FollowerState.WRITTEN, FollowerState.WRITE_FAILED}),
    ),
    FollowerState.WRITTEN: StateConf(
        name="WRITTEN",
        allowed=frozenset({FollowerState.BLOB_REPLY_LOOP}),
    ),
    FollowerState.WRITE_FAILED: StateConf(
        name="WRITE_FAILED",
        allowed=frozenset({FollowerState.BLOB_REPLY_LOOP}),
    ),
    FollowerState.BLOB_REPLY_LOOP: StateConf(
        name="BLOB_REPLY_LOOP",
        allowed=frozenset(
            {FollowerState.BLOB_REPLY_LOOP, FollowerState.WAITING_FOR_MESSAGE}
        ),
    ),
}


class Follower(StateMachine):
    def __init__(
        self, file_path: str | Path, history: History | None = None
    ) -> None:
        super().__init__("follower", FOLLOWER_CONF, history)
        self._file_path = Path(file_path)

        # restart_handled is volatile, per the handbook. A crash resets it to false,
        # which is how the Leader later learns that the file must be sent again.
        self._restart_handled = False

        self._payload: bytes | None = None
        self._pending_reply: Message | None = None
        self._pending_result: int | None = None

        self._handlers = {
            FollowerState.WAITING_FOR_MESSAGE: self._in_waiting_for_message,
            FollowerState.RESTARTED_REPLY_LOOP: self._in_restarted_reply_loop,
            FollowerState.WRITING: self._in_writing,
            FollowerState.BLOB_REPLY_LOOP: self._in_blob_reply_loop,
        }

    @property
    def restart_handled(self) -> bool:
        return self._restart_handled

    @property
    def file_path(self) -> Path:
        return self._file_path

    def start(self) -> list[Action]:
        self.move(FollowerState.WAITING_FOR_MESSAGE, "restart_handled set to false")
        return []

    def handle(self, event: Event) -> list[Action]:
        handler = self._handlers.get(self.state)
        if handler is None:
            return []
        return handler(event)

    def _in_waiting_for_message(self, event: Event) -> list[Action]:
        if event.kind is EventKind.RECV_FAILED:
            self.move(
                FollowerState.WAITING_FOR_MESSAGE, "recv(2) failed, link error"
            )
            return []
        if event.kind is EventKind.RECV_COMPLETED:
            if isinstance(event.message, RestartedQuery):
                self._pending_reply = RestartedReply(restarted=not self._restart_handled)
                self.move(
                    FollowerState.RESTARTED_REPLY_LOOP,
                    "recv(2) completed, Restarted?() received",
                )
                return [actions.send_message(self._pending_reply)]
            if isinstance(event.message, Blob):
                # A duplicate Blob() is not special cased. Writing the whole file again
                # to the same path is a full overwrite, so repeating it is harmless.
                self._payload = event.message.payload
                self.move(
                    FollowerState.WRITING, "recv(2) completed, Blob(payload) received"
                )
                return [actions.write_payload(self._payload)]
        return []

    def _in_restarted_reply_loop(self, event: Event) -> list[Action]:
        if event.kind is EventKind.SEND_COMPLETED:
            self.move(
                FollowerState.WAITING_FOR_MESSAGE,
                "send(2) completed, RestartedReply(restarted) accepted by the kernel",
            )
            self._pending_reply = None
            return []
        if event.kind is EventKind.SEND_FAILED:
            self.move(
                FollowerState.RESTARTED_REPLY_LOOP, "send(2) failed, link error"
            )
            assert self._pending_reply is not None
            return [actions.send_message(self._pending_reply)]
        return []

    def _in_writing(self, event: Event) -> list[Action]:
        if event.kind is EventKind.WRITE_COMPLETED:
            self._restart_handled = True
            self.move(
                FollowerState.WRITTEN, "write(2) completed, restart_handled set to true"
            )
            return self._prepare_blob_reply(SUCCESS, "result set to 0")
        if event.kind is EventKind.WRITE_FAILED:
            result = event.error if event.error is not None else errno.EIO
            self._pending_result = result
            self.move(FollowerState.WRITE_FAILED, "write(2) failed")
            return self._prepare_blob_reply(result, f"result set to {result}")
        return []

    def _prepare_blob_reply(self, result: int, cause: str) -> list[Action]:
        self._pending_result = result
        self._pending_reply = BlobReply(result=result)
        self.move(FollowerState.BLOB_REPLY_LOOP, cause)
        return [actions.send_message(self._pending_reply)]

    def _in_blob_reply_loop(self, event: Event) -> list[Action]:
        if event.kind is EventKind.SEND_COMPLETED:
            self.move(
                FollowerState.WAITING_FOR_MESSAGE,
                "send(2) completed, BlobReply(result) accepted by the kernel",
            )
            self._pending_reply = None
            self._pending_result = None
            return []
        if event.kind is EventKind.SEND_FAILED:
            self.move(FollowerState.BLOB_REPLY_LOOP, "send(2) failed, link error")
            assert self._pending_reply is not None
            return [actions.send_message(self._pending_reply)]
        return []

    def invariant(self, previous_state: Enum) -> None:
        state = self.state
        if state is FollowerState.WRITING:
            self.require(self._payload is not None, "no payload was received")
        if state is FollowerState.WRITTEN:
            self.require(
                self._restart_handled, "restart_handled was not set after the write"
            )
        if state is FollowerState.WRITE_FAILED:
            self.require(
                self._pending_result not in (None, SUCCESS),
                "a failed write did not produce an error result",
            )
        if (
            state is FollowerState.BLOB_REPLY_LOOP
            and previous_state is not FollowerState.BLOB_REPLY_LOOP
        ):
            self.require(
                previous_state
                in (FollowerState.WRITTEN, FollowerState.WRITE_FAILED),
                "a reply was prepared without a completed write attempt",
            )
            if self._pending_result == SUCCESS:
                self.require(
                    previous_state is FollowerState.WRITTEN,
                    "success would be acknowledged ahead of the disk",
                )
