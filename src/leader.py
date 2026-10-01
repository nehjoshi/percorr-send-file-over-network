from __future__ import annotations

import errno
from enum import Enum, auto
from pathlib import Path

from . import actions
from .actions import Action
from .events import Event, EventKind, Timer
from .messages import SUCCESS, Blob, BlobReply, RestartedQuery, RestartedReply
from .statemachine import History, StateConf, StateMachine

DEFAULT_RESTARTED_TIMEOUT_SECONDS = 5.0  # N in the handbook
DEFAULT_BLOB_TIMEOUT_SECONDS = 30.0  # M in the handbook


RETRYABLE_RESULTS = frozenset({errno.ENOMEM})


class LeaderState(Enum):
    INITIAL = auto()
    RESTARTED_LOOP = auto()
    RESTARTED_SENT = auto()
    RESTARTED_REPLIED = auto()
    BLOB_LOOP = auto()
    BLOB_SENT = auto()
    BLOB_REPLIED = auto()
    FINAL = auto()
    FAILED = auto()


LEADER_CONF = {
    LeaderState.INITIAL: StateConf(
        name="INITIAL",
        allowed=frozenset({LeaderState.RESTARTED_LOOP}),
        initial=True,
    ),
    LeaderState.RESTARTED_LOOP: StateConf(
        name="RESTARTED_LOOP",
        allowed=frozenset({LeaderState.RESTARTED_LOOP, LeaderState.RESTARTED_SENT}),
    ),
    LeaderState.RESTARTED_SENT: StateConf(
        name="RESTARTED_SENT",
        allowed=frozenset(
            {
                LeaderState.RESTARTED_SENT,
                LeaderState.RESTARTED_LOOP,
                LeaderState.RESTARTED_REPLIED,
            }
        ),
    ),
    LeaderState.RESTARTED_REPLIED: StateConf(
        name="RESTARTED_REPLIED",
        allowed=frozenset({LeaderState.BLOB_LOOP, LeaderState.FINAL}),
    ),
    LeaderState.BLOB_LOOP: StateConf(
        name="BLOB_LOOP",
        allowed=frozenset({LeaderState.BLOB_LOOP, LeaderState.BLOB_SENT}),
    ),
    LeaderState.BLOB_SENT: StateConf(
        name="BLOB_SENT",
        allowed=frozenset(
            {LeaderState.BLOB_SENT, LeaderState.BLOB_LOOP, LeaderState.BLOB_REPLIED}
        ),
    ),
    LeaderState.BLOB_REPLIED: StateConf(
        name="BLOB_REPLIED",
        allowed=frozenset(
            {LeaderState.BLOB_LOOP, LeaderState.FINAL, LeaderState.FAILED}
        ),
    ),
    LeaderState.FINAL: StateConf(name="FINAL", final=True),
    LeaderState.FAILED: StateConf(name="FAILED", final=True),
}


class Leader(StateMachine):
    def __init__(
        self,
        file_path: str | Path,
        restarted_timeout_seconds: float = DEFAULT_RESTARTED_TIMEOUT_SECONDS,
        blob_timeout_seconds: float = DEFAULT_BLOB_TIMEOUT_SECONDS,
        history: History | None = None,
    ) -> None:
        super().__init__("leader", LEADER_CONF, history)
        self._file_path = Path(file_path)
        self._restarted_timeout_seconds = restarted_timeout_seconds
        self._blob_timeout_seconds = blob_timeout_seconds

        # All of the following is volatile and is lost when the Leader crashes.
        self._payload: bytes | None = None
        self._armed_timers: set[Timer] = set()
        self._completion_evidence: str | None = None
        self._failure_result: int | None = None

        self._handlers = {
            LeaderState.RESTARTED_LOOP: self._in_restarted_loop,
            LeaderState.RESTARTED_SENT: self._in_restarted_sent,
            LeaderState.BLOB_LOOP: self._in_blob_loop,
            LeaderState.BLOB_SENT: self._in_blob_sent,
        }

    @property
    def completion_evidence(self) -> str | None:
        """Why the Leader believes the Follower holds the file. None until FINAL."""
        return self._completion_evidence

    @property
    def failure_result(self) -> int | None:
        return self._failure_result

    def start(self) -> list[Action]:
        self.move(LeaderState.RESTARTED_LOOP, "start transfer")
        return self._send_restarted_query()

    def handle(self, event: Event) -> list[Action]:
        handler = self._handlers.get(self.state)
        if handler is None:
            # No edge leaves the current state for any event, so the event is dropped.
            return []
        return handler(event)

    def _in_restarted_loop(self, event: Event) -> list[Action]:
        if event.kind is EventKind.SEND_COMPLETED:
            armed = self._arm(Timer.RESTARTED_TIMEOUT, self._restarted_timeout_seconds)
            self.move(
                LeaderState.RESTARTED_SENT,
                "send(2) completed, Restarted?() accepted by the kernel",
            )
            return [armed]
        if event.kind is EventKind.SEND_FAILED:
            self.move(LeaderState.RESTARTED_LOOP, "send(2) failed, link error")
            return self._send_restarted_query()
        return []

    def _in_restarted_sent(self, event: Event) -> list[Action]:
        if (
            event.kind is EventKind.TIMEOUT_EXPIRED
            and event.timer is Timer.RESTARTED_TIMEOUT
        ):
            self._disarm(Timer.RESTARTED_TIMEOUT)
            self.move(LeaderState.RESTARTED_LOOP, "timeout of N seconds expired")
            return self._send_restarted_query()
        if event.kind is EventKind.RECV_FAILED:
            cancelled = self._cancel(Timer.RESTARTED_TIMEOUT)
            self.move(LeaderState.RESTARTED_LOOP, "recv(2) failed, link error")
            return [cancelled, *self._send_restarted_query()]
        if event.kind is EventKind.RECV_COMPLETED:
            if isinstance(event.message, RestartedReply):
                cancelled = self._cancel(Timer.RESTARTED_TIMEOUT)
                self.move(
                    LeaderState.RESTARTED_REPLIED,
                    "recv(2) completed, RestartedReply(restarted) received",
                )
                return [cancelled, *self._evaluate_restarted_reply(event.message)]
            self.move(
                LeaderState.RESTARTED_SENT,
                "duplicate or unexpected message received, ignored",
            )
            return []
        return []

    def _evaluate_restarted_reply(self, reply: RestartedReply) -> list[Action]:
        if reply.restarted:
            self.move(LeaderState.BLOB_LOOP, "restarted is true")
            return self._send_blob()
        self._completion_evidence = "RestartedReply.restarted is false"
        self.move(
            LeaderState.FINAL, "restarted is false, Follower already holds the file"
        )
        return []

    def _in_blob_loop(self, event: Event) -> list[Action]:
        if event.kind is EventKind.SEND_COMPLETED:
            armed = self._arm(Timer.BLOB_TIMEOUT, self._blob_timeout_seconds)
            self.move(
                LeaderState.BLOB_SENT,
                "send(2) completed, Blob(payload) accepted by the kernel",
            )
            return [armed]
        if event.kind is EventKind.SEND_FAILED:
            self.move(LeaderState.BLOB_LOOP, "send(2) failed, link error")
            return self._send_blob()
        return []

    def _in_blob_sent(self, event: Event) -> list[Action]:
        if event.kind is EventKind.TIMEOUT_EXPIRED and event.timer is Timer.BLOB_TIMEOUT:
            self._disarm(Timer.BLOB_TIMEOUT)
            self.move(LeaderState.BLOB_LOOP, "timeout of M seconds expired")
            return self._send_blob()
        if event.kind is EventKind.RECV_FAILED:
            cancelled = self._cancel(Timer.BLOB_TIMEOUT)
            self.move(LeaderState.BLOB_LOOP, "recv(2) failed, link error")
            return [cancelled, *self._send_blob()]
        if event.kind is EventKind.RECV_COMPLETED:
            if isinstance(event.message, BlobReply):
                cancelled = self._cancel(Timer.BLOB_TIMEOUT)
                self.move(
                    LeaderState.BLOB_REPLIED,
                    "recv(2) completed, BlobReply(result) received",
                )
                return [cancelled, *self._evaluate_blob_reply(event.message)]
            self.move(
                LeaderState.BLOB_SENT,
                "duplicate or unexpected message received, ignored",
            )
            return []
        return []

    def _evaluate_blob_reply(self, reply: BlobReply) -> list[Action]:
        if reply.result == SUCCESS:
            self._completion_evidence = "BlobReply.result is 0"
            self.move(LeaderState.FINAL, "result is 0, success")
            return []
        if reply.result in RETRYABLE_RESULTS:
            self.move(LeaderState.BLOB_LOOP, f"result is {reply.result}, retryable")
            return self._send_blob()
        self._failure_result = reply.result
        self.move(LeaderState.FAILED, f"result is {reply.result}, terminal")
        return []

    def _send_restarted_query(self) -> list[Action]:
        return [actions.send_message(RestartedQuery())]

    def _send_blob(self) -> list[Action]:
        # V0 assumes the Leader's disk never fails, so the read has no failure edge.
        self._payload = self._file_path.read_bytes()
        return [actions.send_message(Blob(payload=self._payload))]

    def _arm(self, timer: Timer, seconds: float) -> Action:
        self._armed_timers.add(timer)
        return actions.arm_timeout(timer, seconds)

    def _cancel(self, timer: Timer) -> Action:
        self._armed_timers.discard(timer)
        return actions.cancel_timeout(timer)

    def _disarm(self, timer: Timer) -> None:
        self._armed_timers.discard(timer)

    def invariant(self, previous_state: Enum) -> None:
        state = self.state
        if state is LeaderState.RESTARTED_SENT:
            self.require(
                Timer.RESTARTED_TIMEOUT in self._armed_timers,
                "waiting for a reply with no timeout armed",
            )
        if state is LeaderState.BLOB_SENT:
            self.require(
                Timer.BLOB_TIMEOUT in self._armed_timers,
                "waiting for a reply with no timeout armed",
            )
            self.require(self._payload is not None, "no payload was prepared")
        if state is LeaderState.FINAL:
            self.require(
                self._completion_evidence is not None,
                "reached without positive evidence from the Follower",
            )
            self.require(
                previous_state
                in (LeaderState.RESTARTED_REPLIED, LeaderState.BLOB_REPLIED),
                "reached from a state that did not receive a reply",
            )
        if state is LeaderState.FAILED:
            self.require(
                self._failure_result not in (None, SUCCESS),
                "reached without a terminal result code",
            )
