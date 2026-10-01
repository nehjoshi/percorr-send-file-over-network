from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from .events import Timer
from .messages import Message


class ActionKind(Enum):
    SEND_MESSAGE = auto()
    ARM_TIMEOUT = auto()
    CANCEL_TIMEOUT = auto()
    WRITE_PAYLOAD = auto()


@dataclass(frozen=True)
class Action:
    kind: ActionKind
    message: Message | None = None
    timer: Timer | None = None
    seconds: float | None = None
    payload: bytes | None = None

    def __str__(self) -> str:
        parts = [self.kind.name]
        if self.message is not None:
            parts.append(repr(self.message))
        if self.timer is not None:
            parts.append(self.timer.name)
        if self.seconds is not None:
            parts.append(f"{self.seconds}s")
        if self.payload is not None:
            parts.append(f"{len(self.payload)} bytes")
        return " ".join(parts)


def send_message(message: Message) -> Action:
    return Action(ActionKind.SEND_MESSAGE, message=message)


def arm_timeout(timer: Timer, seconds: float) -> Action:
    return Action(ActionKind.ARM_TIMEOUT, timer=timer, seconds=seconds)


def cancel_timeout(timer: Timer) -> Action:
    return Action(ActionKind.CANCEL_TIMEOUT, timer=timer)


def write_payload(payload: bytes) -> Action:
    return Action(ActionKind.WRITE_PAYLOAD, payload=payload)
