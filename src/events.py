from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

from .messages import Message


class Timer(Enum):
    RESTARTED_TIMEOUT = auto()  # N seconds, guards Restarted?()
    BLOB_TIMEOUT = auto()  # M seconds, guards Blob()


class EventKind(Enum):
    SEND_COMPLETED = auto()  # message accepted by the kernel, which is not delivery
    SEND_FAILED = auto()
    RECV_COMPLETED = auto()
    RECV_FAILED = auto()
    WRITE_COMPLETED = auto()
    WRITE_FAILED = auto()
    TIMEOUT_EXPIRED = auto()


@dataclass(frozen=True)
class Event:
    kind: EventKind
    message: Message | None = None
    error: int | None = None
    timer: Timer | None = None

    def __str__(self) -> str:
        parts = [self.kind.name]
        if self.message is not None:
            parts.append(repr(self.message))
        if self.error is not None:
            parts.append(f"error={self.error}")
        if self.timer is not None:
            parts.append(self.timer.name)
        return " ".join(parts)


def send_completed() -> Event:
    return Event(EventKind.SEND_COMPLETED)


def send_failed(error: int) -> Event:
    return Event(EventKind.SEND_FAILED, error=error)


def recv_completed(message: Message) -> Event:
    return Event(EventKind.RECV_COMPLETED, message=message)


def recv_failed(error: int) -> Event:
    return Event(EventKind.RECV_FAILED, error=error)


def write_completed() -> Event:
    return Event(EventKind.WRITE_COMPLETED)


def write_failed(error: int) -> Event:
    return Event(EventKind.WRITE_FAILED, error=error)


def timeout_expired(timer: Timer) -> Event:
    return Event(EventKind.TIMEOUT_EXPIRED, timer=timer)
