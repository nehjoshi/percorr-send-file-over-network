from __future__ import annotations

from dataclasses import dataclass

SUCCESS = 0


@dataclass(frozen=True)
class RestartedQuery:
    """`Restarted?()` — Leader to Follower."""


@dataclass(frozen=True)
class RestartedReply:
    """Follower to Leader. `restarted` is the negation of `restart_handled`."""

    restarted: bool


@dataclass(frozen=True)
class Blob:
    """Leader to Follower. In V0 the payload is the whole file."""

    payload: bytes

    def __repr__(self) -> str:
        return f"Blob(payload={len(self.payload)} bytes)"


@dataclass(frozen=True)
class BlobReply:
    """Follower to Leader. `result` is SUCCESS or an errno value."""

    result: int


Message = RestartedQuery | RestartedReply | Blob | BlobReply
