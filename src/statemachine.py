from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterator, Mapping


class IllegalTransition(Exception):
    """A move was requested that the state table forbids."""


class InvariantViolation(Exception):
    """A state machine invariant did not hold after a transition."""


@dataclass(frozen=True)
class StateConf:
    """One row of the state table."""

    name: str
    allowed: frozenset[Enum] = frozenset()
    initial: bool = False
    final: bool = False


@dataclass(frozen=True)
class HistoryRecord:
    timestamp: float
    machine: str
    previous_state: str
    next_state: str
    cause: str

    def __str__(self) -> str:
        return (
            f"{self.timestamp:12.6f} {self.machine:<8} "
            f"{self.previous_state} => {self.next_state} [{self.cause}]"
        )


@dataclass
class History:
    """Ordered sequence of state transitions.

    Sharing one instance between the Leader and the Follower interleaves both
    machines into a single timeline.
    """

    records: list[HistoryRecord] = field(default_factory=list)

    def append(self, record: HistoryRecord) -> None:
        self.records.append(record)

    def __iter__(self) -> Iterator[HistoryRecord]:
        return iter(self.records)

    def __len__(self) -> int:
        return len(self.records)

    def format(self) -> str:
        return "\n".join(str(record) for record in self.records)


class StateMachine:
    def __init__(
        self,
        name: str,
        conf: Mapping[Enum, StateConf],
        history: History | None = None,
    ) -> None:
        initial = [state for state, row in conf.items() if row.initial]
        if len(initial) != 1:
            raise ValueError(
                f"{name}: exactly one initial state is required, found {len(initial)}"
            )
        for state, row in conf.items():
            unknown = row.allowed - set(conf)
            if unknown:
                raise ValueError(f"{name}: {row.name} allows unknown states {unknown}")
            if row.final and row.allowed:
                raise ValueError(
                    f"{name}: final state {row.name} must have no allowed transitions"
                )

        self.name = name
        self.history = history if history is not None else History()
        self._conf = conf
        self._state = initial[0]
        self.history.append(
            HistoryRecord(
                timestamp=time.monotonic(),
                machine=name,
                previous_state="-",
                next_state=self._conf[self._state].name,
                cause="initialised",
            )
        )

    @property
    def state(self) -> Enum:
        return self._state

    @property
    def state_name(self) -> str:
        return self._conf[self._state].name

    @property
    def is_final(self) -> bool:
        return self._conf[self._state].final

    def is_move_allowed(self, previous_state: Enum, next_state: Enum) -> bool:
        return next_state in self._conf[previous_state].allowed

    def move(self, next_state: Enum, cause: str) -> None:
        previous_state = self._state
        if not self.is_move_allowed(previous_state, next_state):
            raise IllegalTransition(
                f"{self.name}: {self._conf[previous_state].name} => "
                f"{self._conf[next_state].name} is not allowed [{cause}]"
            )
        self._state = next_state
        self.history.append(
            HistoryRecord(
                timestamp=time.monotonic(),
                machine=self.name,
                previous_state=self._conf[previous_state].name,
                next_state=self._conf[next_state].name,
                cause=cause,
            )
        )
        self.invariant(previous_state)

    def invariant(self, previous_state: Enum) -> None:
        """Checked after every transition. Subclasses raise InvariantViolation."""

    def require(self, condition: bool, message: str) -> None:
        if not condition:
            raise InvariantViolation(f"{self.name}: in {self.state_name}, {message}")

    def __repr__(self) -> str:
        return f"<{type(self).__name__} {self.state_name}>"
