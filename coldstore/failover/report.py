"""Failover projections."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class SwitchOutcome:
    """The result of one serialized changeover attempt."""

    status: str
    reason: str
    origin: str
    target: str
    detail: str

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "reason": self.reason,
            "origin": self.origin,
            "target": self.target,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class FailoverState:
    """Duty, ledger and policy view for the monitoring page."""

    primary: str
    active: str
    standbys: list[str] = field(default_factory=list)
    faulted: list[str] = field(default_factory=list)
    runtimes: dict[str, float] = field(default_factory=dict)
    faults: dict[str, int] = field(default_factory=dict)
    stretch: float = 0.0
    rotate_hours: float = 0.0
    soak_hours: float = 0.0
    window_start: int = 0
    window_end: int = 0
    pending_failback: str = ""
    deferred: str = ""
    defrost_blocked: bool = False
    switching: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "primary": self.primary,
            "active": self.active,
            "standbys": list(self.standbys),
            "faulted": list(self.faulted),
            "runtimes": dict(self.runtimes),
            "faults": dict(self.faults),
            "stretch": self.stretch,
            "rotate_hours": self.rotate_hours,
            "soak_hours": self.soak_hours,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "pending_failback": self.pending_failback,
            "deferred": self.deferred,
            "defrost_blocked": self.defrost_blocked,
            "switching": self.switching,
        }
