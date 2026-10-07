"""Failover projections."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SwitchResult:
    """Outcome of one serialized switch attempt."""

    status: str
    from_fan: str
    to_fan: str
    reason: str
    detail: str
    epoch: int

    @property
    def switched(self) -> bool:
        return self.status in ("switched", "confirming")

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "from": self.from_fan,
            "to": self.to_fan,
            "reason": self.reason,
            "detail": self.detail,
            "epoch": self.epoch,
            "switched": self.switched,
        }


@dataclass(frozen=True)
class FailoverState:
    """Everything the monitoring page needs to show current duty."""

    primary: str
    active: str
    epoch: int
    duty_hours: float
    runtime_limit_hours: float
    rotation_due: bool
    confirming: str
    pending: str
    failback_target: str
    defrosting: bool
    runtimes: dict[str, float]
    policy: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "primary": self.primary,
            "active": self.active,
            "epoch": self.epoch,
            "duty_hours": self.duty_hours,
            "runtime_limit_hours": self.runtime_limit_hours,
            "rotation_due": self.rotation_due,
            "confirming": self.confirming,
            "pending": self.pending,
            "failback_target": self.failback_target,
            "defrosting": self.defrosting,
            "runtimes": dict(self.runtimes),
            "policy": dict(self.policy),
        }
