"""Automatic primary/standby failover for one cold air fan group.

The manager is the single arbiter for duty changes so two fans can never
grab primary at the same time: the whole begin, confirm, commit or
rollback sequence runs inside one serialized section and concurrent
requests are answered with a busy outcome. Switching is refused while a
defrost is active and deferred work is retried by the next tick. Every
decision, including the airflow confirmation and the resulting duty
state, is written to the audit trail.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Callable

from coldstore.audit import Auditor
from coldstore.fan import STANDBY, FanSet
from coldstore.room import RoomRegistry
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from .report import FailoverState, SwitchOutcome

RUNTIME_KEY_PREFIX = "failover:runtime:"
FAULT_KEY_PREFIX = "failover:faults:"
REPAIRED_KEY_PREFIX = "failover:repaired:"
STRETCH_KEY = "failover:stretch"
PENDING_KEY = "failover:pending"
DEFER_KEY = "failover:deferred"
ROTATE_KEY = "failover:rotate_hours"
SOAK_KEY = "failover:soak_hours"
WINDOW_START_KEY = "failover:window_start"
WINDOW_END_KEY = "failover:window_end"

DEFAULT_ROTATE_HOURS = 720.0
DEFAULT_SOAK_HOURS = 24.0
DEFAULT_WINDOW_START = 2
DEFAULT_WINDOW_END = 4

STATUS_SWITCHED = "switched"
STATUS_ROLLED_BACK = "rolled_back"
STATUS_DEFERRED = "deferred"
STATUS_BUSY = "busy"
STATUS_NOOP = "noop"
STATUS_REFUSED = "refused"

MAX_HOURS = 100_000.0


def validate_window(start: int, end: int) -> None:
    """Failback window hours must be distinct whole hours of the day."""

    for hour in (start, end):
        if hour < 0 or hour > 23:
            raise ValueError("window hours must be between 0 and 23")
    if start == end:
        raise ValueError("window start and end must differ")


def validate_policy(rotate_hours: float, soak_hours: float) -> None:
    """Rotation needs a positive period and the soak cannot be negative."""

    if rotate_hours <= 0 or rotate_hours > MAX_HOURS:
        raise ValueError("rotate_hours must be positive")
    if soak_hours < 0 or soak_hours > MAX_HOURS:
        raise ValueError("soak_hours must be non-negative")


def _default_confirmer(fans: FanSet, fan_id: str) -> bool:
    """Airflow stands confirmed when the started fan runs healthy."""

    unit = fans.fan(fan_id)
    return unit is not None and unit.running and not unit.faulted


class FailoverManager:
    """Serializes and audits every primary/standby changeover."""

    def __init__(
        self,
        fans: FanSet,
        rooms: RoomRegistry,
        store: Store,
        auditor: Auditor,
        clock: Callable[[], float] = time.time,
        confirmer: Callable[[FanSet, str], bool] | None = None,
    ) -> None:
        self._fans = fans
        self._rooms = rooms
        self._store = store
        self._auditor = auditor
        self._clock = clock
        self._confirmer = confirmer or _default_confirmer
        self._switch_lock = threading.Lock()

    def rotate_hours(self) -> float:
        value, present = load_float(self._store, ROTATE_KEY)
        return value if present else DEFAULT_ROTATE_HOURS

    def soak_hours(self) -> float:
        value, present = load_float(self._store, SOAK_KEY)
        return value if present else DEFAULT_SOAK_HOURS

    def set_policy(self, rotate_hours: float, soak_hours: float) -> None:
        validate_policy(rotate_hours, soak_hours)
        save_float(self._store, ROTATE_KEY, rotate_hours)
        save_float(self._store, SOAK_KEY, soak_hours)

    def window(self) -> tuple[int, int]:
        start, present = load_float(self._store, WINDOW_START_KEY)
        end, end_present = load_float(self._store, WINDOW_END_KEY)
        if not present or not end_present:
            return DEFAULT_WINDOW_START, DEFAULT_WINDOW_END
        return int(start), int(end)

    def set_window(self, start: int, end: int) -> None:
        validate_window(start, end)
        save_float(self._store, WINDOW_START_KEY, float(start))
        save_float(self._store, WINDOW_END_KEY, float(end))

    def runtime_hours(self, fan_id: str) -> float:
        value, _ = load_float(self._store, f"{RUNTIME_KEY_PREFIX}{fan_id}")
        return value

    def fault_count(self, fan_id: str) -> int:
        value, _ = load_float(self._store, f"{FAULT_KEY_PREFIX}{fan_id}")
        return int(value)

    def stretch(self) -> float:
        """Hours the current primary has been on duty without a break."""

        value, _ = load_float(self._store, STRETCH_KEY)
        return value

    def pending_failback(self) -> str:
        value, present = self._store.get(PENDING_KEY)
        return value if present else ""

    def deferred(self) -> str:
        value, present = self._store.get(DEFER_KEY)
        return value if present else ""

    def tick(self, hours: float = 1.0) -> SwitchOutcome:
        """Accumulate runtime for running fans, then evaluate triggers."""

        if hours <= 0 or hours > MAX_HOURS:
            raise ValueError("hours must be positive")
        for fan_id in self._fans.running_ids():
            save_float(
                self._store,
                f"{RUNTIME_KEY_PREFIX}{fan_id}",
                self.runtime_hours(fan_id) + hours,
            )
        if self._fans.active():
            save_float(self._store, STRETCH_KEY, self.stretch() + hours)
        return self.evaluate("tick")

    def on_fault(self, fan_id: str) -> SwitchOutcome:
        """Bookkeep a fault and evaluate right away, unless defrosting."""

        save_float(
            self._store, f"{FAULT_KEY_PREFIX}{fan_id}", self.fault_count(fan_id) + 1
        )
        if self.pending_failback() == fan_id:
            self._store.delete(PENDING_KEY)
        return self.evaluate("fault")

    def on_repair(self, fan_id: str) -> None:
        """Start the soak clock for a repaired unit."""

        save_float(self._store, f"{REPAIRED_KEY_PREFIX}{fan_id}", self._clock())

    def request_switch(self, target: str, reason: str = "manual") -> SwitchOutcome:
        """Operator requested changeover, still through the serialized path."""

        unit = self._fans.fan(target)
        if unit is None:
            raise ValueError(f"fan {target} not found")
        if unit.faulted:
            raise ValueError(f"fan {target} is faulted")
        if self._rooms.defrosting_ids():
            return self._defer(reason, target)
        return self._locked_switch(target, reason)

    def evaluate(self, reason: str) -> SwitchOutcome:
        """Run the automatic triggers in priority order."""

        if self._rooms.defrosting_ids():
            return self._defer(reason, "")
        self._store.delete(DEFER_KEY)
        primary = self._fans.primary()
        primary_unit = self._fans.fan(primary) if primary else None
        if primary_unit is None or primary_unit.faulted:
            target = self._best_standby()
            if not target:
                return SwitchOutcome(STATUS_REFUSED, reason, primary, "", "no healthy standby")
            return self._locked_switch(target, "fault")
        if self.stretch() >= self.rotate_hours():
            target = self._best_standby()
            if target:
                return self._locked_switch(target, "rotate")
        pending = self.pending_failback()
        if pending and self._failback_due(pending):
            return self._locked_switch(pending, "failback")
        return SwitchOutcome(STATUS_NOOP, reason, primary, "", "no trigger")

    def state(self) -> FailoverState:
        fans = self._fans.fans()
        start, end = self.window()
        return FailoverState(
            primary=self._fans.primary(),
            active=self._fans.active(),
            standbys=[unit.id for unit in fans if unit.role == STANDBY],
            faulted=self._fans.faulted_ids(),
            runtimes={unit.id: self.runtime_hours(unit.id) for unit in fans},
            faults={unit.id: self.fault_count(unit.id) for unit in fans},
            stretch=self.stretch(),
            rotate_hours=self.rotate_hours(),
            soak_hours=self.soak_hours(),
            window_start=start,
            window_end=end,
            pending_failback=self.pending_failback(),
            deferred=self.deferred(),
            defrost_blocked=bool(self._rooms.defrosting_ids()),
            switching=self._switch_lock.locked(),
        )

    def describe(self) -> str:
        state = self.state()
        return (
            f"failover primary={state.primary or ''} stretch={state.stretch:.1f}h "
            f"pending={state.pending_failback or ''}"
        )

    def _locked_switch(self, target: str, reason: str) -> SwitchOutcome:
        if not self._switch_lock.acquire(blocking=False):
            return SwitchOutcome(
                STATUS_BUSY, reason, self._fans.primary(), target, "switch already in progress"
            )
        try:
            return self._switch(target, reason)
        finally:
            self._switch_lock.release()

    def _switch(self, target: str, reason: str) -> SwitchOutcome:
        origin = self._fans.primary()
        if origin == target:
            return SwitchOutcome(STATUS_NOOP, reason, origin, target, "already primary")
        origin_unit = self._fans.fan(origin) if origin else None
        origin_running = bool(origin_unit and origin_unit.running)
        self._fans.start(target)
        confirmed = self._confirmer(self._fans, target)
        self._audit("fan_airflow", {"fan": target, "confirmed": confirmed})
        if not confirmed:
            self._fans.stop(target)
            if origin and origin_running:
                origin_now = self._fans.fan(origin)
                if origin_now is not None and not origin_now.faulted:
                    self._fans.start(origin)
            self._audit("fan_rollback", {"from": origin, "to": target, "reason": reason})
            return SwitchOutcome(STATUS_ROLLED_BACK, reason, origin, target, "airflow not confirmed")
        self._fans.switch_over(target)
        if origin and origin != target and origin_running:
            self._fans.stop(origin)
        save_float(self._store, STRETCH_KEY, 0.0)
        self._update_pending(reason, origin)
        self._audit("fan_switch", {"from": origin, "to": target, "reason": reason})
        return SwitchOutcome(STATUS_SWITCHED, reason, origin, target, "airflow confirmed")

    def _update_pending(self, reason: str, origin: str) -> None:
        if reason == "fault" and origin:
            self._store.put(PENDING_KEY, origin)
        elif reason in ("failback", "manual"):
            self._store.delete(PENDING_KEY)

    def _defer(self, reason: str, target: str) -> SwitchOutcome:
        self._store.put(DEFER_KEY, reason)
        self._audit(
            "fan_defer",
            {"reason": reason, "target": target, "defrosting": self._rooms.defrosting_ids()},
        )
        return SwitchOutcome(STATUS_DEFERRED, reason, self._fans.primary(), target, "defrost active")

    def _best_standby(self) -> str:
        """Least faulted, then least worn healthy standby takes duty next."""

        candidates = [
            unit for unit in self._fans.fans() if unit.role == STANDBY and not unit.faulted
        ]
        if not candidates:
            return ""
        candidates.sort(
            key=lambda unit: (
                self.fault_count(unit.id),
                self.runtime_hours(unit.id),
                unit.id,
            )
        )
        return candidates[0].id

    def _failback_due(self, fan_id: str) -> bool:
        unit = self._fans.fan(fan_id)
        if unit is None or unit.faulted:
            return False
        if not self._in_window():
            return False
        repaired, present = load_float(self._store, f"{REPAIRED_KEY_PREFIX}{fan_id}")
        if not present:
            return False
        return self._clock() - repaired >= self.soak_hours() * 3600

    def _in_window(self) -> bool:
        start, end = self.window()
        hour = time.localtime(self._clock()).tm_hour
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def _audit(self, kind: str, payload: dict[str, object]) -> None:
        payload.setdefault("primary", self._fans.primary())
        payload.setdefault(
            "standby", [unit.id for unit in self._fans.fans() if unit.role == STANDBY]
        )
        self._auditor.record(kind, json.dumps(payload, ensure_ascii=False, sort_keys=True))
