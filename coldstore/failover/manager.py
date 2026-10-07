"""Automatic primary/standby failover for the cold air fan pair.

Every switch, whether triggered by runtime rotation, a fault, or an
operator, funnels through :meth:`FailoverManager.request_switch`. A single
non-reentrant lock plus a persisted fencing epoch serializes concurrent
preemption so the two fans can never promote themselves at the same time.
Switches are refused while a defrost is running, every failure rolls back
to the previous primary, and a switch is only committed once airflow is
confirmed on the new unit.
"""

from __future__ import annotations

import json
import threading
import time

from coldstore.audit import Auditor
from coldstore.fan import FanSet
from coldstore.room import RoomRegistry
from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

from .policy import Policy, load_policy
from .report import FailoverState, SwitchResult

EPOCH_KEY = "failover:epoch"
DUTY_HOURS_KEY = "failover:duty-hours"
RUNTIME_KEY_PREFIX = "failover:runtime:"
PENDING_KEY = "failover:pending"
CONFIRMING_KEY = "failover:confirming"
FAILBACK_TARGET_KEY = "failover:failback-target"
LAST_SWITCH_KEY = "failover:last-switch"

AUDIT_KINDS = (
    "fan.failover.attempt",
    "fan.failover.switch",
    "fan.failover.rollback",
    "fan.failover.defer",
    "fan.failover.reject",
    "fan.airflow.confirm",
)

REASON_MANUAL = "manual"
REASON_RUNTIME = "runtime"
REASON_FAULT = "fault"
REASON_FAILBACK = "failback"


class FailoverManager:
    """Runs serialized, audited, reversible fan duty switches."""

    def __init__(self, fans: FanSet, rooms: RoomRegistry, store: Store, auditor: Auditor) -> None:
        self._fans = fans
        self._rooms = rooms
        self._store = store
        self._auditor = auditor
        self._switch_lock = threading.Lock()

    # -- policy ---------------------------------------------------------

    def policy(self) -> Policy:
        return load_policy(self._store)

    # -- switch entry point ----------------------------------------------

    def request_switch(
        self,
        target: str = "",
        reason: str = REASON_MANUAL,
        automatic: bool = False,
        now: float | None = None,
    ) -> SwitchResult:
        """Attempt one serialized switch.

        The lock is taken without blocking: a second preemption that arrives
        while a switch is in flight is rejected outright instead of piling
        up, so the pair can never race itself.
        """

        if not self._switch_lock.acquire(blocking=False):
            self._auditor.record("fan.failover.reject", f"target={target or 'auto'} busy")
            return SwitchResult("rejected", self._fans.primary(), target, reason, "switch in progress", self.epoch())
        try:
            epoch = self._bump_epoch()
            return self._attempt(target, reason, automatic, epoch, self._now(now))
        finally:
            self._switch_lock.release()

    def _attempt(
        self, target: str, reason: str, automatic: bool, epoch: int, now: float
    ) -> SwitchResult:
        primary = self._fans.primary()
        if not target:
            target = self._fans.healthy_standby()
        self._auditor.record(
            "fan.failover.attempt", f"{primary or ''}->{target or ''} reason={reason} epoch={epoch}"
        )

        if self._confirming() is not None:
            return self._reject(primary, target, reason, epoch, "switch awaiting confirmation")
        if self._rooms.defrosting_ids():
            if automatic:
                self._store.put(PENDING_KEY, json.dumps({"target": target, "reason": reason}))
                self._auditor.record("fan.failover.defer", f"{primary or ''}->{target} reason={reason}")
                return SwitchResult("deferred", primary, target, reason, "defrost in progress", epoch)
            return self._reject(primary, target, reason, epoch, "defrost in progress")
        if not target:
            return self._reject(primary, target, reason, epoch, "no healthy standby")
        unit = self._fans.fan(target)
        if unit is None:
            raise ValueError(f"fan {target} not found")
        if unit.faulted:
            return self._reject(primary, target, reason, epoch, f"fan {target} is faulted")
        if target == primary:
            return SwitchResult("noop", primary, target, reason, "already primary", epoch)

        self._fans.switch_over(target)
        self._fans.start(target)

        policy = self.policy()
        if policy.confirm_mode == "manual":
            deadline = now + policy.confirm_timeout_seconds
            self._store.put(
                CONFIRMING_KEY,
                json.dumps(
                    {"epoch": epoch, "from": primary, "to": target, "reason": reason, "deadline": deadline}
                ),
            )
            return SwitchResult("confirming", primary, target, reason, "awaiting airflow confirmation", epoch)

        if not self._airflow_ok(target):
            self._rollback(primary, target, f"{target}->{primary or ''} reason=airflow-self-check")
            return SwitchResult("rolled-back", primary, target, reason, "airflow self-check failed", epoch)
        self._commit(primary, target, reason, now)
        return SwitchResult("switched", primary, target, reason, "airflow confirmed", epoch)

    # -- airflow confirmation --------------------------------------------

    def confirm_airflow(self, ok: bool, now: float | None = None) -> SwitchResult:
        """Close a manual-mode switch once the field reports airflow."""

        with self._switch_lock:
            epoch = self._bump_epoch()
            confirming = self._confirming()
            if confirming is None:
                return SwitchResult("noop", self._fans.primary(), "", REASON_MANUAL, "nothing to confirm", epoch)
            previous = str(confirming["from"])
            target = str(confirming["to"])
            reason = str(confirming.get("reason", REASON_MANUAL))
            if ok and self._airflow_ok(target):
                self._store.delete(CONFIRMING_KEY)
                self._commit(previous, target, reason, self._now(now))
                self._auditor.record("fan.airflow.confirm", f"{target} ok=true")
                return SwitchResult("switched", previous, target, reason, "airflow confirmed", epoch)
            self._store.delete(CONFIRMING_KEY)
            self._rollback(previous, target, f"{target}->{previous or ''} reason=airflow-rejected")
            self._auditor.record("fan.airflow.confirm", f"{target} ok=false")
            return SwitchResult("rolled-back", previous, target, reason, "airflow not confirmed", epoch)

    # -- runtime accounting -----------------------------------------------

    def tick(self, hours: float) -> float:
        """Accumulate runtime against the fan currently holding duty."""

        if hours <= 0:
            raise ValueError("tick hours must be positive")
        primary = self._fans.primary()
        if not primary:
            return self.duty_hours()
        duty = self.duty_hours() + hours
        save_float(self._store, DUTY_HOURS_KEY, duty)
        lifetime, _ = load_float(self._store, f"{RUNTIME_KEY_PREFIX}{primary}")
        save_float(self._store, f"{RUNTIME_KEY_PREFIX}{primary}", lifetime + hours)
        return duty

    def duty_hours(self) -> float:
        value, _ = load_float(self._store, DUTY_HOURS_KEY)
        return value

    def runtime_hours(self, fan_id: str) -> float:
        value, _ = load_float(self._store, f"{RUNTIME_KEY_PREFIX}{fan_id}")
        return value

    def rotation_due(self) -> bool:
        policy = self.policy()
        return policy.trigger == "runtime" and self.duty_hours() >= policy.runtime_limit_hours

    # -- fault and failback -------------------------------------------------

    def notify_fault(self, fan_id: str, now: float | None = None) -> SwitchResult:
        """Fail over immediately when the primary faults.

        A fault is an override event, not a rotation trigger: it jumps the
        queue but still passes through the same serialized switch path.
        """

        if fan_id != self._fans.primary():
            return SwitchResult("noop", self._fans.primary(), "", REASON_FAULT, "not the primary", self.epoch())
        target = self._fans.healthy_standby()
        if not target:
            self._auditor.record("fan.failover.reject", f"{fan_id} faulted, no healthy standby")
            return SwitchResult("rejected", fan_id, "", REASON_FAULT, "no healthy standby", self.epoch())
        return self.request_switch(target, REASON_FAULT, automatic=True, now=now)

    def failback(self, force: bool = False, hour: int | None = None, now: float | None = None) -> SwitchResult:
        """Return duty to a repaired unit, inside the window unless forced."""

        target = self.failback_target()
        if not target:
            return SwitchResult("noop", self._fans.primary(), "", REASON_FAILBACK, "no failback target", self.epoch())
        policy = self.policy()
        if not force and policy.failback_mode == "window" and not policy.in_window(self._hour(hour)):
            return SwitchResult(
                "rejected",
                self._fans.primary(),
                target,
                REASON_FAILBACK,
                f"outside failback window {policy.window_start_hour:02d}-{policy.window_end_hour:02d}",
                self.epoch(),
            )
        result = self.request_switch(target, REASON_FAILBACK, automatic=False, now=now)
        if result.switched:
            self._store.delete(FAILBACK_TARGET_KEY)
        return result

    def failback_target(self) -> str:
        raw, present = self._store.get(FAILBACK_TARGET_KEY)
        return raw if present else ""

    # -- deferred and scheduled work -----------------------------------------

    def evaluate(self, hour: int | None = None, now: float | None = None) -> SwitchResult | None:
        """Run one automatic pass: timeouts, deferred faults, rotation, failback."""

        moment = self._now(now)
        confirming = self._confirming()
        if confirming is not None:
            if moment <= float(confirming["deadline"]):
                return None
            self._store.delete(CONFIRMING_KEY)
            previous = str(confirming["from"])
            target = str(confirming["to"])
            self._rollback(previous, target, f"{target}->{previous or ''} reason=confirm-timeout")
            return SwitchResult("rolled-back", previous, target, REASON_MANUAL, "confirm timeout", self.epoch())

        if self._rooms.defrosting_ids():
            return None

        pending = self._pending()
        if pending is not None:
            self._store.delete(PENDING_KEY)
            return self.request_switch(str(pending["target"]), str(pending["reason"]), automatic=True, now=moment)

        if self.rotation_due():
            standby = self._fans.healthy_standby()
            if standby:
                return self.request_switch(standby, REASON_RUNTIME, automatic=True, now=moment)

        target = self.failback_target()
        policy = self.policy()
        if target and policy.failback_mode == "window" and policy.in_window(self._hour(hour)):
            unit = self._fans.fan(target)
            if unit is not None and not unit.faulted:
                result = self.request_switch(target, REASON_FAILBACK, automatic=True, now=moment)
                if result.switched:
                    self._store.delete(FAILBACK_TARGET_KEY)
                return result
        return None

    # -- projections ---------------------------------------------------------

    def epoch(self) -> int:
        value, _ = load_float(self._store, EPOCH_KEY)
        return int(value)

    def last_switch(self) -> str:
        raw, present = self._store.get(LAST_SWITCH_KEY)
        return raw if present else ""

    def state(self) -> FailoverState:
        confirming = self._confirming()
        pending = self._pending()
        policy = self.policy()
        return FailoverState(
            primary=self._fans.primary(),
            active=self._fans.active(),
            epoch=self.epoch(),
            duty_hours=self.duty_hours(),
            runtime_limit_hours=policy.runtime_limit_hours,
            rotation_due=self.rotation_due(),
            confirming="" if confirming is None else str(confirming["to"]),
            pending="" if pending is None else str(pending["target"]),
            failback_target=self.failback_target(),
            defrosting=bool(self._rooms.defrosting_ids()),
            runtimes={unit.id: self.runtime_hours(unit.id) for unit in self._fans.fans()},
            policy=policy.as_dict(),
        )

    def history(self) -> list[dict[str, str]]:
        """Switch records and airflow confirmations from the audit trail."""

        return [
            entry.as_dict() for entry in self._auditor.entries() if entry.kind in AUDIT_KINDS
        ]

    def describe(self) -> str:
        state = self.state()
        return (
            f"failover primary={state.primary or ''} epoch={state.epoch} "
            f"duty_hours={state.duty_hours:.4f}"
        )

    # -- internals -------------------------------------------------------------

    def _commit(self, previous: str, target: str, reason: str, now: float) -> None:
        if previous and previous != target:
            self._fans.stop(previous)
        save_float(self._store, DUTY_HOURS_KEY, 0.0)
        if reason == REASON_FAULT and previous:
            self._store.put(FAILBACK_TARGET_KEY, previous)
        self._store.put(LAST_SWITCH_KEY, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)))
        self._auditor.record("fan.failover.switch", f"{previous or ''}->{target} reason={reason}")
        self._auditor.record("fan.airflow.confirm", f"{target} ok=true")

    def _rollback(self, previous: str, target: str, detail: str) -> None:
        self._fans.stop(target)
        if previous:
            try:
                self._fans.switch_over(previous)
            except ValueError:
                self._fans.demote(target)
            else:
                try:
                    self._fans.start(previous)
                except ValueError:
                    pass
        else:
            self._fans.demote(target)
        self._auditor.record("fan.failover.rollback", detail)

    def _reject(self, primary: str, target: str, reason: str, epoch: int, detail: str) -> SwitchResult:
        self._auditor.record("fan.failover.reject", f"{primary or ''}->{target or ''} {detail}")
        return SwitchResult("rejected", primary, target, reason, detail, epoch)

    def _airflow_ok(self, target: str) -> bool:
        return self._fans.active() == target

    def _confirming(self) -> dict[str, object] | None:
        raw, present = self._store.get(CONFIRMING_KEY)
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def _pending(self) -> dict[str, object] | None:
        raw, present = self._store.get(PENDING_KEY)
        if not present:
            return None
        try:
            payload = json.loads(raw)
        except ValueError:
            return None
        return payload if isinstance(payload, dict) else None

    def _bump_epoch(self) -> int:
        epoch = self.epoch() + 1
        save_float(self._store, EPOCH_KEY, float(epoch))
        return epoch

    @staticmethod
    def _now(now: float | None) -> float:
        return time.time() if now is None else now

    @staticmethod
    def _hour(hour: int | None) -> int:
        return time.localtime().tm_hour if hour is None else hour
