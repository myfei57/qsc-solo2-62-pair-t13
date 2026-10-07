"""Persisted failover policy for the cold air fan pair.

Two decisions are pinned down here:

* Rotation follows accumulated runtime, never fault count. A fault on the
  primary is an immediate override event, not a rotation strategy, so the
  only supported trigger is ``runtime``.
* Failback waits for the maintenance window instead of cutting back the
  moment a unit is repaired. The window defaults to 02:00-04:00, the low
  load hours of the store, and can be moved or widened.
"""

from __future__ import annotations

from dataclasses import dataclass

from coldstore.store.epoch import load_float, save_float
from coldstore.store.store import Store

TRIGGER_KEY = "failover:trigger"
RUNTIME_LIMIT_KEY = "failover:runtime-limit-hours"
FAILBACK_MODE_KEY = "failover:failback-mode"
WINDOW_START_KEY = "failover:window-start-hour"
WINDOW_END_KEY = "failover:window-end-hour"
CONFIRM_MODE_KEY = "failover:confirm-mode"
CONFIRM_TIMEOUT_KEY = "failover:confirm-timeout-seconds"

TRIGGER_RUNTIME = "runtime"
TRIGGERS = (TRIGGER_RUNTIME,)

FAILBACK_WINDOW = "window"
FAILBACK_IMMEDIATE = "immediate"
FAILBACK_MODES = (FAILBACK_WINDOW, FAILBACK_IMMEDIATE)

CONFIRM_AUTO = "auto"
CONFIRM_MANUAL = "manual"
CONFIRM_MODES = (CONFIRM_AUTO, CONFIRM_MANUAL)

DEFAULT_TRIGGER = TRIGGER_RUNTIME
DEFAULT_RUNTIME_LIMIT_HOURS = 720.0
DEFAULT_FAILBACK_MODE = FAILBACK_WINDOW
DEFAULT_WINDOW_START = 2
DEFAULT_WINDOW_END = 4
DEFAULT_CONFIRM_MODE = CONFIRM_AUTO
DEFAULT_CONFIRM_TIMEOUT_SECONDS = 30.0

MAX_RUNTIME_LIMIT_HOURS = 8760.0
MAX_CONFIRM_TIMEOUT_SECONDS = 3600.0


def validate_trigger(trigger: str) -> None:
    if trigger not in TRIGGERS:
        raise ValueError(f"failover trigger must be one of {', '.join(TRIGGERS)}")


def validate_failback_mode(mode: str) -> None:
    if mode not in FAILBACK_MODES:
        raise ValueError(f"failback mode must be one of {', '.join(FAILBACK_MODES)}")


def validate_confirm_mode(mode: str) -> None:
    if mode not in CONFIRM_MODES:
        raise ValueError(f"confirm mode must be one of {', '.join(CONFIRM_MODES)}")


def validate_runtime_limit(hours: float) -> None:
    if hours <= 0:
        raise ValueError("runtime limit must be positive")
    if hours > MAX_RUNTIME_LIMIT_HOURS:
        raise ValueError(f"runtime limit must be at most {MAX_RUNTIME_LIMIT_HOURS} hours")


def validate_window_hour(hour: int) -> None:
    if hour < 0 or hour > 23:
        raise ValueError("window hour must be between 0 and 23")


def validate_confirm_timeout(seconds: float) -> None:
    if seconds <= 0:
        raise ValueError("confirm timeout must be positive")
    if seconds > MAX_CONFIRM_TIMEOUT_SECONDS:
        raise ValueError(f"confirm timeout must be at most {MAX_CONFIRM_TIMEOUT_SECONDS} seconds")


def _load_text(store: Store, key: str, default: str) -> str:
    raw, present = store.get(key)
    return raw if present else default


def _load_int(store: Store, key: str, default: int) -> int:
    value, present = load_float(store, key)
    return int(value) if present else default


@dataclass(frozen=True)
class Policy:
    """One consistent read of every failover setting."""

    trigger: str
    runtime_limit_hours: float
    failback_mode: str
    window_start_hour: int
    window_end_hour: int
    confirm_mode: str
    confirm_timeout_seconds: float

    def in_window(self, hour: int) -> bool:
        """True when ``hour`` falls inside the failback window.

        The window may wrap midnight, so 22-2 covers 22:00 through 02:00.
        """

        start, end = self.window_start_hour, self.window_end_hour
        if start == end:
            return True
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def as_dict(self) -> dict[str, object]:
        return {
            "trigger": self.trigger,
            "runtime_limit_hours": self.runtime_limit_hours,
            "failback_mode": self.failback_mode,
            "window_start_hour": self.window_start_hour,
            "window_end_hour": self.window_end_hour,
            "confirm_mode": self.confirm_mode,
            "confirm_timeout_seconds": self.confirm_timeout_seconds,
        }


def load_policy(store: Store) -> Policy:
    """Read the persisted policy, falling back to the pinned defaults."""

    confirm_timeout, present = load_float(store, CONFIRM_TIMEOUT_KEY)
    runtime_limit, limit_present = load_float(store, RUNTIME_LIMIT_KEY)
    return Policy(
        trigger=_load_text(store, TRIGGER_KEY, DEFAULT_TRIGGER),
        runtime_limit_hours=runtime_limit if limit_present else DEFAULT_RUNTIME_LIMIT_HOURS,
        failback_mode=_load_text(store, FAILBACK_MODE_KEY, DEFAULT_FAILBACK_MODE),
        window_start_hour=_load_int(store, WINDOW_START_KEY, DEFAULT_WINDOW_START),
        window_end_hour=_load_int(store, WINDOW_END_KEY, DEFAULT_WINDOW_END),
        confirm_mode=_load_text(store, CONFIRM_MODE_KEY, DEFAULT_CONFIRM_MODE),
        confirm_timeout_seconds=confirm_timeout if present else DEFAULT_CONFIRM_TIMEOUT_SECONDS,
    )


def save_policy(store: Store, policy: Policy) -> None:
    """Validate and persist every setting in one go."""

    validate_trigger(policy.trigger)
    validate_runtime_limit(policy.runtime_limit_hours)
    validate_failback_mode(policy.failback_mode)
    validate_window_hour(policy.window_start_hour)
    validate_window_hour(policy.window_end_hour)
    validate_confirm_mode(policy.confirm_mode)
    validate_confirm_timeout(policy.confirm_timeout_seconds)
    store.put(TRIGGER_KEY, policy.trigger)
    save_float(store, RUNTIME_LIMIT_KEY, policy.runtime_limit_hours)
    store.put(FAILBACK_MODE_KEY, policy.failback_mode)
    save_float(store, WINDOW_START_KEY, float(policy.window_start_hour))
    save_float(store, WINDOW_END_KEY, float(policy.window_end_hour))
    store.put(CONFIRM_MODE_KEY, policy.confirm_mode)
    save_float(store, CONFIRM_TIMEOUT_KEY, policy.confirm_timeout_seconds)
