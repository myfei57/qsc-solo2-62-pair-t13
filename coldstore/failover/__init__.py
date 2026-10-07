"""Automatic primary/standby failover for the cold air fans."""

from .manager import (
    DEFAULT_ROTATE_HOURS,
    DEFAULT_SOAK_HOURS,
    DEFAULT_WINDOW_END,
    DEFAULT_WINDOW_START,
    FailoverManager,
    validate_policy,
    validate_window,
)
from .report import FailoverState, SwitchOutcome

__all__ = [
    "DEFAULT_ROTATE_HOURS",
    "DEFAULT_SOAK_HOURS",
    "DEFAULT_WINDOW_END",
    "DEFAULT_WINDOW_START",
    "FailoverManager",
    "FailoverState",
    "SwitchOutcome",
    "validate_policy",
    "validate_window",
]
