"""Automatic primary/standby failover for the cold air fans."""

from .manager import FailoverManager
from .policy import Policy, load_policy, save_policy
from .report import FailoverState, SwitchResult

__all__ = ["FailoverManager", "FailoverState", "Policy", "SwitchResult", "load_policy", "save_policy"]
