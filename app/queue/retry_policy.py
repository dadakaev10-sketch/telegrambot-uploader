"""Retry policy for queue workers."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_seconds: int = 300
    max_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.base_seconds < 1 or self.max_seconds < 1:
            raise ValueError('Retry-Werte müssen positiv sein')

    def delay(self, failure_count: int) -> int:
        count = max(1, int(failure_count))
        return min(self.base_seconds * (2 ** (count - 1)), self.max_seconds)

    def exhausted(self, failure_count: int) -> bool:
        return int(failure_count) >= self.max_attempts
