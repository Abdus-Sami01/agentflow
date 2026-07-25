from __future__ import annotations

import random
import time
from typing import Any, Callable

from agentflow.types import NodeOutput, SharedContext


class RetryStrategy:
    def delay(self, attempt: int) -> float:
        raise NotImplementedError

    def should_retry(self, attempt: int, error: str) -> bool:
        return True


class FixedDelay(RetryStrategy):
    def __init__(self, seconds: float = 1.0, max_retries: int = 3):
        self._seconds = seconds
        self._max = max_retries

    def delay(self, attempt: int) -> float:
        return self._seconds

    def should_retry(self, attempt: int, error: str) -> bool:
        return attempt < self._max


class ExponentialBackoff(RetryStrategy):
    def __init__(self, base: float = 1.0, factor: float = 2.0, max_delay: float = 60.0, max_retries: int = 5):
        self._base = base
        self._factor = factor
        self._max_delay = max_delay
        self._max = max_retries

    def delay(self, attempt: int) -> float:
        d = self._base * (self._factor ** attempt)
        return min(d, self._max_delay)

    def should_retry(self, attempt: int, error: str) -> bool:
        return attempt < self._max


class ExponentialBackoffWithJitter(ExponentialBackoff):
    def delay(self, attempt: int) -> float:
        base_delay = super().delay(attempt)
        return base_delay * random.uniform(0.5, 1.5)


class LinearBackoff(RetryStrategy):
    def __init__(self, initial: float = 1.0, increment: float = 1.0, max_retries: int = 5):
        self._initial = initial
        self._increment = increment
        self._max = max_retries

    def delay(self, attempt: int) -> float:
        return self._initial + (self._increment * attempt)

    def should_retry(self, attempt: int, error: str) -> bool:
        return attempt < self._max


class ConditionalRetry(RetryStrategy):
    def __init__(self, inner: RetryStrategy, retryable_errors: set[str] | None = None, non_retryable: set[str] | None = None):
        self._inner = inner
        self._retryable = retryable_errors
        self._non_retryable = non_retryable or set()

    def delay(self, attempt: int) -> float:
        return self._inner.delay(attempt)

    def should_retry(self, attempt: int, error: str) -> bool:
        if not self._inner.should_retry(attempt, error):
            return False
        for pattern in self._non_retryable:
            if pattern in error:
                return False
        if self._retryable:
            return any(pattern in error for pattern in self._retryable)
        return True


def execute_with_retry(
    fn: Callable[..., NodeOutput],
    args: tuple = (),
    kwargs: dict | None = None,
    strategy: RetryStrategy | None = None,
) -> tuple[NodeOutput, int]:
    strategy = strategy or FixedDelay()
    kwargs = kwargs or {}
    attempt = 0

    while True:
        try:
            result = fn(*args, **kwargs)
            if result.success:
                return result, attempt + 1
            if not strategy.should_retry(attempt, result.error):
                return result, attempt + 1
        except Exception as e:
            if not strategy.should_retry(attempt, str(e)):
                return NodeOutput(error=str(e)), attempt + 1

        delay = strategy.delay(attempt)
        if delay > 0:
            time.sleep(delay)
        attempt += 1
