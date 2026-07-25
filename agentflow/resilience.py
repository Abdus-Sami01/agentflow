from __future__ import annotations

import threading
import time
from collections import deque
from enum import Enum
from typing import Any, Callable


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpen(Exception):
    pass


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_s: float = 30.0,
        success_threshold: int = 2,
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout_s
        self._success_threshold = success_threshold
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at = 0.0
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if (time.time() - self._opened_at) >= self._recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._successes = 0
                    return True
                return False

            return True

    def record_success(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._successes += 1
                if self._successes >= self._success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failures = 0
                    self._successes = 0
            else:
                self._failures = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                self._opened_at = time.time()
                self._successes = 0
                return

            self._failures += 1
            if self._failures >= self._failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.time()

    def call(self, fn: Callable, *args, **kwargs) -> Any:
        if not self.allow():
            raise CircuitBreakerOpen(f"circuit open, retry in {self.time_until_retry:.1f}s")
        try:
            result = fn(*args, **kwargs)
            self.record_success()
            return result
        except Exception:
            self.record_failure()
            raise

    def reset(self) -> None:
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failures = 0
            self._successes = 0

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failures

    @property
    def time_until_retry(self) -> float:
        if self._state != CircuitState.OPEN:
            return 0.0
        remaining = self._recovery_timeout - (time.time() - self._opened_at)
        return max(0.0, remaining)


class RateLimiter:
    def __init__(self, max_calls: int, period_s: float = 1.0):
        self._max = max_calls
        self._period = period_s
        self._calls: deque[float] = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = time.time()
            while self._calls and (now - self._calls[0]) > self._period:
                self._calls.popleft()

            if len(self._calls) < self._max:
                self._calls.append(now)
                return True
            return False

    def wait_time(self) -> float:
        with self._lock:
            if len(self._calls) < self._max:
                return 0.0
            oldest = self._calls[0]
            return max(0.0, self._period - (time.time() - oldest))

    def acquire(self, timeout_s: float = 0) -> bool:
        deadline = time.time() + timeout_s if timeout_s > 0 else None
        while True:
            if self.allow():
                return True
            if deadline and time.time() >= deadline:
                return False
            wait = self.wait_time()
            time.sleep(min(wait, 0.05) if wait > 0 else 0.01)

    @property
    def current_usage(self) -> int:
        with self._lock:
            now = time.time()
            while self._calls and (now - self._calls[0]) > self._period:
                self._calls.popleft()
            return len(self._calls)


class Bulkhead:
    def __init__(self, max_concurrent: int = 10):
        self._sem = threading.Semaphore(max_concurrent)
        self._max = max_concurrent
        self._active = 0
        self._lock = threading.Lock()

    def acquire(self, timeout_s: float = 0) -> bool:
        acquired = self._sem.acquire(timeout=timeout_s if timeout_s > 0 else None)
        if acquired:
            with self._lock:
                self._active += 1
        return acquired

    def release(self) -> None:
        with self._lock:
            if self._active > 0:
                self._active -= 1
        self._sem.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()

    @property
    def active(self) -> int:
        return self._active

    @property
    def available(self) -> int:
        return self._max - self._active
