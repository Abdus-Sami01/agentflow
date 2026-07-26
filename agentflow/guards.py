from __future__ import annotations

from typing import Any

from agentflow.nodes.base import BaseNode
from agentflow.resilience import Bulkhead, CircuitBreaker, CircuitBreakerOpen, RateLimiter
from agentflow.types import NodeOutput, SharedContext


class CircuitBreakerNode(BaseNode):
    def __init__(self, inner: BaseNode, breaker: CircuitBreaker):
        super().__init__(inner.name)
        self._inner = inner
        self._breaker = breaker

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if not self._breaker.allow():
            return NodeOutput(
                error=f"circuit open for {self.name!r}, retry in {self._breaker.time_until_retry:.1f}s",
                metadata={"circuit": self._breaker.state.value},
            )

        try:
            output = self._inner.execute(inputs, context)
        except Exception as e:
            self._breaker.record_failure()
            return NodeOutput(error=str(e), metadata={"circuit": self._breaker.state.value})

        if output.success:
            self._breaker.record_success()
        else:
            self._breaker.record_failure()

        return output


class RateLimitedNode(BaseNode):
    def __init__(self, inner: BaseNode, limiter: RateLimiter, wait_timeout_s: float = 0):
        super().__init__(inner.name)
        self._inner = inner
        self._limiter = limiter
        self._wait = wait_timeout_s

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if self._wait > 0:
            if not self._limiter.acquire(timeout_s=self._wait):
                return NodeOutput(error=f"rate limit wait exceeded {self._wait}s for {self.name!r}")
        elif not self._limiter.allow():
            return NodeOutput(
                error=f"rate limit exceeded for {self.name!r}",
                metadata={"retry_after_s": round(self._limiter.wait_time(), 2)},
            )

        return self._inner.execute(inputs, context)


class BulkheadNode(BaseNode):
    def __init__(self, inner: BaseNode, bulkhead: Bulkhead, wait_timeout_s: float = 5.0):
        super().__init__(inner.name)
        self._inner = inner
        self._bulkhead = bulkhead
        self._wait = wait_timeout_s

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if not self._bulkhead.acquire(timeout_s=self._wait):
            return NodeOutput(error=f"bulkhead full for {self.name!r} (waited {self._wait}s)")
        try:
            return self._inner.execute(inputs, context)
        finally:
            self._bulkhead.release()


def protect(
    node: BaseNode,
    breaker: CircuitBreaker | None = None,
    limiter: RateLimiter | None = None,
    bulkhead: Bulkhead | None = None,
    rate_wait_s: float = 0,
    bulkhead_wait_s: float = 5.0,
) -> BaseNode:
    wrapped = node
    if bulkhead is not None:
        wrapped = BulkheadNode(wrapped, bulkhead, bulkhead_wait_s)
    if breaker is not None:
        wrapped = CircuitBreakerNode(wrapped, breaker)
    if limiter is not None:
        wrapped = RateLimitedNode(wrapped, limiter, rate_wait_s)
    return wrapped


def protect_all(
    nodes: dict[str, BaseNode],
    only: set[str] | None = None,
    **kwargs,
) -> dict[str, BaseNode]:
    return {
        name: (protect(node, **kwargs) if (only is None or name in only) else node)
        for name, node in nodes.items()
    }
