from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


@dataclass(frozen=True)
class CostEntry:
    node_name: str
    amount: float
    unit: str = "usd"
    detail: str = ""


class BudgetExceeded(Exception):
    pass


class CostTracker:
    def __init__(self, budget: float = 0.0, unit: str = "usd", hard_stop: bool = True):
        self._entries: list[CostEntry] = []
        self._budget = budget
        self._unit = unit
        self._hard_stop = hard_stop
        self._lock = threading.Lock()

    def charge(self, node_name: str, amount: float, detail: str = "") -> None:
        with self._lock:
            self._entries.append(CostEntry(node_name, amount, self._unit, detail))

    def would_exceed(self, amount: float) -> bool:
        if self._budget <= 0:
            return False
        return (self.total + amount) > self._budget

    def check_affordable(self, node_name: str, amount: float) -> None:
        if self.would_exceed(amount):
            raise BudgetExceeded(
                f"{node_name!r} would cost {amount:.4f} {self._unit}, "
                f"exceeding budget {self._budget:.4f} (spent {self.total:.4f})"
            )

    @property
    def total(self) -> float:
        with self._lock:
            return sum(e.amount for e in self._entries)

    @property
    def remaining(self) -> float:
        return max(0.0, self._budget - self.total) if self._budget > 0 else float("inf")

    @property
    def exhausted(self) -> bool:
        return self._budget > 0 and self.total >= self._budget

    def by_node(self) -> dict[str, float]:
        out: dict[str, float] = {}
        with self._lock:
            for e in self._entries:
                out[e.node_name] = out.get(e.node_name, 0.0) + e.amount
        return out

    def report(self) -> str:
        per_node = self.by_node()
        if not per_node:
            return "No costs recorded."

        lines = [f"Total: {self.total:.4f} {self._unit}"]
        if self._budget > 0:
            pct = self.total / self._budget * 100
            lines.append(f"Budget: {self._budget:.4f} ({pct:.1f}% used, {self.remaining:.4f} left)")
        lines.append("")
        for name, amount in sorted(per_node.items(), key=lambda kv: -kv[1]):
            share = amount / self.total * 100 if self.total else 0
            lines.append(f"  {name:<24}{amount:>10.4f}  ({share:.0f}%)")
        return "\n".join(lines)

    def reset(self) -> None:
        with self._lock:
            self._entries.clear()

    @property
    def entries(self) -> list[CostEntry]:
        with self._lock:
            return list(self._entries)

    @property
    def hard_stop(self) -> bool:
        return self._hard_stop


TOKEN_PRICES_PER_1K = {
    "input": 0.003,
    "output": 0.015,
}


def estimate_token_cost(text: str, kind: str = "input", price_per_1k: float | None = None) -> float:
    price = price_per_1k if price_per_1k is not None else TOKEN_PRICES_PER_1K.get(kind, 0.0)
    approx_tokens = max(1, len(text) // 4)
    return approx_tokens / 1000.0 * price


class CostedNode(BaseNode):
    def __init__(
        self,
        inner: BaseNode,
        tracker: CostTracker,
        cost_fn: Callable[[dict[str, Any], NodeOutput | None], float] | None = None,
        fixed_cost: float = 0.0,
        estimate_fn: Callable[[dict[str, Any]], float] | None = None,
    ):
        super().__init__(inner.name)
        self._inner = inner
        self._tracker = tracker
        self._cost_fn = cost_fn
        self._fixed = fixed_cost
        self._estimate_fn = estimate_fn

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        estimate = self._fixed
        if self._estimate_fn:
            try:
                estimate = self._estimate_fn(inputs)
            except Exception as e:
                return NodeOutput(error=f"cost estimation failed: {e}")

        if self._tracker.hard_stop and self._tracker.would_exceed(estimate):
            return NodeOutput(
                error=(
                    f"budget exceeded before running {self.name!r}: "
                    f"estimated {estimate:.4f}, remaining {self._tracker.remaining:.4f}"
                ),
                metadata={"budget_blocked": True, "estimated_cost": estimate},
            )

        output = self._inner.execute(inputs, context)

        actual = self._fixed
        if self._cost_fn:
            try:
                actual = self._cost_fn(inputs, output)
            except Exception:
                actual = estimate

        if actual:
            self._tracker.charge(self.name, actual)

        merged = dict(output.metadata)
        merged["cost"] = round(actual, 6)
        merged["cost_total"] = round(self._tracker.total, 6)
        return NodeOutput(data=output.data, error=output.error, metadata=merged)


def with_cost(
    nodes: dict[str, BaseNode],
    tracker: CostTracker,
    costs: dict[str, float] | None = None,
    cost_fns: dict[str, Callable] | None = None,
    default_cost: float = 0.0,
) -> dict[str, BaseNode]:
    costs = costs or {}
    cost_fns = cost_fns or {}

    wrapped = {}
    for name, node in nodes.items():
        fixed = costs.get(name, default_cost)
        fn = cost_fns.get(name)
        if fixed or fn:
            wrapped[name] = CostedNode(node, tracker, cost_fn=fn, fixed_cost=fixed,
                                       estimate_fn=lambda i, _f=fixed: _f)
        else:
            wrapped[name] = node
    return wrapped
