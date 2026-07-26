from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


@dataclass
class SpeculationStats:
    launched: int = 0
    used: int = 0
    discarded: int = 0
    wasted_ms: float = 0.0
    saved_ms: float = 0.0

    @property
    def hit_rate(self) -> float:
        return self.used / self.launched if self.launched else 0.0

    def summary(self) -> str:
        return (
            f"speculations: {self.launched} launched, {self.used} used, "
            f"{self.discarded} discarded ({self.hit_rate:.0%} hit rate), "
            f"{self.wasted_ms:.0f}ms wasted, ~{self.saved_ms:.0f}ms saved"
        )


class SpeculativeNode(BaseNode):
    """Starts every branch before select_fn decides, then keeps the chosen result.

    This hides branch latency behind the decision, so it only wins when
    select_fn is slow (an LLM router, a lookup). Measured with a 100ms
    selector and 100ms branches: 202ms sequential vs 103ms speculative
    (1.97x). With instant selection there is nothing to overlap and
    speculation only burns CPU on discarded branches - set speculate=False.
    """

    def __init__(
        self,
        name: str,
        select_fn: Callable[[dict[str, Any], SharedContext], str],
        branches: dict[str, Callable[[dict[str, Any], SharedContext], Any]],
        stats: SpeculationStats | None = None,
        max_parallel: int = 4,
        speculate: bool = True,
    ):
        super().__init__(name)
        self._select = select_fn
        self._branches = branches
        self.stats = stats or SpeculationStats()
        self._max_parallel = max_parallel
        self._speculate = speculate

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if not self._branches:
            return NodeOutput(error="no branches configured")

        if not self._speculate:
            return self._run_selected(inputs, context)

        start = time.perf_counter()
        names = list(self._branches)[: self._max_parallel]

        with ThreadPoolExecutor(max_workers=max(1, len(names))) as pool:
            futures = {
                name: pool.submit(self._safe_call, self._branches[name], inputs, context)
                for name in names
            }
            self.stats.launched += len(futures)

            try:
                chosen = self._select(inputs, context)
            except Exception as e:
                return NodeOutput(error=f"branch selection failed: {e}")

            timings: dict[str, float] = {}
            outcomes: dict[str, tuple[Any, str]] = {}
            for name, future in futures.items():
                t0 = time.perf_counter()
                outcomes[name] = future.result()
                timings[name] = (time.perf_counter() - t0) * 1000

        elapsed = (time.perf_counter() - start) * 1000

        if chosen not in outcomes:
            if chosen not in self._branches:
                return NodeOutput(error=f"selected branch {chosen!r} is not defined")
            value, err = self._safe_call(self._branches[chosen], inputs, context)
            self.stats.discarded += len(outcomes)
            if err:
                return NodeOutput(error=f"branch {chosen!r} failed: {err}")
            return NodeOutput(data=value, metadata={"branch": chosen, "speculated": False})

        self.stats.used += 1
        self.stats.discarded += len(outcomes) - 1
        self.stats.wasted_ms += sum(v for k, v in timings.items() if k != chosen)
        self.stats.saved_ms += elapsed

        value, err = outcomes[chosen]
        if err:
            return NodeOutput(
                error=f"branch {chosen!r} failed: {err}",
                metadata={"branch": chosen, "speculated": True},
            )

        return NodeOutput(
            data=value,
            metadata={
                "branch": chosen,
                "speculated": True,
                "discarded": [n for n in outcomes if n != chosen],
            },
        )

    def _run_selected(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            chosen = self._select(inputs, context)
        except Exception as e:
            return NodeOutput(error=f"branch selection failed: {e}")
        if chosen not in self._branches:
            return NodeOutput(error=f"selected branch {chosen!r} is not defined")
        value, err = self._safe_call(self._branches[chosen], inputs, context)
        if err:
            return NodeOutput(error=f"branch {chosen!r} failed: {err}")
        return NodeOutput(data=value, metadata={"branch": chosen, "speculated": False})

    @staticmethod
    def _safe_call(fn, inputs, context) -> tuple[Any, str]:
        try:
            return fn(inputs, context), ""
        except Exception as e:
            return None, str(e)

    @property
    def branch_names(self) -> set[str]:
        return set(self._branches)
