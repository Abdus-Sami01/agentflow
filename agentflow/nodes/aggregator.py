from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class AggregatorNode(BaseNode):
    def __init__(
        self,
        name: str,
        strategy: str = "merge",
        merge_fn: Callable[[dict[str, Any]], Any] | None = None,
        **config,
    ):
        super().__init__(name, **config)
        self._strategy = strategy
        self._merge_fn = merge_fn

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            if self._merge_fn:
                result = self._merge_fn(inputs)
                return NodeOutput(data=result)

            if self._strategy == "merge":
                return NodeOutput(data=dict(inputs))

            if self._strategy == "list":
                return NodeOutput(data=list(inputs.values()))

            if self._strategy == "first":
                return NodeOutput(data=next(iter(inputs.values())) if inputs else None)

            if self._strategy == "concat":
                parts = [str(v) for v in inputs.values()]
                return NodeOutput(data="\n".join(parts))

            if self._strategy == "vote":
                return self._majority_vote(inputs)

            return NodeOutput(error=f"unknown aggregation strategy: {self._strategy!r}")
        except Exception as e:
            return NodeOutput(error=str(e))

    def _majority_vote(self, inputs: dict[str, Any]) -> NodeOutput:
        counts: dict[Any, int] = {}
        for value in inputs.values():
            key = str(value)
            counts[key] = counts.get(key, 0) + 1

        if not counts:
            return NodeOutput(error="no inputs for voting")

        winner = max(counts, key=counts.get)
        return NodeOutput(
            data=winner,
            metadata={"vote_counts": counts, "total_votes": len(inputs)},
        )
