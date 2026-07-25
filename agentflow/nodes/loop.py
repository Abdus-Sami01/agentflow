from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class LoopNode(BaseNode):
    def __init__(
        self,
        name: str,
        body_fn: Callable[[Any, int, SharedContext], Any],
        condition_fn: Callable[[Any, int, SharedContext], bool],
        max_iterations: int = 10,
        **config,
    ):
        super().__init__(name, **config)
        self._body_fn = body_fn
        self._condition_fn = condition_fn
        self._max_iterations = max_iterations

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        current = inputs
        iteration_outputs = []

        for i in range(self._max_iterations):
            try:
                should_continue = self._condition_fn(current, i, context)
            except Exception as e:
                return NodeOutput(error=f"loop condition error at iteration {i}: {e}")

            if not should_continue:
                break

            try:
                current = self._body_fn(current, i, context)
                iteration_outputs.append(current)
            except Exception as e:
                return NodeOutput(error=f"loop body error at iteration {i}: {e}")
        else:
            return NodeOutput(
                data=current,
                metadata={
                    "iterations": self._max_iterations,
                    "exhausted": True,
                    "history_len": len(iteration_outputs),
                },
            )

        return NodeOutput(
            data=current,
            metadata={
                "iterations": len(iteration_outputs),
                "exhausted": False,
                "history_len": len(iteration_outputs),
            },
        )
