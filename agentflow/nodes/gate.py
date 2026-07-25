from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class GateNode(BaseNode):
    def __init__(
        self,
        name: str,
        check_fn: Callable[[dict[str, Any], SharedContext], bool],
        fail_message: str = "gate check failed",
        **config,
    ):
        super().__init__(name, **config)
        self._check_fn = check_fn
        self._fail_message = fail_message

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            passed = self._check_fn(inputs, context)
        except Exception as e:
            return NodeOutput(error=f"gate check error: {e}")

        if passed:
            return NodeOutput(data=inputs, metadata={"gate": "passed"})
        return NodeOutput(error=self._fail_message, metadata={"gate": "blocked"})
