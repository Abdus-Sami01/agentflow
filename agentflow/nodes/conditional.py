from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class ConditionalNode(BaseNode):
    def __init__(
        self,
        name: str,
        condition: Callable[[dict[str, Any], SharedContext], str],
        branches: dict[str, str] | None = None,
        **config,
    ):
        super().__init__(name, **config)
        self._condition = condition
        self._branches = branches or {}

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            branch_key = self._condition(inputs, context)
            target = self._branches.get(branch_key, branch_key)
            return NodeOutput(
                data={"branch": branch_key, "target": target},
                metadata={"selected_branch": branch_key},
            )
        except Exception as e:
            return NodeOutput(error=f"condition evaluation failed: {e}")

    @property
    def branches(self) -> dict[str, str]:
        return dict(self._branches)
