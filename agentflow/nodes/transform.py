from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class TransformNode(BaseNode):
    def __init__(self, name: str, transform_fn: Callable[[dict[str, Any]], Any], **config):
        super().__init__(name, **config)
        self._fn = transform_fn

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            result = self._fn(inputs)
            return NodeOutput(data=result)
        except Exception as e:
            return NodeOutput(error=str(e))
