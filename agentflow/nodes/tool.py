from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class ToolNode(BaseNode):
    def __init__(self, name: str, fn: Callable[..., Any], arg_map: dict[str, str] | None = None, **config):
        super().__init__(name, **config)
        self._fn = fn
        self._arg_map = arg_map or {}

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        kwargs = {}
        for param_name, input_key in self._arg_map.items():
            if input_key in inputs:
                kwargs[param_name] = inputs[input_key]
            elif context.get(input_key) is not None:
                kwargs[param_name] = context.get(input_key)

        for key, value in inputs.items():
            if key not in kwargs:
                kwargs[key] = value

        try:
            result = self._fn(**kwargs)
            return NodeOutput(data=result)
        except Exception as e:
            return NodeOutput(error=str(e))
