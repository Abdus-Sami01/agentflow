from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class MemoryWriteNode(BaseNode):
    def __init__(self, name: str, key: str, value_fn: Callable[[dict[str, Any]], Any] | None = None, **config):
        super().__init__(name, **config)
        self._key = key
        self._value_fn = value_fn

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        try:
            if self._value_fn:
                value = self._value_fn(inputs)
            else:
                value = next(iter(inputs.values())) if len(inputs) == 1 else inputs
        except Exception as e:
            return NodeOutput(error=f"value builder failed: {e}")

        context.set(self._key, value)
        return NodeOutput(data=value, metadata={"key": self._key, "action": "write"})


class MemoryReadNode(BaseNode):
    def __init__(self, name: str, key: str, default: Any = None, required: bool = False, **config):
        super().__init__(name, **config)
        self._key = key
        self._default = default
        self._required = required

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if self._required and self._key not in context.data:
            return NodeOutput(error=f"required memory key {self._key!r} not set")

        value = context.get(self._key, self._default)
        return NodeOutput(data=value, metadata={"key": self._key, "action": "read"})


class MemoryAppendNode(BaseNode):
    def __init__(self, name: str, key: str, max_len: int = 1000, **config):
        super().__init__(name, **config)
        self._key = key
        self._max_len = max_len

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        value = next(iter(inputs.values())) if len(inputs) == 1 else inputs

        current = context.get(self._key)
        if current is None:
            current = []
        elif not isinstance(current, list):
            return NodeOutput(error=f"memory key {self._key!r} holds {type(current).__name__}, not a list")

        appended = current + [value]
        if len(appended) > self._max_len:
            appended = appended[-self._max_len:]

        context.set(self._key, appended)
        return NodeOutput(data=appended, metadata={"key": self._key, "length": len(appended), "action": "append"})
