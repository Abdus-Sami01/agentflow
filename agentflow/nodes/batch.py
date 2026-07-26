from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class BatchNode(BaseNode):
    def __init__(
        self,
        name: str,
        batch_fn: Callable[[list[Any], SharedContext], Any],
        batch_size: int = 10,
        flatten: bool = True,
        stop_on_error: bool = False,
        **config,
    ):
        super().__init__(name, **config)
        self._batch_fn = batch_fn
        self._batch_size = max(1, batch_size)
        self._flatten = flatten
        self._stop_on_error = stop_on_error

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        items = self._extract_items(inputs)
        if items is None:
            return NodeOutput(error="BatchNode requires a list/tuple input")

        if not items:
            return NodeOutput(data=[], metadata={"batches": 0, "items": 0})

        outputs: list[Any] = []
        errors: list[str] = []
        batch_count = 0

        for start in range(0, len(items), self._batch_size):
            chunk = items[start:start + self._batch_size]
            batch_count += 1
            try:
                result = self._batch_fn(chunk, context)
            except Exception as e:
                msg = f"batch {batch_count} (items {start}-{start + len(chunk) - 1}): {e}"
                if self._stop_on_error:
                    return NodeOutput(error=msg)
                errors.append(msg)
                continue

            if self._flatten and isinstance(result, (list, tuple)):
                outputs.extend(result)
            else:
                outputs.append(result)

        return NodeOutput(
            data=outputs,
            metadata={
                "batches": batch_count,
                "items": len(items),
                "batch_size": self._batch_size,
                "failed_batches": len(errors),
                "errors": errors[:10],
            },
        )

    def _extract_items(self, inputs: dict[str, Any]) -> list[Any] | None:
        if not inputs:
            return None
        if len(inputs) == 1:
            value = next(iter(inputs.values()))
            return list(value) if isinstance(value, (list, tuple)) else None
        for value in inputs.values():
            if isinstance(value, (list, tuple)):
                return list(value)
        return None
