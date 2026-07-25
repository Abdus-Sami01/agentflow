from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class ForEachNode(BaseNode):
    def __init__(
        self,
        name: str,
        item_fn: Callable[[Any, SharedContext], Any],
        max_parallel: int = 4,
        max_items: int = 1000,
        fail_fast: bool = False,
        item_timeout_s: float = 0,
        **config,
    ):
        super().__init__(name, **config)
        self._item_fn = item_fn
        self._max_parallel = max_parallel
        self._max_items = max_items
        self._fail_fast = fail_fast
        self._item_timeout = item_timeout_s

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        items = self._extract_items(inputs)
        if items is None:
            return NodeOutput(error="ForEachNode requires a list/tuple input")

        if len(items) > self._max_items:
            return NodeOutput(error=f"item count {len(items)} exceeds max_items {self._max_items}")

        if not items:
            return NodeOutput(data=[], metadata={"processed": 0, "failed": 0})

        results: list[Any] = [None] * len(items)
        errors: dict[int, str] = {}

        workers = min(len(items), self._max_parallel)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(self._run_item, item, context): idx
                for idx, item in enumerate(items)
            }
            for future in futures:
                idx = futures[future]
                try:
                    if self._item_timeout > 0:
                        results[idx] = future.result(timeout=self._item_timeout)
                    else:
                        results[idx] = future.result()
                except FuturesTimeout:
                    errors[idx] = f"item {idx} timed out after {self._item_timeout}s"
                except Exception as e:
                    errors[idx] = str(e)

        if errors and self._fail_fast:
            first_idx = min(errors)
            return NodeOutput(error=f"item {first_idx} failed: {errors[first_idx]}")

        succeeded = [r for i, r in enumerate(results) if i not in errors]
        return NodeOutput(
            data=succeeded,
            metadata={
                "processed": len(succeeded),
                "failed": len(errors),
                "errors": {str(k): v for k, v in list(errors.items())[:10]},
            },
        )

    def _run_item(self, item: Any, context: SharedContext) -> Any:
        return self._item_fn(item, context)

    def _extract_items(self, inputs: dict[str, Any]) -> list[Any] | None:
        if not inputs:
            return None

        if len(inputs) == 1:
            value = next(iter(inputs.values()))
            if isinstance(value, (list, tuple)):
                return list(value)
            return None

        for value in inputs.values():
            if isinstance(value, (list, tuple)):
                return list(value)
        return None
