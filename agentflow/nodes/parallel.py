from __future__ import annotations

import pickle
from concurrent.futures import ProcessPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


def is_picklable(obj: Any) -> bool:
    try:
        pickle.dumps(obj)
        return True
    except (pickle.PicklingError, AttributeError, TypeError):
        return False


class ProcessPoolNode(BaseNode):
    """Runs item_fn across processes, giving real CPU parallelism past the GIL.

    Process startup is a fixed cost (roughly 100-300ms total on Windows spawn),
    so this only pays off when per-item CPU work exceeds it. Measured on a
    4-item workload: 0.24x at ~200k ops/item (slower than threads), break-even
    near 1M, 2.07x at 5M. Use ForEachNode for I/O-bound or light work.
    """

    def __init__(
        self,
        name: str,
        item_fn: Callable[[Any], Any],
        max_workers: int = 4,
        chunk_timeout_s: float = 0,
        max_items: int = 10_000,
        **config,
    ):
        super().__init__(name, **config)
        self._item_fn = item_fn
        self._max_workers = max_workers
        self._timeout = chunk_timeout_s
        self._max_items = max_items

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if not is_picklable(self._item_fn):
            return NodeOutput(
                error=(
                    f"{self.name!r}: item_fn is not picklable, so it cannot run in a process pool. "
                    "Use a module-level function (not a lambda, closure, or local function), "
                    "or use ForEachNode for thread-based parallelism."
                ),
                metadata={"unpicklable": True},
            )

        items = self._extract_items(inputs)
        if items is None:
            return NodeOutput(error="ProcessPoolNode requires a list/tuple input")
        if len(items) > self._max_items:
            return NodeOutput(error=f"item count {len(items)} exceeds max_items {self._max_items}")
        if not items:
            return NodeOutput(data=[], metadata={"processed": 0, "workers": 0})

        unpicklable = [i for i, item in enumerate(items[:20]) if not is_picklable(item)]
        if unpicklable:
            return NodeOutput(
                error=f"{self.name!r}: input items at {unpicklable} are not picklable",
                metadata={"unpicklable": True},
            )

        workers = min(self._max_workers, len(items))
        results: list[Any] = [None] * len(items)
        errors: dict[int, str] = {}

        try:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(self._item_fn, item): idx for idx, item in enumerate(items)}
                for future, idx in futures.items():
                    try:
                        results[idx] = future.result(timeout=self._timeout or None)
                    except FuturesTimeout:
                        errors[idx] = f"item {idx} timed out after {self._timeout}s"
                    except Exception as e:
                        errors[idx] = str(e)
        except Exception as e:
            return NodeOutput(error=f"process pool failed: {e}")

        succeeded = [r for i, r in enumerate(results) if i not in errors]
        return NodeOutput(
            data=succeeded,
            metadata={
                "processed": len(succeeded),
                "failed": len(errors),
                "workers": workers,
                "errors": {str(k): v for k, v in list(errors.items())[:10]},
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
