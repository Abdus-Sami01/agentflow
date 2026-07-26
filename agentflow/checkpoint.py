from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from typing import Any

from agentflow.persistence import deserialize_context, serialize_context
from agentflow.types import NodeResult, NodeStatus, SharedContext, WorkflowHooks


@dataclass
class CheckpointMeta:
    workflow_id: str
    completed: list[str]
    updated_at: float
    version: int = 1


class Checkpointer:
    def __init__(self, path: str, every_n_nodes: int = 1, keep_on_success: bool = False):
        self.path = path
        self._every = max(1, every_n_nodes)
        self._keep = keep_on_success
        self._since_write = 0
        self._lock = threading.Lock()
        self._context: SharedContext | None = None

    def bind(self, context: SharedContext) -> SharedContext:
        self._context = context
        return context

    def record(self, node_name: str, result: NodeResult, context: SharedContext) -> None:
        self._context = context
        with self._lock:
            if node_name not in context.results:
                context.results[node_name] = result
            self._since_write += 1
            if self._since_write >= self._every:
                self._since_write = 0
                self._write(context)

    def as_hooks(self) -> WorkflowHooks:
        return WorkflowHooks(
            on_node_complete=self.record,
            on_workflow_complete=lambda result: self._finalize(result),
        )

    def _finalize(self, result: Any) -> None:
        if self._keep:
            if self._context is not None:
                self._write(self._context)
            return
        if getattr(result, "status", None) is not None and result.status.value == "completed":
            self.clear()

    def _write(self, context: SharedContext) -> None:
        payload = {
            "meta": {
                "workflow_id": context.workflow_id,
                "completed": [
                    n for n, nr in context.results.items()
                    if nr.status == NodeStatus.COMPLETED
                ],
                "updated_at": time.time(),
                "version": 1,
            },
            "context": serialize_context(context),
        }
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)

        fd, tmp = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(payload, f, default=str)
            os.replace(tmp, self.path)
        except Exception:
            if os.path.exists(tmp):
                os.unlink(tmp)
            raise

    def exists(self) -> bool:
        return os.path.exists(self.path)

    def load(self) -> SharedContext | None:
        if not self.exists():
            return None
        try:
            with open(self.path) as f:
                payload = json.load(f)
        except (json.JSONDecodeError, ValueError, OSError):
            return None
        raw = payload.get("context")
        if not raw:
            return None
        return deserialize_context(raw)

    def meta(self) -> CheckpointMeta | None:
        if not self.exists():
            return None
        try:
            with open(self.path) as f:
                payload = json.load(f)
        except (json.JSONDecodeError, ValueError, OSError):
            return None
        m = payload.get("meta", {})
        return CheckpointMeta(
            workflow_id=m.get("workflow_id", ""),
            completed=m.get("completed", []),
            updated_at=m.get("updated_at", 0.0),
            version=m.get("version", 1),
        )

    def clear(self) -> None:
        if os.path.exists(self.path):
            try:
                os.unlink(self.path)
            except OSError:
                pass

    def completed_nodes(self) -> set[str]:
        meta = self.meta()
        return set(meta.completed) if meta else set()


def resume_or_start(executor: Any, checkpointer: Checkpointer, workflow_id: str = "") -> Any:
    restored = checkpointer.load()
    if restored is None:
        context = checkpointer.bind(SharedContext(workflow_id=workflow_id))
        return executor.run(context)

    checkpointer.bind(restored)
    return executor.resume(restored)
