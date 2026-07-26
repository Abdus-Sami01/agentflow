from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

from agentflow.types import NodeResult, NodeStatus, SharedContext, WorkflowHooks, WorkflowResult


@dataclass(frozen=True)
class DeadLetter:
    node_name: str
    error: str
    workflow_id: str = ""
    attempts: int = 1
    timestamp: float = field(default_factory=time.time)
    inputs_preview: str = ""


class DeadLetterQueue:
    def __init__(self, max_size: int = 1000):
        self._letters: list[DeadLetter] = []
        self._max = max_size
        self._lock = threading.Lock()

    def add(self, letter: DeadLetter) -> None:
        with self._lock:
            self._letters.append(letter)
            if len(self._letters) > self._max:
                self._letters = self._letters[-self._max:]

    def record_failure(self, node_name: str, error: str, context: SharedContext) -> None:
        result = context.results.get(node_name)
        self.add(DeadLetter(
            node_name=node_name,
            error=error,
            workflow_id=context.workflow_id,
            attempts=result.attempts if result else 1,
        ))

    def as_hooks(self) -> WorkflowHooks:
        return WorkflowHooks(on_node_error=self.record_failure)

    def by_node(self, node_name: str) -> list[DeadLetter]:
        with self._lock:
            return [l for l in self._letters if l.node_name == node_name]

    def failure_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        with self._lock:
            for letter in self._letters:
                counts[letter.node_name] = counts.get(letter.node_name, 0) + 1
        return counts

    def top_errors(self, limit: int = 5) -> list[tuple[str, int]]:
        counts: dict[str, int] = {}
        with self._lock:
            for letter in self._letters:
                counts[letter.error] = counts.get(letter.error, 0) + 1
        return sorted(counts.items(), key=lambda kv: kv[1], reverse=True)[:limit]

    def drain(self) -> list[DeadLetter]:
        with self._lock:
            letters = list(self._letters)
            self._letters.clear()
            return letters

    def clear(self) -> None:
        with self._lock:
            self._letters.clear()

    def report(self) -> str:
        with self._lock:
            if not self._letters:
                return "No failures recorded."
            lines = [f"Dead letters: {len(self._letters)}", ""]
        for node, count in sorted(self.failure_counts().items(), key=lambda kv: -kv[1]):
            lines.append(f"  {node}: {count} failure{'s' if count > 1 else ''}")
        lines.append("")
        lines.append("Most common errors:")
        for error, count in self.top_errors():
            lines.append(f"  [{count}x] {error[:100]}")
        return "\n".join(lines)

    @property
    def count(self) -> int:
        return len(self._letters)

    @property
    def letters(self) -> list[DeadLetter]:
        with self._lock:
            return list(self._letters)


def collect_failures(result: WorkflowResult) -> list[DeadLetter]:
    return [
        DeadLetter(
            node_name=name,
            error=nr.error,
            workflow_id=result.workflow_id,
            attempts=nr.attempts,
        )
        for name, nr in result.results.items()
        if nr.status == NodeStatus.FAILED
    ]
