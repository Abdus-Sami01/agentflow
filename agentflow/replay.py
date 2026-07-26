from __future__ import annotations

import json
from typing import Any

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, NodeStatus, SharedContext, WorkflowResult


class Recording:
    def __init__(self, outputs: dict[str, Any] | None = None):
        self._outputs: dict[str, Any] = outputs or {}

    @classmethod
    def from_result(cls, result: WorkflowResult) -> Recording:
        return cls({
            name: nr.output.data
            for name, nr in result.results.items()
            if nr.status == NodeStatus.COMPLETED and nr.output is not None
        })

    def get(self, node_name: str) -> Any:
        return self._outputs.get(node_name)

    def has(self, node_name: str) -> bool:
        return node_name in self._outputs

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self._outputs, indent=indent, default=str)

    @classmethod
    def from_json(cls, raw: str) -> Recording:
        return cls(json.loads(raw))

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_json())

    @classmethod
    def load(cls, path: str) -> Recording:
        with open(path) as f:
            return cls.from_json(f.read())

    @property
    def node_names(self) -> set[str]:
        return set(self._outputs)

    def __len__(self) -> int:
        return len(self._outputs)


class ReplayNode(BaseNode):
    def __init__(self, inner: BaseNode, recording: Recording, strict: bool = False):
        super().__init__(inner.name)
        self._inner = inner
        self._recording = recording
        self._strict = strict

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if self._recording.has(self.name):
            return NodeOutput(
                data=self._recording.get(self.name),
                metadata={"replayed": True},
            )

        if self._strict:
            return NodeOutput(error=f"no recording for node {self.name!r} and strict replay is on")

        return self._inner.execute(inputs, context)


def replay_nodes(
    nodes: dict[str, BaseNode],
    recording: Recording,
    strict: bool = False,
    only: set[str] | None = None,
) -> dict[str, BaseNode]:
    return {
        name: (ReplayNode(node, recording, strict) if (only is None or name in only) else node)
        for name, node in nodes.items()
    }


def compare_outputs(baseline: Recording, candidate: WorkflowResult) -> dict[str, Any]:
    diffs: dict[str, dict[str, Any]] = {}
    matched: list[str] = []

    for name, nr in candidate.results.items():
        if not baseline.has(name):
            continue
        expected = baseline.get(name)
        actual = nr.output.data if nr.output else None
        if str(expected) == str(actual):
            matched.append(name)
        else:
            diffs[name] = {
                "expected": str(expected)[:200],
                "actual": str(actual)[:200],
            }

    missing = sorted(baseline.node_names - set(candidate.results))

    return {
        "matched": sorted(matched),
        "differing": diffs,
        "missing": missing,
        "identical": not diffs and not missing,
    }
