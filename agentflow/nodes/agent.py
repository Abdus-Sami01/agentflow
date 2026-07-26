from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class AgentNode(BaseNode):
    def __init__(
        self,
        name: str,
        propose_fn: Callable[[dict[str, Any], SharedContext, list[str]], Any],
        verify_fn: Callable[[Any, SharedContext], tuple[bool, str]],
        execute_fn: Callable[[Any, SharedContext], Any] | None = None,
        max_attempts: int = 3,
        **config,
    ):
        super().__init__(name, **config)
        self._propose = propose_fn
        self._verify = verify_fn
        self._execute = execute_fn
        self._max_attempts = max_attempts

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        violations: list[str] = []

        for attempt in range(self._max_attempts):
            try:
                proposal = self._propose(inputs, context, violations)
            except Exception as e:
                return NodeOutput(error=f"proposal failed on attempt {attempt + 1}: {e}")

            try:
                passed, reason = self._verify(proposal, context)
            except Exception as e:
                return NodeOutput(error=f"verification errored on attempt {attempt + 1}: {e}")

            if not passed:
                violations.append(reason)
                continue

            if self._execute is None:
                return NodeOutput(
                    data=proposal,
                    metadata={"attempts": attempt + 1, "rejections": len(violations), "violations": violations[:5]},
                )

            try:
                result = self._execute(proposal, context)
            except Exception as e:
                return NodeOutput(error=f"execution failed after verification passed: {e}")

            return NodeOutput(
                data=result,
                metadata={"attempts": attempt + 1, "rejections": len(violations), "violations": violations[:5]},
            )

        return NodeOutput(
            error=f"no proposal passed verification in {self._max_attempts} attempts",
            metadata={"attempts": self._max_attempts, "violations": violations[:5]},
        )
