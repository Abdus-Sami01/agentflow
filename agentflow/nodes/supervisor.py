from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class SupervisorNode(BaseNode):
    def __init__(
        self,
        name: str,
        evaluate_fn: Callable[[dict[str, Any], SharedContext], dict[str, Any]],
        max_rounds: int = 3,
        **config,
    ):
        super().__init__(name, **config)
        self._evaluate_fn = evaluate_fn
        self._max_rounds = max_rounds

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        round_num = context.get(f"_supervisor_{self.name}_round", 0)
        try:
            decision = self._evaluate_fn(inputs, context)
        except Exception as e:
            return NodeOutput(error=f"supervisor evaluation failed: {e}")

        action = decision.get("action", "accept")
        round_num += 1
        context.set(f"_supervisor_{self.name}_round", round_num)

        if action == "accept":
            return NodeOutput(
                data=decision.get("result", inputs),
                metadata={"action": "accept", "round": round_num},
            )

        if action == "reject":
            return NodeOutput(
                error=decision.get("reason", "rejected by supervisor"),
                metadata={"action": "reject", "round": round_num},
            )

        if action == "revise" and round_num < self._max_rounds:
            return NodeOutput(
                data=decision.get("feedback", {}),
                metadata={"action": "revise", "round": round_num, "target": decision.get("target", "")},
            )

        if action == "delegate":
            return NodeOutput(
                data=decision.get("task", inputs),
                metadata={"action": "delegate", "round": round_num, "target": decision.get("target", "")},
            )

        return NodeOutput(
            data=decision.get("result", inputs),
            metadata={"action": action, "round": round_num, "forced_accept": round_num >= self._max_rounds},
        )
