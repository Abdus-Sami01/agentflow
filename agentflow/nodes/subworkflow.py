from __future__ import annotations

from typing import Any

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext, WorkflowStatus


class SubworkflowNode(BaseNode):
    def __init__(self, name: str, workflow_factory: Any = None, **config):
        super().__init__(name, **config)
        self._factory = workflow_factory

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if self._factory is None:
            return NodeOutput(error="no workflow factory configured")

        try:
            from agentflow.execution.executor import WorkflowExecutor
            workflow = self._factory(inputs, context)
            sub_context = SharedContext(
                workflow_id=f"{context.workflow_id}.{self.name}",
                data=dict(inputs),
            )
            executor = WorkflowExecutor(workflow)
            result = executor.run(sub_context)

            if result.status == WorkflowStatus.COMPLETED:
                return NodeOutput(
                    data=result.final_output,
                    metadata={"sub_steps": result.completed_count, "sub_ms": result.total_ms},
                )
            return NodeOutput(
                error=result.error or "subworkflow failed",
                metadata={"sub_status": result.status.value},
            )
        except Exception as e:
            return NodeOutput(error=f"subworkflow error: {e}")
