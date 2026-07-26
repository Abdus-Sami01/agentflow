from __future__ import annotations

from typing import Any

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext, WorkflowStatus


class SubworkflowNode(BaseNode):
    def __init__(
        self,
        name: str,
        workflow_factory: Any = None,
        inherit_memory: bool = False,
        max_depth: int = 5,
        **config,
    ):
        super().__init__(name, **config)
        self._factory = workflow_factory
        self._inherit_memory = inherit_memory
        self._max_depth = max_depth

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if self._factory is None:
            return NodeOutput(error="no workflow factory configured")

        depth = context.metadata.get("_subworkflow_depth", 0)
        if depth >= self._max_depth:
            return NodeOutput(error=f"subworkflow nesting exceeded max_depth {self._max_depth}")

        try:
            produced = self._factory(inputs, context)
        except Exception as e:
            return NodeOutput(error=f"workflow factory failed: {e}")

        try:
            executor = self._build_executor(produced)
        except Exception as e:
            return NodeOutput(error=f"cannot build subworkflow: {e}")

        sub_context = SharedContext(
            workflow_id=f"{context.workflow_id}.{self.name}" if context.workflow_id else self.name,
            data=dict(context.data) if self._inherit_memory else dict(inputs),
        )
        sub_context.metadata["_subworkflow_depth"] = depth + 1

        try:
            result = executor.run(sub_context)
        except Exception as e:
            return NodeOutput(error=f"subworkflow raised: {e}")

        if self._inherit_memory:
            context.data.update(sub_context.data)

        if result.status != WorkflowStatus.COMPLETED:
            failed = [n for n, nr in result.results.items() if nr.error]
            return NodeOutput(
                error=result.error or f"subworkflow failed at: {', '.join(failed) or 'unknown'}",
                metadata={"sub_status": result.status.value, "sub_failed": failed},
            )

        return NodeOutput(
            data=result.final_output,
            metadata={
                "sub_completed": result.completed_count,
                "sub_ms": round(result.total_ms, 1),
                "depth": depth + 1,
            },
        )

    def _build_executor(self, produced: Any):
        from agentflow.execution.executor import WorkflowExecutor
        from agentflow.graph import DAG

        if hasattr(produced, "build") and callable(produced.build):
            return produced.build()

        if isinstance(produced, WorkflowExecutor):
            return produced

        if isinstance(produced, tuple) and len(produced) == 2:
            dag, nodes = produced
            return WorkflowExecutor(dag, nodes)

        if isinstance(produced, DAG):
            raise ValueError("factory returned a bare DAG with no node implementations")

        raise ValueError(f"factory returned unsupported type {type(produced).__name__}")
