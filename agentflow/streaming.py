from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Iterator

from agentflow.execution.executor import WorkflowExecutor
from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.types import (
    NodeResult,
    NodeStatus,
    SharedContext,
    WorkflowConfig,
    WorkflowHooks,
    WorkflowResult,
    WorkflowStatus,
)


@dataclass(frozen=True)
class StreamEvent:
    kind: str
    node_name: str = ""
    result: NodeResult | None = None
    level: int = -1
    final: WorkflowResult | None = None


class StreamingExecutor(WorkflowExecutor):
    def stream(self, context: SharedContext | None = None) -> Iterator[StreamEvent]:
        errors = self._dag.validate()
        if errors:
            failed = WorkflowResult(
                workflow_id="",
                status=WorkflowStatus.FAILED,
                error=f"validation failed: {'; '.join(errors)}",
            )
            yield StreamEvent(kind="workflow_failed", final=failed)
            return

        context = context or SharedContext()
        start = time.perf_counter()

        yield StreamEvent(kind="workflow_start")

        schedule = self._dag.parallel_schedule()
        status_map: dict[str, NodeStatus] = {n: NodeStatus.PENDING for n in self._dag.nodes}

        for level_idx, level in enumerate(schedule):
            yield StreamEvent(kind="level_start", level=level_idx)

            runnable = [n for n in level if self._should_run(n, status_map, context)]
            skipped = [n for n in level if n not in runnable]

            for node_name in skipped:
                status_map[node_name] = NodeStatus.SKIPPED
                nr = NodeResult(node_name=node_name, status=NodeStatus.SKIPPED)
                context.results[node_name] = nr
                yield StreamEvent(kind="node_skipped", node_name=node_name, result=nr, level=level_idx)

            if not runnable:
                continue

            for node_name in runnable:
                yield StreamEvent(kind="node_start", node_name=node_name, level=level_idx)

            if len(runnable) == 1:
                result = self._execute_node(runnable[0], context)
                context.results[runnable[0]] = result
                status_map[runnable[0]] = result.status
                yield StreamEvent(
                    kind="node_complete" if result.status == NodeStatus.COMPLETED else "node_failed",
                    node_name=runnable[0],
                    result=result,
                    level=level_idx,
                )
                if result.status == NodeStatus.FAILED and self._config.fail_fast:
                    final = self._build_result(context, start, WorkflowStatus.FAILED, result.error)
                    yield StreamEvent(kind="workflow_failed", final=final)
                    return
            else:
                workers = min(len(runnable), self._config.max_parallel)
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futures = {
                        pool.submit(self._execute_node, name, context): name
                        for name in runnable
                    }
                    for future in futures:
                        name = futures[future]
                        try:
                            result = future.result()
                        except Exception as e:
                            result = NodeResult(node_name=name, status=NodeStatus.FAILED, error=str(e))

                        context.results[name] = result
                        status_map[name] = result.status
                        yield StreamEvent(
                            kind="node_complete" if result.status == NodeStatus.COMPLETED else "node_failed",
                            node_name=name,
                            result=result,
                            level=level_idx,
                        )

                        if result.status == NodeStatus.FAILED and self._config.fail_fast:
                            final = self._build_result(context, start, WorkflowStatus.FAILED, result.error)
                            yield StreamEvent(kind="workflow_failed", final=final)
                            return

            yield StreamEvent(kind="level_complete", level=level_idx)

        any_failed = any(s == NodeStatus.FAILED for s in status_map.values())
        status = WorkflowStatus.FAILED if any_failed else WorkflowStatus.COMPLETED
        final = self._build_result(context, start, status, final_output=self._get_final_output(context))
        yield StreamEvent(
            kind="workflow_complete" if status == WorkflowStatus.COMPLETED else "workflow_failed",
            final=final,
        )
