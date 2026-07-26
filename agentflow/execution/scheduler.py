from __future__ import annotations

import heapq
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any

from agentflow.execution.executor import WorkflowExecutor
from agentflow.types import (
    EdgeType,
    NodeResult,
    NodeStatus,
    SharedContext,
    WorkflowResult,
    WorkflowStatus,
)


class DependencyScheduler(WorkflowExecutor):
    def run(self, context: SharedContext | None = None, skip_nodes: set[str] | None = None) -> WorkflowResult:
        errors = self._dag.validate()
        if errors:
            return WorkflowResult(
                workflow_id="",
                status=WorkflowStatus.FAILED,
                error=f"validation failed: {'; '.join(errors)}",
            )

        context = context or SharedContext()
        start = time.perf_counter()
        skip_nodes = skip_nodes or set()

        if self._hooks.on_workflow_start:
            self._hooks.on_workflow_start(context)

        ranks = self._critical_path_ranks()
        remaining: dict[str, int] = {}
        for name in self._dag.nodes:
            remaining[name] = sum(
                1 for e in self._dag.predecessors(name) if e.source not in skip_nodes
            )

        status_map: dict[str, NodeStatus] = {
            n: (NodeStatus.COMPLETED if n in skip_nodes else NodeStatus.PENDING)
            for n in self._dag.nodes
        }

        ready: list[tuple[int, int, str]] = []
        counter = 0
        for name, deg in remaining.items():
            if deg == 0 and name not in skip_nodes:
                heapq.heappush(ready, self._key(name, ranks, counter))
                counter += 1

        deadline = start + self._config.workflow_timeout_s if self._config.workflow_timeout_s > 0 else 0
        lock = threading.Lock()
        in_flight: dict[Any, str] = {}
        aborted_error = ""

        with ThreadPoolExecutor(max_workers=self._config.max_parallel) as pool:
            while ready or in_flight:
                if deadline and time.perf_counter() >= deadline:
                    aborted_error = f"workflow exceeded {self._config.workflow_timeout_s}s budget"
                    break

                while ready and len(in_flight) < self._config.max_parallel:
                    _, _, name = heapq.heappop(ready)

                    if not self._should_run(name, status_map, context):
                        status_map[name] = NodeStatus.SKIPPED
                        context.results[name] = NodeResult(node_name=name, status=NodeStatus.SKIPPED)
                        counter = self._release(name, remaining, ready, ranks, counter, skip_nodes)
                        continue

                    status_map[name] = NodeStatus.RUNNING
                    future = pool.submit(self._execute_node, name, context)
                    in_flight[future] = name

                if not in_flight:
                    continue

                timeout = max(0.0, deadline - time.perf_counter()) if deadline else None
                done, _ = wait(list(in_flight), timeout=timeout, return_when=FIRST_COMPLETED)

                if not done:
                    aborted_error = f"workflow exceeded {self._config.workflow_timeout_s}s budget"
                    break

                for future in done:
                    name = in_flight.pop(future)
                    try:
                        result = future.result()
                    except Exception as e:
                        result = NodeResult(node_name=name, status=NodeStatus.FAILED, error=str(e))

                    with lock:
                        context.results[name] = result
                        status_map[name] = result.status

                    if result.status == NodeStatus.FAILED and self._config.fail_fast:
                        aborted_error = result.error
                        break

                    counter = self._release(name, remaining, ready, ranks, counter, skip_nodes)

                if aborted_error:
                    break

        if aborted_error:
            return self._build_result(context, start, WorkflowStatus.FAILED, aborted_error)

        any_failed = any(s == NodeStatus.FAILED for s in status_map.values())
        status = WorkflowStatus.FAILED if any_failed else WorkflowStatus.COMPLETED
        return self._build_result(context, start, status, final_output=self._get_final_output(context))

    def _release(self, name, remaining, ready, ranks, counter, skip_nodes) -> int:
        for edge in self._dag.successors(name):
            remaining[edge.target] -= 1
            if remaining[edge.target] == 0 and edge.target not in skip_nodes:
                heapq.heappush(ready, self._key(edge.target, ranks, counter))
                counter += 1
        return counter

    def _key(self, name: str, ranks: dict[str, int], counter: int) -> tuple[int, int, str]:
        spec = self._dag.nodes.get(name)
        priority = spec.priority if spec else 0
        return (-priority, -ranks.get(name, 0), name)

    def _critical_path_ranks(self) -> dict[str, int]:
        ranks: dict[str, int] = {}

        def depth(node: str) -> int:
            if node in ranks:
                return ranks[node]
            succs = self._dag.successors(node)
            ranks[node] = 1 + max((depth(e.target) for e in succs), default=0)
            return ranks[node]

        for name in self._dag.nodes:
            depth(name)
        return ranks
