from __future__ import annotations

import asyncio
import time
from typing import Any

from agentflow.cache import NodeCache, compute_cache_key
from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.types import (
    EdgeType,
    NodeOutput,
    NodeResult,
    NodeStatus,
    SharedContext,
    WorkflowConfig,
    WorkflowHooks,
    WorkflowResult,
    WorkflowStatus,
)


class AsyncWorkflowExecutor:
    def __init__(
        self,
        dag: DAG,
        nodes: dict[str, BaseNode] | None = None,
        config: WorkflowConfig | None = None,
        hooks: WorkflowHooks | None = None,
        cache: NodeCache | None = None,
    ):
        self._dag = dag
        self._nodes = nodes or {}
        self._config = config or WorkflowConfig()
        self._hooks = hooks or WorkflowHooks()
        self._cache = cache

    async def resume(self, context: SharedContext) -> WorkflowResult:
        completed = {
            name for name, nr in context.results.items()
            if nr.status == NodeStatus.COMPLETED
        }
        return await self.run(context, skip_nodes=completed)

    async def run(self, context: SharedContext | None = None, skip_nodes: set[str] | None = None) -> WorkflowResult:
        errors = self._dag.validate()
        if errors:
            return WorkflowResult(
                workflow_id="",
                status=WorkflowStatus.FAILED,
                error=f"validation failed: {'; '.join(errors)}",
            )

        context = context or SharedContext()
        start = time.perf_counter()

        if self._hooks.on_workflow_start:
            self._hooks.on_workflow_start(context)

        schedule = self._dag.parallel_schedule(by_priority=self._config.respect_priority)
        status_map: dict[str, NodeStatus] = {n: NodeStatus.PENDING for n in self._dag.nodes}
        skip_nodes = skip_nodes or set()
        deadline = start + self._config.workflow_timeout_s if self._config.workflow_timeout_s > 0 else 0

        for name in skip_nodes:
            if name in status_map:
                status_map[name] = NodeStatus.COMPLETED

        for level in schedule:
            if deadline and time.perf_counter() >= deadline:
                return self._build_result(
                    context, start, WorkflowStatus.FAILED,
                    f"workflow exceeded {self._config.workflow_timeout_s}s budget",
                )

            pending = [n for n in level if n not in skip_nodes]
            runnable = [n for n in pending if self._should_run(n, status_map, context)]
            skipped = [n for n in pending if n not in runnable]

            for node_name in skipped:
                status_map[node_name] = NodeStatus.SKIPPED
                context.results[node_name] = NodeResult(
                    node_name=node_name, status=NodeStatus.SKIPPED,
                )

            if not runnable:
                continue

            sem = asyncio.Semaphore(self._config.max_parallel)

            async def run_with_sem(name: str) -> tuple[str, NodeResult]:
                async with sem:
                    return name, await self._execute_node(name, context)

            tasks = [asyncio.create_task(run_with_sem(n)) for n in runnable]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for item in results:
                if isinstance(item, Exception):
                    continue
                node_name, result = item
                context.results[node_name] = result
                status_map[node_name] = result.status

                if result.status == NodeStatus.FAILED and self._config.fail_fast:
                    return self._build_result(context, start, WorkflowStatus.FAILED, result.error)

        any_failed = any(s == NodeStatus.FAILED for s in status_map.values())
        final_status = WorkflowStatus.FAILED if any_failed else WorkflowStatus.COMPLETED
        final_output = self._get_final_output(context)
        return self._build_result(context, start, final_status, final_output=final_output)

    def _should_run(self, node_name: str, status_map: dict[str, NodeStatus], context: SharedContext) -> bool:
        for edge in self._dag.predecessors(node_name):
            source_status = status_map.get(edge.source, NodeStatus.PENDING)
            if source_status in (NodeStatus.FAILED, NodeStatus.SKIPPED):
                return False
            if edge.edge_type == EdgeType.CONDITIONAL and edge.condition:
                source_result = context.results.get(edge.source)
                if source_result and source_result.output:
                    if not edge.condition(source_result.output):
                        return False
        return True

    async def _execute_node(self, node_name: str, context: SharedContext) -> NodeResult:
        node = self._nodes.get(node_name)
        if node is None:
            return NodeResult(node_name=node_name, status=NodeStatus.FAILED, error=f"no node implementation for {node_name!r}")

        spec = self._dag.nodes.get(node_name)
        timeout = (spec.timeout_s if spec and spec.timeout_s else self._config.default_timeout_s) or 0
        max_retries = (spec.retry_count if spec else 0) or self._config.default_retries

        if self._hooks.on_node_start:
            self._hooks.on_node_start(node_name, context)

        inputs = self._gather_inputs(node_name, context)

        cache_key = ""
        if self._cache is not None:
            cache_key = compute_cache_key(node_name, inputs)
            cached = self._cache.get(cache_key)
            if cached is not None:
                result = NodeResult(
                    node_name=node_name,
                    status=NodeStatus.COMPLETED,
                    output=cached,
                    attempts=0,
                    elapsed_ms=0,
                )
                if self._hooks.on_node_complete:
                    self._hooks.on_node_complete(node_name, result, context)
                return result

        strategy = self._config.retry_strategy
        last_error = ""
        last_output: NodeOutput | None = None
        elapsed = 0.0
        attempt = -1

        while True:
            attempt += 1
            if attempt > 0:
                if strategy is not None:
                    if not strategy.should_retry(attempt - 1, last_error):
                        break
                    delay = strategy.delay(attempt - 1)
                    if delay > 0:
                        await asyncio.sleep(delay)
                elif attempt > max_retries:
                    break

            start = time.perf_counter()
            try:
                if timeout > 0:
                    output = await asyncio.wait_for(
                        asyncio.get_event_loop().run_in_executor(None, node.execute, inputs, context),
                        timeout=timeout,
                    )
                else:
                    output = await asyncio.get_event_loop().run_in_executor(None, node.execute, inputs, context)

                elapsed = (time.perf_counter() - start) * 1000

                if output.success:
                    if self._cache is not None and cache_key:
                        self._cache.put(cache_key, output)
                    result = NodeResult(
                        node_name=node_name,
                        status=NodeStatus.COMPLETED,
                        output=output,
                        attempts=attempt + 1,
                        elapsed_ms=elapsed,
                    )
                    if self._hooks.on_node_complete:
                        self._hooks.on_node_complete(node_name, result, context)
                    return result

                last_error = output.error
                last_output = output

            except asyncio.TimeoutError:
                last_error = f"node {node_name!r} timed out after {timeout}s"
                elapsed = timeout * 1000
            except Exception as e:
                last_error = str(e)
                elapsed = (time.perf_counter() - start) * 1000

        result = NodeResult(
            node_name=node_name,
            status=NodeStatus.FAILED,
            output=last_output,
            error=last_error,
            attempts=max(attempt, 1),
            elapsed_ms=elapsed,
        )
        if self._hooks.on_node_error:
            self._hooks.on_node_error(node_name, last_error, context)
        return result

    def _gather_inputs(self, node_name: str, context: SharedContext) -> dict[str, Any]:
        inputs = {}
        for edge in self._dag.predecessors(node_name):
            source_result = context.results.get(edge.source)
            if source_result and source_result.output and source_result.output.success:
                key = edge.key or edge.source
                inputs[key] = source_result.output.data
        return inputs

    def _get_final_output(self, context: SharedContext) -> Any:
        if self._dag.terminal_node:
            return context.get_output(self._dag.terminal_node)
        leaves = self._dag.leaf_nodes()
        if len(leaves) == 1:
            return context.get_output(leaves[0])
        return {leaf: context.get_output(leaf) for leaf in leaves}

    def _build_result(self, context, start, status, error="", final_output=None):
        total_ms = (time.perf_counter() - start) * 1000
        result = WorkflowResult(
            workflow_id=context.workflow_id,
            status=status,
            results=dict(context.results),
            final_output=final_output,
            total_ms=total_ms,
            error=error,
        )
        if self._hooks.on_workflow_complete:
            self._hooks.on_workflow_complete(result)
        return result
