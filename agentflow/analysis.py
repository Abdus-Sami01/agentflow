from __future__ import annotations

from collections import defaultdict
from typing import Any

from agentflow.graph import DAG
from agentflow.types import NodeResult, NodeStatus, WorkflowResult


def find_critical_path(dag: DAG, result: WorkflowResult) -> list[str]:
    memo: dict[str, float] = {}

    def longest_to(node: str) -> float:
        if node in memo:
            return memo[node]
        nr = result.results.get(node)
        own_time = nr.elapsed_ms if nr else 0

        preds = dag.predecessors(node)
        if not preds:
            memo[node] = own_time
            return own_time

        best = max(longest_to(e.source) for e in preds) + own_time
        memo[node] = best
        return best

    if not dag.nodes:
        return []

    leaves = dag.leaf_nodes() or list(dag.nodes.keys())
    terminal = max(leaves, key=lambda n: longest_to(n))

    path = []
    current = terminal
    while current:
        path.append(current)
        preds = dag.predecessors(current)
        if not preds:
            break
        current = max((e.source for e in preds), key=lambda n: memo.get(n, 0))

    path.reverse()
    return path


def find_bottlenecks(result: WorkflowResult, threshold_pct: float = 0.3) -> list[str]:
    if result.total_ms == 0:
        return []
    threshold = result.total_ms * threshold_pct
    return [
        name for name, nr in result.results.items()
        if nr.elapsed_ms >= threshold and nr.status == NodeStatus.COMPLETED
    ]


def compute_parallelism(dag: DAG) -> dict[str, Any]:
    schedule = dag.parallel_schedule()
    if not schedule:
        return {"levels": 0, "max_width": 0, "avg_width": 0, "depth": 0}

    widths = [len(level) for level in schedule]
    return {
        "levels": len(schedule),
        "max_width": max(widths),
        "avg_width": sum(widths) / len(widths),
        "depth": len(schedule),
        "width_per_level": widths,
    }


def deadlock_check(dag: DAG) -> list[str]:
    issues = []

    cycle = dag.detect_cycle()
    if cycle:
        issues.append(f"cycle: {' -> '.join(cycle)}")

    roots = dag.root_nodes()
    if not roots and dag.nodes:
        issues.append("no root nodes — every node has dependencies, nothing can start")

    for name in dag.nodes:
        deps = dag.get_dependencies(name)
        if name in deps:
            issues.append(f"node {name!r} transitively depends on itself")

    return issues


def compute_node_stats(result: WorkflowResult) -> dict[str, Any]:
    times = []
    retries = 0
    for nr in result.results.values():
        if nr.status == NodeStatus.COMPLETED:
            times.append(nr.elapsed_ms)
        if nr.attempts > 1:
            retries += nr.attempts - 1

    if not times:
        return {"count": 0, "total_ms": 0, "mean_ms": 0, "max_ms": 0, "min_ms": 0, "total_retries": retries}

    return {
        "count": len(times),
        "total_ms": sum(times),
        "mean_ms": sum(times) / len(times),
        "max_ms": max(times),
        "min_ms": min(times),
        "total_retries": retries,
    }


def dependency_matrix(dag: DAG) -> dict[str, set[str]]:
    return {name: dag.get_dependencies(name) for name in dag.nodes}


def impact_analysis(dag: DAG, node_name: str) -> dict[str, Any]:
    deps = dag.get_dependencies(node_name)
    dependents = dag.get_dependents(node_name)
    return {
        "node": node_name,
        "depends_on": sorted(deps),
        "blocks": sorted(dependents),
        "total_upstream": len(deps),
        "total_downstream": len(dependents),
        "is_root": len(deps) == 0,
        "is_leaf": len(dependents) == 0,
    }
