from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from agentflow.graph import DAG
from agentflow.types import Edge


def transitive_reduction(dag: DAG) -> list[Edge]:
    reachable: dict[str, set[str]] = {}

    def compute(node: str) -> set[str]:
        if node in reachable:
            return reachable[node]
        acc: set[str] = set()
        for edge in dag.successors(node):
            acc.add(edge.target)
            acc |= compute(edge.target)
        reachable[node] = acc
        return acc

    for name in dag.nodes:
        compute(name)

    redundant = []
    for edge in dag.edges:
        indirect = set()
        for other in dag.successors(edge.source):
            if other.target == edge.target:
                continue
            indirect |= reachable.get(other.target, set())
        if edge.target in indirect:
            redundant.append(edge)

    return redundant


def all_paths(dag: DAG, source: str, target: str, max_paths: int = 100) -> list[list[str]]:
    if source not in dag.nodes or target not in dag.nodes:
        return []

    paths: list[list[str]] = []

    def walk(node: str, path: list[str]) -> None:
        if len(paths) >= max_paths:
            return
        if node == target:
            paths.append(list(path))
            return
        for edge in dag.successors(node):
            if edge.target in path:
                continue
            path.append(edge.target)
            walk(edge.target, path)
            path.pop()

    walk(source, [source])
    return paths


def shortest_path(dag: DAG, source: str, target: str) -> list[str] | None:
    if source not in dag.nodes or target not in dag.nodes:
        return None
    if source == target:
        return [source]

    prev: dict[str, str] = {}
    seen = {source}
    queue = deque([source])

    while queue:
        node = queue.popleft()
        for edge in dag.successors(node):
            if edge.target in seen:
                continue
            seen.add(edge.target)
            prev[edge.target] = node
            if edge.target == target:
                path = [target]
                while path[-1] != source:
                    path.append(prev[path[-1]])
                path.reverse()
                return path
            queue.append(edge.target)

    return None


def betweenness_centrality(dag: DAG, max_paths_per_pair: int = 20) -> dict[str, float]:
    scores: dict[str, float] = {n: 0.0 for n in dag.nodes}
    roots = dag.root_nodes()
    leaves = dag.leaf_nodes()

    total_paths = 0
    for root in roots:
        for leaf in leaves:
            if root == leaf:
                continue
            paths = all_paths(dag, root, leaf, max_paths=max_paths_per_pair)
            total_paths += len(paths)
            for path in paths:
                for node in path[1:-1]:
                    scores[node] += 1

    if total_paths:
        for node in scores:
            scores[node] /= total_paths

    return scores


def longest_chain(dag: DAG) -> list[str]:
    memo: dict[str, list[str]] = {}

    def longest_from(node: str) -> list[str]:
        if node in memo:
            return memo[node]
        best: list[str] = []
        for edge in dag.successors(node):
            candidate = longest_from(edge.target)
            if len(candidate) > len(best):
                best = candidate
        chain = [node] + best
        memo[node] = chain
        return chain

    overall: list[str] = []
    for name in dag.nodes:
        chain = longest_from(name)
        if len(chain) > len(overall):
            overall = chain
    return overall


def articulation_nodes(dag: DAG) -> list[str]:
    roots = dag.root_nodes()
    leaves = dag.leaf_nodes()
    if not roots or not leaves:
        return []

    baseline = _reachable_leaf_count(dag, roots, set(leaves))
    critical = []

    for name in dag.nodes:
        if name in roots or name in leaves:
            continue
        reduced = _reachable_leaf_count(dag, roots, set(leaves), removed=name)
        if reduced < baseline:
            critical.append(name)

    return critical


def _reachable_leaf_count(dag: DAG, roots: list[str], leaves: set[str], removed: str = "") -> int:
    seen = set()
    queue = deque(r for r in roots if r != removed)
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        for edge in dag.successors(node):
            if edge.target != removed:
                queue.append(edge.target)
    return len(seen & leaves)


def graph_density(dag: DAG) -> float:
    n = len(dag.nodes)
    if n < 2:
        return 0.0
    max_edges = n * (n - 1) / 2
    return len(dag.edges) / max_edges


def level_of(dag: DAG) -> dict[str, int]:
    levels: dict[str, int] = {}
    for depth, level in enumerate(dag.parallel_schedule()):
        for name in level:
            levels[name] = depth
    return levels
