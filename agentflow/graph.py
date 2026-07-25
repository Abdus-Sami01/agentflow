from __future__ import annotations

from collections import defaultdict, deque
from typing import Any

from agentflow.types import Edge, EdgeType, NodeOutput, NodeSpec


class DAG:
    def __init__(self):
        self._nodes: dict[str, NodeSpec] = {}
        self._edges: list[Edge] = []
        self._adj: dict[str, list[Edge]] = defaultdict(list)
        self._rev_adj: dict[str, list[Edge]] = defaultdict(list)
        self._terminal_node: str | None = None

    def add_node(self, spec: NodeSpec) -> None:
        if spec.name in self._nodes:
            raise ValueError(f"duplicate node: {spec.name!r}")
        self._nodes[spec.name] = spec

    def add_edge(self, edge: Edge) -> None:
        if edge.source not in self._nodes:
            raise ValueError(f"source node {edge.source!r} not found")
        if edge.target not in self._nodes:
            raise ValueError(f"target node {edge.target!r} not found")
        self._edges.append(edge)
        self._adj[edge.source].append(edge)
        self._rev_adj[edge.target].append(edge)

    def set_terminal(self, node_name: str) -> None:
        if node_name not in self._nodes:
            raise ValueError(f"terminal node {node_name!r} not found")
        self._terminal_node = node_name

    @property
    def terminal_node(self) -> str | None:
        return self._terminal_node

    @property
    def nodes(self) -> dict[str, NodeSpec]:
        return dict(self._nodes)

    @property
    def edges(self) -> list[Edge]:
        return list(self._edges)

    def successors(self, node_name: str) -> list[Edge]:
        return list(self._adj.get(node_name, []))

    def predecessors(self, node_name: str) -> list[Edge]:
        return list(self._rev_adj.get(node_name, []))

    def root_nodes(self) -> list[str]:
        return [n for n in self._nodes if not self._rev_adj.get(n)]

    def leaf_nodes(self) -> list[str]:
        return [n for n in self._nodes if not self._adj.get(n)]

    def in_degree(self, node_name: str) -> int:
        return len(self._rev_adj.get(node_name, []))

    def out_degree(self, node_name: str) -> int:
        return len(self._adj.get(node_name, []))

    def detect_cycle(self) -> list[str] | None:
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {n: WHITE for n in self._nodes}
        parent = {}
        cycle_path = []

        def dfs(node: str) -> bool:
            color[node] = GRAY
            for edge in self._adj.get(node, []):
                neighbor = edge.target
                if color[neighbor] == GRAY:
                    path = [neighbor, node]
                    cur = node
                    while cur != neighbor and cur in parent:
                        cur = parent[cur]
                        path.append(cur)
                    cycle_path.extend(reversed(path))
                    return True
                if color[neighbor] == WHITE:
                    parent[neighbor] = node
                    if dfs(neighbor):
                        return True
            color[node] = BLACK
            return False

        for node in self._nodes:
            if color[node] == WHITE:
                if dfs(node):
                    return cycle_path
        return None

    def topological_sort(self) -> list[str]:
        cycle = self.detect_cycle()
        if cycle:
            raise ValueError(f"cycle detected: {' -> '.join(cycle)}")

        in_deg = {n: 0 for n in self._nodes}
        for edge in self._edges:
            in_deg[edge.target] += 1

        queue = deque(n for n, d in in_deg.items() if d == 0)
        order = []

        while queue:
            batch = sorted(queue)
            queue.clear()
            for node in batch:
                order.append(node)
                for edge in self._adj.get(node, []):
                    in_deg[edge.target] -= 1
                    if in_deg[edge.target] == 0:
                        queue.append(edge.target)

        if len(order) != len(self._nodes):
            raise ValueError("topological sort incomplete — unreachable nodes exist")

        return order

    def parallel_schedule(self) -> list[list[str]]:
        cycle = self.detect_cycle()
        if cycle:
            raise ValueError(f"cycle detected: {' -> '.join(cycle)}")

        in_deg = {n: 0 for n in self._nodes}
        for edge in self._edges:
            in_deg[edge.target] += 1

        levels: list[list[str]] = []
        current = sorted(n for n, d in in_deg.items() if d == 0)

        while current:
            levels.append(current)
            next_level = []
            for node in current:
                for edge in self._adj.get(node, []):
                    in_deg[edge.target] -= 1
                    if in_deg[edge.target] == 0:
                        next_level.append(edge.target)
            current = sorted(next_level)

        return levels

    def get_dependencies(self, node_name: str) -> set[str]:
        deps = set()
        queue = deque([node_name])
        while queue:
            current = queue.popleft()
            for edge in self._rev_adj.get(current, []):
                if edge.source not in deps:
                    deps.add(edge.source)
                    queue.append(edge.source)
        return deps

    def get_dependents(self, node_name: str) -> set[str]:
        deps = set()
        queue = deque([node_name])
        while queue:
            current = queue.popleft()
            for edge in self._adj.get(current, []):
                if edge.target not in deps:
                    deps.add(edge.target)
                    queue.append(edge.target)
        return deps

    def subgraph(self, node_names: set[str]) -> DAG:
        sub = DAG()
        for name in node_names:
            if name in self._nodes:
                sub.add_node(self._nodes[name])
        for edge in self._edges:
            if edge.source in node_names and edge.target in node_names:
                sub.add_edge(edge)
        return sub

    def validate(self) -> list[str]:
        errors = []

        if not self._nodes:
            errors.append("workflow has no nodes")
            return errors

        roots = self.root_nodes()
        if not roots:
            errors.append("no root nodes found — every node has incoming edges")

        cycle = self.detect_cycle()
        if cycle:
            errors.append(f"cycle detected: {' -> '.join(cycle)}")

        for edge in self._edges:
            if edge.source == edge.target:
                errors.append(f"self-loop on node {edge.source!r}")

        reachable = set()
        queue = deque(roots)
        while queue:
            node = queue.popleft()
            if node in reachable:
                continue
            reachable.add(node)
            for edge in self._adj.get(node, []):
                queue.append(edge.target)

        unreachable = set(self._nodes.keys()) - reachable
        if unreachable:
            errors.append(f"unreachable nodes: {unreachable}")

        if self._terminal_node and self._terminal_node not in self._nodes:
            errors.append(f"terminal node {self._terminal_node!r} not in graph")

        return errors
