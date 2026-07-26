from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agentflow.graph import DAG
from agentflow.graph_algos import transitive_reduction
from agentflow.nodes.base import BaseNode
from agentflow.types import Edge, EdgeType, NodeOutput, NodeSpec, SharedContext


@dataclass
class OptimizationReport:
    fused: list[list[str]] = field(default_factory=list)
    removed_edges: list[tuple[str, str]] = field(default_factory=list)
    removed_nodes: list[str] = field(default_factory=list)
    nodes_before: int = 0
    nodes_after: int = 0
    edges_before: int = 0
    edges_after: int = 0

    @property
    def changed(self) -> bool:
        return bool(self.fused or self.removed_edges or self.removed_nodes)

    def summary(self) -> str:
        lines = [
            f"Nodes: {self.nodes_before} -> {self.nodes_after}",
            f"Edges: {self.edges_before} -> {self.edges_after}",
        ]
        if self.fused:
            for chain in self.fused:
                lines.append(f"  fused: {' -> '.join(chain)}")
        for src, tgt in self.removed_edges:
            lines.append(f"  removed redundant edge: {src} -> {tgt}")
        for node in self.removed_nodes:
            lines.append(f"  removed unreachable node: {node}")
        if not self.changed:
            lines.append("  (no changes)")
        return "\n".join(lines)


class FusedNode(BaseNode):
    def __init__(self, name: str, chain: list[BaseNode]):
        super().__init__(name)
        self._chain = chain

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        current = inputs
        last: NodeOutput | None = None

        for node in self._chain:
            last = node.execute(current, context)
            if not last.success:
                return NodeOutput(
                    error=f"{node.name}: {last.error}",
                    metadata={"failed_stage": node.name, "fused": [n.name for n in self._chain]},
                )
            current = {node.name: last.data}

        if last is None:
            return NodeOutput(error="fused node has empty chain")

        return NodeOutput(
            data=last.data,
            metadata={"fused": [n.name for n in self._chain], "stages": len(self._chain)},
        )

    @property
    def stage_names(self) -> list[str]:
        return [n.name for n in self._chain]


FUSABLE_TYPES = {"transform", "tool", "memory_read", "memory_write"}


def find_fusable_chains(dag: DAG, fusable_types: set[str] | None = None) -> list[list[str]]:
    types = fusable_types or FUSABLE_TYPES
    chains: list[list[str]] = []
    consumed: set[str] = set()

    def is_fusable(name: str) -> bool:
        spec = dag.nodes.get(name)
        return spec is not None and spec.node_type in types

    for name in dag.nodes:
        if name in consumed or not is_fusable(name):
            continue
        if dag.in_degree(name) == 1:
            pred = dag.predecessors(name)[0].source
            if is_fusable(pred) and dag.out_degree(pred) == 1 and pred != dag.terminal_node:
                continue

        chain = [name]
        current = name
        while True:
            succs = dag.successors(current)
            if len(succs) != 1:
                break
            nxt = succs[0].target
            if nxt == dag.terminal_node:
                break
            if not is_fusable(nxt) or dag.in_degree(nxt) != 1 or nxt in consumed:
                break
            chain.append(nxt)
            current = nxt

        if len(chain) > 1:
            chains.append(chain)
            consumed.update(chain)

    return chains


def optimize(
    dag: DAG,
    nodes: dict[str, BaseNode],
    fuse: bool = True,
    prune_edges: bool = True,
    prune_unreachable: bool = True,
) -> tuple[DAG, dict[str, BaseNode], OptimizationReport]:
    report = OptimizationReport(
        nodes_before=len(dag.nodes),
        edges_before=len(dag.edges),
    )

    keep_specs = dict(dag.nodes)
    keep_nodes = dict(nodes)
    edges = list(dag.edges)

    if prune_unreachable:
        reachable = _reachable(dag)
        dead = set(keep_specs) - reachable
        for name in sorted(dead):
            keep_specs.pop(name, None)
            keep_nodes.pop(name, None)
            report.removed_nodes.append(name)
        edges = [e for e in edges if e.source not in dead and e.target not in dead]

    if prune_edges:
        redundant = [e for e in transitive_reduction(dag) if e.edge_type == EdgeType.CONTROL]
        redundant_pairs = {(e.source, e.target) for e in redundant}
        if redundant_pairs:
            edges = [
                e for e in edges
                if not (e.edge_type == EdgeType.CONTROL and (e.source, e.target) in redundant_pairs)
            ]
            report.removed_edges.extend(sorted(redundant_pairs))

    if fuse:
        working = _rebuild(keep_specs, edges, dag.terminal_node)
        chains = find_fusable_chains(working)
        for chain in chains:
            if not all(c in keep_nodes for c in chain):
                continue
            fused_name = "+".join(chain)
            keep_nodes[fused_name] = FusedNode(fused_name, [keep_nodes[c] for c in chain])
            keep_specs[fused_name] = NodeSpec(name=fused_name, node_type="fused")

            head, tail = chain[0], chain[-1]
            rewired = []
            for e in edges:
                if e.source in chain and e.target in chain:
                    continue
                src = fused_name if e.source == tail else e.source
                tgt = fused_name if e.target == head else e.target
                if src in chain or tgt in chain:
                    continue
                key = e.key
                if not key and e.source == tail:
                    key = tail
                rewired.append(Edge(source=src, target=tgt, edge_type=e.edge_type,
                                    condition=e.condition, key=key))
            edges = rewired

            for c in chain:
                keep_specs.pop(c, None)
                keep_nodes.pop(c, None)
            report.fused.append(chain)

    terminal = dag.terminal_node
    for chain in report.fused:
        if terminal in chain:
            terminal = "+".join(chain)

    optimized = _rebuild(keep_specs, edges, terminal)
    report.nodes_after = len(optimized.nodes)
    report.edges_after = len(optimized.edges)
    return optimized, keep_nodes, report


def _reachable(dag: DAG) -> set[str]:
    from collections import deque

    seen: set[str] = set()
    queue = deque(dag.root_nodes())
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        for edge in dag.successors(node):
            queue.append(edge.target)
    return seen


def _rebuild(specs: dict[str, NodeSpec], edges: list[Edge], terminal: str | None) -> DAG:
    dag = DAG()
    for spec in specs.values():
        dag.add_node(spec)
    seen_pairs = set()
    for edge in edges:
        if edge.source not in specs or edge.target not in specs:
            continue
        pair = (edge.source, edge.target)
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        dag.add_edge(edge)
    if terminal and terminal in specs:
        dag.set_terminal(terminal)
    return dag
