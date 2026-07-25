from __future__ import annotations

from typing import Any

from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.types import Edge, EdgeType, NodeSpec


def merge_workflows(
    workflows: list[tuple[DAG, dict[str, BaseNode]]],
    prefix_names: bool = True,
) -> tuple[DAG, dict[str, BaseNode]]:
    merged_dag = DAG()
    merged_nodes: dict[str, BaseNode] = {}

    for i, (dag, nodes) in enumerate(workflows):
        prefix = f"wf{i}_" if prefix_names else ""

        name_map = {}
        for name, spec in dag.nodes.items():
            new_name = f"{prefix}{name}"
            name_map[name] = new_name
            merged_dag.add_node(NodeSpec(
                name=new_name,
                node_type=spec.node_type,
                config=spec.config,
                retry_count=spec.retry_count,
                timeout_s=spec.timeout_s,
                priority=spec.priority,
            ))
            if name in nodes:
                node = nodes[name]
                node.name = new_name
                merged_nodes[new_name] = node

        for edge in dag.edges:
            merged_dag.add_edge(Edge(
                source=name_map[edge.source],
                target=name_map[edge.target],
                edge_type=edge.edge_type,
                condition=edge.condition,
                key=edge.key,
            ))

    return merged_dag, merged_nodes


def chain_workflows(
    workflows: list[tuple[DAG, dict[str, BaseNode]]],
    bridge_keys: list[str] | None = None,
) -> tuple[DAG, dict[str, BaseNode]]:
    merged_dag, merged_nodes = merge_workflows(workflows)

    for i in range(len(workflows) - 1):
        prefix_cur = f"wf{i}_"
        prefix_next = f"wf{i+1}_"

        cur_dag = workflows[i][0]
        next_dag = workflows[i + 1][0]

        cur_leaves = [f"{prefix_cur}{n}" for n in cur_dag.leaf_nodes()]
        next_roots = [f"{prefix_next}{n}" for n in next_dag.root_nodes()]

        key = bridge_keys[i] if bridge_keys and i < len(bridge_keys) else ""

        for leaf in cur_leaves:
            for root in next_roots:
                merged_dag.add_edge(Edge(source=leaf, target=root, key=key))

    return merged_dag, merged_nodes


def parallel_workflows(
    workflows: list[tuple[DAG, dict[str, BaseNode]]],
    aggregator_node: BaseNode | None = None,
    aggregator_name: str = "final_merge",
) -> tuple[DAG, dict[str, BaseNode]]:
    merged_dag, merged_nodes = merge_workflows(workflows)

    if aggregator_node:
        merged_dag.add_node(NodeSpec(name=aggregator_name, node_type="aggregator"))
        merged_nodes[aggregator_name] = aggregator_node

        for i, (dag, _) in enumerate(workflows):
            prefix = f"wf{i}_"
            for leaf in dag.leaf_nodes():
                merged_dag.add_edge(Edge(
                    source=f"{prefix}{leaf}",
                    target=aggregator_name,
                    key=f"wf{i}",
                ))

        merged_dag.set_terminal(aggregator_name)

    return merged_dag, merged_nodes
