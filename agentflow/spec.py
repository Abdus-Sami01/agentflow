from __future__ import annotations

import json
from typing import Any, Callable

from agentflow.builder import WorkflowBuilder
from agentflow.types import SharedContext


class FunctionRegistry:
    def __init__(self):
        self._fns: dict[str, Callable] = {}

    def register(self, name: str, fn: Callable) -> None:
        self._fns[name] = fn

    def register_many(self, mapping: dict[str, Callable]) -> None:
        self._fns.update(mapping)

    def resolve(self, name: str) -> Callable:
        fn = self._fns.get(name)
        if fn is None:
            raise ValueError(f"function {name!r} not registered. Available: {sorted(self._fns.keys())}")
        return fn

    def has(self, name: str) -> bool:
        return name in self._fns

    @property
    def names(self) -> set[str]:
        return set(self._fns.keys())


SPEC_REQUIRED_NODE_KEYS = {"name", "type"}

SPEC_TYPE_REQUIREMENTS: dict[str, set[str]] = {
    "llm": {"llm_fn"},
    "tool": {"fn"},
    "transform": {"fn"},
    "conditional": {"condition"},
    "aggregator": set(),
    "supervisor": {"evaluate_fn"},
    "gate": {"check_fn"},
    "loop": {"body_fn", "condition_fn"},
    "foreach": {"item_fn"},
    "router": {"routes"},
    "http": set(),
    "memory_read": {"key"},
    "memory_write": {"key"},
    "memory_append": {"key"},
    "agent": {"propose_fn", "verify_fn"},
    "batch": {"batch_fn"},
    "subworkflow": {"workflow_factory"},
}

SPEC_ALLOWED_NODE_TYPES = set(SPEC_TYPE_REQUIREMENTS)

SPEC_FORBIDDEN_KEYS = {"allow_private"}


def validate_spec(spec: dict[str, Any]) -> list[str]:
    errors = []

    if not isinstance(spec, dict):
        return ["spec must be a mapping"]

    nodes = spec.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        errors.append("spec must contain a non-empty 'nodes' list")
        return errors

    seen_names = set()
    for i, node in enumerate(nodes):
        if not isinstance(node, dict):
            errors.append(f"node[{i}] must be a mapping")
            continue

        missing = SPEC_REQUIRED_NODE_KEYS - set(node.keys())
        if missing:
            errors.append(f"node[{i}] missing keys: {sorted(missing)}")
            continue

        name = node["name"]
        node_type = node["type"]

        if name in seen_names:
            errors.append(f"duplicate node name: {name!r}")
        seen_names.add(name)

        if node_type not in SPEC_ALLOWED_NODE_TYPES:
            errors.append(f"node {name!r} has unsupported type {node_type!r}. Allowed: {sorted(SPEC_ALLOWED_NODE_TYPES)}")
            continue

        missing_fields = SPEC_TYPE_REQUIREMENTS[node_type] - set(node.keys())
        if missing_fields:
            errors.append(f"node {name!r} of type {node_type!r} missing required: {sorted(missing_fields)}")

        forbidden = SPEC_FORBIDDEN_KEYS & set(node.keys())
        if forbidden:
            errors.append(f"node {name!r} sets forbidden key(s) {sorted(forbidden)}; not permitted from a spec file")

        if node_type == "router" and not isinstance(node.get("routes"), dict):
            errors.append(f"node {name!r} 'routes' must be a mapping of route name to predicate name")

    for i, edge in enumerate(spec.get("edges", [])):
        if not isinstance(edge, dict):
            errors.append(f"edge[{i}] must be a mapping")
            continue
        src, tgt = edge.get("from"), edge.get("to")
        if not src or not tgt:
            errors.append(f"edge[{i}] needs both 'from' and 'to'")
            continue
        if src not in seen_names:
            errors.append(f"edge[{i}] source {src!r} is not a declared node")
        if tgt not in seen_names:
            errors.append(f"edge[{i}] target {tgt!r} is not a declared node")

    terminal = spec.get("terminal")
    if terminal and terminal not in seen_names:
        errors.append(f"terminal {terminal!r} is not a declared node")

    return errors


def build_from_spec(spec: dict[str, Any], registry: FunctionRegistry) -> WorkflowBuilder:
    errors = validate_spec(spec)
    if errors:
        raise ValueError(f"invalid spec: {'; '.join(errors)}")

    wb = WorkflowBuilder(spec.get("id", ""))

    cfg = spec.get("config", {})
    if cfg:
        wb.config(
            max_parallel=cfg.get("max_parallel", 4),
            fail_fast=cfg.get("fail_fast", False),
            default_timeout=cfg.get("default_timeout", 60.0),
            default_retries=cfg.get("default_retries", 0),
        )

    for node in spec["nodes"]:
        _add_node_from_spec(wb, node, registry)
        if node.get("priority"):
            wb.priority(node["name"], node["priority"])

    for edge in spec.get("edges", []):
        condition_name = edge.get("condition")
        if condition_name:
            wb.conditional_edge(
                edge["from"], edge["to"],
                condition=registry.resolve(condition_name),
                key=edge.get("key", ""),
            )
        else:
            wb.edge(edge["from"], edge["to"], key=edge.get("key", ""))

    if spec.get("terminal"):
        wb.terminal(spec["terminal"])

    return wb


def _add_node_from_spec(wb: WorkflowBuilder, node: dict[str, Any], registry: FunctionRegistry) -> None:
    name = node["name"]
    node_type = node["type"]
    retry = node.get("retry", 0)
    timeout = node.get("timeout", 0)

    if node_type == "llm":
        wb.llm(
            name,
            registry.resolve(node["llm_fn"]),
            prompt_template=node.get("prompt", ""),
            retry=retry,
            timeout=timeout,
        )
    elif node_type == "tool":
        wb.tool(name, registry.resolve(node["fn"]), arg_map=node.get("arg_map"), retry=retry, timeout=timeout)
    elif node_type == "transform":
        wb.transform(name, registry.resolve(node["fn"]))
    elif node_type == "conditional":
        wb.conditional(name, registry.resolve(node["condition"]), branches=node.get("branches"))
    elif node_type == "aggregator":
        merge_fn = registry.resolve(node["merge_fn"]) if node.get("merge_fn") else None
        wb.aggregator(name, strategy=node.get("strategy", "merge"), merge_fn=merge_fn)
    elif node_type == "supervisor":
        wb.supervisor(name, registry.resolve(node["evaluate_fn"]), max_rounds=node.get("max_rounds", 3))
    elif node_type == "gate":
        wb.gate(name, registry.resolve(node["check_fn"]), fail_message=node.get("fail_message", "gate check failed"))
    elif node_type == "loop":
        wb.loop_node(
            name,
            registry.resolve(node["body_fn"]),
            registry.resolve(node["condition_fn"]),
            max_iterations=node.get("max_iterations", 10),
        )
    elif node_type == "foreach":
        wb.foreach(
            name,
            registry.resolve(node["item_fn"]),
            max_parallel=node.get("max_parallel", 4),
            max_items=node.get("max_items", 1000),
            fail_fast=node.get("fail_fast", False),
            item_timeout=node.get("item_timeout", 0),
        )
    elif node_type == "router":
        wb.router(
            name,
            routes={k: registry.resolve(v) for k, v in node["routes"].items()},
            default_route=node.get("default_route", ""),
            match_all=node.get("match_all", False),
        )
    elif node_type == "http":
        wb.http(
            name,
            url=node.get("url", ""),
            method=node.get("method", "GET"),
            headers=node.get("headers"),
            timeout=node.get("timeout", 10.0),
            parse_json=node.get("parse_json", True),
            url_fn=registry.resolve(node["url_fn"]) if node.get("url_fn") else None,
            retry=retry,
        )
    elif node_type == "memory_read":
        wb.memory_read(name, node["key"], default=node.get("default"), required=node.get("required", False))
    elif node_type == "memory_write":
        value_fn = registry.resolve(node["value_fn"]) if node.get("value_fn") else None
        wb.memory_write(name, node["key"], value_fn=value_fn)
    elif node_type == "memory_append":
        wb.memory_append(name, node["key"], max_len=node.get("max_len", 1000))
    elif node_type == "agent":
        wb.agent(
            name,
            registry.resolve(node["propose_fn"]),
            registry.resolve(node["verify_fn"]),
            execute_fn=registry.resolve(node["execute_fn"]) if node.get("execute_fn") else None,
            max_attempts=node.get("max_attempts", 3),
            priority=node.get("priority", 0),
        )
    elif node_type == "batch":
        wb.batch(
            name,
            registry.resolve(node["batch_fn"]),
            batch_size=node.get("batch_size", 10),
            flatten=node.get("flatten", True),
            stop_on_error=node.get("stop_on_error", False),
        )
    elif node_type == "subworkflow":
        wb.subworkflow(
            name,
            registry.resolve(node["workflow_factory"]),
            inherit_memory=node.get("inherit_memory", False),
            max_depth=node.get("max_depth", 5),
        )


def load_spec_json(path: str) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


def load_spec_yaml(path: str) -> dict[str, Any]:
    try:
        import yaml
    except ImportError:
        raise ImportError("pyyaml required for YAML specs: pip install pyyaml")
    with open(path) as f:
        return yaml.safe_load(f)


def spec_from_builder(wb: WorkflowBuilder, workflow_id: str = "") -> dict[str, Any]:
    dag = wb.dag
    return {
        "id": workflow_id or "exported",
        "nodes": [
            {"name": name, "type": spec.node_type, "retry": spec.retry_count, "timeout": spec.timeout_s}
            for name, spec in dag.nodes.items()
        ],
        "edges": [
            {"from": e.source, "to": e.target, "key": e.key}
            for e in dag.edges
        ],
        "terminal": dag.terminal_node,
    }
