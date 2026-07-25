from __future__ import annotations

from agentflow.graph import DAG
from agentflow.types import EdgeType, NodeStatus, WorkflowResult


NODE_SHAPES = {
    "llm": ("([", "])"),
    "tool": ("[", "]"),
    "transform": ("[/", "/]"),
    "conditional": ("{", "}"),
    "aggregator": ("[(", ")]"),
    "supervisor": ("[[", "]]"),
    "gate": ("{{", "}}"),
    "loop": ("[/", "\\]"),
    "subworkflow": ("[[", "]]"),
}

STATUS_COLORS = {
    NodeStatus.COMPLETED: "#2d6a4f",
    NodeStatus.FAILED: "#9b2226",
    NodeStatus.SKIPPED: "#6c757d",
    NodeStatus.RUNNING: "#0077b6",
    NodeStatus.PENDING: "#adb5bd",
}


def to_mermaid(dag: DAG, result: WorkflowResult | None = None, direction: str = "TD") -> str:
    lines = [f"graph {direction}"]

    for name, spec in dag.nodes.items():
        open_b, close_b = NODE_SHAPES.get(spec.node_type, ("[", "]"))
        label = _escape_mermaid(name)
        node_id = _safe_id(name)
        lines.append(f"    {node_id}{open_b}{label}{close_b}")

    for edge in dag.edges:
        src, tgt = _safe_id(edge.source), _safe_id(edge.target)
        if edge.edge_type == EdgeType.CONDITIONAL:
            label = edge.key or "cond"
            lines.append(f"    {src} -.->|{_escape_mermaid(label)}| {tgt}")
        elif edge.key:
            lines.append(f"    {src} -->|{_escape_mermaid(edge.key)}| {tgt}")
        else:
            lines.append(f"    {src} --> {tgt}")

    if result:
        for name, nr in result.results.items():
            if name not in dag.nodes:
                continue
            color = STATUS_COLORS.get(nr.status)
            if color:
                lines.append(f"    style {_safe_id(name)} fill:{color},color:#fff")

    if dag.terminal_node:
        lines.append(f"    style {_safe_id(dag.terminal_node)} stroke-width:4px")

    return "\n".join(lines)


def to_dot(dag: DAG, result: WorkflowResult | None = None) -> str:
    lines = ["digraph workflow {", "    rankdir=TB;", '    node [shape=box, style="rounded,filled", fillcolor="#e9ecef"];']

    for name, spec in dag.nodes.items():
        attrs = [f'label="{_escape_dot(name)}\\n({spec.node_type})"']
        if result:
            nr = result.results.get(name)
            if nr:
                color = STATUS_COLORS.get(nr.status)
                if color:
                    attrs.append(f'fillcolor="{color}"')
                    attrs.append('fontcolor="#ffffff"')
        if name == dag.terminal_node:
            attrs.append("penwidth=3")
        lines.append(f'    "{_escape_dot(name)}" [{", ".join(attrs)}];')

    for edge in dag.edges:
        attrs = []
        if edge.edge_type == EdgeType.CONDITIONAL:
            attrs.append("style=dashed")
        if edge.key:
            attrs.append(f'label="{_escape_dot(edge.key)}"')
        attr_str = f' [{", ".join(attrs)}]' if attrs else ""
        lines.append(f'    "{_escape_dot(edge.source)}" -> "{_escape_dot(edge.target)}"{attr_str};')

    lines.append("}")
    return "\n".join(lines)


def to_ascii(dag: DAG, result: WorkflowResult | None = None) -> str:
    try:
        levels = dag.parallel_schedule()
    except ValueError as e:
        return f"cannot render: {e}"

    lines = []
    for depth, level in enumerate(levels):
        lines.append(f"Level {depth}:")
        for name in level:
            spec = dag.nodes.get(name)
            node_type = spec.node_type if spec else "?"
            marker = " "
            timing = ""
            if result:
                nr = result.results.get(name)
                if nr:
                    marker = {
                        NodeStatus.COMPLETED: "+",
                        NodeStatus.FAILED: "x",
                        NodeStatus.SKIPPED: "-",
                    }.get(nr.status, "?")
                    timing = f"  {nr.elapsed_ms:.0f}ms"
            terminal_mark = "  <- terminal" if name == dag.terminal_node else ""
            lines.append(f"  [{marker}] {name} ({node_type}){timing}{terminal_mark}")

            succs = dag.successors(name)
            if succs:
                targets = ", ".join(e.target for e in succs)
                lines.append(f"      -> {targets}")
        lines.append("")

    return "\n".join(lines)


def to_summary(dag: DAG) -> str:
    try:
        levels = dag.parallel_schedule()
        depth = len(levels)
        max_width = max((len(lv) for lv in levels), default=0)
    except ValueError:
        depth = max_width = 0

    type_counts: dict[str, int] = {}
    for spec in dag.nodes.values():
        type_counts[spec.node_type] = type_counts.get(spec.node_type, 0) + 1

    cond_edges = sum(1 for e in dag.edges if e.edge_type == EdgeType.CONDITIONAL)

    lines = [
        f"Nodes: {len(dag.nodes)}",
        f"Edges: {len(dag.edges)} ({cond_edges} conditional)",
        f"Depth: {depth} levels, max width {max_width}",
        f"Roots: {', '.join(dag.root_nodes()) or 'none'}",
        f"Leaves: {', '.join(dag.leaf_nodes()) or 'none'}",
        f"Terminal: {dag.terminal_node or 'auto'}",
        "Node types: " + ", ".join(f"{t}={c}" for t, c in sorted(type_counts.items())),
    ]
    return "\n".join(lines)


def _safe_id(name: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)


def _escape_mermaid(text: str) -> str:
    return text.replace('"', "'").replace("|", "/").replace("[", "(").replace("]", ")")


def _escape_dot(text: str) -> str:
    return text.replace('"', '\\"')
