from __future__ import annotations

import json
from typing import Any

from agentflow.types import NodeResult, NodeStatus, WorkflowResult, WorkflowStatus


def workflow_to_dict(result: WorkflowResult) -> dict[str, Any]:
    return {
        "workflow_id": result.workflow_id,
        "status": result.status.value,
        "total_ms": round(result.total_ms, 1),
        "completed": result.completed_count,
        "failed": result.failed_count,
        "skipped": result.skipped_count,
        "final_output": _safe_str(result.final_output),
        "error": result.error or None,
        "nodes": {
            name: _node_result_to_dict(nr)
            for name, nr in result.results.items()
        },
    }


def workflow_to_json(result: WorkflowResult, indent: int = 2) -> str:
    return json.dumps(workflow_to_dict(result), indent=indent, default=str)


def workflow_to_text(result: WorkflowResult) -> str:
    lines = [
        f"Workflow: {result.workflow_id or '<unnamed>'}",
        f"Status: {result.status.value}",
        f"Nodes: {result.completed_count} completed, {result.failed_count} failed, {result.skipped_count} skipped",
        f"Time: {result.total_ms:.0f}ms",
    ]
    if result.error:
        lines.append(f"Error: {result.error}")
    lines.append("")

    for name, nr in result.results.items():
        icon = {"completed": "OK", "failed": "FAIL", "skipped": "SKIP"}.get(nr.status.value, "?")
        lines.append(f"  [{icon}] {name} ({nr.elapsed_ms:.0f}ms, {nr.attempts} attempt{'s' if nr.attempts > 1 else ''})")
        if nr.error:
            lines.append(f"    Error: {nr.error}")
        if nr.output and nr.output.data is not None:
            lines.append(f"    Output: {_safe_str(nr.output.data)[:120]}")
    lines.append("")

    if result.final_output is not None:
        lines.append(f"Final: {_safe_str(result.final_output)[:200]}")

    return "\n".join(lines)


def _node_result_to_dict(nr: NodeResult) -> dict[str, Any]:
    d: dict[str, Any] = {
        "status": nr.status.value,
        "attempts": nr.attempts,
        "elapsed_ms": round(nr.elapsed_ms, 1),
    }
    if nr.error:
        d["error"] = nr.error
    if nr.output:
        d["output"] = _safe_str(nr.output.data)[:500] if nr.output.data is not None else None
        if nr.output.metadata:
            d["metadata"] = nr.output.metadata
    return d


def _safe_str(obj: Any) -> str:
    try:
        return str(obj)
    except Exception:
        return "<unrepresentable>"
