from __future__ import annotations

import json
from typing import Any

from agentflow.types import (
    NodeOutput,
    NodeResult,
    NodeStatus,
    SharedContext,
    WorkflowResult,
    WorkflowStatus,
)


def serialize_context(context: SharedContext) -> dict[str, Any]:
    return {
        "workflow_id": context.workflow_id,
        "data": _safe_serialize(context.data),
        "metadata": _safe_serialize(context.metadata),
        "results": {
            name: _serialize_node_result(nr)
            for name, nr in context.results.items()
        },
    }


def deserialize_context(raw: dict[str, Any]) -> SharedContext:
    ctx = SharedContext(
        workflow_id=raw.get("workflow_id", ""),
        data=raw.get("data", {}),
        metadata=raw.get("metadata", {}),
    )
    for name, nr_data in raw.get("results", {}).items():
        ctx.results[name] = _deserialize_node_result(nr_data)
    return ctx


def serialize_result(result: WorkflowResult) -> dict[str, Any]:
    return {
        "workflow_id": result.workflow_id,
        "status": result.status.value,
        "total_ms": result.total_ms,
        "final_output": _safe_serialize(result.final_output),
        "error": result.error,
        "results": {
            name: _serialize_node_result(nr)
            for name, nr in result.results.items()
        },
    }


def deserialize_result(raw: dict[str, Any]) -> WorkflowResult:
    return WorkflowResult(
        workflow_id=raw.get("workflow_id", ""),
        status=WorkflowStatus(raw.get("status", "failed")),
        total_ms=raw.get("total_ms", 0),
        final_output=raw.get("final_output"),
        error=raw.get("error", ""),
        results={
            name: _deserialize_node_result(nr_data)
            for name, nr_data in raw.get("results", {}).items()
        },
    )


def save_context(context: SharedContext, path: str) -> None:
    with open(path, "w") as f:
        json.dump(serialize_context(context), f, indent=2, default=str)


def load_context(path: str) -> SharedContext:
    with open(path) as f:
        return deserialize_context(json.load(f))


def save_result(result: WorkflowResult, path: str) -> None:
    with open(path, "w") as f:
        json.dump(serialize_result(result), f, indent=2, default=str)


def load_result(path: str) -> WorkflowResult:
    with open(path) as f:
        return deserialize_result(json.load(f))


def _serialize_node_result(nr: NodeResult) -> dict[str, Any]:
    d: dict[str, Any] = {
        "node_name": nr.node_name,
        "status": nr.status.value,
        "attempts": nr.attempts,
        "elapsed_ms": nr.elapsed_ms,
        "error": nr.error,
    }
    if nr.output:
        d["output"] = {
            "data": _safe_serialize(nr.output.data),
            "error": nr.output.error,
            "metadata": _safe_serialize(nr.output.metadata),
        }
    return d


def _deserialize_node_result(raw: dict[str, Any]) -> NodeResult:
    output = None
    if "output" in raw and raw["output"]:
        output = NodeOutput(
            data=raw["output"].get("data"),
            error=raw["output"].get("error", ""),
            metadata=raw["output"].get("metadata", {}),
        )
    return NodeResult(
        node_name=raw.get("node_name", ""),
        status=NodeStatus(raw.get("status", "pending")),
        output=output,
        attempts=raw.get("attempts", 1),
        elapsed_ms=raw.get("elapsed_ms", 0),
        error=raw.get("error", ""),
    )


def _safe_serialize(obj: Any) -> Any:
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, dict):
        return {str(k): _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(v) for v in obj]
    return str(obj)
