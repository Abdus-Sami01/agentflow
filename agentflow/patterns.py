from __future__ import annotations

from typing import Any, Callable

from agentflow.builder import WorkflowBuilder
from agentflow.types import SharedContext


def map_reduce(
    workflow_id: str,
    items: list[Any],
    map_fn: Callable[[Any], Any],
    reduce_fn: Callable[[dict[str, Any]], Any],
    max_parallel: int = 4,
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)
    wb.config(max_parallel=max_parallel)

    wb.tool("source", lambda: items)

    mapper_names = []
    for i, item in enumerate(items):
        name = f"map_{i}"
        captured = item
        wb.tool(name, lambda _captured=captured, **_kw: map_fn(_captured))
        wb.edge("source", name)
        mapper_names.append(name)

    wb.aggregator("reduce", merge_fn=reduce_fn)
    for name in mapper_names:
        wb.edge(name, "reduce")
    wb.terminal("reduce")

    return wb


def pipeline(
    workflow_id: str,
    steps: list[tuple[str, Callable[..., Any]]],
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    for i, (name, fn) in enumerate(steps):
        if i == 0:
            wb.tool(name, fn)
        else:
            wb.transform(name, lambda inputs, _fn=fn: _fn(next(iter(inputs.values()))))

    for i in range(len(steps) - 1):
        wb.edge(steps[i][0], steps[i + 1][0])

    if steps:
        wb.terminal(steps[-1][0])

    return wb


def fan_out_fan_in(
    workflow_id: str,
    source_fn: Callable[[], Any],
    branch_fns: dict[str, Callable[[Any], Any]],
    merge_strategy: str = "merge",
    merge_fn: Callable[[dict[str, Any]], Any] | None = None,
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    wb.tool("source", source_fn)

    for name, fn in branch_fns.items():
        wb.transform(name, lambda inputs, _fn=fn: _fn(next(iter(inputs.values()))))
        wb.edge("source", name)

    wb.aggregator("merge", strategy=merge_strategy, merge_fn=merge_fn)
    for name in branch_fns:
        wb.edge(name, "merge")
    wb.terminal("merge")

    return wb


def chain_of_thought(
    workflow_id: str,
    llm_fn: Callable[[str], str],
    steps: list[tuple[str, str]],
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    for name, prompt_template in steps:
        wb.llm(name, llm_fn, prompt_template)

    for i in range(len(steps) - 1):
        wb.edge(steps[i][0], steps[i + 1][0])

    if steps:
        wb.terminal(steps[-1][0])

    return wb


def voting_ensemble(
    workflow_id: str,
    voter_fns: dict[str, Callable[[Any], Any]],
    input_fn: Callable[[], Any],
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    wb.tool("input", input_fn)

    for name, fn in voter_fns.items():
        wb.transform(name, lambda inputs, _fn=fn: _fn(next(iter(inputs.values()))))
        wb.edge("input", name)

    wb.aggregator("vote", strategy="vote")
    for name in voter_fns:
        wb.edge(name, "vote")
    wb.terminal("vote")

    return wb


def guarded_pipeline(
    workflow_id: str,
    steps: list[tuple[str, Callable[..., Any]]],
    gate_checks: dict[str, Callable[[dict[str, Any], SharedContext], bool]],
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    node_order = []
    for i, (name, fn) in enumerate(steps):
        if i == 0:
            wb.tool(name, fn)
        else:
            wb.transform(name, lambda inputs, _fn=fn: _fn(next(iter(inputs.values()))))
        node_order.append(name)

        gate_name = f"gate_{name}"
        if name in gate_checks:
            wb.gate(gate_name, gate_checks[name], fail_message=f"gate after {name} failed")
            node_order.append(gate_name)

    for i in range(len(node_order) - 1):
        wb.edge(node_order[i], node_order[i + 1])

    if node_order:
        wb.terminal(node_order[-1])

    return wb


def supervisor_loop(
    workflow_id: str,
    worker_fn: Callable[..., Any],
    supervisor_fn: Callable[[dict[str, Any], SharedContext], dict[str, Any]],
    max_rounds: int = 3,
) -> WorkflowBuilder:
    wb = WorkflowBuilder(workflow_id)

    wb.tool("worker", worker_fn)
    wb.supervisor("supervisor", supervisor_fn, max_rounds=max_rounds)
    wb.edge("worker", "supervisor")
    wb.terminal("supervisor")

    return wb
