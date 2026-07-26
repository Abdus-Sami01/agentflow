from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any

from agentflow.graph import DAG
from agentflow.types import WorkflowResult


@dataclass
class SimulationResult:
    makespan_ms: float
    critical_path: list[str]
    start_times: dict[str, float] = field(default_factory=dict)
    finish_times: dict[str, float] = field(default_factory=dict)
    max_concurrency: int = 0
    idle_ratio: float = 0.0
    total_work_ms: float = 0.0

    @property
    def speedup(self) -> float:
        return self.total_work_ms / self.makespan_ms if self.makespan_ms else 0.0

    def summary(self) -> str:
        return "\n".join([
            f"Makespan:        {self.makespan_ms:.1f}ms",
            f"Total work:      {self.total_work_ms:.1f}ms",
            f"Speedup:         {self.speedup:.2f}x",
            f"Peak concurrency:{self.max_concurrency:>4}",
            f"Idle ratio:      {self.idle_ratio:.1%}",
            f"Critical path:   {' -> '.join(self.critical_path)}",
        ])


def simulate(
    dag: DAG,
    durations: dict[str, float],
    max_parallel: int = 4,
    default_duration_ms: float = 1.0,
) -> SimulationResult:
    remaining = {n: dag.in_degree(n) for n in dag.nodes}
    finish: dict[str, float] = {}
    start_times: dict[str, float] = {}

    ready = [n for n, d in remaining.items() if d == 0]
    running: list[tuple[float, str]] = []
    clock = 0.0
    peak = 0

    def dur(name: str) -> float:
        return durations.get(name, default_duration_ms)

    while ready or running:
        while ready and len(running) < max_parallel:
            name = ready.pop(0)
            start_times[name] = clock
            heapq.heappush(running, (clock + dur(name), name))
        peak = max(peak, len(running))

        if not running:
            break

        clock, done_name = heapq.heappop(running)
        finish[done_name] = clock

        for edge in dag.successors(done_name):
            remaining[edge.target] -= 1
            if remaining[edge.target] == 0:
                ready.append(edge.target)

        while running and running[0][0] <= clock:
            t, other = heapq.heappop(running)
            finish[other] = t
            for edge in dag.successors(other):
                remaining[edge.target] -= 1
                if remaining[edge.target] == 0:
                    ready.append(edge.target)

    makespan = max(finish.values(), default=0.0)
    total_work = sum(dur(n) for n in dag.nodes)

    path = _critical_path(dag, durations, finish, default_duration_ms)
    capacity = makespan * max_parallel
    idle = 1.0 - (total_work / capacity) if capacity else 0.0

    return SimulationResult(
        makespan_ms=makespan,
        critical_path=path,
        start_times=start_times,
        finish_times=finish,
        max_concurrency=peak,
        idle_ratio=max(0.0, idle),
        total_work_ms=total_work,
    )


def _critical_path(dag: DAG, durations: dict[str, float], finish: dict[str, float], default: float) -> list[str]:
    if not finish:
        return []
    node = max(finish, key=lambda n: finish[n])
    path = [node]
    while True:
        preds = dag.predecessors(node)
        if not preds:
            break
        node = max((e.source for e in preds), key=lambda n: finish.get(n, 0.0))
        path.append(node)
    path.reverse()
    return path


def durations_from_result(result: WorkflowResult) -> dict[str, float]:
    return {name: nr.elapsed_ms for name, nr in result.results.items()}


def recommend_parallelism(
    dag: DAG,
    durations: dict[str, float],
    candidates: list[int] | None = None,
    default_duration_ms: float = 1.0,
) -> dict[str, Any]:
    options = candidates or [1, 2, 4, 8, 16]
    trials = {}
    for width in options:
        sim = simulate(dag, durations, max_parallel=width, default_duration_ms=default_duration_ms)
        trials[width] = sim.makespan_ms

    best_span = min(trials.values())
    knee = min(w for w, span in trials.items() if span <= best_span * 1.05)

    return {
        "makespan_by_width": trials,
        "best_makespan_ms": best_span,
        "recommended": knee,
        "reason": f"width {knee} reaches within 5% of the best achievable makespan",
    }


def what_if(
    dag: DAG,
    durations: dict[str, float],
    node_name: str,
    new_duration_ms: float,
    max_parallel: int = 4,
) -> dict[str, Any]:
    base = simulate(dag, durations, max_parallel)
    altered = dict(durations)
    altered[node_name] = new_duration_ms
    after = simulate(dag, altered, max_parallel)

    delta = after.makespan_ms - base.makespan_ms
    return {
        "node": node_name,
        "before_ms": durations.get(node_name, 0.0),
        "after_ms": new_duration_ms,
        "makespan_before": base.makespan_ms,
        "makespan_after": after.makespan_ms,
        "makespan_delta": delta,
        "on_critical_path": node_name in base.critical_path,
        "verdict": (
            "no effect - node has slack" if abs(delta) < 1e-9
            else f"makespan {'increases' if delta > 0 else 'decreases'} by {abs(delta):.1f}ms"
        ),
    }
