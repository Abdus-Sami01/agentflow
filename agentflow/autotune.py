from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agentflow.graph import DAG
from agentflow.profiling import Profiler
from agentflow.simulate import recommend_parallelism, simulate
from agentflow.types import WorkflowConfig


@dataclass
class TuningPlan:
    max_parallel: int
    node_timeouts_s: dict[str, float] = field(default_factory=dict)
    node_retries: dict[str, int] = field(default_factory=dict)
    workflow_timeout_s: float = 0.0
    predicted_makespan_ms: float = 0.0
    baseline_makespan_ms: float = 0.0
    notes: list[str] = field(default_factory=list)

    @property
    def predicted_gain_pct(self) -> float:
        if not self.baseline_makespan_ms:
            return 0.0
        delta = self.baseline_makespan_ms - self.predicted_makespan_ms
        return delta / self.baseline_makespan_ms * 100

    def apply_to(self, config: WorkflowConfig) -> WorkflowConfig:
        return WorkflowConfig(
            max_parallel=self.max_parallel,
            fail_fast=config.fail_fast,
            default_timeout_s=config.default_timeout_s,
            default_retries=config.default_retries,
            workflow_timeout_s=self.workflow_timeout_s or config.workflow_timeout_s,
            retry_strategy=config.retry_strategy,
            respect_priority=config.respect_priority,
        )

    def summary(self) -> str:
        lines = [
            f"max_parallel:      {self.max_parallel}",
            f"workflow_timeout:  {self.workflow_timeout_s:.2f}s" if self.workflow_timeout_s else "workflow_timeout:  unset",
            f"predicted makespan:{self.predicted_makespan_ms:>9.1f}ms "
            f"(baseline {self.baseline_makespan_ms:.1f}ms, {self.predicted_gain_pct:+.1f}%)",
        ]
        if self.node_timeouts_s:
            lines.append("node timeouts:")
            for name, t in sorted(self.node_timeouts_s.items()):
                lines.append(f"    {name}: {t:.2f}s")
        if self.node_retries:
            lines.append("node retries:")
            for name, r in sorted(self.node_retries.items()):
                lines.append(f"    {name}: {r}")
        for note in self.notes:
            lines.append(f"note: {note}")
        return "\n".join(lines)


class AutoTuner:
    def __init__(
        self,
        profiler: Profiler | None = None,
        timeout_safety_factor: float = 3.0,
        failure_threshold: float = 0.1,
        workflow_timeout_factor: float = 2.5,
    ):
        self.profiler = profiler or Profiler()
        self._safety = timeout_safety_factor
        self._failure_threshold = failure_threshold
        self._wf_factor = workflow_timeout_factor

    def as_hooks(self):
        return self.profiler.as_hooks()

    def plan(self, dag: DAG, candidates: list[int] | None = None) -> TuningPlan:
        durations = self.profiler.durations(percentile=0.95)
        notes: list[str] = []

        if not durations:
            notes.append("no profiling samples collected; run the workflow before tuning")
            return TuningPlan(max_parallel=1, notes=notes)

        unprofiled = set(dag.nodes) - set(durations)
        if unprofiled:
            notes.append(f"no samples for {sorted(unprofiled)}; treated as negligible")

        rec = recommend_parallelism(dag, durations, candidates)
        width = rec["recommended"]

        serial = simulate(dag, durations, max_parallel=1)
        tuned = simulate(dag, durations, max_parallel=width)

        timeouts = self.profiler.suggest_timeouts(safety_factor=self._safety)
        retries = self.profiler.suggest_retries(threshold=self._failure_threshold)

        unstable = self.profiler.unstable_nodes()
        if unstable:
            notes.append(f"high-variance nodes (timeouts padded): {', '.join(unstable)}")

        wf_timeout = tuned.makespan_ms * self._wf_factor / 1000.0

        return TuningPlan(
            max_parallel=width,
            node_timeouts_s=timeouts,
            node_retries=retries,
            workflow_timeout_s=wf_timeout,
            predicted_makespan_ms=tuned.makespan_ms,
            baseline_makespan_ms=serial.makespan_ms,
            notes=notes,
        )

    def tune(self, dag: DAG, config: WorkflowConfig | None = None) -> tuple[WorkflowConfig, TuningPlan]:
        plan = self.plan(dag)
        base = config or WorkflowConfig()
        return plan.apply_to(base), plan

    def apply_node_settings(self, dag: DAG, plan: TuningPlan) -> int:
        changed = 0
        for name, spec in dag.nodes.items():
            if name in plan.node_timeouts_s:
                spec.timeout_s = plan.node_timeouts_s[name]
                changed += 1
            if name in plan.node_retries:
                spec.retry_count = plan.node_retries[name]
                changed += 1
        return changed
