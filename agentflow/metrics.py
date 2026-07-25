from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any

from agentflow.types import NodeResult, NodeStatus, WorkflowHooks, WorkflowResult


class Histogram:
    def __init__(self, buckets: list[float] | None = None):
        self._buckets = sorted(buckets or [1, 5, 10, 50, 100, 500, 1000, 5000])
        self._counts: dict[float, int] = {b: 0 for b in self._buckets}
        self._inf = 0
        self._sum = 0.0
        self._n = 0

    def observe(self, value: float) -> None:
        self._sum += value
        self._n += 1
        for b in self._buckets:
            if value <= b:
                self._counts[b] += 1
                return
        self._inf += 1

    def percentile(self, p: float) -> float:
        if self._n == 0:
            return 0.0
        target = self._n * p
        cumulative = 0
        for b in self._buckets:
            cumulative += self._counts[b]
            if cumulative >= target:
                return b
        return float("inf") if self._inf else self._buckets[-1]

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "count": self._n,
            "sum": round(self._sum, 2),
            "mean": round(self._sum / self._n, 2) if self._n else 0.0,
            "p50": self.percentile(0.5),
            "p95": self.percentile(0.95),
            "p99": self.percentile(0.99),
            "buckets": dict(self._counts),
            "overflow": self._inf,
        }


class MetricsCollector:
    def __init__(self):
        self._lock = threading.Lock()
        self._counters: dict[str, int] = defaultdict(int)
        self._node_durations: dict[str, Histogram] = defaultdict(Histogram)
        self._workflow_durations = Histogram(buckets=[10, 50, 100, 500, 1000, 5000, 30000])
        self._node_status: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._retries: dict[str, int] = defaultdict(int)

    def increment(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def record_node(self, node_name: str, result: NodeResult) -> None:
        with self._lock:
            self._node_durations[node_name].observe(result.elapsed_ms)
            self._node_status[node_name][result.status.value] += 1
            self._counters[f"node.{result.status.value}"] += 1
            if result.attempts > 1:
                self._retries[node_name] += result.attempts - 1

    def record_workflow(self, result: WorkflowResult) -> None:
        with self._lock:
            self._workflow_durations.observe(result.total_ms)
            self._counters[f"workflow.{result.status.value}"] += 1

    def as_hooks(self) -> WorkflowHooks:
        return WorkflowHooks(
            on_node_complete=lambda name, result, ctx: self.record_node(name, result),
            on_node_error=lambda name, err, ctx: self.increment("node.error"),
            on_workflow_complete=self.record_workflow,
        )

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "counters": dict(self._counters),
                "workflow_duration": self._workflow_durations.stats,
                "node_durations": {n: h.stats for n, h in self._node_durations.items()},
                "node_status": {n: dict(s) for n, s in self._node_status.items()},
                "retries": dict(self._retries),
            }

    def to_prometheus(self) -> str:
        snap = self.snapshot()
        lines = []

        for name, value in sorted(snap["counters"].items()):
            metric = f"agentflow_{name.replace('.', '_')}_total"
            lines.append(f"# TYPE {metric} counter")
            lines.append(f"{metric} {value}")

        wf = snap["workflow_duration"]
        lines.append("# TYPE agentflow_workflow_duration_ms summary")
        lines.append(f'agentflow_workflow_duration_ms{{quantile="0.5"}} {wf["p50"]}')
        lines.append(f'agentflow_workflow_duration_ms{{quantile="0.95"}} {wf["p95"]}')
        lines.append(f"agentflow_workflow_duration_ms_count {wf['count']}")
        lines.append(f"agentflow_workflow_duration_ms_sum {wf['sum']}")

        lines.append("# TYPE agentflow_node_duration_ms summary")
        for node, stats in sorted(snap["node_durations"].items()):
            safe = node.replace('"', "")
            lines.append(f'agentflow_node_duration_ms{{node="{safe}",quantile="0.5"}} {stats["p50"]}')
            lines.append(f'agentflow_node_duration_ms{{node="{safe}",quantile="0.95"}} {stats["p95"]}')
            lines.append(f'agentflow_node_duration_ms_count{{node="{safe}"}} {stats["count"]}')

        for node, retries in sorted(snap["retries"].items()):
            safe = node.replace('"', "")
            lines.append(f'agentflow_node_retries_total{{node="{safe}"}} {retries}')

        return "\n".join(lines)

    def reset(self) -> None:
        with self._lock:
            self._counters.clear()
            self._node_durations.clear()
            self._node_status.clear()
            self._retries.clear()
            self._workflow_durations = Histogram()
