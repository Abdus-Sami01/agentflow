from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from typing import Any

from agentflow.types import NodeResult, NodeStatus, SharedContext, WorkflowHooks, WorkflowResult


@dataclass
class NodeProfile:
    name: str
    samples: list[float] = field(default_factory=list)
    successes: int = 0
    failures: int = 0
    retries: int = 0

    def observe(self, result: NodeResult) -> None:
        if result.status == NodeStatus.COMPLETED:
            self.samples.append(result.elapsed_ms)
            self.successes += 1
        elif result.status == NodeStatus.FAILED:
            self.failures += 1
        if result.attempts > 1:
            self.retries += result.attempts - 1

    @property
    def runs(self) -> int:
        return self.successes + self.failures

    @property
    def mean_ms(self) -> float:
        return sum(self.samples) / len(self.samples) if self.samples else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.samples) if self.samples else 0.0

    def percentile(self, p: float) -> float:
        if not self.samples:
            return 0.0
        ordered = sorted(self.samples)
        idx = min(len(ordered) - 1, max(0, int(round(p * (len(ordered) - 1)))))
        return ordered[idx]

    @property
    def failure_rate(self) -> float:
        return self.failures / self.runs if self.runs else 0.0


class Profiler:
    def __init__(self):
        self._profiles: dict[str, NodeProfile] = {}
        self._lock = threading.Lock()
        self._workflow_runs = 0

    def observe(self, node_name: str, result: NodeResult, context: SharedContext | None = None) -> None:
        with self._lock:
            profile = self._profiles.get(node_name)
            if profile is None:
                profile = NodeProfile(name=node_name)
                self._profiles[node_name] = profile
            profile.observe(result)

    def observe_workflow(self, result: WorkflowResult) -> None:
        with self._lock:
            self._workflow_runs += 1
        for name, nr in result.results.items():
            if name not in self._profiles or nr.status == NodeStatus.FAILED:
                self.observe(name, nr)

    def as_hooks(self) -> WorkflowHooks:
        return WorkflowHooks(
            on_node_complete=lambda name, result, ctx: self.observe(name, result, ctx),
        )

    def durations(self, percentile: float = 0.5) -> dict[str, float]:
        with self._lock:
            return {n: p.percentile(percentile) for n, p in self._profiles.items() if p.samples}

    def suggest_timeouts(self, safety_factor: float = 3.0, minimum_ms: float = 50.0) -> dict[str, float]:
        with self._lock:
            profiles = list(self._profiles.values())
        return {
            p.name: max(minimum_ms, p.percentile(0.95) * safety_factor) / 1000.0
            for p in profiles if p.samples
        }

    def suggest_retries(self, threshold: float = 0.1, max_retries: int = 3) -> dict[str, int]:
        with self._lock:
            profiles = list(self._profiles.values())
        out = {}
        for p in profiles:
            if p.runs < 2 or p.failure_rate <= threshold:
                continue
            out[p.name] = min(max_retries, 1 + int(p.failure_rate * max_retries))
        return out

    def unstable_nodes(self, cv_threshold: float = 0.5) -> list[str]:
        with self._lock:
            profiles = list(self._profiles.values())
        unstable = []
        for p in profiles:
            if len(p.samples) < 3 or p.mean_ms == 0:
                continue
            mean = p.mean_ms
            variance = sum((s - mean) ** 2 for s in p.samples) / len(p.samples)
            if (variance ** 0.5) / mean > cv_threshold:
                unstable.append(p.name)
        return unstable

    def report(self) -> str:
        with self._lock:
            profiles = sorted(self._profiles.values(), key=lambda p: -p.mean_ms)
        if not profiles:
            return "No profiling data collected."

        lines = [f"Profiled {len(profiles)} nodes over {self._workflow_runs or 'n'} workflow run(s)", ""]
        lines.append(f"{'node':<24}{'runs':>6}{'mean':>10}{'p95':>10}{'fail%':>8}")
        for p in profiles:
            lines.append(
                f"{p.name[:23]:<24}{p.runs:>6}{p.mean_ms:>9.1f}m{p.percentile(0.95):>9.1f}m{p.failure_rate * 100:>7.0f}%"
            )
        unstable = self.unstable_nodes()
        if unstable:
            lines.append("")
            lines.append(f"High-variance nodes: {', '.join(unstable)}")
        return "\n".join(lines)

    def to_json(self, indent: int = 2) -> str:
        with self._lock:
            payload = {
                n: {
                    "runs": p.runs,
                    "mean_ms": round(p.mean_ms, 2),
                    "p95_ms": round(p.percentile(0.95), 2),
                    "max_ms": round(p.max_ms, 2),
                    "failure_rate": round(p.failure_rate, 3),
                    "retries": p.retries,
                }
                for n, p in self._profiles.items()
            }
        return json.dumps(payload, indent=indent)

    def reset(self) -> None:
        with self._lock:
            self._profiles.clear()
            self._workflow_runs = 0

    @property
    def profiles(self) -> dict[str, NodeProfile]:
        with self._lock:
            return dict(self._profiles)
