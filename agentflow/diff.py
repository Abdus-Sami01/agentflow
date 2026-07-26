from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agentflow.graph import DAG
from agentflow.types import NodeStatus, WorkflowResult


@dataclass
class WorkflowDiff:
    added_nodes: list[str] = field(default_factory=list)
    removed_nodes: list[str] = field(default_factory=list)
    retyped_nodes: list[tuple[str, str, str]] = field(default_factory=list)
    added_edges: list[tuple[str, str]] = field(default_factory=list)
    removed_edges: list[tuple[str, str]] = field(default_factory=list)
    terminal_changed: tuple[str | None, str | None] | None = None

    @property
    def identical(self) -> bool:
        return not (
            self.added_nodes or self.removed_nodes or self.retyped_nodes
            or self.added_edges or self.removed_edges or self.terminal_changed
        )

    def summary(self) -> str:
        if self.identical:
            return "Workflows are structurally identical."

        lines = []
        for n in self.added_nodes:
            lines.append(f"  + node {n}")
        for n in self.removed_nodes:
            lines.append(f"  - node {n}")
        for name, old, new in self.retyped_nodes:
            lines.append(f"  ~ node {name}: {old} -> {new}")
        for src, tgt in self.added_edges:
            lines.append(f"  + edge {src} -> {tgt}")
        for src, tgt in self.removed_edges:
            lines.append(f"  - edge {src} -> {tgt}")
        if self.terminal_changed:
            old, new = self.terminal_changed
            lines.append(f"  ~ terminal: {old} -> {new}")
        return "\n".join(lines)


def diff_dags(before: DAG, after: DAG) -> WorkflowDiff:
    before_names = set(before.nodes)
    after_names = set(after.nodes)

    retyped = []
    for name in sorted(before_names & after_names):
        old_type = before.nodes[name].node_type
        new_type = after.nodes[name].node_type
        if old_type != new_type:
            retyped.append((name, old_type, new_type))

    before_edges = {(e.source, e.target) for e in before.edges}
    after_edges = {(e.source, e.target) for e in after.edges}

    terminal_changed = None
    if before.terminal_node != after.terminal_node:
        terminal_changed = (before.terminal_node, after.terminal_node)

    return WorkflowDiff(
        added_nodes=sorted(after_names - before_names),
        removed_nodes=sorted(before_names - after_names),
        retyped_nodes=retyped,
        added_edges=sorted(after_edges - before_edges),
        removed_edges=sorted(before_edges - after_edges),
        terminal_changed=terminal_changed,
    )


@dataclass
class RunDiff:
    status_changes: dict[str, tuple[str, str]] = field(default_factory=dict)
    timing_changes: dict[str, tuple[float, float]] = field(default_factory=dict)
    only_in_before: list[str] = field(default_factory=list)
    only_in_after: list[str] = field(default_factory=list)
    makespan_before: float = 0.0
    makespan_after: float = 0.0

    @property
    def makespan_delta(self) -> float:
        return self.makespan_after - self.makespan_before

    def summary(self, slow_threshold_pct: float = 0.2) -> str:
        lines = [
            f"Makespan: {self.makespan_before:.1f}ms -> {self.makespan_after:.1f}ms "
            f"({self.makespan_delta:+.1f}ms)",
        ]
        for name, (old, new) in sorted(self.status_changes.items()):
            lines.append(f"  ~ {name}: {old} -> {new}")

        regressions = []
        for name, (old, new) in self.timing_changes.items():
            if old > 0 and (new - old) / old > slow_threshold_pct:
                regressions.append((name, old, new))
        for name, old, new in sorted(regressions, key=lambda r: -(r[2] - r[1])):
            lines.append(f"  slower {name}: {old:.1f}ms -> {new:.1f}ms")

        for name in self.only_in_before:
            lines.append(f"  - ran before, not after: {name}")
        for name in self.only_in_after:
            lines.append(f"  + ran after, not before: {name}")

        return "\n".join(lines)


def diff_runs(before: WorkflowResult, after: WorkflowResult) -> RunDiff:
    before_names = set(before.results)
    after_names = set(after.results)

    status_changes = {}
    timing_changes = {}
    for name in before_names & after_names:
        b, a = before.results[name], after.results[name]
        if b.status != a.status:
            status_changes[name] = (b.status.value, a.status.value)
        timing_changes[name] = (b.elapsed_ms, a.elapsed_ms)

    return RunDiff(
        status_changes=status_changes,
        timing_changes=timing_changes,
        only_in_before=sorted(before_names - after_names),
        only_in_after=sorted(after_names - before_names),
        makespan_before=before.total_ms,
        makespan_after=after.total_ms,
    )
