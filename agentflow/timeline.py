from __future__ import annotations

import html
from typing import Any

from agentflow.simulate import SimulationResult
from agentflow.types import NodeStatus, WorkflowResult


STATUS_FILL = {
    NodeStatus.COMPLETED: "#2d6a4f",
    NodeStatus.FAILED: "#9b2226",
    NodeStatus.SKIPPED: "#6c757d",
    NodeStatus.RUNNING: "#0077b6",
    NodeStatus.PENDING: "#adb5bd",
}


def _ordered(result: WorkflowResult) -> list[tuple[str, Any]]:
    return sorted(result.results.items(), key=lambda kv: -kv[1].elapsed_ms)


def timeline_text(result: WorkflowResult, width: int = 44) -> str:
    if not result.results:
        return "No nodes executed."

    longest = max((nr.elapsed_ms for nr in result.results.values()), default=0) or 1.0
    name_w = min(24, max(len(n) for n in result.results))

    lines = [
        f"Workflow: {result.workflow_id or '<unnamed>'}  "
        f"({result.status.value}, {result.total_ms:.0f}ms total)",
        "",
    ]

    for name, nr in _ordered(result):
        filled = int(round(nr.elapsed_ms / longest * width))
        marker = {
            NodeStatus.COMPLETED: "#",
            NodeStatus.FAILED: "x",
            NodeStatus.SKIPPED: ".",
        }.get(nr.status, "-")
        bar = marker * max(filled, 1 if nr.elapsed_ms else 0)
        retry = f" x{nr.attempts}" if nr.attempts > 1 else ""
        lines.append(f"  {name[:name_w]:<{name_w}} |{bar:<{width}}| {nr.elapsed_ms:>7.1f}ms{retry}")

    total_work = sum(nr.elapsed_ms for nr in result.results.values())
    if result.total_ms:
        lines.append("")
        lines.append(f"  work {total_work:.0f}ms across {len(result.results)} nodes, "
                     f"wall {result.total_ms:.0f}ms ({total_work / result.total_ms:.2f}x parallel)")
    return "\n".join(lines)


def timeline_html(result: WorkflowResult, title: str = "") -> str:
    rows = _ordered(result)
    longest = max((nr.elapsed_ms for _, nr in rows), default=0) or 1.0
    heading = html.escape(title or result.workflow_id or "workflow")

    bars = []
    for name, nr in rows:
        pct = max(0.4, nr.elapsed_ms / longest * 100)
        fill = STATUS_FILL.get(nr.status, "#adb5bd")
        label = html.escape(name)
        detail = html.escape(nr.error[:120]) if nr.error else ""
        retry = f" &times;{nr.attempts}" if nr.attempts > 1 else ""
        bars.append(
            f'<div class="row"><div class="name" title="{label}">{label}</div>'
            f'<div class="track"><div class="bar" style="width:{pct:.2f}%;background:{fill}"></div></div>'
            f'<div class="ms">{nr.elapsed_ms:.1f}ms{retry}</div></div>'
            + (f'<div class="err">{detail}</div>' if detail else "")
        )

    status_class = "ok" if result.status.value == "completed" else "bad"
    return f"""<section class="agentflow-timeline">
<style>
.agentflow-timeline {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }}
.agentflow-timeline h3 {{ margin: 0 0 4px; font-size: 15px; }}
.agentflow-timeline .meta {{ color: #6c757d; margin-bottom: 12px; }}
.agentflow-timeline .meta .ok {{ color: #2d6a4f; }}
.agentflow-timeline .meta .bad {{ color: #9b2226; }}
.agentflow-timeline .row {{ display: flex; align-items: center; gap: 8px; margin: 3px 0; }}
.agentflow-timeline .name {{ flex: 0 0 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
.agentflow-timeline .track {{ flex: 1 1 auto; background: rgba(127,127,127,.15); border-radius: 3px; height: 16px; }}
.agentflow-timeline .bar {{ height: 16px; border-radius: 3px; }}
.agentflow-timeline .ms {{ flex: 0 0 90px; text-align: right; color: #6c757d; }}
.agentflow-timeline .err {{ margin: 0 0 6px 168px; color: #9b2226; font-size: 12px; }}
@media (prefers-color-scheme: dark) {{
  .agentflow-timeline .meta, .agentflow-timeline .ms {{ color: #9aa0a6; }}
}}
</style>
<h3>{heading}</h3>
<div class="meta">
  <span class="{status_class}">{html.escape(result.status.value)}</span>
  &middot; {len(rows)} nodes &middot; {result.completed_count} ok
  &middot; {result.failed_count} failed &middot; {result.skipped_count} skipped
  &middot; {result.total_ms:.0f}ms wall
</div>
{''.join(bars)}
</section>"""


def simulated_timeline_text(sim: SimulationResult, width: int = 44) -> str:
    if not sim.finish_times:
        return "Nothing simulated."

    span = sim.makespan_ms or 1.0
    name_w = min(24, max(len(n) for n in sim.finish_times))
    critical = set(sim.critical_path)

    lines = [f"Simulated makespan {sim.makespan_ms:.1f}ms "
             f"(peak concurrency {sim.max_concurrency}, {sim.speedup:.2f}x speedup)", ""]

    for name in sorted(sim.finish_times, key=lambda n: sim.start_times.get(n, 0)):
        start = sim.start_times.get(name, 0.0)
        end = sim.finish_times[name]
        lead = int(round(start / span * width))
        length = max(1, int(round((end - start) / span * width)))
        marker = "#" if name in critical else "-"
        bar = " " * lead + marker * length
        flag = " *" if name in critical else ""
        lines.append(f"  {name[:name_w]:<{name_w}} |{bar:<{width}}| {start:>7.1f} - {end:<7.1f}{flag}")

    lines.append("")
    lines.append("  * = critical path")
    return "\n".join(lines)
