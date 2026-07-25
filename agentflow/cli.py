from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from agentflow.analysis import compute_parallelism, deadlock_check
from agentflow.spec import FunctionRegistry, build_from_spec, load_spec_json, load_spec_yaml, validate_spec
from agentflow.trace import workflow_to_json, workflow_to_text
from agentflow.visualize import to_ascii, to_dot, to_mermaid, to_summary


def _load_spec(path: str) -> dict[str, Any]:
    if path.endswith((".yaml", ".yml")):
        return load_spec_yaml(path)
    return load_spec_json(path)


def cmd_validate(args) -> int:
    spec = _load_spec(args.spec)
    errors = validate_spec(spec)

    if errors:
        print("INVALID:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print("Spec is structurally valid.")
    print(f"  nodes: {len(spec.get('nodes', []))}")
    print(f"  edges: {len(spec.get('edges', []))}")
    return 0


def cmd_visualize(args) -> int:
    spec = _load_spec(args.spec)
    errors = validate_spec(spec)
    if errors:
        print("INVALID SPEC:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    registry = FunctionRegistry()
    for node in spec.get("nodes", []):
        for key in ("fn", "llm_fn", "condition", "merge_fn", "evaluate_fn", "check_fn", "body_fn", "condition_fn"):
            fn_name = node.get(key)
            if fn_name and not registry.has(fn_name):
                registry.register(fn_name, lambda *a, **k: None)
    for edge in spec.get("edges", []):
        cond = edge.get("condition")
        if cond and not registry.has(cond):
            registry.register(cond, lambda *a, **k: True)

    wb = build_from_spec(spec, registry)
    dag = wb.dag

    if args.format == "mermaid":
        print(to_mermaid(dag, direction=args.direction))
    elif args.format == "dot":
        print(to_dot(dag))
    elif args.format == "ascii":
        print(to_ascii(dag))
    else:
        print(to_summary(dag))

    return 0


def cmd_inspect(args) -> int:
    spec = _load_spec(args.spec)
    errors = validate_spec(spec)
    if errors:
        print("INVALID SPEC:", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    registry = FunctionRegistry()
    for node in spec.get("nodes", []):
        for key in ("fn", "llm_fn", "condition", "merge_fn", "evaluate_fn", "check_fn", "body_fn", "condition_fn"):
            fn_name = node.get(key)
            if fn_name and not registry.has(fn_name):
                registry.register(fn_name, lambda *a, **k: None)
    for edge in spec.get("edges", []):
        cond = edge.get("condition")
        if cond and not registry.has(cond):
            registry.register(cond, lambda *a, **k: True)

    wb = build_from_spec(spec, registry)
    dag = wb.dag

    print(to_summary(dag))
    print()

    par = compute_parallelism(dag)
    print(f"Parallelism: {par['levels']} levels, max width {par['max_width']}, avg {par['avg_width']:.1f}")
    print(f"Width per level: {par['width_per_level']}")
    print()

    issues = deadlock_check(dag)
    if issues:
        print("Issues:")
        for issue in issues:
            print(f"  - {issue}")
        return 1

    print("No deadlock or cycle issues.")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentflow", description="Multi-agent DAG workflow orchestrator")
    sub = parser.add_subparsers(dest="command", required=True)

    p_val = sub.add_parser("validate", help="Check a workflow spec for structural errors")
    p_val.add_argument("spec", help="Path to .json/.yaml workflow spec")
    p_val.set_defaults(func=cmd_validate)

    p_viz = sub.add_parser("visualize", help="Render a workflow spec as a diagram")
    p_viz.add_argument("spec", help="Path to .json/.yaml workflow spec")
    p_viz.add_argument("--format", choices=["mermaid", "dot", "ascii", "summary"], default="mermaid")
    p_viz.add_argument("--direction", default="TD", help="Mermaid direction: TD, LR, BT, RL")
    p_viz.set_defaults(func=cmd_visualize)

    p_ins = sub.add_parser("inspect", help="Analyze a workflow's structure and parallelism")
    p_ins.add_argument("spec", help="Path to .json/.yaml workflow spec")
    p_ins.set_defaults(func=cmd_inspect)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FileNotFoundError as e:
        print(f"file not found: {e.filename}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
