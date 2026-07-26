from __future__ import annotations

import hashlib
import inspect
import json
from typing import Any

from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, NodeStatus, SharedContext, WorkflowResult


def fingerprint_value(value: Any) -> str:
    try:
        payload = json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        payload = repr(value)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def fingerprint_callable(fn: Any) -> str:
    try:
        source = inspect.getsource(fn)
    except (OSError, TypeError):
        code = getattr(fn, "__code__", None)
        source = f"{getattr(fn, '__qualname__', repr(fn))}:{getattr(code, 'co_code', b'').hex()}"

    parts = [source]

    closure = getattr(fn, "__closure__", None)
    if closure:
        names = getattr(getattr(fn, "__code__", None), "co_freevars", ())
        for name, cell in zip(names, closure):
            try:
                value = cell.cell_contents
            except ValueError:
                parts.append(f"{name}=<empty>")
                continue
            parts.append(f"{name}={_stable_repr(value)}")

    defaults = getattr(fn, "__defaults__", None)
    if defaults:
        parts.append("defaults=" + ",".join(_stable_repr(d) for d in defaults))

    kwdefaults = getattr(fn, "__kwdefaults__", None)
    if kwdefaults:
        parts.append("kwdefaults=" + ",".join(
            f"{k}={_stable_repr(kwdefaults[k])}" for k in sorted(kwdefaults)
        ))

    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


def _stable_repr(value: Any, _depth: int = 0) -> str:
    if _depth > 3:
        return "..."
    if callable(value):
        code = getattr(value, "__code__", None)
        if code is not None:
            return f"fn:{code.co_name}:{code.co_firstlineno}"
        return f"fn:{type(value).__name__}"
    if isinstance(value, (str, int, float, bool, type(None))):
        return repr(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_stable_repr(v, _depth + 1) for v in value[:20]) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(
            f"{k}:{_stable_repr(value[k], _depth + 1)}" for k in sorted(value, key=str)[:20]
        ) + "}"
    return f"<{type(value).__name__}>"


def node_version(node: BaseNode) -> str:
    parts = [type(node).__name__]
    for attr in sorted(vars(node)):
        if attr in ("name", "config"):
            continue
        value = getattr(node, attr)
        if callable(value):
            parts.append(f"{attr}={fingerprint_callable(value)}")
        elif isinstance(value, (str, int, float, bool, type(None))):
            parts.append(f"{attr}={value}")
        elif isinstance(value, dict):
            inner = []
            for k in sorted(value, key=str):
                v = value[k]
                inner.append(f"{k}:{fingerprint_callable(v) if callable(v) else v}")
            parts.append(f"{attr}={{{','.join(inner)}}}")
        elif isinstance(value, (list, tuple, set)):
            parts.append(f"{attr}=[{len(value)}]")
    return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]


class BuildCache:
    def __init__(self):
        self._entries: dict[str, dict[str, Any]] = {}

    def key(self, node_name: str, version: str, input_fp: str) -> str:
        return hashlib.sha256(f"{node_name}|{version}|{input_fp}".encode()).hexdigest()[:24]

    def get(self, key: str) -> Any:
        entry = self._entries.get(key)
        return entry["output"] if entry else None

    def has(self, key: str) -> bool:
        return key in self._entries

    def put(self, key: str, node_name: str, output: Any) -> None:
        self._entries[key] = {"node": node_name, "output": output}

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self._entries, indent=indent, default=str)

    @classmethod
    def from_json(cls, raw: str) -> BuildCache:
        cache = cls()
        cache._entries = json.loads(raw)
        return cache

    def save(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.to_json())

    @classmethod
    def load(cls, path: str) -> BuildCache:
        with open(path) as f:
            return cls.from_json(f.read())

    def clear(self) -> None:
        self._entries.clear()

    @property
    def size(self) -> int:
        return len(self._entries)


class IncrementalPlanner:
    def __init__(self, cache: BuildCache | None = None):
        self.cache = cache or BuildCache()
        self._fingerprints: dict[str, str] = {}
        self._keys: dict[str, str] = {}

    def plan(
        self,
        dag: DAG,
        nodes: dict[str, BaseNode],
        initial_inputs: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._fingerprints.clear()
        self._keys.clear()

        reusable: set[str] = set()
        stale: set[str] = set()
        base_fp = fingerprint_value(initial_inputs or {})

        for name in dag.topological_sort():
            node = nodes.get(name)
            version = node_version(node) if node else "missing"

            preds = dag.predecessors(name)
            if preds:
                upstream = "|".join(
                    f"{e.key or e.source}={self._keys.get(e.source, '?')}"
                    for e in sorted(preds, key=lambda e: e.source)
                )
            else:
                upstream = base_fp

            key = self.cache.key(name, version, fingerprint_value(upstream))
            self._keys[name] = key
            self._fingerprints[name] = key

            if self.cache.has(key):
                reusable.add(name)
            else:
                stale.add(name)

        return {
            "reusable": sorted(reusable),
            "stale": sorted(stale),
            "total": len(dag.nodes),
            "reuse_ratio": len(reusable) / len(dag.nodes) if dag.nodes else 0.0,
        }

    def cached_outputs(self, reusable: set[str]) -> dict[str, Any]:
        return {
            name: self.cache.get(self._keys[name])
            for name in reusable
            if name in self._keys and self.cache.has(self._keys[name])
        }

    def record(self, result: WorkflowResult) -> int:
        stored = 0
        for name, nr in result.results.items():
            if nr.status != NodeStatus.COMPLETED or nr.output is None:
                continue
            key = self._keys.get(name)
            if key and not self.cache.has(key):
                self.cache.put(key, name, nr.output.data)
                stored += 1
        return stored

    def key_for(self, node_name: str) -> str:
        return self._keys.get(node_name, "")


class MemoizedNode(BaseNode):
    def __init__(self, inner: BaseNode, cached_value: Any):
        super().__init__(inner.name)
        self._inner = inner
        self._value = cached_value

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        return NodeOutput(data=self._value, metadata={"incremental": "reused"})


def apply_incremental(
    nodes: dict[str, BaseNode],
    cached: dict[str, Any],
) -> dict[str, BaseNode]:
    return {
        name: (MemoizedNode(node, cached[name]) if name in cached else node)
        for name, node in nodes.items()
    }
