from __future__ import annotations

import time
from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class NodeFailure(Exception):
    pass


class Middleware:
    def before(self, node_name: str, inputs: dict[str, Any], context: SharedContext) -> dict[str, Any]:
        return inputs

    def after(self, node_name: str, output: NodeOutput, context: SharedContext) -> NodeOutput:
        return output

    def on_error(self, node_name: str, error: Exception, context: SharedContext) -> NodeOutput | None:
        return None


class LoggingMiddleware(Middleware):
    def __init__(self, sink: Callable[[str], None] = print):
        self._sink = sink

    def before(self, node_name, inputs, context):
        self._sink(f"[start] {node_name} inputs={list(inputs.keys())}")
        return inputs

    def after(self, node_name, output, context):
        status = "ok" if output.success else f"error: {output.error}"
        self._sink(f"[done ] {node_name} {status}")
        return output

    def on_error(self, node_name, error, context):
        self._sink(f"[error] {node_name} {type(error).__name__}: {error}")
        return None


class TimingMiddleware(Middleware):
    def __init__(self):
        self.timings: dict[str, float] = {}
        self._starts: dict[str, float] = {}

    def before(self, node_name, inputs, context):
        self._starts[node_name] = time.perf_counter()
        return inputs

    def after(self, node_name, output, context):
        start = self._starts.pop(node_name, None)
        if start is not None:
            self.timings[node_name] = (time.perf_counter() - start) * 1000
        return output


class RedactionMiddleware(Middleware):
    def __init__(self, sensitive_keys: set[str], placeholder: str = "[REDACTED]"):
        self._keys = {k.lower() for k in sensitive_keys}
        self._placeholder = placeholder

    def after(self, node_name, output, context):
        if not isinstance(output.data, dict):
            return output
        cleaned = {
            k: (self._placeholder if k.lower() in self._keys else v)
            for k, v in output.data.items()
        }
        return NodeOutput(data=cleaned, error=output.error, metadata=output.metadata)


class ValidationMiddleware(Middleware):
    def __init__(self, validator: Callable[[str, NodeOutput], str], ):
        self._validator = validator

    def after(self, node_name, output, context):
        if not output.success:
            return output
        problem = self._validator(node_name, output)
        if problem:
            return NodeOutput(error=f"output validation failed: {problem}", metadata=output.metadata)
        return output


class MiddlewareChain:
    def __init__(self, middlewares: list[Middleware] | None = None):
        self._middlewares = middlewares or []

    def add(self, middleware: Middleware) -> MiddlewareChain:
        self._middlewares.append(middleware)
        return self

    def wrap(self, node: BaseNode) -> BaseNode:
        return _WrappedNode(node, self._middlewares)

    def wrap_all(self, nodes: dict[str, BaseNode]) -> dict[str, BaseNode]:
        return {name: self.wrap(node) for name, node in nodes.items()}

    @property
    def count(self) -> int:
        return len(self._middlewares)


class _WrappedNode(BaseNode):
    def __init__(self, inner: BaseNode, middlewares: list[Middleware]):
        super().__init__(inner.name)
        self._inner = inner
        self._middlewares = middlewares

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        current = inputs
        for mw in self._middlewares:
            try:
                current = mw.before(self.name, current, context)
            except Exception as e:
                return NodeOutput(error=f"middleware before() failed: {e}")

        try:
            output = self._inner.execute(current, context)
        except Exception as e:
            recovered = self._try_recover(e, context)
            if recovered is None:
                return NodeOutput(error=str(e))
            output = recovered

        if not output.success:
            recovered = self._try_recover(NodeFailure(output.error), context)
            if recovered is not None:
                output = recovered

        for mw in reversed(self._middlewares):
            try:
                output = mw.after(self.name, output, context)
            except Exception as e:
                return NodeOutput(error=f"middleware after() failed: {e}")

        return output

    def _try_recover(self, error: Exception, context: SharedContext) -> NodeOutput | None:
        for mw in reversed(self._middlewares):
            try:
                recovered = mw.on_error(self.name, error, context)
            except Exception:
                continue
            if recovered is not None:
                return recovered
        return None
