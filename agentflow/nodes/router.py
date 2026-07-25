from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class RouterNode(BaseNode):
    def __init__(
        self,
        name: str,
        routes: dict[str, Callable[[Any, SharedContext], bool]],
        default_route: str = "",
        match_all: bool = False,
        **config,
    ):
        super().__init__(name, **config)
        self._routes = routes
        self._default = default_route
        self._match_all = match_all

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        payload = next(iter(inputs.values())) if len(inputs) == 1 else inputs

        matched: list[str] = []
        for route_name, predicate in self._routes.items():
            try:
                if predicate(payload, context):
                    matched.append(route_name)
                    if not self._match_all:
                        break
            except Exception as e:
                return NodeOutput(error=f"route {route_name!r} predicate failed: {e}")

        if not matched:
            if not self._default:
                return NodeOutput(error="no route matched and no default_route configured")
            matched = [self._default]

        return NodeOutput(
            data=payload,
            metadata={
                "routes": matched,
                "selected": matched[0],
                "used_default": matched == [self._default] and self._default not in self._routes,
            },
        )

    def routed_to(self, route_name: str) -> Callable[[NodeOutput], bool]:
        def check(output: NodeOutput) -> bool:
            return route_name in output.metadata.get("routes", [])
        return check

    @property
    def route_names(self) -> set[str]:
        return set(self._routes.keys())
