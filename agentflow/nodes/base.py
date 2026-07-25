from __future__ import annotations

from typing import Any, Callable

from agentflow.types import NodeOutput, SharedContext


class BaseNode:
    def __init__(self, name: str, **config):
        self.name = name
        self.config = config

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


class NodeRegistry:
    _registry: dict[str, type[BaseNode]] = {}

    @classmethod
    def register(cls, node_type: str, node_class: type[BaseNode]) -> None:
        cls._registry[node_type] = node_class

    @classmethod
    def create(cls, node_type: str, name: str, **config) -> BaseNode:
        node_class = cls._registry.get(node_type)
        if node_class is None:
            raise ValueError(f"unknown node type: {node_type!r}. Available: {set(cls._registry.keys())}")
        return node_class(name=name, **config)

    @classmethod
    def available_types(cls) -> set[str]:
        return set(cls._registry.keys())

    @classmethod
    def has_type(cls, node_type: str) -> bool:
        return node_type in cls._registry
