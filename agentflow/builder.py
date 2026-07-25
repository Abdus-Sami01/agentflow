from __future__ import annotations

from typing import Any, Callable

from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.nodes.llm import LLMNode
from agentflow.nodes.tool import ToolNode
from agentflow.nodes.conditional import ConditionalNode
from agentflow.nodes.aggregator import AggregatorNode
from agentflow.nodes.transform import TransformNode
from agentflow.nodes.supervisor import SupervisorNode
from agentflow.nodes.gate import GateNode
from agentflow.nodes.loop import LoopNode
from agentflow.execution.executor import WorkflowExecutor
from agentflow.types import (
    Edge,
    EdgeType,
    NodeSpec,
    SharedContext,
    WorkflowConfig,
    WorkflowHooks,
    WorkflowResult,
)


class WorkflowBuilder:
    def __init__(self, workflow_id: str = ""):
        self._dag = DAG()
        self._nodes: dict[str, BaseNode] = {}
        self._workflow_id = workflow_id
        self._config = WorkflowConfig()
        self._hooks = WorkflowHooks()

    def llm(
        self,
        name: str,
        llm_fn: Callable[[str], str],
        prompt_template: str = "",
        retry: int = 0,
        timeout: float = 0,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="llm", retry_count=retry, timeout_s=timeout))
        self._nodes[name] = LLMNode(name, llm_fn, prompt_template)
        return self

    def tool(
        self,
        name: str,
        fn: Callable[..., Any],
        arg_map: dict[str, str] | None = None,
        retry: int = 0,
        timeout: float = 0,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="tool", retry_count=retry, timeout_s=timeout))
        self._nodes[name] = ToolNode(name, fn, arg_map)
        return self

    def conditional(
        self,
        name: str,
        condition: Callable[[dict[str, Any], SharedContext], str],
        branches: dict[str, str] | None = None,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="conditional"))
        self._nodes[name] = ConditionalNode(name, condition, branches)
        return self

    def aggregator(
        self,
        name: str,
        strategy: str = "merge",
        merge_fn: Callable[[dict[str, Any]], Any] | None = None,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="aggregator"))
        self._nodes[name] = AggregatorNode(name, strategy, merge_fn)
        return self

    def transform(self, name: str, fn: Callable[[dict[str, Any]], Any]) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="transform"))
        self._nodes[name] = TransformNode(name, fn)
        return self

    def supervisor(
        self,
        name: str,
        evaluate_fn: Callable[[dict[str, Any], SharedContext], dict[str, Any]],
        max_rounds: int = 3,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="supervisor"))
        self._nodes[name] = SupervisorNode(name, evaluate_fn, max_rounds)
        return self

    def gate(
        self,
        name: str,
        check_fn: Callable[[dict[str, Any], SharedContext], bool],
        fail_message: str = "gate check failed",
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="gate"))
        self._nodes[name] = GateNode(name, check_fn, fail_message)
        return self

    def loop_node(
        self,
        name: str,
        body_fn: Callable[[Any, int, SharedContext], Any],
        condition_fn: Callable[[Any, int, SharedContext], bool],
        max_iterations: int = 10,
    ) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type="loop"))
        self._nodes[name] = LoopNode(name, body_fn, condition_fn, max_iterations)
        return self

    def node(self, name: str, node: BaseNode, node_type: str = "custom", **spec_kwargs) -> WorkflowBuilder:
        self._dag.add_node(NodeSpec(name=name, node_type=node_type, **spec_kwargs))
        self._nodes[name] = node
        return self

    def edge(self, source: str, target: str, key: str = "") -> WorkflowBuilder:
        self._dag.add_edge(Edge(source=source, target=target, key=key))
        return self

    def conditional_edge(
        self,
        source: str,
        target: str,
        condition: Callable,
        key: str = "",
    ) -> WorkflowBuilder:
        self._dag.add_edge(Edge(
            source=source, target=target,
            edge_type=EdgeType.CONDITIONAL, condition=condition, key=key,
        ))
        return self

    def terminal(self, node_name: str) -> WorkflowBuilder:
        self._dag.set_terminal(node_name)
        return self

    def config(
        self,
        max_parallel: int = 4,
        fail_fast: bool = False,
        default_timeout: float = 60.0,
        default_retries: int = 0,
    ) -> WorkflowBuilder:
        self._config = WorkflowConfig(
            max_parallel=max_parallel,
            fail_fast=fail_fast,
            default_timeout_s=default_timeout,
            default_retries=default_retries,
        )
        return self

    def with_hooks(self, hooks: WorkflowHooks) -> WorkflowBuilder:
        self._hooks = hooks
        return self

    def build(self) -> WorkflowExecutor:
        errors = self._dag.validate()
        if errors:
            raise ValueError(f"invalid workflow: {'; '.join(errors)}")
        return WorkflowExecutor(self._dag, self._nodes, self._config, self._hooks)

    def run(self, initial_data: dict[str, Any] | None = None) -> WorkflowResult:
        executor = self.build()
        context = SharedContext(workflow_id=self._workflow_id, data=initial_data or {})
        return executor.run(context)

    @property
    def dag(self) -> DAG:
        return self._dag
