from agentflow.nodes.base import BaseNode, NodeRegistry
from agentflow.nodes.llm import LLMNode
from agentflow.nodes.tool import ToolNode
from agentflow.nodes.conditional import ConditionalNode
from agentflow.nodes.aggregator import AggregatorNode
from agentflow.nodes.transform import TransformNode
from agentflow.nodes.supervisor import SupervisorNode
from agentflow.nodes.subworkflow import SubworkflowNode
from agentflow.nodes.gate import GateNode
from agentflow.nodes.loop import LoopNode

NodeRegistry.register("llm", LLMNode)
NodeRegistry.register("tool", ToolNode)
NodeRegistry.register("conditional", ConditionalNode)
NodeRegistry.register("aggregator", AggregatorNode)
NodeRegistry.register("transform", TransformNode)
NodeRegistry.register("supervisor", SupervisorNode)
NodeRegistry.register("subworkflow", SubworkflowNode)
NodeRegistry.register("gate", GateNode)
NodeRegistry.register("loop", LoopNode)

__all__ = [
    "BaseNode",
    "NodeRegistry",
    "LLMNode",
    "ToolNode",
    "ConditionalNode",
    "AggregatorNode",
    "TransformNode",
    "SupervisorNode",
    "SubworkflowNode",
    "GateNode",
    "LoopNode",
]
