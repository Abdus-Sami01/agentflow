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
from agentflow.nodes.foreach import ForEachNode
from agentflow.nodes.router import RouterNode
from agentflow.nodes.http import HTTPNode, SSRFBlocked, check_url_safety
from agentflow.nodes.memory import MemoryAppendNode, MemoryReadNode, MemoryWriteNode
from agentflow.nodes.agent import AgentNode

NodeRegistry.register("agent", AgentNode)
NodeRegistry.register("foreach", ForEachNode)
NodeRegistry.register("router", RouterNode)
NodeRegistry.register("http", HTTPNode)
NodeRegistry.register("memory_read", MemoryReadNode)
NodeRegistry.register("memory_write", MemoryWriteNode)
NodeRegistry.register("memory_append", MemoryAppendNode)
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
    "ForEachNode",
    "RouterNode",
    "HTTPNode",
    "SSRFBlocked",
    "check_url_safety",
    "MemoryReadNode",
    "MemoryWriteNode",
    "MemoryAppendNode",
    "AgentNode",
]
