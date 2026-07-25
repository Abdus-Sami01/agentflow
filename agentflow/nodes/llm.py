from __future__ import annotations

from typing import Any, Callable

from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


class LLMNode(BaseNode):
    def __init__(self, name: str, llm_fn: Callable[[str], str], prompt_template: str = "", **config):
        super().__init__(name, **config)
        self._llm_fn = llm_fn
        self._template = prompt_template

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        prompt = self._build_prompt(inputs, context)
        try:
            response = self._llm_fn(prompt)
            return NodeOutput(data=response, metadata={"prompt_len": len(prompt), "response_len": len(response)})
        except Exception as e:
            return NodeOutput(error=str(e))

    def _build_prompt(self, inputs: dict[str, Any], context: SharedContext) -> str:
        if self._template:
            prompt = self._template
            for key, value in inputs.items():
                prompt = prompt.replace(f"{{{{{key}}}}}", str(value))
            for key, value in context.data.items():
                prompt = prompt.replace(f"{{{{ctx.{key}}}}}", str(value))
            return prompt
        parts = []
        for key, value in inputs.items():
            parts.append(f"{key}: {value}")
        return "\n".join(parts) if parts else "No input provided."
