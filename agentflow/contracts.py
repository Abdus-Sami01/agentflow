from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from agentflow.graph import DAG
from agentflow.nodes.base import BaseNode
from agentflow.types import NodeOutput, SharedContext


@dataclass(frozen=True)
class Contract:
    accepts: type | tuple[type, ...] | None = None
    produces: type | tuple[type, ...] | None = None
    validate_in: Callable[[Any], str] | None = None
    validate_out: Callable[[Any], str] | None = None
    description: str = ""

    def check_input(self, value: Any) -> str:
        if self.accepts is not None and not isinstance(value, self.accepts):
            return f"expected {_type_name(self.accepts)}, got {type(value).__name__}"
        if self.validate_in:
            return self.validate_in(value) or ""
        return ""

    def check_output(self, value: Any) -> str:
        if self.produces is not None and not isinstance(value, self.produces):
            return f"expected {_type_name(self.produces)}, got {type(value).__name__}"
        if self.validate_out:
            return self.validate_out(value) or ""
        return ""


def _type_name(t: type | tuple[type, ...]) -> str:
    if isinstance(t, tuple):
        return " | ".join(x.__name__ for x in t)
    return t.__name__


class ContractViolation(Exception):
    pass


class ContractedNode(BaseNode):
    def __init__(self, inner: BaseNode, contract: Contract, strict: bool = True):
        super().__init__(inner.name)
        self._inner = inner
        self._contract = contract
        self._strict = strict

    def execute(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput:
        if inputs and self._contract.accepts is not None or self._contract.validate_in:
            payload = next(iter(inputs.values())) if len(inputs) == 1 else inputs
            problem = self._contract.check_input(payload)
            if problem:
                message = f"input contract violated on {self.name!r}: {problem}"
                if self._strict:
                    return NodeOutput(error=message, metadata={"contract": "input"})

        output = self._inner.execute(inputs, context)
        if not output.success:
            return output

        problem = self._contract.check_output(output.data)
        if problem:
            message = f"output contract violated on {self.name!r}: {problem}"
            if self._strict:
                return NodeOutput(error=message, metadata={"contract": "output"})

        return output


def apply_contracts(
    nodes: dict[str, BaseNode],
    contracts: dict[str, Contract],
    strict: bool = True,
) -> dict[str, BaseNode]:
    return {
        name: (ContractedNode(node, contracts[name], strict) if name in contracts else node)
        for name, node in nodes.items()
    }


@dataclass
class CompatibilityReport:
    mismatches: list[tuple[str, str, str]] = field(default_factory=list)
    unchecked: list[tuple[str, str]] = field(default_factory=list)

    @property
    def compatible(self) -> bool:
        return not self.mismatches

    def summary(self) -> str:
        if self.compatible and not self.unchecked:
            return "All wired edges are type-compatible."
        lines = []
        for src, tgt, reason in self.mismatches:
            lines.append(f"  INCOMPATIBLE {src} -> {tgt}: {reason}")
        if self.unchecked:
            lines.append(f"  {len(self.unchecked)} edge(s) have no declared contracts")
        return "\n".join(lines)


def check_compatibility(dag: DAG, contracts: dict[str, Contract]) -> CompatibilityReport:
    report = CompatibilityReport()

    for edge in dag.edges:
        src_contract = contracts.get(edge.source)
        tgt_contract = contracts.get(edge.target)

        if src_contract is None or tgt_contract is None:
            report.unchecked.append((edge.source, edge.target))
            continue

        produces = src_contract.produces
        accepts = tgt_contract.accepts

        if produces is None or accepts is None:
            report.unchecked.append((edge.source, edge.target))
            continue

        produced_types = produces if isinstance(produces, tuple) else (produces,)
        accepted_types = accepts if isinstance(accepts, tuple) else (accepts,)

        if dag.in_degree(edge.target) > 1:
            continue

        if not any(issubclass(p, accepted_types) for p in produced_types):
            report.mismatches.append((
                edge.source,
                edge.target,
                f"{_type_name(produces)} is not accepted by {_type_name(accepts)}",
            ))

    return report
