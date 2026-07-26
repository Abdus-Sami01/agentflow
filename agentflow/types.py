from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Protocol


class NodeStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WAITING = "waiting"


class EdgeType(Enum):
    DATA = "data"
    CONTROL = "control"
    CONDITIONAL = "conditional"


class WorkflowStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class NodeCallable(Protocol):
    def __call__(self, inputs: dict[str, Any], context: SharedContext) -> NodeOutput: ...


@dataclass(frozen=True)
class NodeOutput:
    data: Any = None
    error: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return not self.error


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    edge_type: EdgeType = EdgeType.DATA
    condition: Callable[[NodeOutput], bool] | None = None
    key: str = ""


@dataclass
class NodeSpec:
    name: str
    node_type: str
    config: dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    timeout_s: float = 0
    priority: int = 0


@dataclass
class NodeResult:
    node_name: str
    status: NodeStatus
    output: NodeOutput | None = None
    attempts: int = 1
    elapsed_ms: float = 0
    error: str = ""


@dataclass
class SharedContext:
    workflow_id: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    results: dict[str, NodeResult] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def get_result(self, node_name: str) -> NodeResult | None:
        return self.results.get(node_name)

    def get_output(self, node_name: str) -> Any:
        result = self.results.get(node_name)
        if result and result.output:
            return result.output.data
        return None


@dataclass
class WorkflowResult:
    workflow_id: str
    status: WorkflowStatus
    results: dict[str, NodeResult] = field(default_factory=dict)
    final_output: Any = None
    total_ms: float = 0
    error: str = ""

    @property
    def completed_count(self) -> int:
        return sum(1 for r in self.results.values() if r.status == NodeStatus.COMPLETED)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results.values() if r.status == NodeStatus.FAILED)

    @property
    def skipped_count(self) -> int:
        return sum(1 for r in self.results.values() if r.status == NodeStatus.SKIPPED)


@dataclass
class WorkflowHooks:
    on_node_start: Callable[[str, SharedContext], None] | None = None
    on_node_complete: Callable[[str, NodeResult, SharedContext], None] | None = None
    on_node_error: Callable[[str, str, SharedContext], None] | None = None
    on_workflow_start: Callable[[SharedContext], None] | None = None
    on_workflow_complete: Callable[[WorkflowResult], None] | None = None
    on_edge_traverse: Callable[[Edge, SharedContext], None] | None = None


@dataclass
class WorkflowConfig:
    max_parallel: int = 4
    fail_fast: bool = False
    default_timeout_s: float = 60.0
    default_retries: int = 0
    workflow_timeout_s: float = 0
    retry_strategy: Any = None
    respect_priority: bool = True
