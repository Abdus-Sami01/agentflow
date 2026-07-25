from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class EventType(Enum):
    NODE_START = "node_start"
    NODE_COMPLETE = "node_complete"
    NODE_ERROR = "node_error"
    WORKFLOW_START = "workflow_start"
    WORKFLOW_COMPLETE = "workflow_complete"
    EDGE_TRAVERSE = "edge_traverse"
    CUSTOM = "custom"
    MESSAGE = "message"


@dataclass(frozen=True)
class Event:
    event_type: EventType
    source: str
    data: Any = None
    target: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class EventBus:
    def __init__(self):
        self._handlers: dict[EventType, list[Callable[[Event], None]]] = defaultdict(list)
        self._global_handlers: list[Callable[[Event], None]] = []
        self._history: list[Event] = []
        self._record = False

    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        self._handlers[event_type].append(handler)

    def subscribe_all(self, handler: Callable[[Event], None]) -> None:
        self._global_handlers.append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        handlers = self._handlers.get(event_type, [])
        if handler in handlers:
            handlers.remove(handler)

    def emit(self, event: Event) -> None:
        if self._record:
            self._history.append(event)

        for handler in self._handlers.get(event.event_type, []):
            try:
                handler(event)
            except Exception:
                pass

        for handler in self._global_handlers:
            try:
                handler(event)
            except Exception:
                pass

    def start_recording(self) -> None:
        self._record = True

    def stop_recording(self) -> None:
        self._record = False

    def get_history(self, event_type: EventType | None = None) -> list[Event]:
        if event_type is None:
            return list(self._history)
        return [e for e in self._history if e.event_type == event_type]

    def clear_history(self) -> None:
        self._history.clear()

    def send_message(self, source: str, target: str, data: Any) -> None:
        self.emit(Event(
            event_type=EventType.MESSAGE,
            source=source,
            target=target,
            data=data,
        ))

    @property
    def history_count(self) -> int:
        return len(self._history)


class MessageBox:
    def __init__(self, node_name: str, bus: EventBus):
        self._name = node_name
        self._bus = bus
        self._inbox: list[Event] = []
        bus.subscribe(EventType.MESSAGE, self._on_message)

    def _on_message(self, event: Event) -> None:
        if event.target == self._name:
            self._inbox.append(event)

    def send(self, target: str, data: Any) -> None:
        self._bus.send_message(self._name, target, data)

    def receive(self) -> list[Event]:
        messages = list(self._inbox)
        self._inbox.clear()
        return messages

    def peek(self) -> list[Event]:
        return list(self._inbox)

    @property
    def has_messages(self) -> bool:
        return len(self._inbox) > 0
