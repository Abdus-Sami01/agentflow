from __future__ import annotations

import hashlib
import json
import time
from collections import OrderedDict
from typing import Any

from agentflow.types import NodeOutput


def compute_cache_key(node_name: str, inputs: dict[str, Any]) -> str:
    try:
        payload = json.dumps({"node": node_name, "inputs": inputs}, sort_keys=True, default=str)
    except (TypeError, ValueError):
        payload = f"{node_name}:{sorted(str(inputs).encode())}"
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


class NodeCache:
    def __init__(self, max_size: int = 256, ttl_s: float = 0):
        self._store: OrderedDict[str, tuple[NodeOutput, float]] = OrderedDict()
        self._max = max_size
        self._ttl = ttl_s
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> NodeOutput | None:
        entry = self._store.get(key)
        if entry is None:
            self._misses += 1
            return None

        output, stored_at = entry
        if self._ttl > 0 and (time.time() - stored_at) > self._ttl:
            del self._store[key]
            self._misses += 1
            return None

        self._store.move_to_end(key)
        self._hits += 1
        return output

    def put(self, key: str, output: NodeOutput) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = (output, time.time())

        while len(self._store) > self._max:
            self._store.popitem(last=False)

    def invalidate(self, key: str) -> bool:
        if key in self._store:
            del self._store[key]
            return True
        return False

    def clear(self) -> None:
        self._store.clear()

    @property
    def stats(self) -> dict[str, Any]:
        total = self._hits + self._misses
        return {
            "size": len(self._store),
            "max_size": self._max,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / total if total else 0.0,
        }

    @property
    def size(self) -> int:
        return len(self._store)
