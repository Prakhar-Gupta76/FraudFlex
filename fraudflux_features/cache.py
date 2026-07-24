"""Small bounded, thread-safe cache for point-in-time history snapshots."""

from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from typing import Callable, Optional, Protocol

from fraudflux_validation import TransactionEvent
from fraudflux_worker import CustomerHistory


class HistoryLoader(Protocol):
    def load(self, event: TransactionEvent) -> CustomerHistory: ...


@dataclass(frozen=True)
class _CacheEntry:
    customer_id: str
    expires_at: float
    history: CustomerHistory


class CachedHistoryProvider:
    """Cache exact event snapshots without reusing stale customer history."""

    def __init__(
        self,
        provider: HistoryLoader,
        *,
        max_entries: int = 512,
        ttl_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.provider = provider
        self.max_entries = max_entries
        self.ttl_seconds = ttl_seconds
        self.clock = clock
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = RLock()

    def load(self, event: TransactionEvent) -> CustomerHistory:
        key = event.event_id
        now = self.clock()
        with self._lock:
            entry = self._entries.get(key)
            if entry is not None and entry.expires_at > now:
                self._entries.move_to_end(key)
                return entry.history
            if entry is not None:
                del self._entries[key]

        history = self.provider.load(event)
        with self._lock:
            self._entries[key] = _CacheEntry(
                customer_id=event.transaction.customer_id,
                expires_at=now + self.ttl_seconds,
                history=history,
            )
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
        return history

    def invalidate(self, customer_id: Optional[str] = None) -> None:
        with self._lock:
            if customer_id is None:
                self._entries.clear()
                return
            matching = [
                key
                for key, entry in self._entries.items()
                if entry.customer_id == customer_id
            ]
            for key in matching:
                del self._entries[key]

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._entries)
