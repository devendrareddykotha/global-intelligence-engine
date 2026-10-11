# src/infrastructure/persistence/in_memory_signal_repository.py

from typing import List, Dict

from src.application.ports.signal_repository import SignalRepository
from src.domain.entities.signal import Signal


class InMemorySignalRepository(SignalRepository):

    def __init__(self):
        self._storage: Dict[str, Signal] = {}

    def get_active_signals(self) -> List[Signal]:
        return [
            signal
            for signal in self._storage.values()
            if signal.is_active
        ]

    def save(self, signal: Signal) -> None:
        self._storage[str(signal.id)] = signal

    def save_all(self, signals: List[Signal]) -> None:
        self._storage.clear()  # FULL REPLACEMENT
        for signal in signals:
            self.save(signal)

    def add_initial(self, signals: List[Signal]) -> None:
        """Seed signals without clearing any signals already in storage."""
        for signal in signals:
            self.save(signal)

    def clear(self) -> None:
        self._storage.clear()