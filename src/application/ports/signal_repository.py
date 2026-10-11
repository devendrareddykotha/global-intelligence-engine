
from abc import ABC, abstractmethod
from typing import List
from datetime import datetime

from src.domain.entities.signal import Signal


class SignalRepository(ABC):
    """
    Application-level abstraction for retrieving and persisting Signals.

    This is a PORT.

    Infrastructure must implement this.
    """

    @abstractmethod
    def get_active_signals(self) -> List[Signal]:
        """
        Retrieve all active signals.
        """
        pass

    @abstractmethod
    def save(self, signal: Signal) -> None:
        """
        Persist updated signal state.
        """
        pass

    @abstractmethod
    def save_all(self, signals: List[Signal]) -> None:
        """
        Persist multiple signals.
        """
        pass