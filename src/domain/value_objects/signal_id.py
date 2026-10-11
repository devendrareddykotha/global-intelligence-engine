# src/domain/value_objects/signal_id.py

from dataclasses import dataclass
import uuid


@dataclass(frozen=True)
class SignalId:
    """
    Domain identity for a Signal.

    Why not use a plain string?

    Because:
    - Domain must express intent.
    - IDs must be validated.
    - Identity must be immutable.
    - Avoid primitive obsession.

    This enforces architectural discipline.
    """

    value: str

    def __post_init__(self):
        try:
            uuid.UUID(self.value)
        except ValueError:
            raise ValueError(f"Invalid SignalId UUID: {self.value}")

    @staticmethod
    def new() -> "SignalId":
        """
        Factory method controlled by domain.
        """
        return SignalId(str(uuid.uuid4()))

    def __str__(self) -> str:
        return self.value