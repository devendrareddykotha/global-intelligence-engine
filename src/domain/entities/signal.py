# src/domain/entities/signal.py

from dataclasses import dataclass, field
from datetime import datetime, timezone
from src.domain.value_objects.signal_id import SignalId


@dataclass
class Signal:
    """
    Core domain entity representing a global intelligence signal.

    All timestamps MUST be timezone-aware (UTC).
    """

    id: SignalId
    title: str
    source_count: int
    country_count: int
    first_detected_at: datetime
    last_updated_at: datetime
    velocity: float
    impact_score: float = field(default=0.0)
    is_active: bool = field(default=True)
    categories: tuple[str, ...] = field(default_factory=tuple)

    def __post_init__(self):
        if self.first_detected_at.tzinfo is None:
            raise ValueError("first_detected_at must be timezone-aware (UTC).")

        if self.last_updated_at.tzinfo is None:
            raise ValueError("last_updated_at must be timezone-aware (UTC).")

    def update_metrics(
        self,
        source_count: int,
        country_count: int,
        velocity: float,
        timestamp: datetime
    ) -> None:

        if timestamp.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware (UTC).")

        if source_count < self.source_count:
            raise ValueError("Source count cannot decrease.")

        if country_count < self.country_count:
            raise ValueError("Country count cannot decrease.")

        if timestamp < self.last_updated_at:
            raise ValueError("Timestamp cannot go backwards.")

        self.source_count = source_count
        self.country_count = country_count
        self.velocity = velocity
        self.last_updated_at = timestamp

    def deactivate(self) -> None:
        self.is_active = False

    def activate(self) -> None:
        self.is_active = True

    def set_impact_score(self, score: float) -> None:
        if score < 0:
            raise ValueError("Impact score cannot be negative.")
        self.impact_score = score