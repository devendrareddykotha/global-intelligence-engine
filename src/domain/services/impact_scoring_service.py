# src/domain/services/impact_scoring_service.py

from datetime import datetime
from dataclasses import dataclass

from src.domain.entities.signal import Signal


@dataclass
class ImpactScoringService:
    """
    Domain service responsible for calculating impact scores.

    This class contains pure business logic.
    No database.
    No frameworks.
    No side effects.

    It transforms Signal state into a measurable impact score.
    """

    source_weight: float = 2.0
    country_weight: float = 3.0
    velocity_weight: float = 1.5
    freshness_weight: float = 5.0

    def calculate(self, signal: Signal, now: datetime) -> float:
        """
        Compute impact score based on:

        - Source diversity
        - Geographic spread
        - Reporting velocity
        - Freshness decay
        """

        # 1. Freshness factor (decay over time)
        hours_since_update = (now - signal.last_updated_at).total_seconds() / 3600

        freshness_factor = min(1.0, max(0.0, 1 - (hours_since_update / 24)))
        # After 24 hours without updates → freshness = 0

        # 2. Weighted calculation
        score = (
            (self.source_weight * signal.source_count)
            + (self.country_weight * signal.country_count)
            + (self.velocity_weight * signal.velocity)
            + (self.freshness_weight * freshness_factor)
        )

        return round(score, 2)