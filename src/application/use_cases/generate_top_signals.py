# src/application/use_cases/generate_top_signals.py

from datetime import datetime
from typing import List

from src.domain.entities.signal import Signal
from src.domain.services.impact_scoring_service import ImpactScoringService
from src.application.ports.signal_repository import SignalRepository


class GenerateTopSignalsUseCase:
    """
    Application use case for generating ranked top signals.

    Now depends on repository abstraction (port).
    """

    def __init__(
        self,
        repository: SignalRepository,
        scoring_service: ImpactScoringService
    ):
        self._repository = repository
        self._scoring_service = scoring_service

    def execute(
        self,
        now: datetime,
        limit: int = 100
    ) -> List[Signal]:

        if now.tzinfo is None:
            raise ValueError("now must be timezone-aware (UTC).")

        # 1. Retrieve active signals
        signals = self._repository.get_active_signals()

        # 2. Recalculate impact scores
        for signal in signals:
            score = self._scoring_service.calculate(signal, now)
            signal.set_impact_score(score)
            self._repository.save(signal)

        # 3. Sort
        ranked = sorted(
            signals,
            key=lambda s: s.impact_score,
            reverse=True
        )

        return ranked[:limit]