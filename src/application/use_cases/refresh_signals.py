# src/application/use_cases/refresh_signals.py

from typing import List

from src.domain.entities.signal import Signal
from src.domain.entities.raw_article import RawArticle
from src.domain.services.signal_aggregation_service import SignalAggregationService
from src.application.ports.signal_repository import SignalRepository


class RefreshSignalsFromFeedsUseCase:
    """
    Orchestrates:
    - Raw article ingestion (provided externally)
    - Aggregation into Signals
    - Repository replacement
    """

    def __init__(
        self,
        repository: SignalRepository,
        aggregation_service: SignalAggregationService,
    ):
        self._repository = repository
        self._aggregation_service = aggregation_service

    def execute(self, articles: List[RawArticle]) -> List[Signal]:

        # 1. Aggregate articles into signals
        signals = self._aggregation_service.aggregate(articles)

        # 2. Replace repository contents
        self._repository.save_all(signals)

        return signals