from datetime import datetime, timedelta, timezone
from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId
from src.domain.services.impact_scoring_service import ImpactScoringService
from src.infrastructure.persistence.in_memory_signal_repository import InMemorySignalRepository
from src.application.use_cases.generate_top_signals import GenerateTopSignalsUseCase


now = datetime.now(timezone.utc)

signals = []

# Create mock signals with different metrics
for i in range(5):
    signal = Signal(
        id=SignalId.new(),
        title=f"Signal {i}",
        source_count=5 + i * 5,
        country_count=2 + i,
        first_detected_at=now,
        last_updated_at=now - timedelta(hours=i),
        velocity=10 + i * 3,
    )
    signals.append(signal)

scoring_service = ImpactScoringService()
repository = InMemorySignalRepository()
repository.save_all(signals)
use_case = GenerateTopSignalsUseCase(repository, scoring_service)

top_signals = use_case.execute(now, limit=3)

print("Top Signals:")
for s in top_signals:
    print(s.title, "→", s.impact_score)