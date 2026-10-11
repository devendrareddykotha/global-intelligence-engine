from datetime import datetime, timedelta, timezone

from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId
from src.domain.services.impact_scoring_service import ImpactScoringService

from src.infrastructure.persistence.in_memory_signal_repository import (
    InMemorySignalRepository,
)
from src.application.use_cases.generate_top_signals import (
    GenerateTopSignalsUseCase,
)


now = datetime.now(timezone.utc)

# 1. Create repository
repository = InMemorySignalRepository()

# 2. Seed signals
signals = []
for i in range(10):
    signal = Signal(
        id=SignalId.new(),
        title=f"Global Event {i}",
        source_count=5 + i * 3,
        country_count=2 + i,
        first_detected_at=now,
        last_updated_at=now - timedelta(hours=i),
        velocity=10 + i * 2,
    )
    signals.append(signal)

repository.add_initial(signals)

# 3. Create scoring service
scoring_service = ImpactScoringService()

# 4. Create use case
use_case = GenerateTopSignalsUseCase(repository, scoring_service)

# 5. Execute ranking
top_signals = use_case.execute(now, limit=5)

print("Top 5 Global Intelligence Signals:")
for s in top_signals:
    print(f"{s.title} → {s.impact_score}")