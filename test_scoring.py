from datetime import datetime, timedelta, timezone
from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId
from src.domain.services.impact_scoring_service import ImpactScoringService

now = datetime.now(timezone.utc)

signal = Signal(
    id=SignalId.new(),
    title="Global Cyberattack",
    source_count=20,
    country_count=8,
    first_detected_at=now,
    last_updated_at=now - timedelta(hours=1),
    velocity=30.0,
)

scorer = ImpactScoringService()
score = scorer.calculate(signal, now)

print("Impact Score:", score)