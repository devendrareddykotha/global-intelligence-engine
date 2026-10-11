from datetime import datetime, timezone
from src.domain.entities.signal import Signal
from src.domain.value_objects.signal_id import SignalId

now = datetime.now(timezone.utc)

signal = Signal(
    id=SignalId.new(),
    title="Massive Earthquake in Pacific",
    source_count=5,
    country_count=3,
    first_detected_at=now,
    last_updated_at=now,
    velocity=12.5,
)

print("Signal created:", signal)