from app.flight_identity import build_flight_instance_key, normalize_flight_number
from app.models import Direction, FlightSnapshot, FlightStatus


def test_normalize_flight_number_strips_noise() -> None:
    assert normalize_flight_number("SU6519Россия, Аэрофлот") == "SU6519"
    assert normalize_flight_number("SU 6519") == "SU6519"
    assert normalize_flight_number("AFL1975") == "SU1975"


def test_flight_instance_key_uses_schedule_anchor() -> None:
    snapshot = FlightSnapshot(
        flight_number="SU100",
        direction=Direction.ARR,
        scheduled_time="2026-03-01T10:15:00Z",
        status=FlightStatus.SCHEDULED,
        source="test",
        source_timestamp="2026-03-01T10:20:00Z",
    )
    key = build_flight_instance_key(snapshot)
    assert key == "SU100|ARR|202603011015"
