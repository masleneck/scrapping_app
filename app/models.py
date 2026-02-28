from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class Direction(StrEnum):
    ARR = "ARR"
    DEP = "DEP"


class FlightStatus(StrEnum):
    SCHEDULED = "SCHEDULED"
    DELAYED = "DELAYED"
    LANDED = "LANDED"
    BOARDING = "BOARDING"
    DEPARTED = "DEPARTED"
    UNKNOWN = "UNKNOWN"


class FlightSnapshot(BaseModel):
    flight_number: str
    direction: Direction
    scheduled_time: datetime | None = None
    estimated_time: datetime | None = None
    actual_time: datetime | None = None
    aircraft_type: str | None = None
    terminal: str | None = None
    status: FlightStatus = FlightStatus.UNKNOWN
    source: str
    source_timestamp: datetime


class FlightEvent(BaseModel):
    event_type: str = Field(
        description="FLIGHT_DISCOVERED | FLIGHT_TIME_UPDATED | FLIGHT_STATUS_UPDATED"
    )
    flight_number: str
    payload: dict
    confidence_score: float = Field(ge=0.0, le=1.0)
    observed_at: datetime
    source: str


class StoredFlightEvent(FlightEvent):
    id: int
