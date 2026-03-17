from pydantic import BaseModel
from typing import Optional, Any
from datetime import datetime


class TripLeg(BaseModel):
    fromAirportCode: str
    toAirportCode: str
    type: Optional[str] = "flight"
    flight: Optional[dict[str, Any]] = None


class TripStatePayload(BaseModel):
    startAirport: dict[str, Any]
    legs: list[TripLeg]


class SaveTripRequest(BaseModel):
    name: Optional[str] = None
    trip_state: TripStatePayload
    trip_routes: list[dict[str, Any]]


class TripResponse(BaseModel):
    id: int
    user_id: str
    name: Optional[str]
    trip_state: dict[str, Any]
    trip_routes: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
