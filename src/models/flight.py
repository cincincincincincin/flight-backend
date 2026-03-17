from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime, date

# ============================================
# MODELS FOR FLIGHTS (AeroDataBox API)
# ============================================

class FlightTime(BaseModel):
    """Flight time information (UTC and local)"""
    utc: Optional[datetime] = None
    local: Optional[datetime] = None

class AirportInfo(BaseModel):
    """Airport information from API"""
    icao: Optional[str] = None
    iata: Optional[str] = None
    name: Optional[str] = None
    shortName: Optional[str] = None
    municipalityName: Optional[str] = None
    location: Optional[Dict[str, float]] = None
    countryCode: Optional[str] = None
    timeZone: Optional[str] = None

class FlightMovement(BaseModel):
    """Flight movement information (departure or arrival)"""
    airport: Optional[AirportInfo] = None
    scheduledTime: Optional[FlightTime] = None
    revisedTime: Optional[FlightTime] = None
    predictedTime: Optional[FlightTime] = None
    runwayTime: Optional[FlightTime] = None
    terminal: Optional[str] = None
    checkInDesk: Optional[str] = None
    gate: Optional[str] = None
    baggageBelt: Optional[str] = None
    runway: Optional[str] = None

class FlightBase(BaseModel):
    """Base flight model"""
    flight_number: str
    airline_code: Optional[str] = None
    origin_airport_code: str
    destination_airport_code: str
    scheduled_departure_utc: datetime
    scheduled_departure_local: Optional[datetime] = None
    scheduled_arrival_utc: Optional[datetime] = None
    scheduled_arrival_local: Optional[datetime] = None
    departure_terminal: Optional[str] = None
    departure_gate: Optional[str] = None
    arrival_terminal: Optional[str] = None
    arrival_gate: Optional[str] = None

class Flight(FlightBase):
    """Flight with full details"""
    id: Optional[int] = None
    revised_departure_utc: Optional[datetime] = None
    predicted_departure_utc: Optional[datetime] = None
    runway_departure_utc: Optional[datetime] = None
    revised_arrival_utc: Optional[datetime] = None
    predicted_arrival_utc: Optional[datetime] = None
    runway_arrival_utc: Optional[datetime] = None
    search_date: Optional[date] = None
    raw_data: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

    # Extended info
    origin_airport_name: Optional[str] = None
    destination_airport_name: Optional[str] = None
    origin_city_name: Optional[str] = None
    destination_city_name: Optional[str] = None
    origin_city_code: Optional[str] = None
    destination_city_code: Optional[str] = None
    airline_name: Optional[str] = None

    class Config:
        from_attributes = True

class FlightResponse(BaseModel):
    """Response for single flight"""
    success: bool = True
    data: Flight

class FlightsResponse(BaseModel):
    """Response for multiple flights"""
    success: bool = True
    data: List[Flight]
    count: int
    last_fetched_at: Optional[datetime] = None
    range_end_datetime: Optional[str] = None  # End of the fetched 12h window (ISO format)

# ============================================
# MODELS FOR FLIGHT OFFERS (Aviasales API)
# ============================================

class FlightOfferBase(BaseModel):
    """Base flight offer model"""
    origin_city_code: str
    destination_city_code: str
    origin_airport_code: str
    destination_airport_code: str
    price: float
    currency: str
    airline_code: Optional[str] = None
    flight_number: Optional[str] = None
    departure_at: datetime
    transfers: int = 0
    duration_to: Optional[int] = None  # Duration in minutes
    link: Optional[str] = None

class FlightOffer(FlightOfferBase):
    """Flight offer with full details"""
    id: Optional[int] = None
    return_at: Optional[datetime] = None
    return_transfers: Optional[int] = None
    duration: Optional[int] = None
    duration_back: Optional[int] = None
    search_date: Optional[date] = None
    created_at: Optional[datetime] = None

    # Extended info
    origin_city_name: Optional[str] = None
    destination_city_name: Optional[str] = None
    origin_airport_name: Optional[str] = None
    destination_airport_name: Optional[str] = None
    airline_name: Optional[str] = None

    class Config:
        from_attributes = True

class FlightOfferResponse(BaseModel):
    """Response for single flight offer"""
    success: bool = True
    data: FlightOffer

class FlightOffersResponse(BaseModel):
    """Response for multiple flight offers"""
    success: bool = True
    data: List[FlightOffer]
    count: int
    last_fetched_at: Optional[datetime] = None

# ============================================
# COMBINED RESPONSE (Flight + Offer)
# ============================================

class FlightWithOffer(BaseModel):
    """Flight combined with price offer (if available)"""
    flight: Flight
    offer: Optional[FlightOffer] = None

class FlightsWithOffersResponse(BaseModel):
    """Response for flights with their offers"""
    success: bool = True
    data: List[FlightWithOffer]
    count: int
    schedules_last_fetched_at: Optional[datetime] = None
    prices_last_fetched_at: Optional[datetime] = None

# ============================================
# CACHE INFO
# ============================================

class CacheInfo(BaseModel):
    """Information about cached data"""
    has_cache: bool
    last_fetched_at: Optional[datetime] = None
    records_count: Optional[int] = None

class AirportSchedulesCacheInfo(BaseModel):
    """Cache info for airport schedules"""
    airport_code: str
    search_date: date
    direction: str
    cache_info: CacheInfo

class FlightPricesCacheInfo(BaseModel):
    """Cache info for flight prices"""
    origin_city_code: str
    destination_city_code: str
    departure_date: date
    cache_info: CacheInfo
