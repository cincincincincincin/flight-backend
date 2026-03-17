from fastapi import APIRouter, Query, HTTPException, Path, Request
from typing import Optional
from datetime import date, datetime
from src.services.flight_schedule_service import flight_schedule_service, FlightScheduleService
from src.services.flight_price_service import flight_price_service
from src.limiter import limiter
from src.models.flight import (
    FlightsResponse,
    FlightOffersResponse,
    FlightsWithOffersResponse,
    FlightWithOffer,
    AirportSchedulesCacheInfo,
    FlightPricesCacheInfo
)
from src.database import db
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/flights", tags=["flights"])


@router.get("/airport/{airport_code}/info")
@limiter.limit("30/minute")
async def get_airport_info(
    request: Request,
    airport_code: str = Path(..., description="IATA airport code")
):
    """
    Get airport information including timezone and current local datetime
    """
    from datetime import datetime as dt
    import pytz

    try:
        async with db.get_connection() as conn:
            airport = await conn.fetchrow("""
                SELECT code, name, time_zone
                FROM airports
                WHERE code = $1
            """, airport_code.upper())

            if not airport:
                raise HTTPException(status_code=404, detail="Airport not found")

            # Compute current local datetime at the airport
            tz_str = airport['time_zone']
            try:
                tz = pytz.timezone(tz_str) if tz_str else pytz.UTC
            except Exception:
                tz = pytz.UTC

            utc_now = dt.now(pytz.UTC)
            local_now = utc_now.astimezone(tz)
            current_local_datetime = local_now.strftime("%Y-%m-%dT%H:%M:%S")
            current_local_date = local_now.strftime("%Y-%m-%d")

            return {
                "code": airport['code'],
                "name": airport['name'],
                "time_zone": airport['time_zone'],
                "current_local_datetime": current_local_datetime,
                "current_local_date": current_local_date
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting airport info for {airport_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/airport/{airport_code}", response_model=FlightsResponse)
@limiter.limit("30/minute")
async def get_airport_flights(
    request: Request,
    airport_code: str = Path(..., description="IATA airport code (e.g., 'WAW')"),
    from_local_datetime: Optional[str] = Query(None, description="Start of 12h window in local airport time (YYYY-MM-DDTHH:MM:SS). If omitted, uses current time."),
    search_date: Optional[date] = Query(None, description="Fallback: date to search (uses midnight). Ignored if from_local_datetime is provided."),
    limit: int = Query(200, ge=1, le=500, description="Max results (default 200 for full 12h window)"),
    force_refresh: bool = Query(False, description="Force refresh from API")
):
    """
    Get departing flights from airport for a 12h window starting at from_local_datetime.

    - Returns all flights in the 12h window (no offset pagination)
    - Returns range_end_datetime for the next window request
    - Automatically fetches from API if no valid cache covers the requested datetime
    """
    try:
        # Parse from_local_datetime string to datetime object
        parsed_from_dt = None
        if from_local_datetime:
            try:
                parsed_from_dt = datetime.fromisoformat(from_local_datetime)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid from_local_datetime format: {from_local_datetime}. Use YYYY-MM-DDTHH:MM:SS")

        return await FlightScheduleService.get_flights_from_airport(
            airport_code=airport_code.upper(),
            from_local_datetime=parsed_from_dt,
            search_date=search_date,
            limit=limit,
            force_refresh=force_refresh
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting flights for {airport_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/airport/{airport_code}/cache-info", response_model=AirportSchedulesCacheInfo)
async def get_airport_cache_info(
    airport_code: str = Path(..., description="IATA airport code"),
    search_date: date = Query(default_factory=date.today, description="Date to check cache for"),
    direction: str = Query("Departure", description="Flight direction: Departure, Arrival, or Both")
):
    """
    Get cache information for airport flight schedules
    """
    try:
        cache_info = await FlightScheduleService.get_cache_info(
            airport_code=airport_code.upper(),
            search_date=search_date,
            direction=direction
        )

        return AirportSchedulesCacheInfo(
            airport_code=airport_code.upper(),
            search_date=search_date,
            direction=direction,
            cache_info=cache_info
        )
    except Exception as e:
        logger.error(f"Error getting cache info for {airport_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/airport/{airport_code}/refresh")
async def refresh_airport_flights(
    airport_code: str = Path(..., description="IATA airport code"),
    from_local_datetime: Optional[str] = Query(None, description="Start datetime for refresh (YYYY-MM-DDTHH:MM:SS). Defaults to current time.")
):
    """
    Force refresh flight schedules from API starting from given datetime
    """
    try:
        parsed_from_dt = None
        if from_local_datetime:
            try:
                parsed_from_dt = datetime.fromisoformat(from_local_datetime)
            except ValueError:
                raise HTTPException(status_code=400, detail=f"Invalid from_local_datetime format")

        success, last_fetched, fetch_to_local = await FlightScheduleService.fetch_and_cache_schedules(
            airport_code=airport_code.upper(),
            from_local_datetime=parsed_from_dt,
            direction="Departure"
        )

        if not success:
            raise HTTPException(status_code=503, detail="Failed to fetch data from API")

        return {
            "success": True,
            "message": f"Flights refreshed for {airport_code}",
            "last_fetched_at": last_fetched,
            "range_end_datetime": fetch_to_local.isoformat() if fetch_to_local else None
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error refreshing flights for {airport_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/offers/{origin_airport}/{destination_airport}", response_model=FlightOffersResponse)
@limiter.limit("30/minute")
async def get_flight_offers(
    request: Request,
    origin_airport: str = Path(..., description="Origin airport IATA code"),
    destination_airport: str = Path(..., description="Destination airport IATA code"),
    departure_date: date = Query(default_factory=date.today, description="Departure date"),
    currency: str = Query("PLN", description="Currency code: PLN, USD, EUR, GBP"),
    force_refresh: bool = Query(False, description="Force refresh from API")
):
    """
    Get flight offers (prices) for a specific airport-to-airport route

    - Fetches prices from Aviasales API
    - Returns only direct flights (no transfers)
    - Filters results for specific airports (important for cities with multiple airports)
    """
    try:
        return await flight_price_service.get_offers_for_route(
            origin_airport_code=origin_airport.upper(),
            destination_airport_code=destination_airport.upper(),
            departure_date=departure_date,
            currency=currency.upper(),
            force_refresh=force_refresh
        )
    except Exception as e:
        logger.error(f"Error getting offers for {origin_airport}->{destination_airport}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/offers/city/{origin_city}/{destination_city}", response_model=FlightOffersResponse)
@limiter.limit("30/minute")
async def get_city_flight_offers(
    request: Request,
    origin_city: str = Path(..., description="Origin city IATA code"),
    destination_city: str = Path(..., description="Destination city IATA code"),
    departure_date: date = Query(default_factory=date.today, description="Departure date"),
    force_refresh: bool = Query(False, description="Force refresh from API")
):
    """
    Get all flight offers for a city-to-city pair

    Returns offers from all airports in origin city to all airports in destination city
    """
    try:
        return await flight_price_service.get_offers_for_city_pair(
            origin_city_code=origin_city.upper(),
            destination_city_code=destination_city.upper(),
            departure_date=departure_date,
            force_refresh=force_refresh
        )
    except Exception as e:
        logger.error(f"Error getting offers for {origin_city}->{destination_city}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/offers/cache-info/{origin_city}/{destination_city}", response_model=FlightPricesCacheInfo)
async def get_prices_cache_info(
    origin_city: str = Path(..., description="Origin city IATA code"),
    destination_city: str = Path(..., description="Destination city IATA code"),
    departure_date: date = Query(default_factory=date.today, description="Departure date")
):
    """
    Get cache information for flight prices

    Returns information about when price data was last fetched
    """
    try:
        cache_info = await flight_price_service.get_cache_info(
            origin_city_code=origin_city.upper(),
            destination_city_code=destination_city.upper(),
            departure_date=departure_date
        )

        return FlightPricesCacheInfo(
            origin_city_code=origin_city.upper(),
            destination_city_code=destination_city.upper(),
            departure_date=departure_date,
            cache_info=cache_info
        )
    except Exception as e:
        logger.error(f"Error getting price cache info: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/combined/{airport_code}", response_model=FlightsWithOffersResponse)
async def get_flights_with_offers(
    airport_code: str = Path(..., description="IATA airport code"),
    search_date: date = Query(default_factory=date.today, description="Date to search flights"),
    limit: int = Query(50, ge=1, le=200, description="Number of results per page"),
    include_prices: bool = Query(True, description="Include price offers for flights"),
    force_refresh_schedules: bool = Query(False, description="Force refresh flight schedules"),
    force_refresh_prices: bool = Query(False, description="Force refresh prices")
):
    """
    Get flights from airport with their price offers (if available)

    This endpoint combines flight schedules with price offers:
    1. Fetches flights from the airport
    2. For each flight, tries to find matching price offers
    3. Returns combined data

    Note: Price data might not be available for all flights
    """
    try:
        # Get flights
        flights_response = await flight_schedule_service.get_flights_from_airport(
            airport_code=airport_code.upper(),
            search_date=search_date,
            limit=limit,
            force_refresh=force_refresh_schedules
        )

        combined_data = []

        if include_prices:
            # For each flight, try to get price offers
            for flight in flights_response.data:
                # Get offers for this specific route
                offers_response = await flight_price_service.get_offers_for_route(
                    origin_airport_code=flight.origin_airport_code,
                    destination_airport_code=flight.destination_airport_code,
                    departure_date=search_date,
                    origin_city_code=flight.origin_city_code,
                    destination_city_code=flight.destination_city_code,
                    force_refresh=force_refresh_prices
                )

                # Find best matching offer (closest departure time)
                best_offer = None
                if offers_response.data:
                    min_time_diff = None
                    for offer in offers_response.data:
                        time_diff = abs((offer.departure_at - flight.scheduled_departure_utc).total_seconds())
                        if min_time_diff is None or time_diff < min_time_diff:
                            min_time_diff = time_diff
                            best_offer = offer

                combined_data.append(FlightWithOffer(
                    flight=flight,
                    offer=best_offer
                ))
        else:
            # Just flights without offers
            combined_data = [FlightWithOffer(flight=flight, offer=None) for flight in flights_response.data]

        return FlightsWithOffersResponse(
            data=combined_data,
            count=flights_response.count,
            schedules_last_fetched_at=flights_response.last_fetched_at,
            prices_last_fetched_at=None  # Could be multiple different timestamps
        )

    except Exception as e:
        logger.error(f"Error getting combined flights for {airport_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
