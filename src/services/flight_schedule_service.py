from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta, time
from src.database import db
from src.models.flight import Flight, FlightsResponse, CacheInfo, AirportSchedulesCacheInfo
from src.services.api_client import aerodatabox_client
import logging
import json
import pytz
import asyncio
import time as time_module

logger = logging.getLogger(__name__)

# Debug flag - set to False to disable debug logging
DEBUG_FLIGHT_SERVICE = True

def debug_log(message: str):
    """Log debug message if DEBUG_FLIGHT_SERVICE is enabled"""
    if DEBUG_FLIGHT_SERVICE:
        logger.debug(message)


class FlightScheduleService:
    """Service for managing flight schedules from AeroDataBox API"""

    # Cache expiry time in hours
    CACHE_EXPIRY_HOURS = 1

    # Rate limiting: minimum time between API calls (in seconds)
    MIN_API_CALL_INTERVAL = 1.5

    # Track last API call time
    _last_api_call_time = 0.0

    # Lock to serialize API calls and prevent race conditions in rate limiting
    _api_call_lock: Optional[asyncio.Lock] = None

    @classmethod
    def _get_lock(cls) -> asyncio.Lock:
        """Get or create the API call lock (lazy initialization for asyncio loop)"""
        if cls._api_call_lock is None:
            cls._api_call_lock = asyncio.Lock()
        return cls._api_call_lock

    @staticmethod
    async def find_cache_for_datetime(
        airport_code: str,
        from_local_datetime: datetime,
        direction: str = "Departure"
    ) -> Optional[Dict]:
        """Find a valid (non-expired) cache entry covering the given local datetime"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT id, last_fetched_at, fetch_from_local, fetch_to_local
                FROM airport_schedules_cache
                WHERE airport_code = $1
                  AND direction = $2
                  AND fetch_from_local <= $3
                  AND fetch_to_local > $3
                  AND last_fetched_at > (NOW() - INTERVAL '1 hour')
                ORDER BY fetch_from_local DESC
                LIMIT 1
            """, airport_code, direction, from_local_datetime)

            if row:
                return dict(row)
            return None

    @staticmethod
    async def get_cache_info(
        airport_code: str,
        search_date: date,
        direction: str = "Departure"
    ) -> CacheInfo:
        """Get cache information for airport schedules (most recent window for the given date)"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT last_fetched_at, fetch_from_local, fetch_to_local
                FROM airport_schedules_cache
                WHERE airport_code = $1
                  AND direction = $2
                  AND DATE(fetch_from_local) = $3
                ORDER BY fetch_from_local DESC
                LIMIT 1
            """, airport_code, direction, search_date)

            if row:
                return CacheInfo(
                    has_cache=True,
                    last_fetched_at=row['last_fetched_at'],
                    records_count=None
                )

            return CacheInfo(has_cache=False)

    @staticmethod
    def _parse_flight_from_api(
        flight_data: Dict[str, Any],
        search_date: date,
        is_departure: bool
    ) -> Optional[Dict[str, Any]]:
        """Parse flight data from AeroDataBox API response (withLeg=true format)"""
        try:
            flight_number = flight_data.get('number')
            if not flight_number:
                debug_log(f"Skipping flight: no flight number")
                return None

            airline = flight_data.get('airline', {})
            airline_code = airline.get('iata')

            def parse_time_dict(time_obj):
                if not time_obj:
                    return None, None
                utc_str = time_obj.get('utc')
                local_str = time_obj.get('local')
                try:
                    utc = datetime.fromisoformat(utc_str.replace('Z', '+00:00').replace(' ', 'T')) if utc_str else None
                    if local_str:
                        local_with_tz = datetime.fromisoformat(local_str.replace(' ', 'T'))
                        local = local_with_tz.replace(tzinfo=None)
                    else:
                        local = None
                    return utc, local
                except Exception as e:
                    debug_log(f"Error parsing time: {e}")
                    return None, None

            dep_obj = flight_data.get('departure', {})
            arr_obj = flight_data.get('arrival', {})

            dep_sched_utc, dep_sched_local = parse_time_dict(dep_obj.get('scheduledTime'))
            dep_revised_utc, _ = parse_time_dict(dep_obj.get('revisedTime'))
            dep_predicted_utc, _ = parse_time_dict(dep_obj.get('predictedTime'))
            dep_runway_utc, _ = parse_time_dict(dep_obj.get('runwayTime'))

            arr_sched_utc, arr_sched_local = parse_time_dict(arr_obj.get('scheduledTime'))
            arr_revised_utc, _ = parse_time_dict(arr_obj.get('revisedTime'))
            arr_predicted_utc, _ = parse_time_dict(arr_obj.get('predictedTime'))
            arr_runway_utc, _ = parse_time_dict(arr_obj.get('runwayTime'))

            if is_departure:
                arr_airport = arr_obj.get('airport', {})
                dest_airport_code = arr_airport.get('iata') or arr_airport.get('icao')

                if not dep_sched_utc or not dest_airport_code:
                    debug_log(f"Skipping departure {flight_number}: missing departure time or destination")
                    return None

                return {
                    'flight_number': flight_number,
                    'airline_code': airline_code,
                    'destination_airport_code': dest_airport_code,
                    'scheduled_departure_utc': dep_sched_utc,
                    'scheduled_departure_local': dep_sched_local,
                    'scheduled_arrival_utc': arr_sched_utc,
                    'scheduled_arrival_local': arr_sched_local,
                    'revised_departure_utc': dep_revised_utc,
                    'predicted_departure_utc': dep_predicted_utc,
                    'runway_departure_utc': dep_runway_utc,
                    'revised_arrival_utc': arr_revised_utc,
                    'predicted_arrival_utc': arr_predicted_utc,
                    'runway_arrival_utc': arr_runway_utc,
                    'departure_terminal': dep_obj.get('terminal'),
                    'departure_gate': dep_obj.get('gate'),
                    'arrival_terminal': arr_obj.get('terminal'),
                    'arrival_gate': arr_obj.get('gate'),
                    'search_date': search_date,
                    'raw_data': flight_data
                }
            else:
                dep_airport = dep_obj.get('airport', {})
                origin_airport_code = dep_airport.get('iata') or dep_airport.get('icao')

                if not arr_sched_utc or not origin_airport_code:
                    debug_log(f"Skipping arrival {flight_number}: missing arrival time or origin")
                    return None

                return {
                    'flight_number': flight_number,
                    'airline_code': airline_code,
                    'origin_airport_code': origin_airport_code,
                    'scheduled_departure_utc': dep_sched_utc,
                    'scheduled_departure_local': dep_sched_local,
                    'scheduled_arrival_utc': arr_sched_utc,
                    'scheduled_arrival_local': arr_sched_local,
                    'revised_departure_utc': dep_revised_utc,
                    'predicted_departure_utc': dep_predicted_utc,
                    'runway_departure_utc': dep_runway_utc,
                    'revised_arrival_utc': arr_revised_utc,
                    'predicted_arrival_utc': arr_predicted_utc,
                    'runway_arrival_utc': arr_runway_utc,
                    'departure_terminal': dep_obj.get('terminal'),
                    'departure_gate': dep_obj.get('gate'),
                    'arrival_terminal': arr_obj.get('terminal'),
                    'arrival_gate': arr_obj.get('gate'),
                    'search_date': search_date,
                    'raw_data': flight_data
                }
        except Exception as e:
            logger.error(f"Error parsing flight: {str(e)}")
            return None

    @staticmethod
    async def _save_flights_to_db(flights_data: List[Dict[str, Any]]) -> int:
        """Save parsed flights to database"""
        saved_count = 0
        skipped_count = 0
        async with db.get_connection() as conn:
            for flight_data in flights_data:
                try:
                    # Validate that all required foreign keys exist in database
                    # Check origin airport (if not None)
                    if flight_data.get('origin_airport_code'):
                        origin_exists = await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM airports WHERE code = $1)",
                            flight_data['origin_airport_code']
                        )
                        if not origin_exists:
                            debug_log(f"Skipping flight {flight_data.get('flight_number')}: origin airport {flight_data['origin_airport_code']} not in database")
                            skipped_count += 1
                            continue

                    # Check destination airport (if not None)
                    if flight_data.get('destination_airport_code'):
                        dest_exists = await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM airports WHERE code = $1)",
                            flight_data['destination_airport_code']
                        )
                        if not dest_exists:
                            debug_log(f"Skipping flight {flight_data.get('flight_number')}: destination airport {flight_data['destination_airport_code']} not in database")
                            skipped_count += 1
                            continue

                    # Check airline (if not None)
                    if flight_data.get('airline_code'):
                        airline_exists = await conn.fetchval(
                            "SELECT EXISTS(SELECT 1 FROM airlines WHERE code = $1)",
                            flight_data['airline_code']
                        )
                        if not airline_exists:
                            debug_log(f"Skipping flight {flight_data.get('flight_number')}: airline {flight_data['airline_code']} not in database")
                            skipped_count += 1
                            continue

                    # All foreign keys validated, proceed with insert
                    await conn.execute("""
                        INSERT INTO flights (
                            flight_number, airline_code, origin_airport_code, destination_airport_code,
                            scheduled_departure_utc, scheduled_departure_local,
                            scheduled_arrival_utc, scheduled_arrival_local,
                            revised_departure_utc, predicted_departure_utc, runway_departure_utc,
                            revised_arrival_utc, predicted_arrival_utc, runway_arrival_utc,
                            departure_terminal, departure_gate, arrival_terminal, arrival_gate,
                            search_date, raw_data
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                        ON CONFLICT (flight_number, scheduled_departure_utc, origin_airport_code, destination_airport_code)
                        DO UPDATE SET
                            scheduled_arrival_utc = EXCLUDED.scheduled_arrival_utc,
                            scheduled_arrival_local = EXCLUDED.scheduled_arrival_local,
                            revised_departure_utc = EXCLUDED.revised_departure_utc,
                            predicted_departure_utc = EXCLUDED.predicted_departure_utc,
                            runway_departure_utc = EXCLUDED.runway_departure_utc,
                            revised_arrival_utc = EXCLUDED.revised_arrival_utc,
                            predicted_arrival_utc = EXCLUDED.predicted_arrival_utc,
                            runway_arrival_utc = EXCLUDED.runway_arrival_utc,
                            departure_gate = EXCLUDED.departure_gate,
                            arrival_terminal = EXCLUDED.arrival_terminal,
                            arrival_gate = EXCLUDED.arrival_gate,
                            raw_data = EXCLUDED.raw_data
                    """,
                        flight_data['flight_number'],
                        flight_data['airline_code'],
                        flight_data['origin_airport_code'],
                        flight_data['destination_airport_code'],
                        flight_data['scheduled_departure_utc'],
                        flight_data['scheduled_departure_local'],
                        flight_data['scheduled_arrival_utc'],
                        flight_data['scheduled_arrival_local'],
                        flight_data['revised_departure_utc'],
                        flight_data['predicted_departure_utc'],
                        flight_data['runway_departure_utc'],
                        flight_data['revised_arrival_utc'],
                        flight_data['predicted_arrival_utc'],
                        flight_data['runway_arrival_utc'],
                        flight_data['departure_terminal'],
                        flight_data['departure_gate'],
                        flight_data['arrival_terminal'],
                        flight_data['arrival_gate'],
                        flight_data['search_date'],
                        json.dumps(flight_data['raw_data'])
                    )
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving flight {flight_data.get('flight_number')}: {str(e)}")
                    continue

        debug_log(f"Saved {saved_count} flights to database, skipped {skipped_count} flights with missing airports/airlines")
        return saved_count

    @staticmethod
    async def fetch_and_cache_schedules(
        airport_code: str,
        from_local_datetime: Optional[datetime] = None,
        direction: str = "Departure"
    ) -> Tuple[bool, Optional[datetime], Optional[datetime]]:
        """
        Fetch schedules from API for a 12h window starting at from_local_datetime.

        Returns: (success, last_fetched_at, fetch_to_local)
        """
        debug_log(f"Fetching schedules for {airport_code} from {from_local_datetime} ({direction})")

        # Get airport timezone from database
        async with db.get_connection() as conn:
            timezone_str = await conn.fetchval(
                "SELECT time_zone FROM airports WHERE code = $1", airport_code
            )

        if not timezone_str:
            logger.warning(f"No timezone found for {airport_code}, using UTC")
            airport_tz = pytz.UTC
        else:
            try:
                airport_tz = pytz.timezone(timezone_str)
            except Exception:
                logger.warning(f"Invalid timezone {timezone_str} for {airport_code}, using UTC")
                airport_tz = pytz.UTC

        # Determine from_time
        if from_local_datetime is not None:
            from_time = from_local_datetime
        else:
            # Use current time at airport
            utc_now = datetime.now(pytz.UTC)
            local_now = utc_now.astimezone(airport_tz)
            from_time = local_now.replace(tzinfo=None)

        # AeroDataBox allows max 12 hour range
        to_time = from_time + timedelta(hours=12)

        # search_date for flights table (date of from_time)
        search_date = from_time.date()

        # Format times for API (local time without timezone)
        from_local_str = from_time.strftime("%Y-%m-%dT%H:%M")
        to_local_str = to_time.strftime("%Y-%m-%dT%H:%M")

        # Acquire lock to serialize API calls and prevent concurrent rate limit bypass
        lock = FlightScheduleService._get_lock()
        async with lock:
            # Rate limiting: ensure at least MIN_API_CALL_INTERVAL seconds between API calls
            time_since_last_call = time_module.time() - FlightScheduleService._last_api_call_time
            if time_since_last_call < FlightScheduleService.MIN_API_CALL_INTERVAL:
                sleep_time = FlightScheduleService.MIN_API_CALL_INTERVAL - time_since_last_call
                debug_log(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                await asyncio.sleep(sleep_time)

            # Update last API call time before making the call
            FlightScheduleService._last_api_call_time = time_module.time()

            # Fetch from API (inside lock to enforce rate limit)
            api_response = await aerodatabox_client.get_airport_departures(
                airport_code=airport_code,
                from_local=from_local_str,
                to_local=to_local_str,
                direction=direction,
                with_leg=True
            )

        if not api_response:
            logger.error(f"Failed to fetch schedules from API for {airport_code}")
            return False, None, None

        # Save raw cache with fetch time range
        async with db.get_connection() as conn:
            await conn.execute("""
                INSERT INTO airport_schedules_cache (airport_code, direction, data, fetch_from_local, fetch_to_local)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (airport_code, direction, fetch_from_local)
                DO UPDATE SET
                    last_fetched_at = NOW(),
                    data = EXCLUDED.data,
                    fetch_to_local = EXCLUDED.fetch_to_local
            """, airport_code, direction, json.dumps(api_response), from_time, to_time)

        # Parse and save flights
        flights_data = []
        departures = api_response.get('departures', [])
        arrivals = api_response.get('arrivals', [])

        for flight in departures:
            parsed = FlightScheduleService._parse_flight_from_api(flight, search_date, is_departure=True)
            if parsed:
                # Add origin airport code for departures
                parsed['origin_airport_code'] = airport_code
                flights_data.append(parsed)

        for flight in arrivals:
            parsed = FlightScheduleService._parse_flight_from_api(flight, search_date, is_departure=False)
            if parsed:
                # Add destination airport code for arrivals
                parsed['destination_airport_code'] = airport_code
                flights_data.append(parsed)

        await FlightScheduleService._save_flights_to_db(flights_data)

        return True, datetime.now(), to_time

    @staticmethod
    async def get_flights_from_airport(
        airport_code: str,
        from_local_datetime: Optional[datetime] = None,
        search_date: Optional[date] = None,
        limit: int = 200,
        force_refresh: bool = False
    ) -> FlightsResponse:
        """
        Get departing flights from airport starting from from_local_datetime.

        Args:
            airport_code: IATA airport code
            from_local_datetime: Start of time window (local airport time). If None, uses current time.
            search_date: Fallback - if from_local_datetime not provided, uses midnight of this date.
            limit: Max number of results (default 200 to get full 12h window)
            force_refresh: Force refresh from API
        """
        direction = "Departure"

        # Resolve from_local_datetime
        if from_local_datetime is None:
            if search_date is not None:
                from_local_datetime = datetime.combine(search_date, time.min)
            else:
                from_local_datetime = datetime.utcnow().replace(second=0, microsecond=0)

        # Check for valid cache covering from_local_datetime
        cache_info = None
        if not force_refresh:
            cache_info = await FlightScheduleService.find_cache_for_datetime(
                airport_code, from_local_datetime, direction
            )

        if not cache_info:
            # Fetch new 12h window from API
            success, last_fetched, fetch_to_local = await FlightScheduleService.fetch_and_cache_schedules(
                airport_code, from_local_datetime, direction
            )
            if success:
                cache_info = await FlightScheduleService.find_cache_for_datetime(
                    airport_code, from_local_datetime, direction
                )

        if not cache_info:
            debug_log(f"No data available for {airport_code} from {from_local_datetime}")
            return FlightsResponse(data=[], count=0, last_fetched_at=None, range_end_datetime=None)

        fetch_to_local = cache_info['fetch_to_local']
        last_fetched = cache_info['last_fetched_at']

        # Query flights in the window
        async with db.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    f.id, f.flight_number, f.airline_code,
                    f.origin_airport_code, f.destination_airport_code,
                    f.scheduled_departure_utc, f.scheduled_departure_local,
                    f.scheduled_arrival_utc, f.scheduled_arrival_local,
                    f.revised_departure_utc, f.predicted_departure_utc, f.runway_departure_utc,
                    f.revised_arrival_utc, f.predicted_arrival_utc, f.runway_arrival_utc,
                    f.departure_terminal, f.departure_gate,
                    f.arrival_terminal, f.arrival_gate,
                    f.search_date, f.created_at,
                    a1.name as airline_name,
                    ap1.name as origin_airport_name,
                    ap2.name as destination_airport_name,
                    c1.name as origin_city_name,
                    c1.code as origin_city_code,
                    c2.name as destination_city_name,
                    c2.code as destination_city_code
                FROM flights f
                LEFT JOIN airlines a1 ON f.airline_code = a1.code
                LEFT JOIN airports ap1 ON f.origin_airport_code = ap1.code
                LEFT JOIN airports ap2 ON f.destination_airport_code = ap2.code
                LEFT JOIN cities c1 ON ap1.city_code = c1.code
                LEFT JOIN cities c2 ON ap2.city_code = c2.code
                WHERE f.origin_airport_code = $1
                  AND f.scheduled_departure_local >= $2
                  AND f.scheduled_departure_local < $3
                ORDER BY f.scheduled_departure_local ASC
                LIMIT $4
            """, airport_code, from_local_datetime, fetch_to_local, limit)

            # Get total count in window
            total_count = await conn.fetchval("""
                SELECT COUNT(*)
                FROM flights
                WHERE origin_airport_code = $1
                  AND scheduled_departure_local >= $2
                  AND scheduled_departure_local < $3
            """, airport_code, from_local_datetime, fetch_to_local)

            flights = [Flight(**dict(row)) for row in rows]

            return FlightsResponse(
                data=flights,
                count=total_count or 0,
                last_fetched_at=last_fetched,
                range_end_datetime=fetch_to_local.isoformat() if fetch_to_local else None
            )


flight_schedule_service = FlightScheduleService()
