from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from src.database import db
from src.models.flight import FlightOffer, FlightOffersResponse, CacheInfo, FlightPricesCacheInfo
from src.services.api_client import aviasales_client
import logging
import json

logger = logging.getLogger(__name__)

# Debug flag - set to False to disable debug logging
DEBUG_PRICE_SERVICE = True

def debug_log(message: str):
    """Log debug message if DEBUG_PRICE_SERVICE is enabled"""
    if DEBUG_PRICE_SERVICE:
        logger.debug(message)


class FlightPriceService:
    """Service for managing flight prices from Aviasales API"""

    # Cache expiry time in hours
    CACHE_EXPIRY_HOURS = 6

    @staticmethod
    async def get_cache_info(
        origin_city_code: str,
        destination_city_code: str,
        departure_date: date
    ) -> CacheInfo:
        """Get cache information for flight prices"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT last_fetched_at,
                       jsonb_array_length(data->'data') as records_count
                FROM flight_prices_cache
                WHERE origin_city_code = $1
                  AND destination_city_code = $2
                  AND departure_date = $3
            """, origin_city_code, destination_city_code, departure_date)

            if row:
                return CacheInfo(
                    has_cache=True,
                    last_fetched_at=row['last_fetched_at'],
                    records_count=row['records_count']
                )

            return CacheInfo(has_cache=False)

    @staticmethod
    async def is_cache_valid(
        origin_city_code: str,
        destination_city_code: str,
        departure_date: date,
        currency: str = "PLN"
    ) -> Tuple[bool, Optional[datetime]]:
        """Check if cache exists and is still valid for the given currency"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT MAX(created_at) as last_fetched
                FROM flight_offers
                WHERE origin_city_code = $1
                  AND destination_city_code = $2
                  AND DATE(departure_at) = $3
                  AND currency = $4
            """, origin_city_code, destination_city_code, departure_date, currency.upper())

        if not row or not row['last_fetched']:
            debug_log(f"No price cache for {origin_city_code}->{destination_city_code} on {departure_date} in {currency}")
            return False, None

        last_fetched = row['last_fetched']
        expiry_time = last_fetched + timedelta(hours=FlightPriceService.CACHE_EXPIRY_HOURS)
        is_valid = datetime.now(last_fetched.tzinfo) < expiry_time
        debug_log(
            f"Price cache for {origin_city_code}->{destination_city_code} [{currency}]: "
            f"fetched at {last_fetched}, expires at {expiry_time}, valid: {is_valid}"
        )
        return is_valid, last_fetched

    @staticmethod
    def _parse_offer_from_api(
        offer_data: Dict[str, Any],
        origin_city_code: str,
        destination_city_code: str,
        search_date: date,
        currency: str = "PLN"
    ) -> Optional[Dict[str, Any]]:
        """Parse flight offer from Aviasales API response"""
        try:
            # Required fields
            origin_airport = offer_data.get('origin_airport')
            destination_airport = offer_data.get('destination_airport')
            price = offer_data.get('price')
            departure_at = offer_data.get('departure_at')

            if not origin_airport or not destination_airport or not price or not departure_at:
                debug_log(f"Skipping offer: missing required data")
                return None

            # Parse departure time
            departure_dt = datetime.fromisoformat(str(departure_at).replace('Z', '+00:00'))

            # Only include direct flights (transfers = 0)
            transfers = offer_data.get('transfers', 0)
            if transfers > 0:
                return None

            return {
                'origin_city_code': origin_city_code,
                'destination_city_code': destination_city_code,
                'origin_airport_code': origin_airport,
                'destination_airport_code': destination_airport,
                'price': float(price),
                'currency': currency.upper(),
                'airline_code': offer_data.get('airline'),
                'flight_number': str(offer_data.get('flight_number', '')),
                'departure_at': departure_dt,
                'return_at': None,  # We only use one-way tickets
                'transfers': transfers,
                'return_transfers': offer_data.get('return_transfers'),
                'duration': offer_data.get('duration'),
                'duration_to': offer_data.get('duration_to'),
                'duration_back': offer_data.get('duration_back'),
                'link': offer_data.get('link'),
                'search_date': search_date
            }
        except Exception as e:
            logger.error(f"Error parsing offer: {str(e)}")
            return None

    @staticmethod
    async def _save_offers_to_db(offers_data: List[Dict[str, Any]]) -> int:
        """Save parsed offers to database"""
        saved_count = 0
        async with db.get_connection() as conn:
            for offer_data in offers_data:
                try:
                    await conn.execute("""
                        INSERT INTO flight_offers (
                            origin_city_code, destination_city_code,
                            origin_airport_code, destination_airport_code,
                            price, currency, airline_code, flight_number,
                            departure_at, return_at, transfers, return_transfers,
                            duration, duration_to, duration_back, link, search_date
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
                        ON CONFLICT (origin_airport_code, destination_airport_code, departure_at, flight_number, price)
                        DO UPDATE SET
                            duration = EXCLUDED.duration,
                            duration_to = EXCLUDED.duration_to,
                            link = EXCLUDED.link
                    """,
                        offer_data['origin_city_code'],
                        offer_data['destination_city_code'],
                        offer_data['origin_airport_code'],
                        offer_data['destination_airport_code'],
                        offer_data['price'],
                        offer_data['currency'],
                        offer_data['airline_code'],
                        offer_data['flight_number'],
                        offer_data['departure_at'],
                        offer_data['return_at'],
                        offer_data['transfers'],
                        offer_data['return_transfers'],
                        offer_data['duration'],
                        offer_data['duration_to'],
                        offer_data['duration_back'],
                        offer_data['link'],
                        offer_data['search_date']
                    )
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error saving offer: {str(e)}")
                    continue

        debug_log(f"Saved {saved_count} offers to database")
        return saved_count

    @staticmethod
    async def fetch_and_cache_prices(
        origin_city_code: str,
        destination_city_code: str,
        departure_date: date,
        currency: str = "PLN"
    ) -> Tuple[bool, Optional[datetime]]:
        """Fetch prices from API and cache them"""
        debug_log(f"Fetching prices for {origin_city_code}->{destination_city_code} on {departure_date} in {currency}")

        # Format date for API (YYYY-MM-DD)
        departure_at = departure_date.strftime("%Y-%m-%d")

        # Fetch from API
        api_response = await aviasales_client.get_flight_prices(
            origin=origin_city_code,
            destination=destination_city_code,
            departure_at=departure_at,
            currency=currency,
            one_way=True,
            direct=True,
            limit=1000
        )

        if not api_response or not api_response.get('success'):
            logger.error(f"Failed to fetch prices from API for {origin_city_code}->{destination_city_code}")
            return False, None

        # Save raw cache
        async with db.get_connection() as conn:
            await conn.execute("""
                INSERT INTO flight_prices_cache (origin_city_code, destination_city_code, departure_date, data)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (origin_city_code, destination_city_code, departure_date)
                DO UPDATE SET
                    last_fetched_at = NOW(),
                    data = EXCLUDED.data
            """, origin_city_code, destination_city_code, departure_date, json.dumps(api_response))

        # Parse and save offers
        offers_data = []
        for offer in api_response.get('data', []):
            parsed = FlightPriceService._parse_offer_from_api(
                offer, origin_city_code, destination_city_code, departure_date, currency
            )
            if parsed:
                offers_data.append(parsed)

        await FlightPriceService._save_offers_to_db(offers_data)

        return True, datetime.now()

    @staticmethod
    async def get_offers_for_route(
        origin_airport_code: str,
        destination_airport_code: str,
        departure_date: date,
        origin_city_code: Optional[str] = None,
        destination_city_code: Optional[str] = None,
        currency: str = "PLN",
        force_refresh: bool = False
    ) -> FlightOffersResponse:
        """
        Get flight offers for a specific airport-to-airport route

        Args:
            origin_airport_code: Origin airport IATA code
            destination_airport_code: Destination airport IATA code
            departure_date: Date of departure
            origin_city_code: Origin city code (if not provided, will be looked up)
            destination_city_code: Destination city code (if not provided, will be looked up)
            force_refresh: Force refresh from API
        """
        # Get city codes if not provided
        if not origin_city_code or not destination_city_code:
            async with db.get_connection() as conn:
                if not origin_city_code:
                    origin_city_code = await conn.fetchval(
                        "SELECT city_code FROM airports WHERE code = $1", origin_airport_code
                    )
                if not destination_city_code:
                    destination_city_code = await conn.fetchval(
                        "SELECT city_code FROM airports WHERE code = $1", destination_airport_code
                    )

        if not origin_city_code or not destination_city_code:
            logger.error(f"Could not find city codes for airports {origin_airport_code}, {destination_airport_code}")
            return FlightOffersResponse(data=[], count=0)

        # Check cache validity for this currency
        cache_valid, last_fetched = await FlightPriceService.is_cache_valid(
            origin_city_code, destination_city_code, departure_date, currency
        )

        # Fetch from API if needed
        if force_refresh or not cache_valid:
            success, last_fetched = await FlightPriceService.fetch_and_cache_prices(
                origin_city_code, destination_city_code, departure_date, currency
            )
            if not success:
                debug_log("API fetch failed, returning cached data if available")

        # Get offers from database (filtered by specific airports and currency)
        async with db.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    fo.id, fo.origin_city_code, fo.destination_city_code,
                    fo.origin_airport_code, fo.destination_airport_code,
                    fo.price, fo.currency, fo.airline_code, fo.flight_number,
                    fo.departure_at, fo.return_at, fo.transfers, fo.return_transfers,
                    fo.duration, fo.duration_to, fo.duration_back, fo.link,
                    fo.search_date, fo.created_at,
                    a.name as airline_name,
                    oc.name as origin_city_name,
                    dc.name as destination_city_name,
                    oa.name as origin_airport_name,
                    da.name as destination_airport_name
                FROM flight_offers fo
                LEFT JOIN airlines a ON fo.airline_code = a.code
                LEFT JOIN cities oc ON fo.origin_city_code = oc.code
                LEFT JOIN cities dc ON fo.destination_city_code = dc.code
                LEFT JOIN airports oa ON fo.origin_airport_code = oa.code
                LEFT JOIN airports da ON fo.destination_airport_code = da.code
                WHERE fo.origin_airport_code = $1
                  AND fo.destination_airport_code = $2
                  AND DATE(fo.departure_at) = $3
                  AND fo.transfers = 0
                  AND fo.currency = $4
                ORDER BY fo.price ASC
            """, origin_airport_code, destination_airport_code, departure_date, currency.upper())

            offers = [FlightOffer(**dict(row)) for row in rows]

            return FlightOffersResponse(
                data=offers,
                count=len(offers),
                last_fetched_at=last_fetched
            )

    @staticmethod
    async def get_offers_for_city_pair(
        origin_city_code: str,
        destination_city_code: str,
        departure_date: date,
        currency: str = "PLN",
        force_refresh: bool = False
    ) -> FlightOffersResponse:
        """
        Get all flight offers for a city-to-city pair (all airports in both cities)

        Args:
            origin_city_code: Origin city IATA code
            destination_city_code: Destination city IATA code
            departure_date: Date of departure
            currency: Currency code
            force_refresh: Force refresh from API
        """
        # Check cache validity for this currency
        cache_valid, last_fetched = await FlightPriceService.is_cache_valid(
            origin_city_code, destination_city_code, departure_date, currency
        )

        # Fetch from API if needed
        if force_refresh or not cache_valid:
            success, last_fetched = await FlightPriceService.fetch_and_cache_prices(
                origin_city_code, destination_city_code, departure_date, currency
            )
            if not success:
                debug_log("API fetch failed, returning cached data if available")

        # Get all offers for this city pair filtered by currency
        async with db.get_connection() as conn:
            rows = await conn.fetch("""
                SELECT
                    fo.id, fo.origin_city_code, fo.destination_city_code,
                    fo.origin_airport_code, fo.destination_airport_code,
                    fo.price, fo.currency, fo.airline_code, fo.flight_number,
                    fo.departure_at, fo.return_at, fo.transfers, fo.return_transfers,
                    fo.duration, fo.duration_to, fo.duration_back, fo.link,
                    fo.search_date, fo.created_at,
                    a.name as airline_name,
                    oc.name as origin_city_name,
                    dc.name as destination_city_name,
                    oa.name as origin_airport_name,
                    da.name as destination_airport_name
                FROM flight_offers fo
                LEFT JOIN airlines a ON fo.airline_code = a.code
                LEFT JOIN cities oc ON fo.origin_city_code = oc.code
                LEFT JOIN cities dc ON fo.destination_city_code = dc.code
                LEFT JOIN airports oa ON fo.origin_airport_code = oa.code
                LEFT JOIN airports da ON fo.destination_airport_code = da.code
                WHERE fo.origin_city_code = $1
                  AND fo.destination_city_code = $2
                  AND DATE(fo.departure_at) = $3
                  AND fo.transfers = 0
                  AND fo.currency = $4
                ORDER BY fo.price ASC
            """, origin_city_code, destination_city_code, departure_date, currency.upper())

            offers = [FlightOffer(**dict(row)) for row in rows]

            return FlightOffersResponse(
                data=offers,
                count=len(offers),
                last_fetched_at=last_fetched
            )


flight_price_service = FlightPriceService()
