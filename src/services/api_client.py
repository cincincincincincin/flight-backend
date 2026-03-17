import httpx
from typing import Optional, Dict, Any, List
from datetime import datetime, date
from src.config import settings
import logging

logger = logging.getLogger(__name__)

# Debug flag - set to False to disable debug logging
DEBUG_API_CALLS = True

def debug_log(message: str):
    """Log debug message if DEBUG_API_CALLS is enabled"""
    if DEBUG_API_CALLS:
        logger.debug(message)

class AeroDataBoxClient:
    """Client for AeroDataBox API (flight schedules)"""

    BASE_URL = "https://aerodatabox.p.rapidapi.com"

    def __init__(self):
        self.api_key = settings.aerodatabox_api_key
        self.rapidapi_host = settings.rapidapi_host
        self.headers = {
            "Accept": "application/json",
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.rapidapi_host
        }

    async def get_airport_departures(
        self,
        airport_code: str,
        from_local: str,
        to_local: str,
        direction: str = "Departure",
        with_cancelled: bool = False,
        with_cargo: bool = False,
        with_codeshared: bool = True,
        with_leg: bool = True,
        with_private: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get departures/arrivals from airport by local time range

        Args:
            airport_code: IATA airport code (e.g., 'WAW')
            from_local: Start time in format YYYY-MM-DDTHH:mm
            to_local: End time in format YYYY-MM-DDTHH:mm (max 12 hours from start)
            direction: 'Departure', 'Arrival', or 'Both'
            with_cancelled: Include cancelled flights
            with_cargo: Include cargo flights
            with_codeshared: Include codeshared flights
            with_leg: Include opposite leg information (departure/arrival details)
            with_private: Include private flights
        """
        url = f"{self.BASE_URL}/flights/airports/iata/{airport_code}/{from_local}/{to_local}"

        params = {
            "direction": direction,
            "withCancelled": str(with_cancelled).lower(),
            "withCargo": str(with_cargo).lower(),
            "withCodeshared": str(with_codeshared).lower(),
            "withLeg": str(with_leg).lower(),
            "withPrivate": str(with_private).lower()
        }

        debug_log(f"AeroDataBox API call: {url} with params: {params}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, headers=self.headers, params=params)
                response.raise_for_status()
                if response.status_code == 204:
                    debug_log(f"AeroDataBox API returned 204 No Content for {airport_code} ({from_local} – {to_local}): no flights in window")
                    return {"departures": [], "arrivals": []}
                data = response.json()
                debug_log(f"AeroDataBox API response: {len(data.get('departures', []) + data.get('arrivals', []))} flights")
                return data
        except httpx.HTTPStatusError as e:
            logger.error(f"AeroDataBox API HTTP error: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            logger.error(f"AeroDataBox API request error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"AeroDataBox API unexpected error: {str(e)}")
            return None


class AviasalesClient:
    """Client for Aviasales/Travelpayouts API (flight prices)"""

    BASE_URL = "https://api.travelpayouts.com/aviasales/v3/prices_for_dates"

    def __init__(self):
        self.api_token = settings.aviasales_api_token

    async def get_flight_prices(
        self,
        origin: str,
        destination: str,
        departure_at: str,
        currency: str = "USD",
        one_way: bool = True,
        direct: bool = True,
        limit: int = 1000,
        sorting: str = "price"
    ) -> Optional[Dict[str, Any]]:
        """
        Get flight prices for origin-destination pair

        Args:
            origin: IATA city code (e.g., 'WAW')
            destination: IATA city code (e.g., 'BCN')
            departure_at: Departure date in format YYYY-MM or YYYY-MM-DD
            currency: Currency code (default: USD)
            one_way: True for one-way tickets, False for round-trip
            direct: True for direct flights only
            limit: Maximum number of results (max 1000)
            sorting: 'price' or 'route'
        """
        params = {
            "origin": origin,
            "destination": destination,
            "departure_at": departure_at,
            "currency": currency.lower(),
            "one_way": str(one_way).lower(),
            "direct": str(direct).lower(),
            "limit": limit,
            "sorting": sorting,
            "token": self.api_token
        }

        debug_log(f"Aviasales API call: {self.BASE_URL} with params: {params}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()

                if not data.get("success", False):
                    logger.error(f"Aviasales API returned success=false: {data.get('error', 'Unknown error')}")
                    return None

                debug_log(f"Aviasales API response: {len(data.get('data', []))} offers")
                return data
        except httpx.HTTPStatusError as e:
            logger.error(f"Aviasales API HTTP error: {e.response.status_code} - {e.response.text}")
            return None
        except httpx.RequestError as e:
            logger.error(f"Aviasales API request error: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Aviasales API unexpected error: {str(e)}")
            return None


# Singleton instances
aerodatabox_client = AeroDataBoxClient()
aviasales_client = AviasalesClient()
