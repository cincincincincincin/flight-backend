from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional
from src.services.airport_service import airport_service
from src.models.airport import AirportsResponse, AirportResponse
from src.models.geojson import FeatureCollection
from src.cache import cache
from src.limiter import limiter

router = APIRouter(prefix="/airports", tags=["airports"])

TTL_GEOJSON = 86400
TTL_ENTITY = 86400

@router.get("/", response_model=AirportsResponse)
@limiter.limit("200/minute")
async def get_airports(
    request: Request,
    flightable_only: bool = Query(False, description="Show only flightable airports"),
    country_code: Optional[str] = Query(None, description="Filter by country code"),
    city_code: Optional[str] = Query(None, description="Filter by city code"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Get all airports with optional filters"""
    try:
        airports = await airport_service.get_all_airports(
            flightable_only=flightable_only,
            country_code=country_code,
            city_code=city_code,
            limit=limit,
            offset=offset
        )
        
        count = await airport_service.get_airports_count(
            flightable_only=flightable_only,
            country_code=country_code
        )
        
        return AirportsResponse(
            data=airports,
            count=count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/geojson")
@limiter.limit("200/minute")
async def get_airports_geojson(
    request: Request,
    flightable_only: bool = Query(False, description="Show only flightable airports"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)")
):
    """Get airports as GeoJSON for mapping"""
    try:
        key = f"geojson:airports:{flightable_only}:{limit}"
        return await cache.cached(key, TTL_GEOJSON,
            lambda: airport_service.get_airports_as_geojson(flightable_only=flightable_only, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading airports: {str(e)}")

@router.get("/by-country/{country_code}", response_model=AirportsResponse)
@limiter.limit("200/minute")
async def get_airports_by_country(request: Request, country_code: str):
    """Get all flightable airports for a country with timezone data (single cached query)"""
    key = f"airports:country:{country_code.upper()}"
    airports = await cache.cached(key, TTL_ENTITY,
        lambda: airport_service.get_all_airports(country_code=country_code.upper(), flightable_only=True))
    if airports is None:
        airports = []
    return AirportsResponse(data=airports, count=len(airports))

@router.get("/{code}", response_model=AirportResponse)
@limiter.limit("200/minute")
async def get_airport(request: Request, code: str):
    """Get airport by IATA code"""
    key = f"airport:{code.upper()}"
    airport = await cache.cached(key, TTL_ENTITY, lambda: airport_service.get_airport_by_code(code))
    if not airport:
        raise HTTPException(status_code=404, detail="Airport not found")
    return AirportResponse(data=airport)