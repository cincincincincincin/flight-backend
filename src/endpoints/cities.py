from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional
from src.services.city_service import city_service
from src.models.city import CitiesResponse, CityResponse
from src.cache import cache
from src.limiter import limiter

router = APIRouter(prefix="/cities", tags=["cities"])

TTL_GEOJSON = 86400
TTL_ENTITY = 86400

@router.get("/", response_model=CitiesResponse)
@limiter.limit("200/minute")
async def get_cities(
    request: Request,
    has_airport_only: bool = Query(False, description="Show only cities with airports"),
    country_code: Optional[str] = Query(None, description="Filter by country code"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Get all cities with optional filters"""
    try:
        cities = await city_service.get_all_cities(
            has_airport_only=has_airport_only,
            country_code=country_code,
            limit=limit,
            offset=offset
        )
        
        count = await city_service.get_cities_count(
            has_airport_only=has_airport_only,
            country_code=country_code
        )
        
        return CitiesResponse(
            data=cities,
            count=count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/geojson")
@limiter.limit("200/minute")
async def get_cities_geojson(
    request: Request,
    has_airport_only: bool = Query(False, description="Show only cities with airports"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)")
):
    """Get cities as GeoJSON for mapping"""
    try:
        key = f"geojson:cities:{has_airport_only}:{limit}"
        return await cache.cached(key, TTL_GEOJSON,
            lambda: city_service.get_cities_as_geojson(has_airport_only=has_airport_only, limit=limit))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{code}", response_model=CityResponse)
@limiter.limit("200/minute")
async def get_city(request: Request, code: str):
    """Get city by code"""
    key = f"city:{code.upper()}"
    city = await cache.cached(key, TTL_ENTITY, lambda: city_service.get_city_by_code(code))
    if not city:
        raise HTTPException(status_code=404, detail="City not found")
    return CityResponse(data=city)