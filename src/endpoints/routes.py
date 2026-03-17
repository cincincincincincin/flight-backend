from fastapi import APIRouter, Query, HTTPException, Request
from typing import Optional
from src.services.route_service import route_service
from src.models.route import RoutesResponse, RouteResponse
from pydantic import BaseModel
from src.cache import cache
from src.limiter import limiter

router = APIRouter(prefix="/routes", tags=["routes"])

TTL_GEOJSON = 86400

# Model dla odpowiedzi count
class RoutesCountResponse(BaseModel):
    count: int

@router.get("/count", response_model=RoutesCountResponse)
@limiter.limit("200/minute")
async def get_routes_count(request: Request):
    """Get total count of routes"""
    try:
        count = await route_service.get_routes_count()
        return RoutesCountResponse(count=count)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/", response_model=RoutesResponse)
@limiter.limit("200/minute")
async def get_routes(
    request: Request,
    airline_iata: Optional[str] = Query(None, description="Filter by airline IATA code"),
    departure_airport: Optional[str] = Query(None, description="Filter by departure airport IATA"),
    arrival_airport: Optional[str] = Query(None, description="Filter by arrival airport IATA"),
    direct_only: bool = Query(False, description="Show only direct routes (no transfers)"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Get all routes with optional filters"""
    try:
        routes = await route_service.get_all_routes(
            airline_iata=airline_iata,
            departure_airport=departure_airport,
            arrival_airport=arrival_airport,
            direct_only=direct_only,
            limit=limit,
            offset=offset
        )
        
        count = await route_service.get_routes_count(
            airline_iata=airline_iata,
            direct_only=direct_only
        )
        
        return RoutesResponse(
            data=routes,
            count=count
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/geojson")
@limiter.limit("200/minute")
async def get_routes_geojson(
    request: Request,
    airline_iata: Optional[str] = Query(None, description="Filter by airline IATA code"),
    limit: Optional[int] = Query(None, description="Limit results (None = no limit)"),
    offset: int = Query(0, ge=0, description="Offset for pagination")
):
    """Get routes as GeoJSON LineStrings for mapping"""
    try:
        key = f"geojson:routes:{airline_iata}:{limit}:{offset}"
        return await cache.cached(key, TTL_GEOJSON,
            lambda: route_service.get_routes_as_geojson(airline_iata=airline_iata, limit=limit, offset=offset))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{route_id}", response_model=RouteResponse)
@limiter.limit("200/minute")
async def get_route(request: Request, route_id: int):
    """Get route by ID"""
    route = await route_service.get_route_by_id(route_id)
    if not route:
        raise HTTPException(status_code=404, detail="Route not found")
    
    return RouteResponse(data=route)