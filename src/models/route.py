from pydantic import BaseModel
from typing import Optional, Dict, Any, List, Union

class RouteBase(BaseModel):
    id: int
    airline_iata: Optional[str] = None
    departure_airport_iata: str
    arrival_airport_iata: str
    codeshare: bool = False
    transfers: int = 0
    planes: Optional[List[Union[str, Dict[str, Any]]]] = None

class RouteCreate(RouteBase):
    pass

class Route(RouteBase):
    airline_icao: Optional[str] = None
    departure_airport_icao: Optional[str] = None
    arrival_airport_icao: Optional[str] = None
    
    class Config:
        from_attributes = True

class RouteWithDetails(Route):
    airline_name: Optional[str] = None
    departure_airport_name: Optional[str] = None
    departure_city_name: Optional[str] = None
    departure_country_name: Optional[str] = None
    arrival_airport_name: Optional[str] = None
    arrival_city_name: Optional[str] = None
    arrival_country_name: Optional[str] = None

class RouteResponse(BaseModel):
    success: bool = True
    data: RouteWithDetails
    
class RoutesResponse(BaseModel):
    success: bool = True
    data: List[RouteWithDetails]
    count: int

# Helper for GeoJSON conversion
def route_to_geojson_feature(route: Dict[str, Any]) -> Dict[str, Any]:
    """Convert route to GeoJSON feature"""
    feature = {
        "type": "Feature",
        "properties": {
            "id": route.get('id'),
            "airline_iata": route.get('airline_iata'),
            "departure_airport_iata": route.get('departure_airport_iata'),
            "arrival_airport_iata": route.get('arrival_airport_iata'),
            "codeshare": route.get('codeshare', False),
            "transfers": route.get('transfers', 0)
        }
    }
    
    if route.get('dep_coords') and route.get('arr_coords'):
        dep_coords = route['dep_coords']
        arr_coords = route['arr_coords']
        
        feature["geometry"] = {
            "type": "LineString",
            "coordinates": [
                [float(dep_coords.get('lon', 0)), float(dep_coords.get('lat', 0))],
                [float(arr_coords.get('lon', 0)), float(arr_coords.get('lat', 0))]
            ]
        }
    else:
        feature["geometry"] = None
    
    return feature