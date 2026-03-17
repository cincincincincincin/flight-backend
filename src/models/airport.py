from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List

class AirportBase(BaseModel):
    code: str
    name: str
    city_code: Optional[str] = None
    country_code: Optional[str] = None
    time_zone: Optional[str] = None
    flightable: bool = False
    iata_type: Optional[str] = None

class AirportCreate(AirportBase):
    pass

class Airport(AirportBase):
    coordinates: Optional[Dict[str, Any]] = None
    name_translations: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

class AirportResponse(BaseModel):
    success: bool = True
    data: Airport
    
class AirportsResponse(BaseModel):
    success: bool = True
    data: List[Airport]
    count: int

# Helper for GeoJSON conversion
def airport_to_geojson_feature(airport: Airport) -> Dict[str, Any]:
    """Convert airport to GeoJSON feature"""
    feature = {
        "type": "Feature",
        "properties": {
            "code": airport.code,
            "name": airport.name,
            "city_code": airport.city_code,
            "country_code": airport.country_code,
            "flightable": airport.flightable,
            "iata_type": airport.iata_type
        }
    }
    
    if airport.coordinates and 'lat' in airport.coordinates and 'lon' in airport.coordinates:
        feature["geometry"] = {
            "type": "Point",
            "coordinates": [
                float(airport.coordinates['lon']),
                float(airport.coordinates['lat'])
            ]
        }
    else:
        feature["geometry"] = None
    
    return feature