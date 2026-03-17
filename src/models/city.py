from pydantic import BaseModel
from typing import Optional, Dict, Any, List

class CityBase(BaseModel):
    code: str
    name: str
    country_code: Optional[str] = None
    time_zone: Optional[str] = None
    has_flightable_airport: bool = False

class CityCreate(CityBase):
    pass

class City(CityBase):
    coordinates: Optional[Dict[str, Any]] = None
    name_translations: Optional[Dict[str, Any]] = None
    cases: Optional[Dict[str, Any]] = None
    
    class Config:
        from_attributes = True

class CityResponse(BaseModel):
    success: bool = True
    data: City
    
class CitiesResponse(BaseModel):
    success: bool = True
    data: List[City]
    count: int

# Helper for GeoJSON conversion
def city_to_geojson_feature(city: City) -> Dict[str, Any]:
    """Convert city to GeoJSON feature"""
    feature = {
        "type": "Feature",
        "properties": {
            "code": city.code,
            "name": city.name,
            "country_code": city.country_code,
            "has_flightable_airport": city.has_flightable_airport
        }
    }
    
    if city.coordinates and 'lat' in city.coordinates and 'lon' in city.coordinates:
        feature["geometry"] = {
            "type": "Point",
            "coordinates": [
                float(city.coordinates['lon']),
                float(city.coordinates['lat'])
            ]
        }
    else:
        feature["geometry"] = None
    
    return feature