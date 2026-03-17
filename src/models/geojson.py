from pydantic import BaseModel, Field, validator
from typing import List, Union, Dict, Any, Optional, Literal

class PointGeometry(BaseModel):
    type: Literal["Point"] = "Point"
    coordinates: List[float] = Field(...)
    
    @validator('coordinates')
    def validate_coordinates(cls, v):
        if len(v) >= 2:
            lon, lat = v[0], v[1]
            if not (-180 <= lon <= 180):
                raise ValueError('Longitude must be between -180 and 180')
            if not (-90 <= lat <= 90):
                raise ValueError('Latitude must be between -90 and 90')
        return v

class LineStringGeometry(BaseModel):
    type: Literal["LineString"] = "LineString"
    coordinates: List[List[float]]
    
    @validator('coordinates')
    def validate_coordinates(cls, v):
        if len(v) < 2:
            raise ValueError('LineString must have at least 2 coordinates')
        for coord in v:
            if len(coord) < 2:
                raise ValueError('Each coordinate must have at least 2 values')
            lon, lat = coord[0], coord[1]
            if not (-180 <= lon <= 180):
                raise ValueError('Longitude must be between -180 and 180')
            if not (-90 <= lat <= 90):
                raise ValueError('Latitude must be between -90 and 90')
        return v

class Feature(BaseModel):
    type: Literal["Feature"] = "Feature"
    geometry: Union[PointGeometry, LineStringGeometry]
    properties: Dict[str, Any]
    id: Optional[Union[str, int]] = None

class FeatureCollection(BaseModel):
    type: Literal["FeatureCollection"] = "FeatureCollection"
    features: List[Feature]
    
    @classmethod
    def from_features(cls, features: List[Feature]):
        return cls(features=features)