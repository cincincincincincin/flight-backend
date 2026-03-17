from typing import List, Optional, Dict, Any
from src.database import db
from src.models.airport import Airport, airport_to_geojson_feature
import asyncpg

class AirportService:
    
    @staticmethod
    async def get_all_airports(
        flightable_only: bool = False,
        country_code: Optional[str] = None,
        city_code: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Airport]:
        """Get all airports with optional filters"""
        async with db.get_connection() as conn:
            query = """
                SELECT 
                    code, name, city_code, country_code, 
                    time_zone, coordinates, flightable, iata_type,
                    name_translations
                FROM airports
                WHERE 1=1
            """
            params = []
            param_count = 0
            
            if flightable_only:
                query += " AND flightable = TRUE"
            
            if country_code:
                param_count += 1
                query += f" AND country_code = ${param_count}"
                params.append(country_code)
            
            if city_code:
                param_count += 1
                query += f" AND city_code = ${param_count}"
                params.append(city_code)
            
            query += " ORDER BY name"
            
            if limit is not None:
                param_count += 1
                query += f" LIMIT ${param_count}"
                params.append(limit)
            
            if offset > 0:
                param_count += 1
                query += f" OFFSET ${param_count}"
                params.append(offset)
            
            rows = await conn.fetch(query, *params)
            
            airports = []
            for row in rows:
                airport_dict = dict(row)
                airports.append(Airport(**airport_dict))
            
            return airports
    
    @staticmethod
    async def get_airport_by_code(code: str) -> Optional[Airport]:
        """Get airport by IATA code"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    code, name, city_code, country_code, 
                    time_zone, coordinates, flightable, iata_type,
                    name_translations
                FROM airports
                WHERE code = $1
            """, code)
            
            if row:
                return Airport(**dict(row))
            return None
    
    @staticmethod
    async def get_airports_as_geojson(
        flightable_only: bool = False,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get airports as GeoJSON features"""
        async with db.get_connection() as conn:
            query = """
                SELECT
                    a.code, a.name, a.city_code, a.country_code,
                    a.time_zone, a.coordinates, a.flightable, a.iata_type,
                    a.name_translations,
                    c.name as city_name,
                    co.name as country_name
                FROM airports a
                LEFT JOIN cities c ON a.city_code = c.code
                LEFT JOIN countries co ON a.country_code = co.code
                WHERE 1=1
            """
            params = []
            param_count = 0
            
            if flightable_only:
                query += " AND a.flightable = TRUE"
            
            query += " ORDER BY a.name"
            
            if limit is not None:
                param_count += 1
                query += f" LIMIT ${param_count}"
                params.append(limit)
            
            rows = await conn.fetch(query, *params)
            
            features = []
            for row in rows:
                airport_dict = dict(row)
                # Usuń name_translations z properties (opcjonalnie)
                props = {
                    'code': airport_dict['code'],
                    'name': airport_dict['name'],
                    'city_code': airport_dict['city_code'],
                    'city_name': airport_dict['city_name'],
                    'country_code': airport_dict['country_code'],
                    'country_name': airport_dict['country_name'],
                    'flightable': airport_dict['flightable'],
                    'iata_type': airport_dict['iata_type']
                }
                feature = {
                    "type": "Feature",
                    "properties": props,
                    "geometry": None
                }
                if airport_dict.get('coordinates') and 'lat' in airport_dict['coordinates'] and 'lon' in airport_dict['coordinates']:
                    feature["geometry"] = {
                        "type": "Point",
                        "coordinates": [
                            float(airport_dict['coordinates']['lon']),
                            float(airport_dict['coordinates']['lat'])
                        ]
                    }
                if feature.get("geometry"):
                    features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    @staticmethod
    async def get_airports_count(
        flightable_only: bool = False,
        country_code: Optional[str] = None
    ) -> int:
        """Get total count of airports"""
        async with db.get_connection() as conn:
            query = "SELECT COUNT(*) FROM airports WHERE 1=1"
            params = []
            
            if flightable_only:
                query += " AND flightable = TRUE"
            
            if country_code:
                query += " AND country_code = $1"
                params.append(country_code)
            
            count = await conn.fetchval(query, *params)
            return count or 0

airport_service = AirportService()