from typing import List, Optional, Dict, Any
from src.database import db
from src.models.city import City, city_to_geojson_feature
import asyncpg

class CityService:
    
    @staticmethod
    async def get_all_cities(
        has_airport_only: bool = False,
        country_code: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[City]:
        """Get all cities with optional filters"""
        async with db.get_connection() as conn:
            query = """
                SELECT 
                    code, name, country_code, time_zone, 
                    coordinates, has_flightable_airport,
                    name_translations, cases
                FROM cities
                WHERE 1=1
            """
            params = []
            param_count = 0
            
            if has_airport_only:
                query += " AND has_flightable_airport = TRUE"
            
            if country_code:
                param_count += 1
                query += f" AND country_code = ${param_count}"
                params.append(country_code)
            
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
            
            cities = []
            for row in rows:
                city_dict = dict(row)
                cities.append(City(**city_dict))
            
            return cities
    
    @staticmethod
    async def get_city_by_code(code: str) -> Optional[City]:
        """Get city by code"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    code, name, country_code, time_zone, 
                    coordinates, has_flightable_airport,
                    name_translations, cases
                FROM cities
                WHERE code = $1
            """, code)
            
            if row:
                return City(**dict(row))
            return None
    
    @staticmethod
    async def get_cities_as_geojson(
        has_airport_only: bool = False,
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Get cities as GeoJSON features"""
        async with db.get_connection() as conn:
            query = """
                SELECT 
                    code, name, country_code, time_zone, 
                    coordinates, has_flightable_airport,
                    name_translations, cases
                FROM cities
                WHERE 1=1
            """
            params = []
            param_count = 0
            
            if has_airport_only:
                query += " AND has_flightable_airport = TRUE"
            
            query += " ORDER BY name"
            
            if limit is not None:
                param_count += 1
                query += f" LIMIT ${param_count}"
                params.append(limit)
            
            rows = await conn.fetch(query, *params)
            
            features = []
            for row in rows:
                city_dict = dict(row)
                city = City(**city_dict)
                feature = city_to_geojson_feature(city)
                if feature.get("geometry"):  # Only include cities with coordinates
                    features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    @staticmethod
    async def get_cities_count(
        has_airport_only: bool = False,
        country_code: Optional[str] = None
    ) -> int:
        """Get total count of cities"""
        async with db.get_connection() as conn:
            query = "SELECT COUNT(*) FROM cities WHERE 1=1"
            params = []
            
            if has_airport_only:
                query += " AND has_flightable_airport = TRUE"
            
            if country_code:
                query += " AND country_code = $1"
                params.append(country_code)
            
            count = await conn.fetchval(query, *params)
            return count or 0

city_service = CityService()