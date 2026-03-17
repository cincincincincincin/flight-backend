from typing import List, Optional, Dict, Any
from src.database import db
from src.models.route import RouteWithDetails, route_to_geojson_feature
import asyncpg

class RouteService:
    
    @staticmethod
    async def get_all_routes(
        airline_iata: Optional[str] = None,
        departure_airport: Optional[str] = None,
        arrival_airport: Optional[str] = None,
        direct_only: bool = False,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[RouteWithDetails]:
        """Get all routes with optional filters"""
        async with db.get_connection() as conn:
            query = """
                SELECT 
                    r.id,
                    r.airline_iata,
                    r.airline_icao,
                    r.departure_airport_iata,
                    r.departure_airport_icao,
                    r.arrival_airport_iata,
                    r.arrival_airport_icao,
                    r.codeshare,
                    r.transfers,
                    r.planes,
                    a.name as airline_name,
                    ap1.name as departure_airport_name,
                    c1.name as departure_city_name,
                    co1.name as departure_country_name,
                    ap2.name as arrival_airport_name,
                    c2.name as arrival_city_name,
                    co2.name as arrival_country_name
                FROM routes r
                LEFT JOIN airlines a ON r.airline_iata = a.code
                LEFT JOIN airports ap1 ON r.departure_airport_iata = ap1.code
                LEFT JOIN cities c1 ON ap1.city_code = c1.code
                LEFT JOIN countries co1 ON ap1.country_code = co1.code
                LEFT JOIN airports ap2 ON r.arrival_airport_iata = ap2.code
                LEFT JOIN cities c2 ON ap2.city_code = c2.code
                LEFT JOIN countries co2 ON ap2.country_code = co2.code
                WHERE r.departure_airport_iata IS NOT NULL 
                  AND r.arrival_airport_iata IS NOT NULL
            """
            params = []
            param_count = 0
            
            if airline_iata:
                param_count += 1
                query += f" AND r.airline_iata = ${param_count}"
                params.append(airline_iata)
            
            if departure_airport:
                param_count += 1
                query += f" AND r.departure_airport_iata = ${param_count}"
                params.append(departure_airport)
            
            if arrival_airport:
                param_count += 1
                query += f" AND r.arrival_airport_iata = ${param_count}"
                params.append(arrival_airport)
            
            if direct_only:
                query += " AND r.transfers = 0"
            
            query += " ORDER BY r.id"
            
            if limit is not None:
                param_count += 1
                query += f" LIMIT ${param_count}"
                params.append(limit)
            
            if offset > 0:
                param_count += 1
                query += f" OFFSET ${param_count}"
                params.append(offset)
            
            rows = await conn.fetch(query, *params)
            
            routes = []
            for row in rows:
                route_dict = dict(row)
                # Konwertuj planes jeśli to string do listy
                if isinstance(route_dict.get('planes'), str):
                    route_dict['planes'] = [route_dict['planes']]
                elif route_dict.get('planes') is None:
                    route_dict['planes'] = []
                routes.append(RouteWithDetails(**route_dict))
            
            return routes
    
    @staticmethod
    async def get_routes_as_geojson(
        airline_iata: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0  # DODANY PARAMETR OFFSET
    ) -> Dict[str, Any]:
        """Get routes as GeoJSON features (LineStrings)"""
        async with db.get_connection() as conn:
            query = """
                SELECT 
                    r.id,
                    r.airline_iata,
                    r.departure_airport_iata,
                    r.arrival_airport_iata,
                    r.codeshare,
                    r.transfers,
                    ap1.coordinates as dep_coords,
                    ap2.coordinates as arr_coords
                FROM routes r
                LEFT JOIN airports ap1 ON r.departure_airport_iata = ap1.code
                LEFT JOIN airports ap2 ON r.arrival_airport_iata = ap2.code
                WHERE r.departure_airport_iata IS NOT NULL 
                  AND r.arrival_airport_iata IS NOT NULL
                  AND ap1.coordinates IS NOT NULL
                  AND ap2.coordinates IS NOT NULL
                  AND ap1.coordinates->>'lat' IS NOT NULL
                  AND ap1.coordinates->>'lon' IS NOT NULL
                  AND ap2.coordinates->>'lat' IS NOT NULL
                  AND ap2.coordinates->>'lon' IS NOT NULL
            """
            params = []
            param_count = 0
            
            if airline_iata:
                param_count += 1
                query += f" AND r.airline_iata = ${param_count}"
                params.append(airline_iata)
            
            query += " ORDER BY r.id"
            
            if limit is not None:
                param_count += 1
                query += f" LIMIT ${param_count}"
                params.append(limit)
            
            if offset > 0:
                param_count += 1
                query += f" OFFSET ${param_count}"
                params.append(offset)
            
            rows = await conn.fetch(query, *params)
            
            features = []
            for row in rows:
                route_dict = dict(row)
                feature = route_to_geojson_feature(route_dict)
                if feature.get("geometry"):  # Only include routes with coordinates
                    features.append(feature)
        
        return {
            "type": "FeatureCollection",
            "features": features
        }
    
    @staticmethod
    async def get_routes_count(
        airline_iata: Optional[str] = None,
        direct_only: bool = False
    ) -> int:
        """Get total count of routes"""
        async with db.get_connection() as conn:
            query = """
                SELECT COUNT(*) 
                FROM routes 
                WHERE departure_airport_iata IS NOT NULL 
                  AND arrival_airport_iata IS NOT NULL
            """
            params = []
            
            if airline_iata:
                query += " AND airline_iata = $1"
                params.append(airline_iata)
            
            if direct_only:
                query += " AND transfers = 0"
            
            count = await conn.fetchval(query, *params)
            return count or 0

    @staticmethod
    async def get_route_by_id(route_id: int) -> Optional[RouteWithDetails]:
        """Get route by ID"""
        async with db.get_connection() as conn:
            row = await conn.fetchrow("""
                SELECT 
                    r.id,
                    r.airline_iata,
                    r.airline_icao,
                    r.departure_airport_iata,
                    r.departure_airport_icao,
                    r.arrival_airport_iata,
                    r.arrival_airport_icao,
                    r.codeshare,
                    r.transfers,
                    r.planes,
                    a.name as airline_name,
                    ap1.name as departure_airport_name,
                    c1.name as departure_city_name,
                    co1.name as departure_country_name,
                    ap2.name as arrival_airport_name,
                    c2.name as arrival_city_name,
                    co2.name as arrival_country_name
                FROM routes r
                LEFT JOIN airlines a ON r.airline_iata = a.code
                LEFT JOIN airports ap1 ON r.departure_airport_iata = ap1.code
                LEFT JOIN cities c1 ON ap1.city_code = c1.code
                LEFT JOIN countries co1 ON ap1.country_code = co1.code
                LEFT JOIN airports ap2 ON r.arrival_airport_iata = ap2.code
                LEFT JOIN cities c2 ON ap2.city_code = c2.code
                LEFT JOIN countries co2 ON ap2.country_code = co2.code
                WHERE r.id = $1
            """, route_id)

            if row:
                route_dict = dict(row)
                # Konwertuj planes jeśli to string do listy
                if isinstance(route_dict.get('planes'), str):
                    route_dict['planes'] = [route_dict['planes']]
                elif route_dict.get('planes') is None:
                    route_dict['planes'] = []
                return RouteWithDetails(**route_dict)
            return None

route_service = RouteService()