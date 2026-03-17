import statistics
from typing import List, Dict, Optional, Any, Tuple
from src.database import db
import asyncpg
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def filter_outliers(coords_list: List[Tuple[float, float]], max_degrees: float = 5.0) -> List[Tuple[float, float]]:
    """
    Iteracyjnie odrzuca punkty oddalone o więcej niż max_degrees od mediany.
    Zwraca listę współrzędnych GŁÓWNEGO SKUPISKA (kontynent).
    """
    if len(coords_list) <= 1:
        return coords_list

    current = coords_list[:]
    prev_len = 0

    while len(current) != prev_len:
        prev_len = len(current)
        lons = [c[0] for c in current]
        lats = [c[1] for c in current]
        median_lon = statistics.median(lons)
        median_lat = statistics.median(lats)

        filtered = []
        for lon, lat in current:
            dist = ((lon - median_lon) ** 2 + (lat - median_lat) ** 2) ** 0.5
            if dist <= max_degrees:
                filtered.append((lon, lat))

        if not filtered:
            return current[:prev_len]
        current = filtered

    return current

class SearchService:
    """Service for handling sequential phase search operations"""
    
    @staticmethod
    async def determine_search_mode(query: str) -> str:
        """
        Determine search mode (prefix/contains) based on all phases.
        Uses prefix search first, falls back to contains only if ALL phases are empty.
        """
        try:
            logger.info(f"[SEARCH] Determining search mode for query: '{query}'")
            
            # If query is empty, always use prefix (to get all countries)
            if not query or query.strip() == "":
                return "prefix"
            
            # Check if prefix search returns any results in ANY phase
            # We need to check countries, cities, and airports
            has_any_results = False
            
            # Check countries with prefix
            countries_count = await SearchService.get_countries_count_by_prefix(query)
            if countries_count > 0:
                has_any_results = True
            
            # Check cities with prefix
            if not has_any_results:
                cities_count = await SearchService.get_cities_count_by_prefix(query)
                if cities_count > 0:
                    has_any_results = True
            
            # Check airports with prefix
            if not has_any_results:
                airports_count = await SearchService.get_airports_count_by_prefix(query)
                if airports_count > 0:
                    has_any_results = True
            
            if has_any_results:
                logger.info(f"[SEARCH] Using PREFIX mode for query: '{query}'")
                return "prefix"
            else:
                logger.info(f"[SEARCH] No prefix results, using CONTAINS mode for query: '{query}'")
                return "contains"
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to determine search mode: {str(e)}")
            # Default to prefix on error
            return "prefix"
    
    @staticmethod
    async def get_sequential_phase_results(
        query: str,
        offset: int,
        limit: int,
        mode: str
    ) -> Dict[str, Any]:
        """
        Get results for the current phase based on offset.
        
        For empty query: Only phase 1 (countries)
        For non-empty query: 
          Phase 1: Matching countries
          Phase 2: Countries with matching cities
          Phase 3: Full hierarchy with matching airports
        
        Returns appropriate phase data based on offset.
        """
        try:
            logger.info(f"[SEARCH] Getting phase results for query='{query}', "
                       f"offset={offset}, limit={limit}, mode={mode}")
            
            search_query = query.strip()
            
            # For empty query: only phase 1 (countries)
            if not search_query:
                phase = 1
                data = await SearchService.get_phase1_countries(
                    search_query, offset, limit, mode
                )
                # Check if there are more countries
                total_countries = await SearchService.get_countries_count_by_mode(search_query, mode)
                has_more = (offset + len(data)) < total_countries
                
                logger.info(f"[SEARCH] Empty query, Phase 1: {len(data)} countries, total: {total_countries}, has_more: {has_more}")
                return {
                    "phase": phase,
                    "data": data,
                    "has_more": has_more,
                    "total_in_phase": total_countries,
                    "next_phase_available": False  # Empty query has no phase 2 or 3
                }
            
            # For non-empty query: need to determine which phase based on offset
            # and available data
            
            # Get counts for each phase
            phase1_count = await SearchService.get_countries_count_by_mode(search_query, mode)
            phase2_count = await SearchService.get_countries_with_cities_count_by_mode(search_query, mode)
            phase3_count = await SearchService.get_countries_with_airports_count_by_mode(search_query, mode)
            
            logger.info(f"[SEARCH] Phase counts - Countries: {phase1_count}, "
                       f"Countries with cities: {phase2_count}, "
                       f"Countries with airports: {phase3_count}")
            
            # Determine current phase based on offset and phase counts
            if phase1_count > 0 and offset < phase1_count:
                # Still in phase 1 (countries)
                phase = 1
                phase_offset = offset
                data = await SearchService.get_phase1_countries(
                    search_query, phase_offset, limit, mode
                )
                has_more = (phase_offset + len(data)) < phase1_count
                next_phase_available = phase2_count > 0 or phase3_count > 0
                
            elif phase2_count > 0 and offset < phase1_count + phase2_count:
                # In phase 2 (countries with cities)
                phase = 2
                phase_offset = offset - phase1_count
                data = await SearchService.get_phase2_countries_with_cities(
                    search_query, phase_offset, limit, mode
                )
                has_more = (phase_offset + len(data)) < phase2_count
                next_phase_available = phase3_count > 0
                
            elif phase3_count > 0:
                # In phase 3 (full hierarchy)
                phase = 3
                phase_offset = offset - phase1_count - phase2_count
                data = await SearchService.get_phase3_full_hierarchy(
                    search_query, phase_offset, limit, mode
                )
                # For phase 3, we don't know total count, so assume more if we got limit
                has_more = len(data) == limit
                next_phase_available = False
                
            else:
                # No data in any phase
                phase = 1
                data = []
                has_more = False
                next_phase_available = False
            
            logger.info(f"[SEARCH] Phase {phase} results: {len(data)} items, "
                       f"has_more: {has_more}, next_phase_available: {next_phase_available}")
            
            return {
                "phase": phase,
                "data": data,
                "has_more": has_more,
                "total_in_phase": phase1_count if phase == 1 else phase2_count if phase == 2 else 0,
                "next_phase_available": next_phase_available
            }
            
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get phase results: {str(e)}")
            logger.exception(e)
            raise
    
    @staticmethod
    async def has_phase2_results(query: str, mode: str) -> bool:
        """Check if there are any phase 2 results (countries with matching cities)"""
        count = await SearchService.get_countries_with_cities_count_by_mode(query, mode)
        return count > 0
    
    @staticmethod
    async def has_phase3_results(query: str, mode: str) -> bool:
        """Check if there are any phase 3 results (full hierarchy)"""
        count = await SearchService.get_countries_with_airports_count_by_mode(query, mode)
        return count > 0
    
    # ================ COUNTING METHODS ================
    
    @staticmethod
    async def get_countries_count_by_mode(query: str, mode: str) -> int:
        """Get count of countries matching query with given mode"""
        try:
            if not query or query.strip() == "":
                # For empty query, count all countries
                async with db.get_connection() as conn:
                    sql = "SELECT COUNT(*) FROM countries"
                    return await conn.fetchval(sql) or 0
            
            async with db.get_connection() as conn:
                if mode == "prefix":
                    pattern = f"{query}%"
                else:  # contains
                    pattern = f"%{query}%"
                
                sql = """
                    SELECT COUNT(*) 
                    FROM countries 
                    WHERE LOWER(name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count countries: {str(e)}")
            return 0
    
    @staticmethod
    async def get_countries_with_cities_count_by_mode(query: str, mode: str) -> int:
        """Get count of countries with matching cities"""
        try:
            if not query or query.strip() == "":
                return 0
            
            async with db.get_connection() as conn:
                if mode == "prefix":
                    pattern = f"{query}%"
                else:  # contains
                    pattern = f"%{query}%"
                
                sql = """
                    SELECT COUNT(DISTINCT c.code)
                    FROM countries c
                    JOIN cities city ON c.code = city.country_code
                    WHERE LOWER(city.name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count countries with cities: {str(e)}")
            return 0
    
    @staticmethod
    async def get_countries_with_airports_count_by_mode(query: str, mode: str) -> int:
        """Get count of countries with matching airports"""
        try:
            if not query or query.strip() == "":
                return 0
            
            async with db.get_connection() as conn:
                if mode == "prefix":
                    pattern = f"{query}%"
                else:  # contains
                    pattern = f"%{query}%"
                
                sql = """
                    SELECT COUNT(DISTINCT a.country_code)
                    FROM airports a
                    WHERE LOWER(a.name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count countries with airports: {str(e)}")
            return 0
    
    @staticmethod
    async def get_countries_count_by_prefix(query: str) -> int:
        """Get count of countries matching prefix (for mode determination)"""
        try:
            if not query or query.strip() == "":
                return 0
            
            async with db.get_connection() as conn:
                pattern = f"{query}%"
                sql = """
                    SELECT COUNT(*) 
                    FROM countries 
                    WHERE LOWER(name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count countries by prefix: {str(e)}")
            return 0
    
    @staticmethod
    async def get_cities_count_by_prefix(query: str) -> int:
        """Get count of cities matching prefix (for mode determination)"""
        try:
            if not query or query.strip() == "":
                return 0
            
            async with db.get_connection() as conn:
                pattern = f"{query}%"
                sql = """
                    SELECT COUNT(*) 
                    FROM cities 
                    WHERE LOWER(name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count cities by prefix: {str(e)}")
            return 0
    
    @staticmethod
    async def get_airports_count_by_prefix(query: str) -> int:
        """Get count of airports matching prefix (for mode determination)"""
        try:
            if not query or query.strip() == "":
                return 0
            
            async with db.get_connection() as conn:
                pattern = f"{query}%"
                sql = """
                    SELECT COUNT(*) 
                    FROM airports 
                    WHERE LOWER(name) LIKE LOWER($1)
                """
                return await conn.fetchval(sql, pattern) or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count airports by prefix: {str(e)}")
            return 0
    
    # ================ GET ITEM BY CODE METHODS ================
    
    @staticmethod
    async def get_airport_by_code(airport_code: str) -> Optional[Dict[str, Any]]:
        """Get airport by code with full details"""
        try:
            async with db.get_connection() as conn:
                sql = """
                    SELECT 
                        a.code,
                        a.name,
                        a.city_code,
                        a.country_code,
                        a.time_zone,
                        a.coordinates,
                        a.flightable,
                        a.iata_type,
                        a.name_translations,
                        c.name as city_name,
                        co.name as country_name
                    FROM airports a
                    LEFT JOIN cities c ON a.city_code = c.code
                    LEFT JOIN countries co ON a.country_code = co.code
                    WHERE a.code = $1
                """
                row = await conn.fetchrow(sql, airport_code)
                
                if row:
                    airport = dict(row)
                    airport['type'] = 'airport'
                    return airport
                return None
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get airport: {str(e)}")
            raise
    
    @staticmethod
    async def get_city_by_code(city_code: str) -> Optional[Dict[str, Any]]:
        """Get city by code with full details"""
        try:
            async with db.get_connection() as conn:
                sql = """
                    SELECT 
                        c.code,
                        c.name,
                        c.country_code,
                        c.time_zone,
                        c.coordinates,
                        c.has_flightable_airport,
                        c.name_translations,
                        c.cases,
                        co.name as country_name
                    FROM cities c
                    LEFT JOIN countries co ON c.country_code = co.code
                    WHERE c.code = $1
                """
                row = await conn.fetchrow(sql, city_code)
                
                if row:
                    city = dict(row)
                    city['type'] = 'city'
                    return city
                return None
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get city: {str(e)}")
            raise
    
    @staticmethod
    async def get_country_by_code(country_code: str) -> Optional[Dict[str, Any]]:
        """Get country by code with full details"""
        try:
            async with db.get_connection() as conn:
                sql = """
                    SELECT 
                        code,
                        name,
                        name_translations,
                        currency,
                        cases
                    FROM countries 
                    WHERE code = $1
                """
                row = await conn.fetchrow(sql, country_code)
                
                if row:
                    country = dict(row)
                    country['type'] = 'country'
                    return country
                return None
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get country: {str(e)}")
            raise
    
    # ================ PHASE 1: COUNTRIES ONLY ================
    
    @staticmethod
    async def get_phase1_countries(
        query: str,
        offset: int,
        limit: int,
        mode: str
    ) -> List[Dict[str, Any]]:
        """
        Phase 1: Get countries only (collapsed by default)
        
        For empty query: all countries
        For non-empty query: matching countries based on mode
        """
        try:
            logger.info(f"[SEARCH PHASE1] Getting countries: query='{query}', "
                       f"offset={offset}, limit={limit}, mode={mode}")
            start_time = datetime.now()
            
            async with db.get_connection() as conn:
                if not query or query == "":
                    # Empty query: get all countries
                    sql = """
                        SELECT 
                            code,
                            name,
                            currency,
                            name_translations
                        FROM countries 
                        ORDER BY name
                        LIMIT $1 OFFSET $2
                    """
                    rows = await conn.fetch(sql, limit, offset)
                else:
                    # Non-empty query: get matching countries
                    if mode == "prefix":
                        pattern = f"{query}%"
                    else:  # contains
                        pattern = f"%{query}%"
                    
                    sql = """
                        SELECT 
                            code,
                            name,
                            currency,
                            name_translations
                        FROM countries 
                        WHERE LOWER(name) LIKE LOWER($1)
                        ORDER BY name
                        LIMIT $2 OFFSET $3
                    """
                    rows = await conn.fetch(sql, pattern, limit, offset)
                
                countries = []
                for row in rows:
                    country = dict(row)
                    country['type'] = 'country'
                    country['_collapsed'] = True  # Countries collapsed in phase 1
                    countries.append(country)
                
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"[SEARCH PHASE1] Found {len(countries)} countries in {elapsed:.3f}s")
                
                return countries
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed in phase 1: {str(e)}")
            logger.exception(e)
            raise
    
    # ================ PHASE 2: COUNTRIES WITH MATCHING CITIES ================
    
    @staticmethod
    async def get_phase2_countries_with_cities(
        query: str,
        offset: int,
        limit: int,
        mode: str
    ) -> List[Dict[str, Any]]:
        """
        Phase 2: Get countries with matching cities
        
        Returns countries (expanded) with only matching cities (collapsed)
        """
        try:
            logger.info(f"[SEARCH PHASE2] Getting countries with cities: query='{query}', "
                       f"offset={offset}, limit={limit}, mode={mode}")
            start_time = datetime.now()
            
            async with db.get_connection() as conn:
                if mode == "prefix":
                    pattern = f"{query}%"
                else:  # contains
                    pattern = f"%{query}%"
                
                # Get countries that have matching cities
                sql = """
                    SELECT DISTINCT 
                        c.code as country_code,
                        c.name as country_name
                    FROM countries c
                    JOIN cities city ON c.code = city.country_code
                    WHERE LOWER(city.name) LIKE LOWER($1)
                    ORDER BY c.name
                    LIMIT $2 OFFSET $3
                """
                
                country_rows = await conn.fetch(sql, pattern, limit, offset)
                
                if not country_rows:
                    return []
                
                # For each country, get matching cities
                countries_with_cities = []
                for country_row in country_rows:
                    country_code = country_row['country_code']
                    country_name = country_row['country_name']
                    
                    # Get matching cities for this country
                    sql_cities = """
                        SELECT 
                            code,
                            name,
                            coordinates,
                            has_flightable_airport
                        FROM cities
                        WHERE country_code = $1
                          AND LOWER(name) LIKE LOWER($2)
                        ORDER BY name
                        LIMIT 50  -- Limit per country to avoid too many cities
                    """
                    
                    city_rows = await conn.fetch(sql_cities, country_code, pattern)
                    
                    cities = []
                    for city_row in city_rows:
                        city = {
                            'code': city_row['code'],
                            'name': city_row['name'],
                            'type': 'city',
                            'country_code': country_code,
                            'has_flightable_airport': city_row['has_flightable_airport'],
                            '_collapsed': True  # Cities collapsed in phase 2
                        }
                        cities.append(city)
                    
                    country_data = {
                        'type': 'country',
                        'code': country_code,
                        'name': country_name,
                        'cities': cities,
                        '_collapsed': False  # Countries expanded in phase 2
                    }
                    
                    countries_with_cities.append(country_data)
                
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"[SEARCH PHASE2] Found {len(countries_with_cities)} countries with cities in {elapsed:.3f}s")
                
                return countries_with_cities
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed in phase 2: {str(e)}")
            logger.exception(e)
            raise
    
    # ================ PHASE 3: FULL HIERARCHY ================
    
    @staticmethod
    async def get_phase3_full_hierarchy(
        query: str,
        offset: int,
        limit: int,
        mode: str
    ) -> List[Dict[str, Any]]:
        """
        Phase 3: Get full hierarchy: countries → cities → airports
        
        Returns countries (expanded) with cities (expanded) containing only matching airports
        """
        try:
            logger.info(f"[SEARCH PHASE3] Getting full hierarchy: query='{query}', "
                       f"offset={offset}, limit={limit}, mode={mode}")
            start_time = datetime.now()
            
            async with db.get_connection() as conn:
                if mode == "prefix":
                    pattern = f"{query}%"
                else:  # contains
                    pattern = f"%{query}%"
                
                # Get countries that have matching airports
                sql = """
                    SELECT DISTINCT 
                        c.code as country_code,
                        c.name as country_name
                    FROM countries c
                    JOIN airports a ON c.code = a.country_code
                    WHERE LOWER(a.name) LIKE LOWER($1)
                    ORDER BY c.name
                    LIMIT $2 OFFSET $3
                """
                
                country_rows = await conn.fetch(sql, pattern, limit, offset)
                
                if not country_rows:
                    return []
                
                # For each country, build the hierarchy
                countries_with_hierarchy = []
                for country_row in country_rows:
                    country_code = country_row['country_code']
                    country_name = country_row['country_name']
                    
                    # Get cities in this country that have matching airports
                    sql_cities = """
                        SELECT DISTINCT
                            city.code as city_code,
                            city.name as city_name
                        FROM cities city
                        JOIN airports a ON city.code = a.city_code
                        WHERE a.country_code = $1
                          AND LOWER(a.name) LIKE LOWER($2)
                        ORDER BY city.name
                        LIMIT 20  -- Limit per country
                    """
                    
                    city_rows = await conn.fetch(sql_cities, country_code, pattern)
                    
                    cities_with_airports = []
                    for city_row in city_rows:
                        city_code = city_row['city_code']
                        city_name = city_row['city_name']
                        
                        # Get matching airports for this city
                        sql_airports = """
                            SELECT 
                                code,
                                name,
                                coordinates
                                flightable,
                                iata_type
                            FROM airports
                            WHERE city_code = $1
                              AND country_code = $2
                              AND LOWER(name) LIKE LOWER($3)
                            ORDER BY name
                            LIMIT 10  -- Limit per city
                        """
                        
                        airport_rows = await conn.fetch(sql_airports, city_code, country_code, pattern)
                        
                        airports = []
                        for airport_row in airport_rows:
                            airport = {
                                'code': airport_row['code'],
                                'name': airport_row['name'],
                                'type': 'airport',
                                'city_code': city_code,
                                'country_code': country_code,
                                'flightable': airport_row['flightable'],
                                'iata_type': airport_row['iata_type']
                            }
                            airports.append(airport)
                        
                        city_data = {
                            'type': 'city',
                            'code': city_code,
                            'name': city_name,
                            'country_code': country_code,
                            'airports': airports,
                            '_collapsed': False  # Cities expanded in phase 3
                        }
                        
                        cities_with_airports.append(city_data)
                    
                    country_data = {
                        'type': 'country',
                        'code': country_code,
                        'name': country_name,
                        'cities': cities_with_airports,
                        '_collapsed': False  # Countries expanded in phase 3
                    }
                    
                    countries_with_hierarchy.append(country_data)
                
                elapsed = (datetime.now() - start_time).total_seconds()
                logger.info(f"[SEARCH PHASE3] Found {len(countries_with_hierarchy)} countries with hierarchy in {elapsed:.3f}s")
                
                return countries_with_hierarchy
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed in phase 3: {str(e)}")
            logger.exception(e)
            raise
    
    # ================ EXPAND ENDPOINT HELPERS ================
    
    @staticmethod
    async def get_all_cities_in_country(
        country_code: str,
        limit: int,
        offset: int
    ) -> List[Dict[str, Any]]:
        """Get ALL cities in a country (for expanding)"""
        try:
            async with db.get_connection() as conn:
                sql = """
                    SELECT 
                        code,
                        name,
                        coordinates,
                        has_flightable_airport
                    FROM cities 
                    WHERE country_code = $1
                    ORDER BY name
                    LIMIT $2 OFFSET $3
                """
                
                rows = await conn.fetch(sql, country_code, limit, offset)
                
                cities = []
                for row in rows:
                    city = dict(row)
                    city['type'] = 'city'
                    city['country_code'] = country_code
                    cities.append(city)
                
                return cities
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get all cities: {str(e)}")
            raise
    
    @staticmethod
    async def get_all_cities_in_country_count(country_code: str) -> int:
        """Get count of all cities in a country"""
        try:
            async with db.get_connection() as conn:
                sql = "SELECT COUNT(*) FROM cities WHERE country_code = $1"
                count = await conn.fetchval(sql, country_code)
                return count or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count cities: {str(e)}")
            raise
    
    @staticmethod
    async def get_all_airports_in_city(
        city_code: str,
        limit: int,
        offset: int
    ) -> List[Dict[str, Any]]:
        """Get ALL airports in a city (for expanding)"""
        try:
            async with db.get_connection() as conn:
                sql = """
                    SELECT 
                        code,
                        name,
                        coordinates,
                        flightable,
                        iata_type,
                        country_code
                    FROM airports 
                    WHERE city_code = $1
                    ORDER BY name
                    LIMIT $2 OFFSET $3
                """
                
                rows = await conn.fetch(sql, city_code, limit, offset)
                
                airports = []
                for row in rows:
                    airport = dict(row)
                    airport['type'] = 'airport'
                    airport['city_code'] = city_code
                    airports.append(airport)
                
                return airports
                
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to get all airports: {str(e)}")
            raise
    
    @staticmethod
    async def get_all_airports_in_city_count(city_code: str) -> int:
        """Get count of all airports in a city"""
        try:
            async with db.get_connection() as conn:
                sql = "SELECT COUNT(*) FROM airports WHERE city_code = $1"
                count = await conn.fetchval(sql, city_code)
                return count or 0
        except Exception as e:
            logger.error(f"[SEARCH ERROR] Failed to count airports: {str(e)}")
            raise

    @staticmethod
    async def get_country_center(country_code: str) -> Optional[Dict[str, Any]]:
        """
        Zwraca:
        - centroid OBLICZONY TYLKO Z GŁÓWNEGO SKUPISKA lotnisk (kontynent, bez terytoriów zamorskich),
        - rekomendowany zoom OBLICZONY NA PODSTAWIE WSZYSTKICH lotnisk w kraju.
        """
        try:
            async with db.get_connection() as conn:
                # ----- 1. Pobierz WSZYSTKIE lotniska flightable -----
                rows = await conn.fetch("""
                    SELECT 
                        (coordinates->>'lon')::float as lon,
                        (coordinates->>'lat')::float as lat
                    FROM airports
                    WHERE country_code = $1
                      AND flightable = true
                      AND coordinates IS NOT NULL
                      AND coordinates->>'lon' IS NOT NULL
                      AND coordinates->>'lat' IS NOT NULL
                """, country_code)

                total_airports = len(rows)

                # ----- 2. Fallback – jeśli brak lotnisk, użyj miast -----
                if not rows:
                    rows = await conn.fetch("""
                        SELECT 
                            (coordinates->>'lon')::float as lon,
                            (coordinates->>'lat')::float as lat
                        FROM cities
                        WHERE country_code = $1
                          AND coordinates IS NOT NULL
                          AND coordinates->>'lon' IS NOT NULL
                          AND coordinates->>'lat' IS NOT NULL
                    """, country_code)
                    total_airports = len(rows)

                if not rows:
                    logger.warning(f"Brak współrzędnych dla kraju {country_code}")
                    return None

                # ----- 3. Konwersja na listę krotek -----
                all_coords = [(float(r['lon']), float(r['lat'])) for r in rows]

                # ----- 4. ODRZUĆ TERYTORIA ZAMORSKIE (do obliczenia centroidu) -----
                main_cluster = filter_outliers(all_coords, max_degrees=5.0)

                if not main_cluster:
                    # Bezpieczeństwo – jeśli odrzuciliśmy wszystko, użyj wszystkich
                    main_cluster = all_coords

                # ----- 5. Centroid z GŁÓWNEGO SKUPISKA -----
                centroid_lon = sum(c[0] for c in main_cluster) / len(main_cluster)
                centroid_lat = sum(c[1] for c in main_cluster) / len(main_cluster)

                # ----- 6. Zoom z WSZYSTKICH lotnisk -----
                if total_airports == 0:
                    recommended_zoom = 5
                elif total_airports < 5:
                    recommended_zoom = 5
                elif total_airports < 15:
                    recommended_zoom = 4.5
                elif total_airports < 40:
                    recommended_zoom = 3.5
                elif total_airports < 100:
                    recommended_zoom = 2.6
                else:
                    recommended_zoom = 1.8

                return {
                    "lon": centroid_lon,
                    "lat": centroid_lat,
                    "airport_count": total_airports,      # wszystkie lotniska
                    "airport_count_main": len(main_cluster),  # tylko główne skupisko
                    "recommended_zoom": recommended_zoom
                }

        except Exception as e:
            logger.error(f"[SEARCH ERROR] get_country_center: {str(e)}")
            return None

# Singleton instance
search_service = SearchService()