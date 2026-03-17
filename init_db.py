import json
from pathlib import Path
from src.database import db
import logging

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent / "init_data"


async def load_json(file_name: str):
    path = DATA_DIR / file_name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


async def run_init(_app=None):
    """
    Pełna inicjalizacja bazy danych zgodnie z init.sql:
    - usunięcie starych tabel (DROP TABLE ... CASCADE)
    - tworzenie tabel BEZ kluczy obcych
    - wczytanie danych z JSON
    - czyszczenie niepotrzebnych rekordów
    - dodanie kluczy obcych, indeksów, widoków i funkcji
    - ustawienie flagi initialized
    """

    # Pobieramy jedno połączenie na cały proces inicjalizacji
    async with db.get_connection() as conn:
        # Lock PostgreSQL w tym połączeniu
        await conn.execute("SELECT pg_advisory_lock(123456)")

        try:
            # Sprawdź czy baza już jest zainicjalizowana
            row = await conn.fetchrow("SELECT value FROM app_meta WHERE key='initialized'")
            if row:
                logger.info("Baza danych już zainicjalizowana, pomijam init")
                await conn.execute("SELECT pg_advisory_unlock(123456)")
                return

            logger.info("Rozpoczynam inicjalizację bazy danych")

            # ============================================
            # 0. USUNIĘCIE ISTNIEJĄCYCH TABEL (jeśli istnieją)
            # ============================================
            # Kolejność usuwania: najpierw te z zależnościami, potem główne
            await conn.execute("DROP TABLE IF EXISTS trip_flight_prices CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS trip_flights CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS trips CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS user_trips CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS flight_offers CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS flight_prices_cache CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS flights CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS airport_schedules_cache CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS routes CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS planes CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS airports CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS airlines CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS cities CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS countries CASCADE;")
            await conn.execute("DROP TABLE IF EXISTS app_meta CASCADE;")

            # ============================================
            # 1. Tworzenie tabel (bez kluczy obcych)
            # ============================================

            # Tabela metadanych
            await conn.execute("""
            CREATE TABLE app_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            """)

            # Kraje
            await conn.execute("""
            CREATE TABLE countries (
                code VARCHAR(2) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                name_translations JSONB,
                currency VARCHAR(3),
                cases JSONB
            );
            """)

            # Miasta
            await conn.execute("""
            CREATE TABLE cities (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                name_translations JSONB,
                country_code VARCHAR(2),
                time_zone VARCHAR(50),
                coordinates JSONB,
                has_flightable_airport BOOLEAN DEFAULT FALSE,
                cases JSONB
            );
            """)

            # Linie lotnicze
            await conn.execute("""
            CREATE TABLE airlines (
                code VARCHAR(3) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                name_translations JSONB,
                is_lowcost BOOLEAN DEFAULT FALSE
            );
            """)

            # Lotniska
            await conn.execute("""
            CREATE TABLE airports (
                code VARCHAR(4) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                name_translations JSONB,
                city_code VARCHAR(10),
                country_code VARCHAR(2),
                time_zone VARCHAR(50),
                coordinates JSONB,
                flightable BOOLEAN DEFAULT FALSE,
                iata_type VARCHAR(20)
            );
            """)

            # Samoloty
            await conn.execute("""
            CREATE TABLE planes (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(100) NOT NULL
            );
            """)

            # Trasy
            await conn.execute("""
            CREATE TABLE routes (
                id SERIAL PRIMARY KEY,
                airline_iata VARCHAR(3),
                airline_icao VARCHAR(3),
                departure_airport_iata VARCHAR(4),
                departure_airport_icao VARCHAR(4),
                arrival_airport_iata VARCHAR(4),
                arrival_airport_icao VARCHAR(4),
                codeshare BOOLEAN DEFAULT FALSE,
                transfers INTEGER DEFAULT 0,
                planes JSONB
            );
            """)

            # Cache dla rozkładów lotnisk
            await conn.execute("""
            CREATE TABLE airport_schedules_cache (
                id SERIAL PRIMARY KEY,
                airport_code VARCHAR(4) NOT NULL,
                direction VARCHAR(10) NOT NULL CHECK (direction IN ('Departure', 'Arrival', 'Both')),
                last_fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                data JSONB NOT NULL,
                fetch_from_local TIMESTAMP NOT NULL,
                fetch_to_local TIMESTAMP NOT NULL,
                UNIQUE(airport_code, direction, fetch_from_local)
            );
            """)

            # Loty
            await conn.execute("""
            CREATE TABLE flights (
                id SERIAL PRIMARY KEY,
                flight_number VARCHAR(20) NOT NULL,
                airline_code VARCHAR(3),
                origin_airport_code VARCHAR(4) NOT NULL,
                destination_airport_code VARCHAR(4) NOT NULL,
                scheduled_departure_utc TIMESTAMP WITH TIME ZONE NOT NULL,
                scheduled_departure_local TIMESTAMP,
                scheduled_arrival_utc TIMESTAMP WITH TIME ZONE,
                scheduled_arrival_local TIMESTAMP,
                revised_departure_utc TIMESTAMP WITH TIME ZONE,
                predicted_departure_utc TIMESTAMP WITH TIME ZONE,
                runway_departure_utc TIMESTAMP WITH TIME ZONE,
                revised_arrival_utc TIMESTAMP WITH TIME ZONE,
                predicted_arrival_utc TIMESTAMP WITH TIME ZONE,
                runway_arrival_utc TIMESTAMP WITH TIME ZONE,
                departure_terminal VARCHAR(10),
                departure_gate VARCHAR(10),
                arrival_terminal VARCHAR(10),
                arrival_gate VARCHAR(10),
                search_date DATE NOT NULL,
                raw_data JSONB,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                UNIQUE(flight_number, scheduled_departure_utc, origin_airport_code, destination_airport_code)
            );
            """)

            # Cache cen biletów
            await conn.execute("""
            CREATE TABLE flight_prices_cache (
                id SERIAL PRIMARY KEY,
                origin_city_code VARCHAR(3) NOT NULL,
                destination_city_code VARCHAR(3) NOT NULL,
                departure_date DATE NOT NULL,
                last_fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                data JSONB NOT NULL,
                UNIQUE(origin_city_code, destination_city_code, departure_date)
            );
            """)

            # Oferty biletów
            await conn.execute("""
            CREATE TABLE flight_offers (
                id SERIAL PRIMARY KEY,
                origin_city_code VARCHAR(3) NOT NULL,
                destination_city_code VARCHAR(3) NOT NULL,
                origin_airport_code VARCHAR(4) NOT NULL,
                destination_airport_code VARCHAR(4) NOT NULL,
                price DECIMAL(10, 2) NOT NULL,
                currency VARCHAR(3) NOT NULL,
                airline_code VARCHAR(3),
                flight_number VARCHAR(20),
                departure_at TIMESTAMP WITH TIME ZONE NOT NULL,
                return_at TIMESTAMP WITH TIME ZONE,
                transfers INTEGER DEFAULT 0,
                return_transfers INTEGER DEFAULT 0,
                duration INTEGER,
                duration_to INTEGER,
                duration_back INTEGER,
                link TEXT,
                search_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                UNIQUE(origin_airport_code, destination_airport_code, departure_at, flight_number, price)
            );
            """)

            # Podróże użytkowników (nowa tabela)
            await conn.execute("""
            CREATE TABLE user_trips (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                name TEXT,
                trip_state JSONB NOT NULL,
                trip_routes JSONB NOT NULL DEFAULT '[]',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """)

            # Trips (plany podróży)
            await conn.execute("""
            CREATE TABLE trips (
                id SERIAL PRIMARY KEY,
                name VARCHAR(200),
                start_airport_code VARCHAR(4) NOT NULL,
                start_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );
            """)

            # Loty w podróży
            await conn.execute("""
            CREATE TABLE trip_flights (
                id SERIAL PRIMARY KEY,
                trip_id INTEGER NOT NULL,
                flight_id INTEGER NOT NULL,
                flight_order INTEGER NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                UNIQUE(trip_id, flight_order)
            );
            """)

            # Ceny dla lotów w podróży
            await conn.execute("""
            CREATE TABLE trip_flight_prices (
                id SERIAL PRIMARY KEY,
                trip_flight_id INTEGER NOT NULL,
                offer_id INTEGER,
                price DECIMAL(10, 2),
                currency VARCHAR(3),
                link TEXT,
                found_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            );
            """)

            # ============================================
            # 2. Wczytywanie danych z JSON
            # ============================================

            data_files = [
                ("countries.json", "countries", ["code", "name", "name_translations", "currency", "cases"]),
                ("cities.json", "cities", ["code","name", "name_translations", "country_code", "time_zone", "coordinates", "has_flightable_airport", "cases"]),
                ("airlines.json", "airlines", ["code","name", "name_translations", "is_lowcost"]),
                ("airports.json", "airports", ["code","name", "name_translations","city_code","country_code","time_zone","coordinates","flightable","iata_type"]),
                ("planes.json", "planes", ["code","name"]),
                ("routes.json", "routes", ["airline_iata","airline_icao","departure_airport_iata","departure_airport_icao","arrival_airport_iata","arrival_airport_icao","codeshare","transfers","planes"]),
            ]

            for file_name, table, columns in data_files:
                items = await load_json(file_name)
                for item in items:
                    # Domyślne wartości dla brakujących pól
                    for col in columns:
                        if col not in item or item[col] in [None, ""]:
                            if col in ["has_flightable_airport","flightable","codeshare","is_lowcost"]:
                                item[col] = False
                            elif col in ["transfers"]:
                                item[col] = 0
                            else:
                                item[col] = None

                    # JSONB dla słowników
                    values = [json.dumps(item[col]) if isinstance(item.get(col), dict) else item.get(col) for col in columns]
                    placeholders = ",".join(f"${i+1}" for i in range(len(columns)))
                    await conn.execute(
                        f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                        *values
                    )

            # ============================================
            # 3. Czyszczenie danych (DELETE)
            # ============================================

            # 3.1 Usuń lotniska z flightable = false oraz iata_type != 'airport' (jeśli nie jest NULL)
            await conn.execute("""
                DELETE FROM airports
                WHERE flightable = false OR (iata_type IS NOT NULL AND iata_type != 'airport');
            """)

            # 3.2 Usuń miasta z has_flightable_airport = false
            await conn.execute("""
                DELETE FROM cities WHERE has_flightable_airport = false;
            """)

            # 3.3 Usuń miasta z nieistniejącymi krajami
            await conn.execute("""
                DELETE FROM cities
                WHERE country_code IS NOT NULL
                  AND country_code NOT IN (SELECT code FROM countries);
            """)

            # 3.4 Usuń lotniska z nieistniejącymi miastami lub krajami
            await conn.execute("""
                DELETE FROM airports
                WHERE (city_code IS NOT NULL AND city_code NOT IN (SELECT code FROM cities))
                   OR (country_code IS NOT NULL AND country_code NOT IN (SELECT code FROM countries));
            """)

            # 3.5 Usuń trasy z nieistniejącymi referencjami
            await conn.execute("""
                DELETE FROM routes
                WHERE (airline_iata IS NOT NULL AND airline_iata NOT IN (SELECT code FROM airlines))
                   OR (departure_airport_iata IS NOT NULL AND departure_airport_iata NOT IN (SELECT code FROM airports))
                   OR (arrival_airport_iata IS NOT NULL AND arrival_airport_iata NOT IN (SELECT code FROM airports));
            """)

            # ============================================
            # 4. Dodanie kluczy obcych
            # ============================================

            # Kraje → miasta
            await conn.execute("""
                ALTER TABLE cities
                ADD CONSTRAINT fk_cities_country
                FOREIGN KEY (country_code) REFERENCES countries(code)
                ON DELETE CASCADE;
            """)

            # Miasta i kraje → lotniska
            await conn.execute("""
                ALTER TABLE airports
                ADD CONSTRAINT fk_airports_city
                FOREIGN KEY (city_code) REFERENCES cities(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_airports_country
                FOREIGN KEY (country_code) REFERENCES countries(code)
                ON DELETE CASCADE;
            """)

            # Linie i lotniska → trasy
            await conn.execute("""
                ALTER TABLE routes
                ADD CONSTRAINT fk_routes_airline
                FOREIGN KEY (airline_iata) REFERENCES airlines(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_routes_departure_airport
                FOREIGN KEY (departure_airport_iata) REFERENCES airports(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_routes_arrival_airport
                FOREIGN KEY (arrival_airport_iata) REFERENCES airports(code)
                ON DELETE CASCADE;
            """)

            # Lotnisko → cache rozkładów
            await conn.execute("""
                ALTER TABLE airport_schedules_cache
                ADD CONSTRAINT fk_schedules_airport
                FOREIGN KEY (airport_code) REFERENCES airports(code)
                ON DELETE CASCADE;
            """)

            # Loty
            await conn.execute("""
                ALTER TABLE flights
                ADD CONSTRAINT fk_flights_airline
                FOREIGN KEY (airline_code) REFERENCES airlines(code)
                ON DELETE SET NULL,
                ADD CONSTRAINT fk_flights_origin_airport
                FOREIGN KEY (origin_airport_code) REFERENCES airports(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_flights_destination_airport
                FOREIGN KEY (destination_airport_code) REFERENCES airports(code)
                ON DELETE CASCADE;
            """)

            # Cache cen
            await conn.execute("""
                ALTER TABLE flight_prices_cache
                ADD CONSTRAINT fk_prices_origin_city
                FOREIGN KEY (origin_city_code) REFERENCES cities(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_prices_destination_city
                FOREIGN KEY (destination_city_code) REFERENCES cities(code)
                ON DELETE CASCADE;
            """)

            # Oferty biletów
            await conn.execute("""
                ALTER TABLE flight_offers
                ADD CONSTRAINT fk_offers_airline
                FOREIGN KEY (airline_code) REFERENCES airlines(code)
                ON DELETE SET NULL,
                ADD CONSTRAINT fk_offers_origin_city
                FOREIGN KEY (origin_city_code) REFERENCES cities(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_offers_destination_city
                FOREIGN KEY (destination_city_code) REFERENCES cities(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_offers_origin_airport
                FOREIGN KEY (origin_airport_code) REFERENCES airports(code)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_offers_destination_airport
                FOREIGN KEY (destination_airport_code) REFERENCES airports(code)
                ON DELETE CASCADE;
            """)

            # Trips
            await conn.execute("""
                ALTER TABLE trips
                ADD CONSTRAINT fk_trips_start_airport
                FOREIGN KEY (start_airport_code) REFERENCES airports(code)
                ON DELETE CASCADE;
            """)

            # Trip flights
            await conn.execute("""
                ALTER TABLE trip_flights
                ADD CONSTRAINT fk_trip_flights_trip
                FOREIGN KEY (trip_id) REFERENCES trips(id)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_trip_flights_flight
                FOREIGN KEY (flight_id) REFERENCES flights(id)
                ON DELETE CASCADE;
            """)

            # Trip flight prices
            await conn.execute("""
                ALTER TABLE trip_flight_prices
                ADD CONSTRAINT fk_trip_prices_trip_flight
                FOREIGN KEY (trip_flight_id) REFERENCES trip_flights(id)
                ON DELETE CASCADE,
                ADD CONSTRAINT fk_trip_prices_offer
                FOREIGN KEY (offer_id) REFERENCES flight_offers(id)
                ON DELETE SET NULL;
            """)

            # ============================================
            # 5. Indeksy
            # ============================================

            # Kraje
            await conn.execute("CREATE INDEX idx_countries_name ON countries(name);")
            await conn.execute("CREATE INDEX idx_countries_currency ON countries(currency);")
            await conn.execute("CREATE INDEX idx_countries_name_lower ON countries(LOWER(name));")
            await conn.execute("CREATE INDEX idx_countries_translations ON countries USING gin(name_translations);")

            # Miasta
            await conn.execute("CREATE INDEX idx_cities_name ON cities(name);")
            await conn.execute("CREATE INDEX idx_cities_country ON cities(country_code);")
            await conn.execute("CREATE INDEX idx_cities_has_airport ON cities(has_flightable_airport);")
            await conn.execute("CREATE INDEX idx_cities_name_lower ON cities(LOWER(name));")
            await conn.execute("CREATE INDEX idx_cities_translations ON cities USING gin(name_translations);")

            # Linie lotnicze
            await conn.execute("CREATE INDEX idx_airlines_name ON airlines(name);")
            await conn.execute("CREATE INDEX idx_airlines_lowcost ON airlines(is_lowcost);")
            await conn.execute("CREATE INDEX idx_airlines_translations ON airlines USING gin(name_translations);")

            # Lotniska
            await conn.execute("CREATE INDEX idx_airports_name ON airports(name);")
            await conn.execute("CREATE INDEX idx_airports_city ON airports(city_code);")
            await conn.execute("CREATE INDEX idx_airports_country ON airports(country_code);")
            await conn.execute("CREATE INDEX idx_airports_type ON airports(iata_type);")
            await conn.execute("CREATE INDEX idx_airports_flightable ON airports(flightable);")
            await conn.execute("CREATE INDEX idx_airports_name_lower ON airports(LOWER(name));")
            await conn.execute("CREATE INDEX idx_airports_translations ON airports USING gin(name_translations);")

            # Samoloty
            await conn.execute("CREATE INDEX idx_planes_name ON planes(name);")

            # Trasy
            await conn.execute("CREATE INDEX idx_routes_airline ON routes(airline_iata);")
            await conn.execute("CREATE INDEX idx_routes_departure ON routes(departure_airport_iata);")
            await conn.execute("CREATE INDEX idx_routes_arrival ON routes(arrival_airport_iata);")
            await conn.execute("CREATE INDEX idx_routes_codeshare ON routes(codeshare);")
            await conn.execute("CREATE INDEX idx_routes_transfers ON routes(transfers);")
            await conn.execute("CREATE INDEX idx_routes_planes_gin ON routes USING gin(planes);")

            # Cache rozkładów
            await conn.execute("CREATE INDEX idx_schedules_airport_datetime ON airport_schedules_cache(airport_code, direction, fetch_from_local, fetch_to_local);")
            await conn.execute("CREATE INDEX idx_schedules_fetched ON airport_schedules_cache(last_fetched_at);")

            # Loty
            await conn.execute("CREATE INDEX idx_flights_origin_date ON flights(origin_airport_code, search_date);")
            await conn.execute("CREATE INDEX idx_flights_destination_date ON flights(destination_airport_code, search_date);")
            await conn.execute("CREATE INDEX idx_flights_origin_departure_local ON flights(origin_airport_code, scheduled_departure_local);")
            await conn.execute("CREATE INDEX idx_flights_departure_utc ON flights(scheduled_departure_utc);")
            await conn.execute("CREATE INDEX idx_flights_flight_number ON flights(flight_number);")
            await conn.execute("CREATE INDEX idx_flights_airline ON flights(airline_code);")
            await conn.execute("CREATE INDEX idx_flights_origin_dest ON flights(origin_airport_code, destination_airport_code);")

            # Cache cen
            await conn.execute("CREATE INDEX idx_prices_origin_dest_date ON flight_prices_cache(origin_city_code, destination_city_code, departure_date);")
            await conn.execute("CREATE INDEX idx_prices_fetched ON flight_prices_cache(last_fetched_at);")

            # Oferty biletów
            await conn.execute("CREATE INDEX idx_offers_origin_airport_date ON flight_offers(origin_airport_code, search_date);")
            await conn.execute("CREATE INDEX idx_offers_destination_airport_date ON flight_offers(destination_airport_code, search_date);")
            await conn.execute("CREATE INDEX idx_offers_departure_at ON flight_offers(departure_at);")
            await conn.execute("CREATE INDEX idx_offers_city_origin_date ON flight_offers(origin_city_code, destination_city_code, search_date);")
            await conn.execute("CREATE INDEX idx_offers_price ON flight_offers(price);")
            await conn.execute("CREATE INDEX idx_offers_transfers ON flight_offers(transfers);")

            # User trips
            await conn.execute("CREATE INDEX idx_user_trips_user_id ON user_trips(user_id);")
            await conn.execute("CREATE INDEX idx_user_trips_updated ON user_trips(updated_at DESC);")

            # Trips
            await conn.execute("CREATE INDEX idx_trips_start_airport ON trips(start_airport_code);")
            await conn.execute("CREATE INDEX idx_trips_start_date ON trips(start_date);")

            # Trip flights
            await conn.execute("CREATE INDEX idx_trip_flights_trip ON trip_flights(trip_id);")
            await conn.execute("CREATE INDEX idx_trip_flights_order ON trip_flights(trip_id, flight_order);")

            # Trip flight prices
            await conn.execute("CREATE INDEX idx_trip_prices_trip_flight ON trip_flight_prices(trip_flight_id);")

            # ============================================
            # 6. Widoki
            # ============================================

            # routes_details
            await conn.execute("""
            CREATE OR REPLACE VIEW routes_details AS
            SELECT
                r.id,
                a.name as airline_name,
                r.airline_iata,
                ap1.name as departure_airport,
                ap1.code as departure_iata,
                c1.name as departure_city,
                co1.name as departure_country,
                ap2.name as arrival_airport,
                ap2.code as arrival_iata,
                c2.name as arrival_city,
                co2.name as arrival_country,
                r.codeshare,
                r.transfers,
                r.planes,
                ap1.coordinates as departure_coords,
                ap2.coordinates as arrival_coords
            FROM routes r
            LEFT JOIN airlines a ON r.airline_iata = a.code
            LEFT JOIN airports ap1 ON r.departure_airport_iata = ap1.code
            LEFT JOIN cities c1 ON ap1.city_code = c1.code
            LEFT JOIN countries co1 ON ap1.country_code = co1.code
            LEFT JOIN airports ap2 ON r.arrival_airport_iata = ap2.code
            LEFT JOIN cities c2 ON ap2.city_code = c2.code
            LEFT JOIN countries co2 ON ap2.country_code = co2.code
            WHERE r.departure_airport_iata IS NOT NULL
              AND r.arrival_airport_iata IS NOT NULL;
            """)

            # airports_details
            await conn.execute("""
            CREATE OR REPLACE VIEW airports_details AS
            SELECT
                a.code,
                a.name,
                a.city_code,
                a.country_code,
                a.time_zone,
                a.coordinates,
                a.flightable,
                a.iata_type,
                c.name as city_name,
                co.name as country_name
            FROM airports a
            LEFT JOIN cities c ON a.city_code = c.code
            LEFT JOIN countries co ON a.country_code = co.code;
            """)

            # cities_details
            await conn.execute("""
            CREATE OR REPLACE VIEW cities_details AS
            SELECT
                c.code,
                c.name,
                c.country_code,
                c.time_zone,
                c.coordinates,
                c.has_flightable_airport,
                co.name as country_name,
                co.currency,
                COUNT(a.code) as airport_count,
                SUM(CASE WHEN a.flightable THEN 1 ELSE 0 END) as flightable_airport_count
            FROM cities c
            LEFT JOIN countries co ON c.country_code = co.code
            LEFT JOIN airports a ON c.code = a.city_code
            GROUP BY c.code, c.name, c.country_code, c.time_zone, c.coordinates,
                     c.has_flightable_airport, co.name, co.currency;
            """)

            # flights_details
            await conn.execute("""
            CREATE OR REPLACE VIEW flights_details AS
            SELECT
                f.id,
                f.flight_number,
                a.name as airline_name,
                f.airline_code,
                ap1.name as origin_airport_name,
                ap1.code as origin_airport_code,
                c1.name as origin_city_name,
                c1.code as origin_city_code,
                ap2.name as destination_airport_name,
                ap2.code as destination_airport_code,
                c2.name as destination_city_name,
                c2.code as destination_city_code,
                f.scheduled_departure_utc,
                f.scheduled_departure_local,
                f.scheduled_arrival_utc,
                f.scheduled_arrival_local,
                f.departure_terminal,
                f.departure_gate,
                f.arrival_terminal,
                f.arrival_gate,
                f.search_date,
                f.created_at
            FROM flights f
            LEFT JOIN airlines a ON f.airline_code = a.code
            LEFT JOIN airports ap1 ON f.origin_airport_code = ap1.code
            LEFT JOIN cities c1 ON ap1.city_code = c1.code
            LEFT JOIN airports ap2 ON f.destination_airport_code = ap2.code
            LEFT JOIN cities c2 ON ap2.city_code = c2.code;
            """)

            # flight_offers_details
            await conn.execute("""
            CREATE OR REPLACE VIEW flight_offers_details AS
            SELECT
                fo.id,
                fo.flight_number,
                a.name as airline_name,
                fo.airline_code,
                oc.name as origin_city_name,
                oc.code as origin_city_code,
                dc.name as destination_city_name,
                dc.code as destination_city_code,
                oa.name as origin_airport_name,
                oa.code as origin_airport_code,
                da.name as destination_airport_name,
                da.code as destination_airport_code,
                fo.price,
                fo.currency,
                fo.departure_at,
                fo.return_at,
                fo.transfers,
                fo.duration_to,
                fo.link,
                fo.search_date,
                fo.created_at
            FROM flight_offers fo
            LEFT JOIN airlines a ON fo.airline_code = a.code
            LEFT JOIN cities oc ON fo.origin_city_code = oc.code
            LEFT JOIN cities dc ON fo.destination_city_code = dc.code
            LEFT JOIN airports oa ON fo.origin_airport_code = oa.code
            LEFT JOIN airports da ON fo.destination_airport_code = da.code;
            """)

            # ============================================
            # 7. Funkcje
            # ============================================

            # airport_has_routes
            await conn.execute("""
            CREATE OR REPLACE FUNCTION airport_has_routes(airport_code VARCHAR)
            RETURNS BOOLEAN AS $$
            DECLARE
                has_routes BOOLEAN;
            BEGIN
                SELECT EXISTS (
                    SELECT 1 FROM routes
                    WHERE departure_airport_iata = airport_code
                       OR arrival_airport_iata = airport_code
                ) INTO has_routes;
                RETURN has_routes;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # count_airport_routes
            await conn.execute("""
            CREATE OR REPLACE FUNCTION count_airport_routes(airport_code VARCHAR)
            RETURNS INTEGER AS $$
            DECLARE
                route_count INTEGER;
            BEGIN
                SELECT COUNT(*) INTO route_count
                FROM routes
                WHERE departure_airport_iata = airport_code
                   OR arrival_airport_iata = airport_code;
                RETURN route_count;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # find_routes_between_airports
            await conn.execute("""
            CREATE OR REPLACE FUNCTION find_routes_between_airports(
                departure_code VARCHAR,
                arrival_code VARCHAR,
                max_transfers INTEGER DEFAULT 0
            )
            RETURNS TABLE(
                route_id INTEGER,
                airline_iata VARCHAR,
                departure_airport_iata VARCHAR,
                arrival_airport_iata VARCHAR,
                transfers INTEGER,
                codeshare BOOLEAN
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    r.id,
                    r.airline_iata,
                    r.departure_airport_iata,
                    r.arrival_airport_iata,
                    r.transfers,
                    r.codeshare
                FROM routes r
                WHERE r.departure_airport_iata = departure_code
                  AND r.arrival_airport_iata = arrival_code
                  AND r.transfers <= max_transfers
                ORDER BY r.transfers, r.codeshare;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # has_cached_schedules
            await conn.execute("""
            CREATE OR REPLACE FUNCTION has_cached_schedules(
                airport_code_param VARCHAR,
                from_datetime_param TIMESTAMP,
                direction_param VARCHAR DEFAULT 'Departure'
            )
            RETURNS BOOLEAN AS $$
            DECLARE
                has_cache BOOLEAN;
            BEGIN
                SELECT EXISTS (
                    SELECT 1 FROM airport_schedules_cache
                    WHERE airport_code = airport_code_param
                      AND direction = direction_param
                      AND fetch_from_local <= from_datetime_param
                      AND fetch_to_local > from_datetime_param
                      AND last_fetched_at > (NOW() - INTERVAL '1 hour')
                ) INTO has_cache;
                RETURN has_cache;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # has_cached_prices
            await conn.execute("""
            CREATE OR REPLACE FUNCTION has_cached_prices(
                origin_city_param VARCHAR,
                destination_city_param VARCHAR,
                departure_date_param DATE
            )
            RETURNS BOOLEAN AS $$
            DECLARE
                has_cache BOOLEAN;
            BEGIN
                SELECT EXISTS (
                    SELECT 1 FROM flight_prices_cache
                    WHERE origin_city_code = origin_city_param
                      AND destination_city_code = destination_city_param
                      AND departure_date = departure_date_param
                ) INTO has_cache;
                RETURN has_cache;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # get_flights_from_airport
            await conn.execute("""
            CREATE OR REPLACE FUNCTION get_flights_from_airport(
                airport_code_param VARCHAR,
                search_date_param DATE,
                limit_param INTEGER DEFAULT 50,
                offset_param INTEGER DEFAULT 0
            )
            RETURNS TABLE(
                id INTEGER,
                flight_number VARCHAR,
                airline_code VARCHAR,
                origin_airport_code VARCHAR,
                destination_airport_code VARCHAR,
                scheduled_departure_utc TIMESTAMP WITH TIME ZONE,
                scheduled_departure_local TIMESTAMP WITH TIME ZONE,
                scheduled_arrival_utc TIMESTAMP WITH TIME ZONE,
                scheduled_arrival_local TIMESTAMP WITH TIME ZONE,
                departure_terminal VARCHAR,
                departure_gate VARCHAR,
                arrival_terminal VARCHAR,
                arrival_gate VARCHAR
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    f.id,
                    f.flight_number,
                    f.airline_code,
                    f.origin_airport_code,
                    f.destination_airport_code,
                    f.scheduled_departure_utc,
                    f.scheduled_departure_local,
                    f.scheduled_arrival_utc,
                    f.scheduled_arrival_local,
                    f.departure_terminal,
                    f.departure_gate,
                    f.arrival_terminal,
                    f.arrival_gate
                FROM flights f
                WHERE f.origin_airport_code = airport_code_param
                  AND f.search_date = search_date_param
                ORDER BY f.scheduled_departure_utc ASC
                LIMIT limit_param
                OFFSET offset_param;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # get_offers_for_flight
            await conn.execute("""
            CREATE OR REPLACE FUNCTION get_offers_for_flight(
                origin_airport_param VARCHAR,
                destination_airport_param VARCHAR,
                departure_date_param DATE
            )
            RETURNS TABLE(
                id INTEGER,
                price DECIMAL,
                currency VARCHAR,
                airline_code VARCHAR,
                flight_number VARCHAR,
                departure_at TIMESTAMP WITH TIME ZONE,
                duration_to INTEGER,
                link TEXT
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    fo.id,
                    fo.price,
                    fo.currency,
                    fo.airline_code,
                    fo.flight_number,
                    fo.departure_at,
                    fo.duration_to,
                    fo.link
                FROM flight_offers fo
                WHERE fo.origin_airport_code = origin_airport_param
                  AND fo.destination_airport_code = destination_airport_param
                  AND DATE(fo.departure_at) = departure_date_param
                  AND fo.transfers = 0
                ORDER BY fo.price ASC;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # get_trip_details
            await conn.execute("""
            CREATE OR REPLACE FUNCTION get_trip_details(trip_id_param INTEGER)
            RETURNS TABLE(
                trip_id INTEGER,
                trip_name VARCHAR,
                start_airport_code VARCHAR,
                start_date DATE,
                flight_order INTEGER,
                flight_number VARCHAR,
                origin_airport_code VARCHAR,
                destination_airport_code VARCHAR,
                scheduled_departure_utc TIMESTAMP WITH TIME ZONE,
                price DECIMAL,
                currency VARCHAR
            ) AS $$
            BEGIN
                RETURN QUERY
                SELECT
                    t.id,
                    t.name,
                    t.start_airport_code,
                    t.start_date,
                    tf.flight_order,
                    f.flight_number,
                    f.origin_airport_code,
                    f.destination_airport_code,
                    f.scheduled_departure_utc,
                    tfp.price,
                    tfp.currency
                FROM trips t
                JOIN trip_flights tf ON t.id = tf.trip_id
                JOIN flights f ON tf.flight_id = f.id
                LEFT JOIN trip_flight_prices tfp ON tf.id = tfp.trip_flight_id
                WHERE t.id = trip_id_param
                ORDER BY tf.flight_order;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # ============================================
            # 8. Statystyki (logowanie)
            # ============================================

            countries_count = await conn.fetchval("SELECT COUNT(*) FROM countries")
            cities_count = await conn.fetchval("SELECT COUNT(*) FROM cities")
            airports_count = await conn.fetchval("SELECT COUNT(*) FROM airports")
            airlines_count = await conn.fetchval("SELECT COUNT(*) FROM airlines")
            routes_count = await conn.fetchval("SELECT COUNT(*) FROM routes")
            planes_count = await conn.fetchval("SELECT COUNT(*) FROM planes")

            logger.info("============================================")
            logger.info("STATYSTYKI PO CZYSZCZENIU:")
            logger.info("============================================")
            logger.info(f"Kraje: {countries_count}")
            logger.info(f"Miasta: {cities_count}")
            logger.info(f"Lotniska: {airports_count}")
            logger.info(f"Linie lotnicze: {airlines_count}")
            logger.info(f"Trasy: {routes_count}")
            logger.info(f"Samoloty: {planes_count}")
            logger.info("============================================")
            logger.info("NOWE TABELE API: airport_schedules_cache, flights, flight_prices_cache, flight_offers, user_trips, trips, trip_flights, trip_flight_prices")
            logger.info("============================================")

            # ============================================
            # 9. Flaga initialized
            # ============================================

            await conn.execute("""
                INSERT INTO app_meta (key, value) VALUES ('initialized', 'true')
                ON CONFLICT (key) DO NOTHING;
            """)

            logger.info("Inicjalizacja bazy zakończona")

        finally:
            # Zwolnij lock
            await conn.execute("SELECT pg_advisory_unlock(123456)")


def main():
    import asyncio
    asyncio.run(run_init())


if __name__ == "__main__":
    main()