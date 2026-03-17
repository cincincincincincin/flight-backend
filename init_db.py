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
    Pełna inicjalizacja bazy danych w wersji produkcyjnej,
    odwzorowująca init.sql:
    - tworzenie tabel
    - wczytanie JSONów
    - ustawienie flagi initialized
    - czyszczenie danych
    """

    # 🔒 Lock w PostgreSQL
    await db.execute("SELECT pg_advisory_lock(123456)")

    # Sprawdź czy baza już jest zainicjalizowana
    row = await db.fetch_one("SELECT value FROM app_meta WHERE key='initialized'")
    if row:
        logger.info("Baza danych już zainicjalizowana, pomijam init")
        await db.execute("SELECT pg_advisory_unlock(123456)")
        return

    logger.info("Rozpoczynam inicjalizację bazy danych")

    # ---- 1. Tabele
    await db.execute("""
    CREATE TABLE IF NOT EXISTS app_meta (
        key TEXT PRIMARY KEY,
        value TEXT
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        code VARCHAR(2) PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        name_translations JSONB,
        currency VARCHAR(3),
        cases JSONB
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS cities (
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

    await db.execute("""
    CREATE TABLE IF NOT EXISTS airlines (
        code VARCHAR(3) PRIMARY KEY,
        name VARCHAR(200) NOT NULL,
        name_translations JSONB,
        is_lowcost BOOLEAN DEFAULT FALSE
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS airports (
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

    await db.execute("""
    CREATE TABLE IF NOT EXISTS planes (
        code VARCHAR(10) PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS routes (
        id SERIAL PRIMARY KEY,
        airline_iata VARCHAR(3),
        airline_icao VARCHAR(3),
        departure_airport_iata VARCHAR(4),
        departure_airport_icao VARCHAR(4),
        arrival_airport_iata VARCHAR(4),
        arrival_airport_icao VARCHAR(4),
        codeshare BOOLEAN DEFAULT FALSE,
        transfers INT DEFAULT 0,
        planes JSONB
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS airport_schedules_cache (
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

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flights (
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

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flight_prices_cache (
        id SERIAL PRIMARY KEY,
        origin_city_code VARCHAR(3) NOT NULL,
        destination_city_code VARCHAR(3) NOT NULL,
        departure_date DATE NOT NULL,
        last_fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        data JSONB NOT NULL,
        UNIQUE(origin_city_code, destination_city_code, departure_date)
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flight_offers (
        id SERIAL PRIMARY KEY,
        origin_city_code VARCHAR(3) NOT NULL,
        destination_city_code VARCHAR(3) NOT NULL,
        origin_airport_code VARCHAR(4) NOT NULL,
        destination_airport_code VARCHAR(4) NOT NULL,
        price DECIMAL(10,2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        airline_code VARCHAR(3),
        flight_number VARCHAR(20),
        departure_at TIMESTAMP WITH TIME ZONE NOT NULL,
        return_at TIMESTAMP WITH TIME ZONE,
        transfers INT DEFAULT 0,
        return_transfers INT DEFAULT 0,
        duration INT,
        duration_to INT,
        duration_back INT,
        link TEXT,
        search_date DATE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        UNIQUE(origin_airport_code, destination_airport_code, departure_at, flight_number, price)
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS trips (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200),
        start_airport_code VARCHAR(4) NOT NULL,
        start_date DATE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        FOREIGN KEY (start_airport_code) REFERENCES airports(code) ON DELETE CASCADE
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS trip_flights (
        id SERIAL PRIMARY KEY,
        trip_id INT NOT NULL,
        flight_id INT NOT NULL,
        flight_order INT NOT NULL,
        added_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        FOREIGN KEY (trip_id) REFERENCES trips(id) ON DELETE CASCADE,
        FOREIGN KEY (flight_id) REFERENCES flights(id) ON DELETE CASCADE,
        UNIQUE(trip_id, flight_order)
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS trip_flight_prices (
        id SERIAL PRIMARY KEY,
        trip_flight_id INT NOT NULL,
        offer_id INT,
        price DECIMAL(10,2),
        currency VARCHAR(3),
        link TEXT,
        found_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        FOREIGN KEY (trip_flight_id) REFERENCES trip_flights(id) ON DELETE CASCADE,
        FOREIGN KEY (offer_id) REFERENCES flight_offers(id) ON DELETE SET NULL
    );
    """)

    # ---- 2. Wczytywanie danych JSON
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
            # Ustaw domyślne wartości, jeśli brak
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
            await db.execute(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                *values
            )

    # ---- 3. Flaga initialized
    await db.execute("""
        INSERT INTO app_meta (key, value) VALUES ('initialized','true')
        ON CONFLICT (key) DO UPDATE SET value='true';
    """)

    logger.info("Inicjalizacja bazy zakończona")
    await db.execute("SELECT pg_advisory_unlock(123456)")


def main():
    import asyncio
    asyncio.run(run_init())


if __name__ == "__main__":
    main()