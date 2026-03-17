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


async def run_init():
    """
    Pełna inicjalizacja bazy danych:
    - tworzenie tabel
    - wczytanie JSONów
    - ustawienie flagi initialized
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
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        name_translations JSONB,
        country_code VARCHAR(2) REFERENCES countries(code),
        population INT,
        coordinates JSONB
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS airports (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        iata VARCHAR(3),
        icao VARCHAR(4),
        city_id INT REFERENCES cities(id),
        country_code VARCHAR(2) REFERENCES countries(code),
        coordinates JSONB,
        timezone TEXT
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS airlines (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        iata VARCHAR(2),
        icao VARCHAR(3),
        country_code VARCHAR(2) REFERENCES countries(code)
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS planes (
        id SERIAL PRIMARY KEY,
        model TEXT NOT NULL,
        manufacturer TEXT,
        capacity INT
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS routes (
        id SERIAL PRIMARY KEY,
        airline_id INT REFERENCES airlines(id),
        from_airport_id INT REFERENCES airports(id),
        to_airport_id INT REFERENCES airports(id),
        distance INT,
        stops INT
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flights (
        id SERIAL PRIMARY KEY,
        route_id INT REFERENCES routes(id),
        departure TIMESTAMP,
        arrival TIMESTAMP,
        flight_number TEXT
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flight_offers (
        id SERIAL PRIMARY KEY,
        flight_id INT REFERENCES flights(id),
        price NUMERIC,
        currency VARCHAR(3)
    );
    """)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS flight_prices_cache (
        flight_id INT PRIMARY KEY REFERENCES flights(id),
        cached_price NUMERIC,
        currency VARCHAR(3),
        updated_at TIMESTAMP
    );
    """)

    # ---- 2. Wczytywanie danych JSON
    data_files = [
        ("countries.json", "countries", ["code", "name", "name_translations", "currency", "cases"]),
        ("cities.json", "cities", ["name", "name_translations", "country_code", "population", "coordinates"]),
        ("airports.json", "airports", ["name", "iata", "icao", "city_id", "country_code", "coordinates", "timezone"]),
        ("airlines.json", "airlines", ["name", "iata", "icao", "country_code"]),
        ("planes.json", "planes", ["model", "manufacturer", "capacity"]),
        ("routes.json", "routes", ["airline_id", "from_airport_id", "to_airport_id", "distance", "stops"]),
    ]

    for file_name, table, columns in data_files:
        items = await load_json(file_name)
        for item in items:
            values = [json.dumps(item[col]) if isinstance(item.get(col), dict) else item.get(col) for col in columns]
            placeholders = ",".join(f"${i+1}" for i in range(len(columns)))
            await db.execute(
                f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING",
                *values
            )

    # ---- 3. Flaga initialized
    await db.execute(
        """
        INSERT INTO app_meta (key, value) VALUES ('initialized','true')
        ON CONFLICT (key) DO UPDATE SET value='true';
        """
    )

    logger.info("Inicjalizacja bazy zakończona")
    await db.execute("SELECT pg_advisory_unlock(123456)")


def main():
    import asyncio
    asyncio.run(run_init())


if __name__ == "__main__":
    main()