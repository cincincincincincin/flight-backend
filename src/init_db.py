import os
import json
import psycopg2
from psycopg2.extras import execute_batch, Json

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "init_data")

conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

def load_json(filename):
    with open(os.path.join(DATA_DIR, filename), "r", encoding="utf-8") as f:
        return json.load(f)

def run_sql_file(path):
    with open(path, "r", encoding="utf-8") as f:
        cur.execute(f.read())
    conn.commit()

# ========================
# 1. SCHEMA (z SQL)
# ========================
print("Tworzenie schematu...")
run_sql_file("init_schema.sql")  # <-- TU wrzucasz swój SQL BEZ sekcji JSON

# ========================
# 2. INSERTY
# ========================

print("Ładowanie countries...")
countries = load_json("countries.json")
execute_batch(cur, """
    INSERT INTO countries (code, name, name_translations, currency, cases)
    VALUES (%s, %s, %s, %s, %s)
""", [
    (
        c.get("code"),
        c.get("name"),
        Json(c.get("name_translations")),
        c.get("currency"),
        Json(c.get("cases"))
    )
    for c in countries
])
conn.commit()

print("Ładowanie cities...")
cities = load_json("cities.json")
execute_batch(cur, """
    INSERT INTO cities (code, name, name_translations, country_code, time_zone,
                        coordinates, has_flightable_airport, cases)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""", [
    (
        c.get("code"),
        c.get("name"),
        Json(c.get("name_translations")),
        c.get("country_code"),
        c.get("time_zone"),
        Json(c.get("coordinates")),
        c.get("has_flightable_airport", False),
        Json(c.get("cases"))
    )
    for c in cities
])
conn.commit()

print("Ładowanie airlines...")
airlines = load_json("airlines.json")
execute_batch(cur, """
    INSERT INTO airlines (code, name, name_translations, is_lowcost)
    VALUES (%s, %s, %s, %s)
""", [
    (
        a.get("code"),
        a.get("name"),
        Json(a.get("name_translations")),
        a.get("is_lowcost", False)
    )
    for a in airlines
])
conn.commit()

print("Ładowanie airports...")
airports = load_json("airports.json")
execute_batch(cur, """
    INSERT INTO airports (code, name, name_translations, city_code, country_code,
                          time_zone, coordinates, flightable, iata_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", [
    (
        a.get("code"),
        a.get("name"),
        Json(a.get("name_translations")),
        a.get("city_code"),
        a.get("country_code"),
        a.get("time_zone"),
        Json(a.get("coordinates")),
        a.get("flightable", False),
        a.get("iata_type")
    )
    for a in airports
])
conn.commit()

print("Ładowanie planes...")
planes = load_json("planes.json")
execute_batch(cur, """
    INSERT INTO planes (code, name)
    VALUES (%s, %s)
""", [
    (p.get("code"), p.get("name"))
    for p in planes
])
conn.commit()

print("Ładowanie routes...")
routes = load_json("routes.json")
execute_batch(cur, """
    INSERT INTO routes (
        airline_iata, airline_icao,
        departure_airport_iata, departure_airport_icao,
        arrival_airport_iata, arrival_airport_icao,
        codeshare, transfers, planes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""", [
    (
        r.get("airline_iata") or None,
        r.get("airline_icao") or None,
        r.get("departure_airport_iata") or None,
        r.get("departure_airport_icao") or None,
        r.get("arrival_airport_iata") or None,
        r.get("arrival_airport_icao") or None,
        r.get("codeshare", False),
        r.get("transfers", 0),
        Json(r.get("planes"))
    )
    for r in routes
])
conn.commit()

# ========================
# 3. CLEANUP + FK + INDEX
# ========================
print("Finalizacja (clean + index + FK)...")
run_sql_file("post_init.sql")  # <-- reszta SQL (DELETE, FK, INDEX, VIEW)

cur.close()
conn.close()

print("DONE ✅")