import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from src.config import settings
from src.database import db
from src.cache import cache
from src.limiter import limiter

from src.endpoints.airports import router as airports_router
from src.endpoints.cities import router as cities_router
from src.endpoints.routes import router as routes_router
from src.endpoints.search import router as search_router
from src.endpoints.flights import router as flights_router
from src.endpoints.trips import router as trips_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('debug.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events"""
    logger.info("Starting up...")

    # Connect to database
    try:
        await db.connect()
        logger.info("Database connected successfully")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

    # Sprawdzenie, czy baza była już inicjalizowana
    try:
        async with db.get_connection() as conn:
            row = await conn.fetchrow("SELECT value FROM app_meta WHERE key='initialized'")
            initialized = row['value'] if row else None
        if not initialized:
            logger.info("Running DB initialization script...")
            from init_db import run_init
            async with db.get_connection() as conn:
                await run_init(conn)  # Twój init_db musi przyjmować conn
                await conn.execute(
                    "INSERT INTO app_meta(key, value) VALUES($1, $2)",
                    "initialized", "1"
                )
            logger.info("Database initialized successfully")
    except Exception as e:
        logger.error(f"Błąd inicjalizacji bazy: {e}")

    # Redis is optional – failure here does not prevent startup
    try:
        await cache.connect()
        logger.info("Cache connected successfully")
    except Exception as e:
        logger.warning(f"Cache connection failed: {e}")

    yield

    logger.info("Shutting down...")
    await db.disconnect()
    await cache.disconnect()
    logger.info("Database disconnected")


# Create FastAPI app
app = FastAPI(
    title=settings.app_name,
    description="API for flight routes and airport mapping with real-time flight data",
    version="1.0.0",
    lifespan=lifespan,
    debug=settings.debug,
)

# --- Rate limiter ---
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)  # type: ignore[arg-type]

# --- Exception handlers ---
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    logger.warning(f"Validation error on {request.url}: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"success": False, "error": "Validation error", "details": exc.errors()},
    )

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "error": exc.detail},
    )

@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception on {request.method} {request.url}: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"success": False, "error": "Internal server error"},
    )

# --- CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Routers ---
app.include_router(airports_router)
app.include_router(cities_router)
app.include_router(routes_router)
app.include_router(search_router)
app.include_router(flights_router)
app.include_router(trips_router)

# --- Root endpoint ---
@app.get("/")
async def root():
    return {
        "message": "Flight Map API",
        "status": "running",
        "version": "1.0.0",
        "endpoints": {
            "airports": "/airports",
            "airports_geojson": "/airports/geojson",
            "cities": "/cities",
            "cities_geojson": "/cities/geojson",
            "routes": "/routes",
            "routes_geojson": "/routes/geojson",
            "search": "/search",
            "flights": "/flights",
            "trips": "/trips",
        },
    }

# --- Health check ---
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# --- Main entry ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.debug,
    )