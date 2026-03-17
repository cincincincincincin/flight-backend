"""
Supabase JWT authentication dependency.

Verifies JWTs using:
1. HS256 with SUPABASE_JWT_SECRET (legacy, primary)
2. RS256 via JWKS endpoint as fallback (if supabase_url is configured)
"""
import logging
import time
from typing import Optional

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from src.config import settings

logger = logging.getLogger(__name__)

# In-memory JWKS cache (TTL: 1 hour)
_jwks_cache: Optional[dict] = None
_jwks_fetched_at: float = 0.0
JWKS_TTL_SECONDS = 3600

bearer_scheme = HTTPBearer(auto_error=False)


async def _get_jwks() -> Optional[dict]:
    """Fetch and cache Supabase JWKS. Returns None if unavailable."""
    global _jwks_cache, _jwks_fetched_at

    if not settings.supabase_url:
        return None

    now = time.monotonic()
    if _jwks_cache and (now - _jwks_fetched_at) < JWKS_TTL_SECONDS:
        return _jwks_cache

    jwks_url = f"{settings.supabase_url}/auth/v1/.well-known/jwks.json"
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(jwks_url)
            resp.raise_for_status()
        _jwks_cache = resp.json()
        _jwks_fetched_at = now
        logger.info("JWKS fetched successfully from Supabase")
        return _jwks_cache
    except Exception as e:
        logger.warning(f"Failed to fetch JWKS: {e}")
        return None


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> dict:
    """
    FastAPI dependency. Validates Supabase Bearer token and returns the JWT payload.
    Tries HS256 with JWT secret first, then RS256 via JWKS.
    Raises HTTP 401 if the token is missing or invalid.

    Usage:
        @router.get("/trips")
        async def get_trips(user: dict = Depends(get_current_user)):
            user_id = user["sub"]  # Supabase user UUID
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials

    # Try HS256 with JWT secret (legacy, primary method)
    if settings.supabase_jwt_secret:
        try:
            payload = jwt.decode(
                token,
                settings.supabase_jwt_secret,
                algorithms=["HS256"],
                options={"verify_aud": False},
            )
            return payload
        except JWTError as e:
            logger.debug(f"HS256 verification failed: {e}")

    # Fallback: RS256 via JWKS
    try:
        jwks = await _get_jwks()
        if jwks:
            payload = jwt.decode(
                token,
                jwks,
                algorithms=["RS256", "ES256"],
                options={"verify_aud": False},
            )
            return payload
    except Exception as e:
        logger.debug(f"RS256 verification failed: {e}")

    logger.warning("JWT verification failed: all methods exhausted")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or expired token",
        headers={"WWW-Authenticate": "Bearer"},
    )
