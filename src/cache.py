import json
import logging
from typing import Any, Optional, Callable, Awaitable

import redis.asyncio as aioredis
from pydantic import BaseModel

from src.config import settings


class _PydanticEncoder(json.JSONEncoder):
    """JSON encoder that serializes Pydantic models to dicts."""
    def default(self, o: Any) -> Any:
        if isinstance(o, BaseModel):
            return o.model_dump()
        return super().default(o)

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Async Redis cache with graceful degradation.
    If Redis is unavailable all operations are no-ops and callers fall back to DB.
    """

    def __init__(self):
        self._client: Optional[aioredis.Redis] = None

    async def connect(self):
        """Create Redis connection pool. Called from lifespan startup (non-fatal)."""
        try:
            self._client = aioredis.from_url(
                settings.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            await self._client.ping()  # type: ignore[misc]
            logger.info("Redis connected successfully")
        except Exception as e:
            logger.warning(f"Redis unavailable (cache disabled): {e}")
            self._client = None

    async def disconnect(self):
        """Close Redis connection. Called from lifespan shutdown."""
        if self._client:
            await self._client.aclose()
            logger.info("Redis disconnected")

    async def get(self, key: str) -> Optional[Any]:
        """Return cached value or None on miss/error."""
        if not self._client:
            return None
        try:
            raw = await self._client.get(key)
            return json.loads(raw) if raw is not None else None
        except Exception as e:
            logger.warning(f"Redis GET error for '{key}': {e}")
            return None

    async def set(self, key: str, value: Any, ttl: int) -> None:
        """Store value with TTL in seconds. Silently ignores errors."""
        if not self._client:
            return
        try:
            await self._client.set(key, json.dumps(value, cls=_PydanticEncoder), ex=ttl)
        except Exception as e:
            logger.warning(f"Redis SET error for '{key}': {e}")

    async def cached(self, key: str, ttl: int, fn: Callable[[], Awaitable[Any]]) -> Any:
        """
        Cache-aside helper. Returns cached value or calls fn(), stores result.

        Usage:
            result = await cache.cached(
                key="geojson:airports:False:None",
                ttl=86400,
                fn=lambda: airport_service.get_airports_as_geojson(),
            )
        """
        hit = await self.get(key)
        if hit is not None:
            logger.debug(f"Cache HIT: {key}")
            return hit
        logger.debug(f"Cache MISS: {key}")
        result = await fn()
        await self.set(key, result, ttl)
        return result


cache = RedisCache()
