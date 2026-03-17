import asyncpg
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator, Any, List
from src.config import settings
import json
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create connection pool"""
        self.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=5,
            max_size=20,
            command_timeout=60,
            init=self._init_connection
        )

    async def disconnect(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()

    @staticmethod
    async def _init_connection(conn):
        """Initialize connection with JSON support"""
        await conn.set_type_codec(
            'jsonb',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[Any, None]:
        """Get connection from pool"""
        if not self.pool:
            await self.connect()
        assert self.pool is not None
        async with self.pool.acquire() as connection:
            yield connection

    # --- Dodatkowe metody dla init_db.py ---
    async def execute(self, query: str, *args):
        async with self.get_connection() as conn:
            return await conn.execute(query, *args)

    async def fetch_one(self, query: str, *args) -> Optional[asyncpg.Record]:
        async with self.get_connection() as conn:
            return await conn.fetchrow(query, *args)

    async def fetch_all(self, query: str, *args) -> List[asyncpg.Record]:
        async with self.get_connection() as conn:
            return await conn.fetch(query, *args)

    async def fetch_val(self, query: str, *args):
        """
        Wykonuje zapytanie i zwraca pierwszą kolumnę pierwszego wiersza.
        Przydatne do zapytań typu SELECT COUNT(*) ...
        """
        async with self.get_connection() as conn:
            row = await conn.fetchrow(query, *args)
            if row:
                return row[0]
            return None


db = Database()