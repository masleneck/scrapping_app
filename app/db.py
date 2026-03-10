from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_engine(database_url: str) -> AsyncEngine:
    return create_async_engine(database_url, pool_pre_ping=True)


async def ping_database(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


async def close_engine(engine: AsyncEngine) -> None:
    await engine.dispose()
