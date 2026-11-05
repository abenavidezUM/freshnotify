"""
Clientes de Redis, igual que con la base: async para el servidor (escucha
el pub/sub sin frenar el event loop) y sync para los workers (publican).
"""

import redis
import redis.asyncio

from src.shared.config import REDIS_HOST, REDIS_PORT, REDIS_DB


def get_async_redis_client() -> redis.asyncio.Redis:
    return redis.asyncio.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)


def get_sync_redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
