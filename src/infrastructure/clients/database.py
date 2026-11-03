"""
Conexiones a MariaDB.

- get_async_connection(): para el servidor (aiomysql), así esperar a la
  base no frena el event loop.
- get_sync_connection(): para los workers de Celery (PyMySQL), que no
  tienen event loop.
"""

import aiomysql
import pymysql

from src.shared.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


async def get_async_connection():
    """Conexión para el servidor. Con autocommit no quedan transacciones abiertas."""
    return await aiomysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        autocommit=True,
    )


def get_sync_connection():
    """Conexión común (bloqueante) para los workers."""
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        autocommit=True,
    )
