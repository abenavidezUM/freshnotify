"""Cosas que comparten los tests.

Los unitarios no necesitan nada levantado. Los marcados con "integracion"
usan MariaDB y Redis del compose (desde la máquina: localhost:3307 y 6379);
si no están, se saltean en vez de fallar."""

import asyncio

import pymysql
import pytest
import redis

from src.infrastructure import get_sync_connection, get_sync_redis_client


class WriterFalso:
    """Lo mínimo de un StreamWriter: guarda lo que se le escribe."""

    def __init__(self):
        self.datos = b""
        self.cerrado = False

    def write(self, datos: bytes):
        self.datos += datos

    async def drain(self):
        pass

    def close(self):
        self.cerrado = True


def lector(datos: bytes, fin: bool = True) -> asyncio.StreamReader:
    """Un StreamReader con estos bytes ya cargados, como si hubieran llegado
    por la red. Con fin=False el otro lado sigue conectado sin mandar más.
    Se tiene que llamar dentro de un event loop."""
    r = asyncio.StreamReader()
    r.feed_data(datos)
    if fin:
        r.feed_eof()
    return r


@pytest.fixture(scope="session")
def servicios():
    """Revisa una vez que estén MariaDB y Redis; si no, saltea el test."""
    try:
        get_sync_connection().close()
        get_sync_redis_client().ping()
    except (pymysql.err.OperationalError, redis.exceptions.ConnectionError):
        pytest.skip("MariaDB o Redis no están levantados (docker compose up -d)")


@pytest.fixture(scope="session")
def sql(servicios):
    """Ejecuta una consulta en una conexión nueva y devuelve las filas."""
    def ejecutar(consulta, args=()):
        conn = get_sync_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(consulta, args)
                return cur.fetchall()
        finally:
            conn.close()
    return ejecutar


def borrar_sucursal(ejecutar, nombre):
    filas = ejecutar("SELECT id FROM supermercados WHERE nombre = %s", (nombre,))
    if filas:
        for tabla in ("notificaciones", "lotes"):
            ejecutar(f"DELETE FROM {tabla} WHERE supermercado_id = %s", filas[0])
        ejecutar("DELETE FROM supermercados WHERE id = %s", filas[0])
