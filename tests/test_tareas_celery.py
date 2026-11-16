"""Tareas de Celery contra MariaDB y Redis reales: que se puedan repetir o
reintentar sin duplicar avisos ni bajas. Se corren acá mismo (apply), sin
worker."""

import uuid
from unittest import mock

import pymysql.err
import pytest
import redis.exceptions

from src.infrastructure import get_sync_connection, get_sync_redis_client
from src.infrastructure.repositories import sync_workers as repo
from src.workers import tasks
from tests.conftest import borrar_sucursal

pytestmark = pytest.mark.integracion

NOMBRE = "Prueba Pytest Tareas"


@pytest.fixture
def sucursal(sql):
    borrar_sucursal(sql, NOMBRE)
    sql("INSERT INTO supermercados (nombre, umbral_dias) VALUES (%s, 7)", (NOMBRE,))
    yield sql("SELECT id FROM supermercados WHERE nombre = %s", (NOMBRE,))[0][0]
    borrar_sucursal(sql, NOMBRE)


def agregar_lote(sql, sm, codigo, dias):
    sql("INSERT INTO lotes (supermercado_id, codigo, producto, cantidad, fecha_vencimiento) "
        "VALUES (%s, %s, 'Yogur', 4, CURDATE() + INTERVAL %s DAY)", (sm, codigo, dias))
    return sql("SELECT id FROM lotes WHERE supermercado_id = %s AND codigo = %s", (sm, codigo))[0][0]


def avisos(sql, sm, tipo):
    return sql("SELECT clave FROM notificaciones WHERE supermercado_id = %s AND tipo = %s", (sm, tipo))


def test_notificar_evento_reintenta_sin_duplicar(sql, sucursal):
    # Se guarda el aviso, falla el publish (Redis caído) y la tarea se
    # reintenta: el segundo intento no tiene que volver a guardarlo
    real = get_sync_redis_client
    publicados, intentos = [], []

    def redis_que_falla_una_vez():
        cliente = real()

        def publish(canal, datos):
            intentos.append(datos)
            if len(intentos) == 1:
                raise redis.exceptions.ConnectionError("Redis caído (simulado)")
            publicados.append(datos)
        cliente.publish = publish
        return cliente

    with mock.patch.object(tasks, "get_sync_redis_client", redis_que_falla_una_vez), \
         mock.patch.object(tasks.notificar_evento, "retry_backoff", False):
        tarea_id = str(uuid.uuid4())
        r = tasks.notificar_evento.apply(args=(sucursal, "lote_creado", "Lote X creado"),
                                         task_id=tarea_id)

    assert r.successful()
    assert avisos(sql, sucursal, "lote_creado") == ((f"evento:{tarea_id}",),)
    assert len(publicados) == 1


def test_error_que_no_es_transitorio_no_se_reintenta(sucursal):
    with mock.patch.object(tasks.repo, "crear_notificacion", side_effect=ValueError("bug")) as m:
        r = tasks.notificar_evento.apply(args=(sucursal, "lote_creado", "otro"))
    assert r.failed() and m.call_count == 1


def test_alerta_de_vencimiento_una_vez_por_dia(sql, sucursal):
    agregar_lote(sql, sucursal, "PRONTO-1", 2)
    primera = tasks.verificar_sucursal.apply(args=(sucursal,)).result
    segunda = tasks.verificar_sucursal.apply(args=(sucursal,)).result
    assert primera["alertas"] == 1 and segunda["alertas"] == 0
    assert len(avisos(sql, sucursal, "vencimiento_proximo")) == 1


def test_dos_inserts_de_la_misma_clave(sql, sucursal):
    # Lo que pasa cuando dos workers revisan la misma sucursal a la vez:
    # el UNIQUE deja pasar uno solo
    conn = get_sync_connection()
    try:
        args = (conn, sucursal, "vencimiento_proximo", "aviso")
        assert repo.crear_notificacion(*args, clave="proximo:X", diaria=True)
        assert not repo.crear_notificacion(*args, clave="proximo:X", diaria=True)
    finally:
        conn.close()


def test_baja_y_aviso_van_juntos(sql, sucursal):
    # Si la conexión se corta entre la baja y el aviso, se deshacen los dos,
    # así el reintento vuelve a ver el lote activo y avisa
    lote_id = agregar_lote(sql, sucursal, "VENC-1", -1)
    conn = get_sync_connection()
    cursor_real = conn.cursor

    class CursorQueFallaEnElInsert:
        def __init__(self):
            self.c = cursor_real()

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.c.close()

        @property
        def rowcount(self):
            return self.c.rowcount

        def execute(self, consulta, args=None):
            if consulta.lstrip().startswith("INSERT"):
                raise pymysql.err.OperationalError(2013, "Lost connection (simulado)")
            return self.c.execute(consulta, args)

    conn.cursor = CursorQueFallaEnElInsert
    with pytest.raises(pymysql.err.OperationalError):
        repo.dar_baja_vencido(conn, sucursal, lote_id, "VENCIÓ")
    conn.close()
    assert sql("SELECT activo FROM lotes WHERE id = %s", (lote_id,))[0][0] == 1

    r = tasks.verificar_sucursal.apply(args=(sucursal,)).result
    assert r["bajas_automaticas"] == 1
    assert sql("SELECT activo, motivo_baja FROM lotes WHERE id = %s", (lote_id,))[0] == (0, "vencido_automatico")
    assert len(avisos(sql, sucursal, "lote_vencido")) == 1

    # Otra pasada no repite nada
    assert tasks.verificar_sucursal.apply(args=(sucursal,)).result["bajas_automaticas"] == 0
    assert len(avisos(sql, sucursal, "lote_vencido")) == 1
