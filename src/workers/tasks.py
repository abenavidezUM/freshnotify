"""
Tareas de Celery (corren en el worker, no en el servidor).

Cada notificación primero se guarda en MariaDB, así queda en el historial
aunque la sucursal no esté conectada, y después se publica en Redis, donde
el servidor la recibe y se la reenvía si está conectada.
"""

import json

from src.shared import (
    REDIS_NOTIFICATIONS_CHANNEL,
    NOTIFICATION_RETENTION_DAYS,
    obtener_logger,
)
from src.infrastructure import get_sync_connection, get_sync_redis_client
from src.infrastructure.repositories import sync_workers as repo
from src.workers.celery_app import app

logger = obtener_logger("worker")


def _persistir_y_publicar(conn, redis_cli, supermercado_id: int, tipo: str, mensaje: str) -> None:
    """Guarda en la BD y después publica en Redis."""
    repo.crear_notificacion(conn, supermercado_id, tipo, mensaje)
    redis_cli.publish(
        REDIS_NOTIFICATIONS_CHANNEL,
        json.dumps({"supermercado_id": supermercado_id, "tipo": tipo, "mensaje": mensaje}),
    )


@app.task
def notificar_evento(supermercado_id: int, tipo: str, mensaje: str):
    """Aviso de alta/modificación/baja de un lote. El servidor la encola con
    .delay() después de cada operación, así el cliente no tiene que esperarla."""
    conn = get_sync_connection()
    redis_cli = get_sync_redis_client()
    try:
        _persistir_y_publicar(conn, redis_cli, supermercado_id, tipo, mensaje)
        logger.info(f"[notificar_evento] supermercado={supermercado_id} tipo={tipo}")
    finally:
        conn.close()
        redis_cli.close()


@app.task
def verificar_vencimientos():
    """La lanza Beat cada tanto, para cada sucursal activa:
    - lotes que vencen dentro de su umbral → alerta (una vez por día)
    - lotes ya vencidos → baja automática + alerta
    """
    conn = get_sync_connection()
    redis_cli = get_sync_redis_client()
    alertas, bajas = 0, 0
    try:
        for sm in repo.supermercados_activos(conn):

            for lote in repo.lotes_por_vencer(conn, sm["id"], sm["umbral_dias"]):
                dias = int(lote["dias_restantes"])
                cuando = "HOY" if dias == 0 else f"en {dias} día(s)"
                mensaje = (
                    f"⚠ El lote {lote['codigo']} ({lote['producto']}, "
                    f"{lote['cantidad']} u.) vence {cuando}."
                )
                # Para el mismo lote el mensaje sale igual, así se detecta si ya se avisó hoy
                if not repo.ya_notificado_hoy(conn, sm["id"], "vencimiento_proximo", mensaje):
                    _persistir_y_publicar(conn, redis_cli, sm["id"], "vencimiento_proximo", mensaje)
                    alertas += 1

            for lote in repo.dar_baja_lotes_vencidos(conn, sm["id"]):
                mensaje = (
                    f"✗ El lote {lote['codigo']} ({lote['producto']}, {lote['cantidad']} u.) "
                    f"VENCIÓ y fue dado de baja automáticamente. Retirar de góndola."
                )
                _persistir_y_publicar(conn, redis_cli, sm["id"], "lote_vencido", mensaje)
                bajas += 1

        logger.info(f"[verificar_vencimientos] {alertas} alertas nuevas, {bajas} bajas automáticas")
        return {"alertas": alertas, "bajas_automaticas": bajas}
    finally:
        conn.close()
        redis_cli.close()


@app.task
def limpiar_notificaciones_antiguas():
    """Limpieza diaria: borra las notificaciones leídas viejas."""
    conn = get_sync_connection()
    try:
        borradas = repo.limpiar_notificaciones_antiguas(conn, NOTIFICATION_RETENTION_DAYS)
        logger.info(f"[limpiar_notificaciones] {borradas} notificaciones antiguas borradas")
        return {"borradas": borradas}
    finally:
        conn.close()
