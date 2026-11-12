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


def _persistir_y_publicar(conn, redis_cli, supermercado_id: int, tipo: str, mensaje: str,
                          clave: str | None = None) -> bool:
    """Guarda en la BD y después publica en Redis. Si la base la rechaza por
    repetida no se publica nada y devuelve False."""
    if not repo.crear_notificacion(conn, supermercado_id, tipo, mensaje, clave):
        return False
    redis_cli.publish(
        REDIS_NOTIFICATIONS_CHANNEL,
        json.dumps({"supermercado_id": supermercado_id, "tipo": tipo, "mensaje": mensaje}),
    )
    return True


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
    """La lanza Beat cada tanto. No revisa nada: encola una verificar_sucursal
    por cada sucursal activa. Así, con varios workers, las revisiones se
    reparten entre todos en vez de hacerlas un solo proceso de punta a punta."""
    conn = get_sync_connection()
    try:
        ids = repo.ids_supermercados_activos(conn)
    finally:
        conn.close()

    for supermercado_id in ids:
        verificar_sucursal.delay(supermercado_id)
    logger.info(f"[verificar_vencimientos] {len(ids)} sucursales encoladas")
    return {"sucursales": len(ids)}


@app.task
def verificar_sucursal(supermercado_id: int):
    """Revisa una sucursal:
    - lotes que vencen dentro de su umbral → alerta (una vez por día)
    - lotes ya vencidos → baja automática + alerta
    Puede correr a la vez que otra revisión de la misma sucursal (otro worker,
    o una pasada que se forzó desde el monitor) sin repetir avisos.
    """
    conn = get_sync_connection()
    redis_cli = get_sync_redis_client()
    alertas, bajas = 0, 0
    try:
        sm = repo.supermercado_activo(conn, supermercado_id)
        if sm is None:
            return {"alertas": 0, "bajas_automaticas": 0}

        for lote in repo.lotes_por_vencer(conn, sm["id"], sm["umbral_dias"]):
            dias = int(lote["dias_restantes"])
            cuando = "HOY" if dias == 0 else f"en {dias} día(s)"
            mensaje = (
                f"⚠ El lote {lote['codigo']} ({lote['producto']}, "
                f"{lote['cantidad']} u.) vence {cuando}."
            )
            if _persistir_y_publicar(conn, redis_cli, sm["id"], "vencimiento_proximo", mensaje,
                                     clave=f"proximo:{lote['codigo']}"):
                alertas += 1

        for lote in repo.dar_baja_lotes_vencidos(conn, sm["id"]):
            mensaje = (
                f"✗ El lote {lote['codigo']} ({lote['producto']}, {lote['cantidad']} u.) "
                f"VENCIÓ y fue dado de baja automáticamente. Retirar de góndola."
            )
            _persistir_y_publicar(conn, redis_cli, sm["id"], "lote_vencido", mensaje)
            bajas += 1

        logger.info(f"[verificar_sucursal] {sm['nombre']}: {alertas} alertas nuevas, "
                    f"{bajas} bajas automáticas")
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
