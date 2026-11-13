"""
Tareas de Celery (corren en el worker, no en el servidor).

Cada notificación primero se guarda en MariaDB, así queda en el historial
aunque la sucursal no esté conectada, y después se publica en Redis, donde
el servidor la recibe y se la reenvía si está conectada.
"""

import json
import smtplib
from email.message import EmailMessage

import kombu.exceptions
import pymysql.err
import redis.exceptions

from src.shared import (
    REDIS_NOTIFICATIONS_CHANNEL,
    NOTIFICATION_RETENTION_DAYS,
    SMTP_HOST, SMTP_PORT, MAIL_REMITENTE,
    obtener_logger,
)
from src.infrastructure import get_sync_connection, get_sync_redis_client
from src.infrastructure.repositories import sync_workers as repo
from src.workers.celery_app import app

logger = obtener_logger("worker")


# Errores de los que una tarea se puede recuperar sola esperando un poco:
# la base o Redis caídos o reiniciándose, una conexión cortada, un deadlock.
# Un error de SQL o un dato mal armado no está acá: reintentarlo daría lo mismo.
ERRORES_TRANSITORIOS = (
    pymysql.err.OperationalError,       # no conecta, se cortó, deadlock
    pymysql.err.InterfaceError,         # la conexión ya estaba cerrada
    redis.exceptions.ConnectionError,   # publish con Redis caído
    redis.exceptions.TimeoutError,
    kombu.exceptions.OperationalError,  # .delay() de otra tarea con Redis caído
)

# Las esperas son 2, 4, 8, 16 y después 30 s, con jitter: se elige al azar
# entre 0 y ese tope. Sin jitter, si la base vuelve después de una caída,
# todas las tareas que fallaron juntas reintentan en el mismo segundo.
# El tope (30 s) queda por debajo del visibility_timeout (60 s): Redis le
# pasa a otro worker una tarea que lleva más de eso sin confirmar, y un
# reintento con countdown espera en el worker sin confirmar.
_REINTENTOS = dict(autoretry_for=ERRORES_TRANSITORIOS, retry_backoff=2,
                   retry_backoff_max=30, retry_jitter=True)

# Para lo que no tiene otra pasada que lo rehaga (un aviso de lote, la
# limpieza diaria): hasta 8 reintentos, aguanta una caída de un par de minutos
REINTENTOS_LARGOS = dict(_REINTENTOS, max_retries=8)
# Para la revisión de vencimientos: Beat la vuelve a lanzar cada 30 s, así que
# alcanza con salvar un corte de unos segundos. Si dura más, la próxima pasada
# la cubre y no se juntan reintentos viejos con pasadas nuevas.
REINTENTOS_CORTOS = dict(_REINTENTOS, max_retries=2)


def _publicar(redis_cli, supermercado_id: int, tipo: str, mensaje: str) -> None:
    redis_cli.publish(
        REDIS_NOTIFICATIONS_CHANNEL,
        json.dumps({"supermercado_id": supermercado_id, "tipo": tipo, "mensaje": mensaje}),
    )


@app.task(bind=True, **REINTENTOS_LARGOS)
def notificar_evento(self, supermercado_id: int, tipo: str, mensaje: str):
    """Aviso de alta/modificación/baja de un lote. El servidor la encola
    después de cada operación, así el cliente no tiene que esperarla."""
    conn = get_sync_connection()
    redis_cli = get_sync_redis_client()
    try:
        # La clave es el id de la tarea, que no cambia entre reintentos: si la
        # vez anterior se llegó a guardar y falló el publish, no se guarda de nuevo
        repo.crear_notificacion(conn, supermercado_id, tipo, mensaje,
                                clave=f"evento:{self.request.id}")
        # Se publica aunque ya estuviera guardada: si la tarea se repite es
        # porque la vez anterior no terminó, y lo último que hace es publicar
        _publicar(redis_cli, supermercado_id, tipo, mensaje)
        logger.info(f"[notificar_evento] supermercado={supermercado_id} tipo={tipo}")
    finally:
        conn.close()
        redis_cli.close()


@app.task(**REINTENTOS_CORTOS)
def verificar_vencimientos():
    """La lanza Beat cada tanto. No revisa nada: encola una verificar_sucursal
    por cada sucursal activa. Así, con varios workers, las revisiones se
    reparten entre todos en vez de hacerlas un solo proceso de punta a punta."""
    conn = get_sync_connection()
    try:
        ids = repo.ids_supermercados_activos(conn)
    finally:
        conn.close()

    # Si Redis se cae a mitad del for, el reintento vuelve a encolar todas.
    # No pasa nada: verificar_sucursal se puede repetir sin duplicar avisos.
    for supermercado_id in ids:
        verificar_sucursal.delay(supermercado_id)
    logger.info(f"[verificar_vencimientos] {len(ids)} sucursales encoladas")
    return {"sucursales": len(ids)}


@app.task(**REINTENTOS_CORTOS)
def verificar_sucursal(supermercado_id: int):
    """Revisa una sucursal:
    - lotes que vencen dentro de su umbral → alerta (una vez por día)
    - lotes ya vencidos → baja automática + alerta
    Puede correr a la vez que otra revisión de la misma sucursal (otro worker,
    o una pasada que se forzó desde el monitor) sin repetir avisos, y por lo
    mismo se puede reintentar entera.

    Lo que sí se puede perder en un reintento es la 🔔 en vivo y el mail de un
    aviso que se guardó justo antes de que se cayera Redis: al reintentar ya
    está guardado y no se distingue de uno de una pasada anterior. Queda en
    el historial de la sucursal, que es lo que importa.
    """
    conn = get_sync_connection()
    redis_cli = get_sync_redis_client()
    alertas, bajas = 0, 0
    nuevos = []  # avisos de esta pasada, para el mail
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
            if repo.crear_notificacion(conn, sm["id"], "vencimiento_proximo", mensaje,
                                       clave=f"proximo:{lote['codigo']}", diaria=True):
                _publicar(redis_cli, sm["id"], "vencimiento_proximo", mensaje)
                alertas += 1
                nuevos.append(mensaje)

        for lote in repo.lotes_vencidos(conn, sm["id"]):
            mensaje = (
                f"✗ El lote {lote['codigo']} ({lote['producto']}, {lote['cantidad']} u.) "
                f"VENCIÓ y fue dado de baja automáticamente. Retirar de góndola."
            )
            if repo.dar_baja_vencido(conn, sm["id"], lote["id"], mensaje):
                _publicar(redis_cli, sm["id"], "lote_vencido", mensaje)
                bajas += 1
                nuevos.append(mensaje)

        # El mail va en otra tarea (cola "mails"): esta termina enseguida y,
        # si el servidor de correo anda lento o caído, no frena las revisiones
        if nuevos and sm["email"]:
            enviar_mail_alertas.delay(sm["email"], sm["nombre"], nuevos)

        logger.info(f"[verificar_sucursal] {sm['nombre']}: {alertas} alertas nuevas, "
                    f"{bajas} bajas automáticas")
        return {"alertas": alertas, "bajas_automaticas": bajas}
    finally:
        conn.close()
        redis_cli.close()


# OSError incluye los errores de smtplib y los de conexión. Ojo que con
# jitter la espera es al azar entre 0 y 5, 10, 20, 30… s: con pocos reintentos
# se pueden gastar todos en un par de segundos (con 3, un corte de 12 s del
# SMTP perdía mails). Con 10 aguanta un corte de un par de minutos.
@app.task(bind=True, autoretry_for=(OSError,), retry_backoff=5, retry_backoff_max=30,
          retry_jitter=True, max_retries=10)
def enviar_mail_alertas(self, destinatario: str, sucursal: str, avisos: list[str]):
    """Le manda al encargado de la sucursal un resumen con los avisos nuevos."""
    msg = EmailMessage()
    msg["From"] = MAIL_REMITENTE
    msg["To"] = destinatario
    msg["Subject"] = f"FreshNotify · {sucursal}: {len(avisos)} lote(s) para revisar"
    msg.set_content(
        f"Hola, estos son los avisos nuevos de {sucursal}:\n\n"
        + "\n".join(f"  {a}" for a in avisos)
        + "\n\nEl detalle está en el historial de notificaciones de la sucursal.\n"
    )
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as smtp:
        smtp.send_message(msg)

    logger.info(f"[enviar_mail] {sucursal} → {destinatario} ({len(avisos)} avisos)")
    # Quién lo mandó, para ver cómo se reparten los mails entre los workers
    return {"worker": self.request.hostname}


@app.task(**REINTENTOS_LARGOS)
def limpiar_notificaciones_antiguas():
    """Limpieza diaria: borra las notificaciones leídas viejas. Si la base no
    está a las 3 AM, sin reintentos esperaría hasta el otro día."""
    conn = get_sync_connection()
    try:
        borradas = repo.limpiar_notificaciones_antiguas(conn, NOTIFICATION_RETENTION_DAYS)
        logger.info(f"[limpiar_notificaciones] {borradas} notificaciones antiguas borradas")
        return {"borradas": borradas}
    finally:
        conn.close()
