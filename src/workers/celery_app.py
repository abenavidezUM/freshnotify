"""
App de Celery.

El servidor encola tareas en Redis con .delay() y los workers las van
sacando y ejecutando; nunca se conectan entre sí. Beat encola las tareas
periódicas según beat_schedule.

Hay dos colas, porque las tareas no se parecen:
- notificaciones: avisos en vivo y revisión de vencimientos. Son cortas
  (milisegundos) y la sucursal está esperando la 🔔.
- mails: el resumen al encargado. Depende de un servidor de correo externo
  y cada envío tarda. Si estuviera en la misma cola, un aviso urgente
  quedaría esperando detrás de todos los mails pendientes.

Sin -Q un worker atiende las dos (cómodo para desarrollo):
    celery -A src.workers.celery_app worker --loglevel=info
    celery -A src.workers.celery_app beat   --loglevel=info
En el compose cada cola tiene sus propios contenedores (-Q notificaciones / -Q mails).
"""

from celery import Celery
from celery.schedules import crontab
from kombu import Queue

from src.shared.config import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
    CELERY_VISIBILITY_TIMEOUT,
    VERIFICATION_INTERVAL_SECONDS,
)

app = Celery(
    "freshnotify",
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=["src.workers.tasks"],  # módulos donde buscar tareas
)

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    timezone="America/Argentina/Mendoza",
    enable_utc=True,
    broker_connection_retry_on_startup=True,

    task_queues=(Queue("notificaciones"), Queue("mails")),
    task_default_queue="notificaciones",
    task_routes={"src.workers.tasks.enviar_mail_alertas": {"queue": "mails"}},

    # Cada proceso toma una tarea por vez. Por defecto reserva varias de
    # antemano, y con tareas largas un worker se queda con tareas que otro,
    # libre, podría estar haciendo.
    worker_prefetch_multiplier=1,
    # La tarea se confirma al terminar, no al empezar: si el worker muere a
    # mitad, Redis se la da a otro (pasado el visibility_timeout). A cambio
    # una tarea puede correr dos veces, por eso tienen que poder repetirse.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_transport_options={"visibility_timeout": CELERY_VISIBILITY_TIMEOUT},
)

# Tareas periódicas
app.conf.beat_schedule = {
    "verificar-vencimientos": {
        "task": "src.workers.tasks.verificar_vencimientos",
        "schedule": float(VERIFICATION_INTERVAL_SECONDS),
    },
    "limpiar-notificaciones-antiguas": {
        # todos los días a las 3 AM
        "task": "src.workers.tasks.limpiar_notificaciones_antiguas",
        "schedule": crontab(hour=3, minute=0),
    },
}
