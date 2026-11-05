"""
App de Celery.

El servidor encola tareas en Redis con .delay() y el worker las va sacando
y ejecutando; nunca se conectan entre sí. Beat encola las tareas periódicas
según beat_schedule.

Cada uno corre en su propio proceso:
    celery -A src.workers.celery_app worker --loglevel=info
    celery -A src.workers.celery_app beat   --loglevel=info
"""

from celery import Celery
from celery.schedules import crontab

from src.shared.config import (
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
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
