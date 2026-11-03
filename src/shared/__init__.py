"""Config, protocolo y logger que usan todos los componentes."""

from .config import (
    SERVER_LISTEN_HOST,
    SERVER_CONNECT_HOST,
    SERVER_PORT,
    MONITOR_SOCKET_PATH,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_URL,
    REDIS_NOTIFICATIONS_CHANNEL,
    CELERY_BROKER_URL, CELERY_RESULT_BACKEND,
    VERIFICATION_INTERVAL_SECONDS,
    NOTIFICATION_RETENTION_DAYS,
    AUTH_POOL_PROCESSES,
    MAX_LOGINS_SIMULTANEOS,
)
from .logger import obtener_logger
from .protocol import enviar_mensaje, recibir_mensaje
