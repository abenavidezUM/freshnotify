"""Config, protocolo y logger que usan todos los componentes."""

from .config import (
    SERVER_LISTEN_HOST,
    SERVER_CONNECT_HOST,
    SERVER_PORT,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
)
from .logger import obtener_logger
from .protocol import enviar_mensaje, recibir_mensaje
