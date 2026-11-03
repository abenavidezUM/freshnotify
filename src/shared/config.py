"""Configuración del proyecto: lee el .env (si existe) y define los valores por defecto."""

import os
from dotenv import load_dotenv

# Ruta al .env de la raíz, para que funcione desde cualquier directorio
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

# --- Servidor TCP ---

# "" = escuchar en todas las interfaces
SERVER_LISTEN_HOST = os.getenv("SERVER_LISTEN_HOST", "")

# A dónde se conecta el cliente (se puede cambiar con --host)
SERVER_CONNECT_HOST = os.getenv("SERVER_CONNECT_HOST", "localhost")
SERVER_PORT = int(os.getenv("SERVER_PORT", 8888))

# Socket Unix del monitor
MONITOR_SOCKET_PATH = os.getenv("MONITOR_SOCKET_PATH", "/tmp/fresh_monitor.sock")


# --- MariaDB ---
DB_HOST = os.getenv("DB_HOST", "localhost")
# 3307 porque es el que expone docker-compose (el 3306 suele estar
# ocupado por un MySQL local)
DB_PORT = int(os.getenv("DB_PORT", 3307))
DB_NAME = os.getenv("DB_NAME", "fresh_db")
DB_USER = os.getenv("DB_USER", "fresh_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "fresh_pass")


# --- Redis (broker de Celery y pub/sub de notificaciones) ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
REDIS_NOTIFICATIONS_CHANNEL = os.getenv("REDIS_NOTIFICATIONS_CHANNEL", "fresh:notificaciones")


# --- Celery ---
CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL

# Cada cuánto Beat lanza la verificación de vencimientos. 30 s es para
# la demo; en uso real alcanzaría con una vez por hora o por día.
VERIFICATION_INTERVAL_SECONDS = int(os.getenv("VERIFICATION_INTERVAL_SECONDS", 30))

# Días que se guardan las notificaciones leídas antes de borrarlas
NOTIFICATION_RETENTION_DAYS = int(os.getenv("NOTIFICATION_RETENTION_DAYS", 30))


# --- Login (pool de procesos) ---

# Procesos del pool que verifica contraseñas
AUTH_POOL_PROCESSES = int(os.getenv("AUTH_POOL_PROCESSES", 4))

# Cuántas verificaciones se permiten a la vez (semáforo). Va aparte del
# tamaño del pool: sirve para frenar intentos de fuerza bruta.
MAX_LOGINS_SIMULTANEOS = int(os.getenv("MAX_LOGINS_SIMULTANEOS", 2))
