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


# --- MariaDB ---
DB_HOST = os.getenv("DB_HOST", "localhost")
# 3307 porque es el que expone docker-compose (el 3306 suele estar
# ocupado por un MySQL local)
DB_PORT = int(os.getenv("DB_PORT", 3307))
DB_NAME = os.getenv("DB_NAME", "fresh_db")
DB_USER = os.getenv("DB_USER", "fresh_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "fresh_pass")
