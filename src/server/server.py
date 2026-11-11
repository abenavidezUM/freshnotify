"""
Servidor central de FreshNotify: arranca y apaga todo.

Corre en un solo event loop con tres tareas:
  1. servidor TCP para las sucursales (login + CRUD de lotes)      → clientes.py
  2. socket Unix para el monitor (administración local)            → comandos_admin.py
  3. suscripción a Redis para reenviar las notificaciones          → notificaciones.py
Las tres comparten las sesiones activas (sesiones.py) y el pool de procesos
que verifica contraseñas (src/auth).

Casi todo es esperar red o base de datos, así que alcanza con asyncio.
Lo único pesado es bcrypt, que va al pool de procesos.
"""

import argparse
import asyncio

from src.shared import SERVER_LISTEN_HOST, SERVER_PORT, MONITOR_SOCKET_PATH, obtener_logger
from src.auth import PoolAutenticacion
from src.server.sesiones import GestorSesiones
from src.server.notificaciones import ServicioNotificaciones
from src.server.clientes import AtencionClientes
from src.server.comandos_admin import ControladorAdmin

logger = obtener_logger("servidor")


async def iniciar_servidor(host: str, puerto: int, socket_path: str):
    pool_auth = PoolAutenticacion()
    sesiones = GestorSesiones()
    notificaciones = ServicioNotificaciones(sesiones)
    clientes = AtencionClientes(sesiones, pool_auth, notificaciones)
    admin = ControladorAdmin(sesiones, pool_auth)

    host_efectivo = host if host else None
    servidor = await asyncio.start_server(clientes.manejar, host_efectivo, puerto)
    logger.info(f"FreshNotify escuchando en {host_efectivo or '0.0.0.0'}:{puerto}")

    tarea_redis = asyncio.create_task(notificaciones.escuchar())
    tarea_ipc = asyncio.create_task(admin.escuchar(socket_path))

    try:
        async with servidor:
            await servidor.serve_forever()
    finally:
        tarea_redis.cancel()
        tarea_ipc.cancel()
        for t in (tarea_redis, tarea_ipc):
            try:
                await t
            except asyncio.CancelledError:
                pass
        pool_auth.cerrar()


def parsear_argumentos():
    parser = argparse.ArgumentParser(description="FreshNotify — servidor central")
    parser.add_argument("--host", default=SERVER_LISTEN_HOST,
                        help="Host donde escuchar (default: todas las interfaces)")
    parser.add_argument("--puerto", type=int, default=SERVER_PORT,
                        help=f"Puerto donde escuchar (default: {SERVER_PORT})")
    parser.add_argument("--socket", default=MONITOR_SOCKET_PATH,
                        help=f"Ruta del UDS del monitor (default: {MONITOR_SOCKET_PATH})")
    return parser.parse_args()


if __name__ == "__main__":
    args = parsear_argumentos()
    try:
        asyncio.run(iniciar_servidor(args.host, args.puerto, args.socket))
    except KeyboardInterrupt:
        logger.info("Servidor detenido.")
