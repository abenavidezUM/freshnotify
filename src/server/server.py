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
import signal

from src.shared import SERVER_LISTEN_HOST, SERVER_PORT, MONITOR_SOCKET_PATH, obtener_logger
from src.auth import PoolAutenticacion
from src.server.sesiones import GestorSesiones
from src.server.tareas import ColaTareas
from src.server.notificaciones import ServicioNotificaciones
from src.server.clientes import AtencionClientes
from src.server.comandos_admin import ControladorAdmin

logger = obtener_logger("servidor")

# Tiempo máximo de cada paso del apagado, para no quedar colgado esperando
TIEMPO_APAGADO = 5  # segundos


async def iniciar_servidor(host: str, puerto: int, socket_path: str):
    pool_auth = PoolAutenticacion()
    cola_tareas = ColaTareas()
    sesiones = GestorSesiones()
    notificaciones = ServicioNotificaciones(sesiones, cola_tareas)
    clientes = AtencionClientes(sesiones, pool_auth, notificaciones)
    admin = ControladorAdmin(sesiones, pool_auth, cola_tareas)

    # Ctrl+C y SIGTERM (kill, docker stop) no cortan en seco: arrancan el apagado
    apagar = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, apagar.set)

    servidor = None
    tareas = []
    try:
        host_efectivo = host if host else None
        servidor = await asyncio.start_server(clientes.manejar, host_efectivo, puerto)
        logger.info(f"FreshNotify escuchando en {host_efectivo or '0.0.0.0'}:{puerto}")

        tareas = [
            asyncio.create_task(notificaciones.escuchar()),
            asyncio.create_task(admin.escuchar(socket_path)),
        ]
        await apagar.wait()
        logger.info("Apagando el servidor...")
    finally:
        await apagado_ordenado(servidor, tareas, sesiones, cola_tareas, pool_auth)


async def apagado_ordenado(servidor, tareas, sesiones, cola_tareas, pool_auth):
    """De afuera hacia adentro: primero se deja de recibir, después se
    despide a los que están conectados, y al final se cierra lo que ellos
    usaban (Redis, la cola de tareas, el pool de procesos)."""
    if servidor is not None:
        servidor.close()  # no acepta más conexiones
        await sesiones.cerrar_todas("El servidor se está apagando. Volvé a conectarte en un rato.")
        try:
            # Espera a que cada handler termine y libere su conexión a la base
            await asyncio.wait_for(servidor.wait_closed(), TIEMPO_APAGADO)
        except TimeoutError:
            logger.warning("Quedaron conexiones sin cerrar (clientes a mitad del login)")

    # Suscripción a Redis y socket del monitor (que borra su archivo al salir)
    for t in tareas:
        t.cancel()
    await asyncio.gather(*tareas, return_exceptions=True)

    await cola_tareas.cerrar(TIEMPO_APAGADO)
    await pool_auth.cerrar(TIEMPO_APAGADO)
    logger.info("Servidor detenido.")


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
    asyncio.run(iniciar_servidor(args.host, args.puerto, args.socket))
