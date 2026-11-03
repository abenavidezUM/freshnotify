"""
Servidor central de FreshNotify.

Servidor TCP con asyncio. Por cada conexión:
  1. handshake: la sucursal se identifica por nombre
  2. se registra la sesión (una sola por sucursal)
  3. loop de pedidos: cada mensaje es una operación sobre sus lotes
  4. al desconectarse se borra la sesión

Casi todo el tiempo se espera red o base de datos, así que con un solo
event loop alcanza para atender varias sucursales sin threads.
"""

import argparse
import asyncio

from src.shared import (
    SERVER_LISTEN_HOST, SERVER_PORT,
    enviar_mensaje, recibir_mensaje,
    obtener_logger,
)
from src.infrastructure import (
    get_async_connection,
    buscar_supermercado_por_nombre,
    obtener_resumen_supermercado,
    crear_lote, listar_lotes, buscar_lote, actualizar_lote, eliminar_lote,
    ver_notificaciones,
)

logger = obtener_logger("servidor")

# Sesiones activas, una por sucursal.
# No hace falta lock: es un solo hilo y no hay await entre leer y escribir.
sesiones: dict[int, dict] = {}  # id → {"nombre": str, "writer": StreamWriter}


# --- Clientes TCP ---

async def realizar_handshake(conn, reader, writer, direccion) -> dict | None:
    """Primer mensaje: la sucursal dice su nombre. Devuelve sus datos, o None si se rechaza."""
    mensaje = await recibir_mensaje(reader)
    if mensaje is None or mensaje.get("accion") != "conectar":
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Handshake inválido."})
        return None

    nombre = (mensaje.get("nombre") or "").strip()
    if not nombre:
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "El nombre del supermercado es obligatorio."})
        return None

    resultado = await buscar_supermercado_por_nombre(conn, nombre)

    if not resultado["encontrado"]:
        logger.warning(f"Rechazo desde {direccion}: '{nombre}' no registrado")
        await enviar_mensaje(writer, {
            "tipo": "rechazo",
            "mensaje": f"El supermercado '{nombre}' no está registrado.",
        })
        return None

    if not resultado["activo"]:
        logger.warning(f"Rechazo desde {direccion}: '{nombre}' desactivado")
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": f"El supermercado '{nombre}' está desactivado."})
        return None

    if resultado["supermercado_id"] in sesiones:
        logger.warning(f"Rechazo desde {direccion}: '{nombre}' ya tiene sesión activa")
        await enviar_mensaje(writer, {
            "tipo": "rechazo",
            "mensaje": f"'{resultado['nombre']}' ya tiene una sesión activa. Solo se permite una por sucursal.",
        })
        return None

    return resultado


async def despachar_accion(conn, supermercado_id: int, mensaje: dict) -> dict:
    """Ejecuta la acción pedida. El supermercado_id sale de la sesión, no del
    mensaje, así cada sucursal solo puede tocar sus propios lotes."""
    accion = mensaje.get("accion", "")

    if accion == "crear_lote":
        return await crear_lote(
            conn, supermercado_id,
            mensaje.get("codigo"), mensaje.get("producto"),
            mensaje.get("categoria"), mensaje.get("cantidad"),
            mensaje.get("fecha_vencimiento"),
        )
    if accion == "actualizar_lote":
        return await actualizar_lote(
            conn, supermercado_id, mensaje.get("codigo"),
            producto=mensaje.get("producto"),
            categoria=mensaje.get("categoria"),
            cantidad=mensaje.get("cantidad"),
            fecha_vencimiento=mensaje.get("fecha_vencimiento"),
        )
    if accion == "eliminar_lote":
        return await eliminar_lote(conn, supermercado_id, mensaje.get("codigo"))
    if accion == "listar_lotes":
        return await listar_lotes(conn, supermercado_id)
    if accion == "buscar_lote":
        return await buscar_lote(conn, supermercado_id, mensaje.get("codigo"))
    if accion == "ver_notificaciones":
        return await ver_notificaciones(conn, supermercado_id, mensaje.get("solo_no_leidas", False))
    if accion == "resumen_estado":
        return await obtener_resumen_supermercado(conn, supermercado_id)

    return {"ok": False, "mensaje": f"Acción '{accion}' no reconocida."}


async def manejar_cliente(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Una conexión de punta a punta: handshake, sesión, pedidos y limpieza."""
    direccion = writer.get_extra_info("peername")
    logger.info(f"Conexión entrante desde {direccion}")

    conn = None
    supermercado = None

    try:
        conn = await get_async_connection()

        supermercado = await realizar_handshake(conn, reader, writer, direccion)
        if supermercado is None:
            return

        sm_id, sm_nombre = supermercado["supermercado_id"], supermercado["nombre"]
        sesiones[sm_id] = {"nombre": sm_nombre, "writer": writer}
        logger.info(f"'{sm_nombre}' conectado (id={sm_id}). Sesiones activas: {len(sesiones)}")

        resumen = await obtener_resumen_supermercado(conn, sm_id)
        await enviar_mensaje(writer, {**resumen, "tipo": "bienvenida", "nombre": sm_nombre})

        while True:
            mensaje = await recibir_mensaje(reader)
            if mensaje is None:
                break

            accion = mensaje.get("accion", "?")
            resultado = await despachar_accion(conn, sm_id, mensaje)
            logger.info(f"[{sm_nombre}] {accion} → ok={resultado.get('ok')}")
            await enviar_mensaje(writer, {"tipo": "respuesta", **resultado})

    except asyncio.IncompleteReadError:
        logger.info(f"Desconexión abrupta de {direccion}")
    except Exception as e:
        logger.error(f"Error inesperado con {direccion}: {e}")

    finally:
        if supermercado is not None:
            sesiones.pop(supermercado["supermercado_id"], None)
            logger.info(f"'{supermercado['nombre']}' desconectado. Sesiones activas: {len(sesiones)}")
        if conn is not None:
            conn.close()
        writer.close()
        await writer.wait_closed()


async def iniciar_servidor(host: str, puerto: int):
    host_efectivo = host if host else None
    servidor = await asyncio.start_server(manejar_cliente, host_efectivo, puerto)
    logger.info(f"FreshNotify escuchando en {host_efectivo or '0.0.0.0'}:{puerto}")

    async with servidor:
        await servidor.serve_forever()


def parsear_argumentos():
    parser = argparse.ArgumentParser(description="FreshNotify — servidor central")
    parser.add_argument("--host", default=SERVER_LISTEN_HOST,
                        help="Host donde escuchar (default: todas las interfaces)")
    parser.add_argument("--puerto", type=int, default=SERVER_PORT,
                        help=f"Puerto donde escuchar (default: {SERVER_PORT})")
    return parser.parse_args()


if __name__ == "__main__":
    args = parsear_argumentos()
    try:
        asyncio.run(iniciar_servidor(args.host, args.puerto))
    except KeyboardInterrupt:
        logger.info("Servidor detenido.")
