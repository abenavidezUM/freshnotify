"""
Servidor central de FreshNotify.

Corre todo en un solo event loop con tres tareas:
  1. servidor TCP para las sucursales (login + CRUD de lotes)
  2. socket Unix para el monitor (administración local, sin abrir un puerto)
  3. suscripción a Redis para reenviar las notificaciones de los workers

Casi todo es esperar red o base de datos, así que alcanza con asyncio.
Lo único pesado es bcrypt, que va al pool de procesos de src/auth.
"""

import argparse
import asyncio
import json
import os

from src.shared import (
    SERVER_LISTEN_HOST, SERVER_PORT, MONITOR_SOCKET_PATH,
    REDIS_NOTIFICATIONS_CHANNEL,
    enviar_mensaje, recibir_mensaje,
    obtener_logger,
)
from src.infrastructure import (
    get_async_connection,
    get_async_redis_client,
    buscar_supermercado_por_nombre,
    obtener_resumen_supermercado,
    crear_supermercado, listar_supermercados,
    cambiar_activo_supermercado, obtener_estadisticas_globales,
    crear_lote, listar_lotes, buscar_lote, actualizar_lote, eliminar_lote,
    ver_notificaciones,
)
from src.auth import PoolAutenticacion
from src.workers.tasks import (
    notificar_evento,
    verificar_vencimientos,
    limpiar_notificaciones_antiguas,
)

logger = obtener_logger("servidor")

# Sesiones activas, una por sucursal. Sirve para mandar cada notificación
# al writer correcto y para que el monitor vea quién está conectado.
# No hace falta lock: es un solo hilo y no hay await entre leer y escribir.
sesiones: dict[int, dict] = {}  # id → {"nombre": str, "writer": StreamWriter}

# Se crea en iniciar_servidor()
pool_auth: PoolAutenticacion | None = None


# --- Clientes TCP ---

async def realizar_handshake(conn, reader, writer, direccion) -> dict | None:
    """Login de la sucursal con nombre y contraseña. El hash se verifica en
    el pool de procesos para no frenar al resto de los clientes.
    Devuelve los datos del supermercado, o None si se rechaza."""
    mensaje = await recibir_mensaje(reader)
    if mensaje is None or mensaje.get("accion") != "conectar":
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Handshake inválido."})
        return None

    nombre = (mensaje.get("nombre") or "").strip()
    password = mensaje.get("password") or ""
    if not nombre or not password:
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Nombre y contraseña son obligatorios."})
        return None

    resultado = await buscar_supermercado_por_nombre(conn, nombre)

    if not resultado["encontrado"]:
        logger.warning(f"Rechazo desde {direccion}: '{nombre}' no registrado")
        await enviar_mensaje(writer, {
            "tipo": "rechazo",
            "mensaje": f"El supermercado '{nombre}' no está registrado. Pedí el alta al administrador.",
        })
        return None

    if not resultado["activo"]:
        logger.warning(f"Rechazo desde {direccion}: '{nombre}' desactivado")
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": f"El supermercado '{nombre}' está desactivado."})
        return None

    if resultado["password_hash"] is None:
        await enviar_mensaje(writer, {
            "tipo": "rechazo",
            "mensaje": "La sucursal no tiene credencial asignada. Pedila al administrador.",
        })
        return None

    # bcrypt corre en el pool de procesos
    es_valido = await pool_auth.verificar(password, resultado["password_hash"])
    if not es_valido:
        logger.warning(f"Rechazo desde {direccion}: contraseña inválida para '{nombre}'")
        await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Credenciales inválidas."})
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
    """Ejecuta la acción pedida sobre los lotes de la sucursal de la sesión.
    Después de un alta, modificación o baja se encola la notificación con
    .delay(), que es casi instantáneo: el cliente no espera al worker."""
    accion = mensaje.get("accion", "")

    if accion == "crear_lote":
        resultado = await crear_lote(
            conn, supermercado_id,
            mensaje.get("codigo"), mensaje.get("producto"),
            mensaje.get("categoria"), mensaje.get("cantidad"),
            mensaje.get("fecha_vencimiento"),
        )
        if resultado["ok"]:
            notificar_evento.delay(
                supermercado_id, "lote_creado",
                f"Alta: lote {mensaje.get('codigo')} ({mensaje.get('producto')}) ingresó al inventario.",
            )
        return resultado

    if accion == "actualizar_lote":
        resultado = await actualizar_lote(
            conn, supermercado_id, mensaje.get("codigo"),
            producto=mensaje.get("producto"),
            categoria=mensaje.get("categoria"),
            cantidad=mensaje.get("cantidad"),
            fecha_vencimiento=mensaje.get("fecha_vencimiento"),
        )
        if resultado["ok"]:
            notificar_evento.delay(
                supermercado_id, "lote_actualizado",
                f"Modificación: lote {mensaje.get('codigo')} actualizado.",
            )
        return resultado

    if accion == "eliminar_lote":
        resultado = await eliminar_lote(conn, supermercado_id, mensaje.get("codigo"))
        if resultado["ok"]:
            notificar_evento.delay(
                supermercado_id, "lote_eliminado",
                f"Baja manual: lote {mensaje.get('codigo')} salió del inventario.",
            )
        return resultado

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
        logger.info(f"'{sm_nombre}' autenticado (id={sm_id}). Sesiones activas: {len(sesiones)}")

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


# --- Notificaciones desde Redis ---

async def escuchar_notificaciones_redis():
    """Escucha el canal de notificaciones de Redis. Si la sucursal está
    conectada se la reenvía; si no, no pasa nada: ya quedó guardada en la
    base y la va a ver en el historial."""
    cliente = get_async_redis_client()
    pubsub = cliente.pubsub()
    await pubsub.subscribe(REDIS_NOTIFICATIONS_CHANNEL)
    logger.info(f"Suscrito al canal Redis '{REDIS_NOTIFICATIONS_CHANNEL}'")

    try:
        async for crudo in pubsub.listen():
            if crudo["type"] != "message":
                continue  # el primer mensaje es la confirmación de suscripción

            try:
                payload = json.loads(crudo["data"])
                sm_id = payload.get("supermercado_id")

                sesion = sesiones.get(sm_id)
                if sesion is not None:
                    await enviar_mensaje(sesion["writer"], {
                        "tipo": "notificacion",
                        "notif_tipo": payload.get("tipo", ""),
                        "mensaje": payload.get("mensaje", ""),
                    })
                    logger.info(f"Notificación reenviada en vivo a '{sesion['nombre']}'")
                else:
                    logger.info(f"Notificación para id={sm_id}: desconectado, queda en su historial")
            except Exception as e:
                logger.error(f"Error procesando notificación de Redis: {e}")

    except asyncio.CancelledError:
        await pubsub.unsubscribe(REDIS_NOTIFICATIONS_CHANNEL)
        await cliente.aclose()
        raise


# --- Monitor (socket Unix) ---

async def manejar_comando_monitor(conn, comando: dict) -> dict:
    """Ejecuta un comando del monitor y devuelve la respuesta."""
    accion = comando.get("accion", "")

    if accion == "registrar_supermercado":
        password = comando.get("password") or ""
        if len(password) < 4:
            return {"ok": False, "mensaje": "La contraseña debe tener al menos 4 caracteres."}
        # generar el hash también va al pool
        password_hash = await pool_auth.hashear(password)
        return await crear_supermercado(conn, comando.get("nombre"), password_hash,
                                        comando.get("umbral_dias", 7))

    if accion == "listar_supermercados":
        return await listar_supermercados(conn)

    if accion == "activar_supermercado":
        return await cambiar_activo_supermercado(conn, comando.get("nombre"), True)

    if accion == "desactivar_supermercado":
        nombre = comando.get("nombre")
        resultado = await cambiar_activo_supermercado(conn, nombre, False)
        # Si está conectada en este momento, se le avisa y se la desconecta
        if resultado["ok"]:
            datos = await buscar_supermercado_por_nombre(conn, nombre)
            sesion = sesiones.get(datos.get("supermercado_id"))
            if sesion is not None:
                try:
                    await enviar_mensaje(sesion["writer"], {
                        "tipo": "notificacion", "notif_tipo": "administrativa",
                        "mensaje": "Tu sucursal fue desactivada por el administrador. Se cierra la conexión.",
                    })
                    sesion["writer"].close()
                except Exception:
                    pass  # si ya se estaba desconectando, no importa
        return resultado

    if accion == "estadisticas":
        return await obtener_estadisticas_globales(conn)

    if accion == "status":
        # Esto vive en memoria del servidor, no está en la BD
        return {
            "ok": True,
            "sesiones": [s["nombre"] for s in sesiones.values()],
            "total_sesiones": len(sesiones),
        }

    if accion == "stats_auth":
        # Contadores compartidos con los procesos del pool
        return {"ok": True, "auth": pool_auth.estadisticas()}

    if accion == "run_tarea":
        tarea = comando.get("tarea", "")
        if tarea == "verificar_vencimientos":
            verificar_vencimientos.delay()
            return {"ok": True, "mensaje": "Tarea 'verificar_vencimientos' encolada."}
        if tarea == "limpiar_notificaciones":
            limpiar_notificaciones_antiguas.delay()
            return {"ok": True, "mensaje": "Tarea 'limpiar_notificaciones_antiguas' encolada."}
        return {"ok": False, "mensaje": f"Tarea '{tarea}' no reconocida."}

    return {"ok": False, "mensaje": f"Acción '{accion}' no reconocida."}


async def manejar_conexion_monitor(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """El monitor abre, manda un comando, recibe la respuesta y cierra."""
    conn = None
    try:
        conn = await get_async_connection()
        comando = await recibir_mensaje(reader)
        if comando is None:
            return

        logger.info(f"[monitor] comando: {comando.get('accion', '?')}")
        respuesta = await manejar_comando_monitor(conn, comando)
        await enviar_mensaje(writer, respuesta)

    except asyncio.IncompleteReadError:
        logger.info("[monitor] conexión IPC cerrada abruptamente")
    except Exception as e:
        logger.error(f"[monitor] error: {e}")
        try:
            await enviar_mensaje(writer, {"ok": False, "mensaje": f"Error interno: {e}"})
        except Exception:
            pass
    finally:
        if conn is not None:
            conn.close()
        writer.close()
        await writer.wait_closed()


async def escuchar_monitor_ipc(socket_path: str):
    """Socket Unix para el monitor. Es un archivo local, así que solo se
    puede administrar desde la misma máquina."""
    # Si quedó el archivo de una corrida anterior, el bind falla con
    # 'Address already in use'.
    if os.path.exists(socket_path):
        os.remove(socket_path)

    servidor = await asyncio.start_unix_server(manejar_conexion_monitor, path=socket_path)
    logger.info(f"Monitor IPC escuchando en {socket_path}")
    async with servidor:
        await servidor.serve_forever()


# --- Arranque ---

async def iniciar_servidor(host: str, puerto: int, socket_path: str):
    global pool_auth
    pool_auth = PoolAutenticacion()

    host_efectivo = host if host else None
    servidor = await asyncio.start_server(manejar_cliente, host_efectivo, puerto)
    logger.info(f"FreshNotify escuchando en {host_efectivo or '0.0.0.0'}:{puerto}")

    tarea_redis = asyncio.create_task(escuchar_notificaciones_redis())
    tarea_ipc = asyncio.create_task(escuchar_monitor_ipc(socket_path))

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
