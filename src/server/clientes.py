"""
Atención de las sucursales por TCP: login, y después los pedidos sobre sus
lotes hasta que se desconectan.
"""

import asyncio

from src.shared import (
    enviar_mensaje, recibir_mensaje, ErrorProtocolo, MensajeInvalido, obtener_logger, esquemas,
)
from src.infrastructure import (
    get_async_connection,
    buscar_supermercado_por_nombre,
    obtener_resumen_supermercado,
    crear_lote, listar_lotes, buscar_lote, actualizar_lote, eliminar_lote,
    ver_notificaciones,
)

logger = obtener_logger("servidor")


class AtencionClientes:

    def __init__(self, sesiones, pool_auth, notificaciones):
        self.sesiones = sesiones
        self.pool_auth = pool_auth
        self.notificaciones = notificaciones

    async def realizar_handshake(self, conn, reader, writer, direccion) -> dict | None:
        """Login de la sucursal con nombre y contraseña. El hash se verifica en
        el pool de procesos para no frenar al resto de los clientes.
        Devuelve los datos del supermercado, o None si se rechaza."""
        try:
            mensaje = await recibir_mensaje(reader)
        except MensajeInvalido:
            mensaje = {}
        if mensaje is None:
            return None
        if esquemas.validar(mensaje, esquemas.LOGIN) is not None:
            await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Handshake inválido."})
            return None

        nombre = mensaje["nombre"].strip()
        password = mensaje["password"]
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
        es_valido = await self.pool_auth.verificar(password, resultado["password_hash"])
        if not es_valido:
            logger.warning(f"Rechazo desde {direccion}: contraseña inválida para '{nombre}'")
            await enviar_mensaje(writer, {"tipo": "rechazo", "mensaje": "Credenciales inválidas."})
            return None

        return resultado

    async def despachar_accion(self, conn, supermercado_id: int, mensaje: dict) -> dict:
        """Ejecuta la acción pedida sobre los lotes de la sucursal de la sesión.
        Después de un alta, modificación o baja se encola la notificación,
        que es casi instantáneo: el cliente no espera al worker."""
        accion = mensaje.get("accion", "")

        if accion == "crear_lote":
            resultado = await crear_lote(
                conn, supermercado_id,
                mensaje.get("codigo"), mensaje.get("producto"),
                mensaje.get("categoria"), mensaje.get("cantidad"),
                mensaje.get("fecha_vencimiento"),
            )
            if resultado["ok"]:
                self.notificaciones.encolar(
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
                self.notificaciones.encolar(
                    supermercado_id, "lote_actualizado",
                    f"Modificación: lote {mensaje.get('codigo')} actualizado.",
                )
            return resultado

        if accion == "eliminar_lote":
            resultado = await eliminar_lote(conn, supermercado_id, mensaje.get("codigo"))
            if resultado["ok"]:
                self.notificaciones.encolar(
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

    async def manejar(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Una conexión de punta a punta: handshake, sesión, pedidos y limpieza."""
        direccion = writer.get_extra_info("peername")
        logger.info(f"Conexión entrante desde {direccion}")

        conn = None
        supermercado = None  # queda con los datos solo si la sesión se registró

        try:
            conn = await get_async_connection()

            datos = await self.realizar_handshake(conn, reader, writer, direccion)
            if datos is None:
                return

            sm_id, sm_nombre = datos["supermercado_id"], datos["nombre"]
            if not self.sesiones.registrar(sm_id, sm_nombre, writer):
                logger.warning(f"Rechazo desde {direccion}: '{sm_nombre}' ya tiene sesión activa")
                await enviar_mensaje(writer, {
                    "tipo": "rechazo",
                    "mensaje": f"'{sm_nombre}' ya tiene una sesión activa. Solo se permite una por sucursal.",
                })
                return
            supermercado = datos
            logger.info(f"'{sm_nombre}' autenticado (id={sm_id}). Sesiones activas: {len(self.sesiones)}")

            resumen = await obtener_resumen_supermercado(conn, sm_id)
            await enviar_mensaje(writer, {**resumen, "tipo": "bienvenida", "nombre": sm_nombre})

            while True:
                # Si el mensaje llegó entero pero no sirve, se contesta y la
                # conexión sigue: el próximo mensaje se puede leer bien
                try:
                    mensaje = await recibir_mensaje(reader)
                except MensajeInvalido as e:
                    await enviar_mensaje(writer, {"tipo": "respuesta", "ok": False, "mensaje": str(e)})
                    continue
                if mensaje is None:
                    break

                accion = mensaje.get("accion", "?")
                if (problema := esquemas.validar(mensaje, esquemas.SUCURSAL)) is not None:
                    logger.warning(f"[{sm_nombre}] pedido inválido: {problema}")
                    await enviar_mensaje(writer, {"tipo": "respuesta", "ok": False, "mensaje": problema})
                    continue

                resultado = await self.despachar_accion(conn, sm_id, mensaje)
                logger.info(f"[{sm_nombre}] {accion} → ok={resultado.get('ok')}")
                await enviar_mensaje(writer, {"tipo": "respuesta", **resultado})

        except asyncio.IncompleteReadError:
            logger.info(f"Desconexión abrupta de {direccion}")
        except ErrorProtocolo as e:
            # No se sabe dónde empieza el próximo mensaje: se avisa y se corta
            logger.warning(f"Error de protocolo con {direccion}: {e}")
            try:
                await enviar_mensaje(writer, {"tipo": "error", "ok": False,
                                              "mensaje": "Mensaje inválido, se cierra la conexión."})
            except OSError:
                pass
        except Exception as e:
            logger.error(f"Error inesperado con {direccion}: {e}")

        finally:
            if supermercado is not None:
                self.sesiones.quitar(supermercado["supermercado_id"])
                logger.info(f"'{supermercado['nombre']}' desconectado. Sesiones activas: {len(self.sesiones)}")
            if conn is not None:
                conn.close()
            writer.close()
            await writer.wait_closed()
