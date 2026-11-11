"""
Comandos del monitor de administración, que llegan por el socket Unix.

El monitor abre, manda un comando, recibe la respuesta y cierra. Es un
archivo local, así que solo se puede administrar desde la misma máquina.
"""

import asyncio
import os

from src.shared import (
    enviar_mensaje, recibir_mensaje, ErrorProtocolo, MensajeInvalido, obtener_logger, esquemas,
)
from src.infrastructure import (
    get_async_connection,
    buscar_supermercado_por_nombre,
    crear_supermercado, listar_supermercados,
    cambiar_activo_supermercado, obtener_estadisticas_globales,
)
from src.workers.tasks import verificar_vencimientos, limpiar_notificaciones_antiguas

logger = obtener_logger("servidor")


class ControladorAdmin:

    def __init__(self, sesiones, pool_auth):
        self.sesiones = sesiones
        self.pool_auth = pool_auth

    async def ejecutar(self, conn, comando: dict) -> dict:
        """Ejecuta un comando del monitor y devuelve la respuesta."""
        accion = comando.get("accion", "")

        if accion == "registrar_supermercado":
            password = comando.get("password") or ""
            if len(password) < 4:
                return {"ok": False, "mensaje": "La contraseña debe tener al menos 4 caracteres."}
            # generar el hash también va al pool
            password_hash = await self.pool_auth.hashear(password)
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
                sesion = self.sesiones.obtener(datos.get("supermercado_id"))
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
                "sesiones": self.sesiones.nombres(),
                "total_sesiones": len(self.sesiones),
            }

        if accion == "stats_auth":
            # Contadores compartidos con los procesos del pool
            return {"ok": True, "auth": self.pool_auth.estadisticas()}

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

    async def manejar_conexion(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """El monitor abre, manda un comando, recibe la respuesta y cierra."""
        conn = None
        try:
            try:
                comando = await recibir_mensaje(reader)
            except MensajeInvalido as e:
                await enviar_mensaje(writer, {"ok": False, "mensaje": str(e)})
                return
            if comando is None:
                return

            logger.info(f"[monitor] comando: {comando.get('accion', '?')}")
            if (problema := esquemas.validar(comando, esquemas.MONITOR)) is not None:
                await enviar_mensaje(writer, {"ok": False, "mensaje": problema})
                return

            conn = await get_async_connection()
            respuesta = await self.ejecutar(conn, comando)
            await enviar_mensaje(writer, respuesta)

        except asyncio.IncompleteReadError:
            logger.info("[monitor] conexión IPC cerrada abruptamente")
        except ErrorProtocolo as e:
            logger.warning(f"[monitor] error de protocolo: {e}")
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

    async def escuchar(self, socket_path: str):
        """Socket Unix para el monitor."""
        # Si quedó el archivo de una corrida anterior, el bind falla con
        # 'Address already in use'.
        if os.path.exists(socket_path):
            os.remove(socket_path)

        servidor = await asyncio.start_unix_server(self.manejar_conexion, path=socket_path)
        logger.info(f"Monitor IPC escuchando en {socket_path}")
        async with servidor:
            await servidor.serve_forever()
