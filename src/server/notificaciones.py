"""
Las dos puntas de las notificaciones del lado del servidor:
- encolar(): después de un cambio en un lote le pide a Celery que genere
  el aviso. El worker lo guarda en la base y lo publica en Redis.
- escuchar(): está suscripto a ese canal de Redis y le reenvía cada aviso
  a la sucursal, si está conectada.
"""

import asyncio
import json

from src.shared import REDIS_NOTIFICATIONS_CHANNEL, enviar_mensaje, obtener_logger
from src.infrastructure import get_async_redis_client
from src.workers.tasks import notificar_evento

logger = obtener_logger("servidor")


class ServicioNotificaciones:

    def __init__(self, sesiones):
        self.sesiones = sesiones

    def encolar(self, supermercado_id: int, tipo: str, mensaje: str) -> None:
        notificar_evento.delay(supermercado_id, tipo, mensaje)

    async def escuchar(self):
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

                    sesion = self.sesiones.obtener(sm_id)
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
