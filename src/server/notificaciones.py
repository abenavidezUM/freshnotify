"""
Las dos puntas de las notificaciones del lado del servidor:
- encolar(): después de un cambio en un lote le pide a Celery que genere
  el aviso. El worker lo guarda en la base y lo publica en Redis.
- escuchar(): está suscripto a ese canal de Redis y le reenvía cada aviso
  a la sucursal, si está conectada.
"""

import asyncio
import json

import redis.exceptions

from src.shared import REDIS_NOTIFICATIONS_CHANNEL, enviar_mensaje, obtener_logger
from src.infrastructure import get_async_redis_client
from src.workers.tasks import notificar_evento

logger = obtener_logger("servidor")

# Espera entre intentos de reconexión a Redis: 1, 2, 4, 8... hasta 30 s
ESPERA_INICIAL = 1
ESPERA_MAXIMA = 30


class ServicioNotificaciones:

    def __init__(self, sesiones, cola_tareas):
        self.sesiones = sesiones
        self.cola_tareas = cola_tareas
        self._suscripto = False  # para saber si la conexión que se cortó venía andando

    def encolar(self, supermercado_id: int, tipo: str, mensaje: str) -> None:
        self.cola_tareas.encolar_sin_esperar(notificar_evento, supermercado_id, tipo, mensaje)

    async def escuchar(self):
        """Mantiene la suscripción al canal de Redis. Si se corta la conexión
        vuelve a intentar con esperas cada vez más largas, hasta que se
        cancele la tarea (apagado del servidor).

        Mientras está desconectado, lo que publiquen los workers se pierde:
        pub/sub no guarda mensajes. No se pierde el aviso, que ya está en la
        base y la sucursal lo ve en su historial; se pierde la 🔔 en vivo."""
        espera = ESPERA_INICIAL
        while True:
            self._suscripto = False
            try:
                await self._escuchar_una_vez()
            except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError) as e:
                if self._suscripto:
                    espera = ESPERA_INICIAL  # venía andando: el primer reintento es rápido
                logger.error(f"Sin conexión con Redis ({e}). Reintento en {espera} s")
            await asyncio.sleep(espera)
            espera = min(espera * 2, ESPERA_MAXIMA)

    async def _escuchar_una_vez(self):
        cliente = get_async_redis_client()
        pubsub = cliente.pubsub()
        # Se liberan siempre: al cancelar la tarea, al caerse Redis o ante
        # cualquier otro error
        try:
            await pubsub.subscribe(REDIS_NOTIFICATIONS_CHANNEL)
            self._suscripto = True
            logger.info(f"Suscrito al canal Redis '{REDIS_NOTIFICATIONS_CHANNEL}'")

            async for crudo in pubsub.listen():
                if crudo["type"] != "message":
                    continue  # el primer mensaje es la confirmación de suscripción
                await self._reenviar(crudo["data"])
        finally:
            try:
                await pubsub.aclose()
                await cliente.aclose()
            except (redis.exceptions.RedisError, OSError):
                pass  # si Redis ya no está, no hay nada que cerrar del otro lado

    async def _reenviar(self, data):
        """Si la sucursal está conectada se le reenvía; si no, no pasa nada:
        ya quedó guardada en la base y la va a ver en el historial."""
        try:
            payload = json.loads(data)
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
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Notificación de Redis con formato inválido: {e}")
        except OSError as e:
            # La sucursal se estaba desconectando; su handler limpia la sesión
            logger.warning(f"No se pudo reenviar la notificación a id={sm_id}: {e}")
