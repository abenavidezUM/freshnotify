"""
Encolar tareas de Celery desde el servidor sin frenar el event loop.

.delay() es bloqueante: habla con Redis por un socket común. Con Redis bien
tarda menos de un milisegundo y no se nota, pero si Redis está caído Celery
reintenta conectarse durante varios segundos, y mientras tanto el event loop
está parado: ninguna sucursal recibe respuesta, ni siquiera la que acaba de
guardar un lote (que ya está en la base).

Por eso se encola desde un thread aparte. Es uno solo para que los avisos
salgan en el mismo orden en que se generaron.

Además se encola con ignore_result=True: el servidor nunca espera el
resultado de una tarea, y si no se lo pide, Celery no se suscribe al result
backend. Con Redis caído esa suscripción reintentaba ~19 s antes de fallar;
sin ella el error llega en menos de un segundo.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor

from src.shared import obtener_logger

logger = obtener_logger("servidor")

# Por si Redis no rechaza la conexión sino que no contesta (red caída)
TIEMPO_MAXIMO_ENCOLAR = 5  # segundos


class ColaTareas:

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="encolar")
        self._pendientes: set[asyncio.Task] = set()

    async def encolar(self, tarea, *args) -> bool:
        """Encola y espera a que Redis la reciba. False si no se pudo."""
        loop = asyncio.get_running_loop()
        envio = loop.run_in_executor(self._executor,
                                     lambda: tarea.apply_async(args, ignore_result=True))
        try:
            await asyncio.wait_for(envio, TIEMPO_MAXIMO_ENCOLAR)
            return True
        except TimeoutError:
            logger.error(f"No se pudo encolar {tarea.name}: Redis no contestó en {TIEMPO_MAXIMO_ENCOLAR} s")
            return False
        except Exception as e:  # kombu tira distintos errores según dónde falló la conexión
            logger.error(f"No se pudo encolar {tarea.name}: {e!r}")
            return False

    def encolar_sin_esperar(self, tarea, *args) -> None:
        """Para los avisos de los lotes: el cliente no tiene por qué esperar
        a que Redis conteste. Si falla queda en el log."""
        t = asyncio.create_task(self.encolar(tarea, *args))
        # Hay que guardar la referencia: asyncio solo guarda una débil y la
        # tarea podría desaparecer antes de terminar
        self._pendientes.add(t)
        t.add_done_callback(self._pendientes.discard)

    async def cerrar(self, tiempo_maximo: float) -> None:
        """Espera a que salga lo que quedó pendiente, hasta tiempo_maximo."""
        if self._pendientes:
            _, sin_terminar = await asyncio.wait(self._pendientes, timeout=tiempo_maximo)
            if sin_terminar:
                logger.warning(f"{len(sin_terminar)} avisos no se llegaron a encolar")
        self._executor.shutdown(wait=False, cancel_futures=True)
