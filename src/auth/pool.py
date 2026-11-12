"""
Pool de procesos para verificar contraseñas.

bcrypt tarda ~0,2 s de CPU por verificación (es lento a propósito, para
que la fuerza bruta sea cara). Si se hiciera en el event loop, cada login
frenaría a todos los clientes. Por eso va a un ProcessPoolExecutor:
procesos y no threads, porque con el GIL dos threads no ejecutan Python
en paralelo. El servidor lo espera con run_in_executor y mientras tanto
sigue atendiendo al resto.

Sincronización entre procesos:
- BoundedSemaphore: limita cuántas verificaciones corren a la vez (para
  frenar fuerza bruta), aparte del tamaño del pool.
- Value + lock: contadores de logins ok/fallidos en memoria compartida.
  El += no es atómico y sin lock se pierden incrementos (ver demo_carrera.py).
- Cada proceso toma una sola primitiva por vez (nunca semáforo y lock
  anidados), así no puede haber deadlock.
"""

import asyncio
import multiprocessing
import signal
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

import bcrypt

from src.shared import AUTH_POOL_PROCESSES, MAX_LOGINS_SIMULTANEOS, obtener_logger

logger = obtener_logger("auth")

# Globales de cada proceso hijo, se cargan en _init_worker
_semaforo = None
_logins_ok = None
_logins_fallidos = None


def _init_worker(semaforo, logins_ok, logins_fallidos):
    """Corre una vez al crear cada proceso hijo. Las primitivas de
    multiprocessing no se pueden pasar como argumento de una tarea del
    pool, por eso se comparten con initializer/initargs."""
    global _semaforo, _logins_ok, _logins_fallidos
    # Ctrl+C le llega a todo el grupo de procesos. El que decide cómo apagar
    # es el servidor (pool.cerrar); si no, los hijos mueren a mitad de un login
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    _semaforo = semaforo
    _logins_ok = logins_ok
    _logins_fallidos = logins_fallidos


def _verificar_en_proceso(password: bytes, password_hash: bytes) -> bool:
    """Corre en el proceso hijo: espera turno en el semáforo, verifica y cuenta."""
    # El with libera el semáforo aunque bcrypt tire una excepción
    with _semaforo:
        es_valido = bcrypt.checkpw(password, password_hash)

    # += sobre memoria compartida no es atómico: va con lock
    contador = _logins_ok if es_valido else _logins_fallidos
    with contador.get_lock():
        contador.value += 1

    return es_valido


def _hashear_en_proceso(password: bytes) -> str:
    """Para el alta de sucursales: generar el hash también es pesado."""
    return bcrypt.hashpw(password, bcrypt.gensalt()).decode()


class PoolAutenticacion:
    """Lo que usa el servidor. Las primitivas se crean en el proceso padre
    (así puede leer los contadores para el monitor) y se pasan a los hijos."""

    def __init__(self, procesos: int = AUTH_POOL_PROCESSES,
                 max_simultaneos: int = MAX_LOGINS_SIMULTANEOS):
        self.procesos = procesos
        self.max_simultaneos = max_simultaneos

        self._semaforo = multiprocessing.BoundedSemaphore(max_simultaneos)
        self._logins_ok = multiprocessing.Value("i", 0)
        self._logins_fallidos = multiprocessing.Value("i", 0)

        self._executor = self._crear_executor()
        logger.info(
            f"Pool de autenticación: {procesos} procesos, "
            f"máx. {max_simultaneos} verificaciones simultáneas"
        )

    def _crear_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self.procesos,
            initializer=_init_worker,
            initargs=(self._semaforo, self._logins_ok, self._logins_fallidos),
        )

    async def _ejecutar(self, funcion, *args):
        """Corre la función en el pool sin frenar el event loop.

        Si se muere uno de los procesos (lo mata el sistema, se queda sin
        memoria) el executor queda roto para siempre: todo lo que se le mande
        falla. En ese caso se arma uno nuevo para los próximos pedidos y este
        se propaga (el servidor contesta un error de autenticación)."""
        loop = asyncio.get_running_loop()
        executor = self._executor
        try:
            return await loop.run_in_executor(executor, funcion, *args)
        except BrokenProcessPool:
            if self._executor is executor:  # otro pedido puede haberlo rearmado ya
                logger.error("Se rompió el pool de autenticación, se crea uno nuevo")
                executor.shutdown(wait=False, cancel_futures=True)
                self._executor = self._crear_executor()
            raise

    async def verificar(self, password: str, password_hash: str) -> bool:
        """Manda la verificación al pool y la espera sin frenar el event loop."""
        return await self._ejecutar(_verificar_en_proceso,
                                    password.encode("utf-8"), password_hash.encode("utf-8"))

    async def hashear(self, password: str) -> str:
        return await self._ejecutar(_hashear_en_proceso, password.encode("utf-8"))

    def estadisticas(self) -> dict:
        """Lee los contadores que van sumando los procesos hijos."""
        with self._logins_ok.get_lock():
            ok = self._logins_ok.value
        with self._logins_fallidos.get_lock():
            fallidos = self._logins_fallidos.value
        return {
            "procesos_pool": self.procesos,
            "max_simultaneos": self.max_simultaneos,
            "logins_exitosos": ok,
            "logins_fallidos": fallidos,
        }

    async def cerrar(self, tiempo_maximo: float = 5) -> None:
        """Apagado ordenado: lo que estaba esperando turno se cancela, y a lo
        que ya se está verificando se le da hasta tiempo_maximo para terminar
        (un bcrypt tarda ~0,2 s, así que sobra). Si igual no termina, se
        terminan los procesos para que el servidor no quede colgado."""
        # shutdown(wait=True) bloquea: se espera desde un thread
        espera = asyncio.to_thread(self._executor.shutdown, wait=True, cancel_futures=True)
        try:
            await asyncio.wait_for(espera, tiempo_maximo)
            logger.info("Pool de autenticación cerrado")
        except TimeoutError:
            logger.warning(f"El pool no terminó en {tiempo_maximo} s, se terminan los procesos")
            # El executor no tiene una forma pública de forzar el cierre
            for proceso in list((self._executor._processes or {}).values()):
                proceso.terminate()
