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
from concurrent.futures import ProcessPoolExecutor

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

        self._executor = ProcessPoolExecutor(
            max_workers=procesos,
            initializer=_init_worker,
            initargs=(self._semaforo, self._logins_ok, self._logins_fallidos),
        )
        logger.info(
            f"Pool de autenticación: {procesos} procesos, "
            f"máx. {max_simultaneos} verificaciones simultáneas"
        )

    async def verificar(self, password: str, password_hash: str) -> bool:
        """Manda la verificación al pool y la espera sin frenar el event loop."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, _verificar_en_proceso,
            password.encode("utf-8"), password_hash.encode("utf-8"),
        )

    async def hashear(self, password: str) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor, _hashear_en_proceso, password.encode("utf-8")
        )

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

    def cerrar(self):
        self._executor.shutdown(wait=False, cancel_futures=True)
