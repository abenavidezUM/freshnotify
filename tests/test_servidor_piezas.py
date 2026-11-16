"""Piezas del servidor que se prueban solas: sesiones y la cola de tareas."""

import asyncio
import threading
import time

from src.server import tareas
from src.server.sesiones import GestorSesiones
from src.server.tareas import ColaTareas
from src.shared.protocol import recibir_mensaje
from tests.conftest import WriterFalso, lector


# --- Sesiones ---

def test_una_sesion_por_sucursal():
    sesiones = GestorSesiones()
    assert sesiones.registrar(1, "Super Vea", WriterFalso())
    assert not sesiones.registrar(1, "Super Vea", WriterFalso())  # segundo login
    assert sesiones.registrar(2, "Atomo", WriterFalso())
    assert sorted(sesiones.nombres()) == ["Atomo", "Super Vea"]

    sesiones.quitar(1)
    assert not sesiones.esta_conectada(1)
    assert sesiones.registrar(1, "Super Vea", WriterFalso())  # después de salir, puede volver


def test_cerrar_todas_avisa_y_cierra():
    async def caso():
        sesiones = GestorSesiones()
        writers = [WriterFalso(), WriterFalso()]
        for i, w in enumerate(writers):
            sesiones.registrar(i, f"S{i}", w)
        await sesiones.cerrar_todas("Me apago")
        for w in writers:
            assert w.cerrado
            aviso = await recibir_mensaje(lector(w.datos))
            assert aviso["tipo"] == "notificacion" and aviso["mensaje"] == "Me apago"
    asyncio.run(caso())


# --- Cola de tareas ---

class TareaFalsa:
    """Imita una tarea de Celery: apply_async puede tardar o fallar."""

    name = "tarea_falsa"

    def __init__(self, demora=0.0, error=None):
        self.demora, self.error = demora, error
        self.recibidas = []
        self.opciones = []

    def apply_async(self, args, **opciones):
        time.sleep(self.demora)  # bloqueante, como el .delay() real con Redis caído
        if self.error:
            raise self.error
        self.recibidas.append(args)
        self.opciones.append(opciones)


def test_encola_sin_pedir_resultado():
    async def caso():
        cola, tarea = ColaTareas(), TareaFalsa()
        assert await cola.encolar(tarea, 7, "lote_creado")
        assert tarea.recibidas == [(7, "lote_creado")]
        # Sin ignore_result, Celery se suscribe al result backend y con Redis
        # caído tarda ~19 s en fallar
        assert tarea.opciones == [{"ignore_result": True}]
        await cola.cerrar(1)
    asyncio.run(caso())


def test_si_falla_devuelve_false():
    async def caso():
        cola = ColaTareas()
        assert not await cola.encolar(TareaFalsa(error=ConnectionError("Redis caído")))
        await cola.cerrar(1)
    asyncio.run(caso())


def test_si_redis_no_contesta_se_corta_por_tiempo(monkeypatch):
    monkeypatch.setattr(tareas, "TIEMPO_MAXIMO_ENCOLAR", 0.1)

    async def caso():
        cola = ColaTareas()
        inicio = time.monotonic()
        assert not await cola.encolar(TareaFalsa(demora=1))
        assert time.monotonic() - inicio < 0.5
        await cola.cerrar(0)
    asyncio.run(caso())


def test_encolar_no_frena_el_event_loop():
    # Mientras el encolado está trabado 0,5 s, el loop sigue atendiendo
    async def caso():
        cola = ColaTareas()
        vueltas = 0

        async def otro_cliente():
            nonlocal vueltas
            while True:
                vueltas += 1
                await asyncio.sleep(0.01)

        atencion = asyncio.create_task(otro_cliente())
        await cola.encolar(TareaFalsa(demora=0.5))
        atencion.cancel()
        assert vueltas > 20  # con el loop bloqueado serían 1 o 2
        await cola.cerrar(1)
    asyncio.run(caso())


def test_los_avisos_salen_en_orden():
    async def caso():
        cola, tarea = ColaTareas(), TareaFalsa(demora=0.005)
        for i in range(20):
            cola.encolar_sin_esperar(tarea, i)
        await cola.cerrar(5)
        assert [args[0] for args in tarea.recibidas] == list(range(20))
    asyncio.run(caso())


def test_un_solo_thread_para_encolar():
    async def caso():
        cola, hilos = ColaTareas(), set()

        class Registra(TareaFalsa):
            def apply_async(self, args, **opciones):
                hilos.add(threading.current_thread().name)

        for i in range(10):
            cola.encolar_sin_esperar(Registra(), i)
        await cola.cerrar(5)
        assert len(hilos) == 1
    asyncio.run(caso())
