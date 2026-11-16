"""El servidor de punta a punta: se levanta de verdad en otro puerto y se le
habla como sucursal (TCP) y como monitor (socket Unix). Usa MariaDB y Redis
del compose, y worker-notif para la notificación en vivo."""

import asyncio
import os
import signal
import struct
import subprocess
import sys
import time

import pytest

from src.shared import enviar_mensaje, recibir_mensaje
from tests.conftest import borrar_sucursal

pytestmark = pytest.mark.integracion

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOMBRE = "Prueba Pytest Servidor"
PUERTO = 8899


class Servidor:
    """Un servidor de FreshNotify corriendo en un proceso aparte."""

    def __init__(self, puerto, socket_path):
        self.puerto, self.socket = puerto, socket_path
        self.proceso = subprocess.Popen(
            [sys.executable, "-m", "src.server.server",
             "--puerto", str(puerto), "--socket", socket_path],
            cwd=RAIZ, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        # Está listo cuando crea el socket del monitor (es lo último que arranca)
        limite = time.monotonic() + 15
        while not os.path.exists(socket_path):
            if self.proceso.poll() is not None or time.monotonic() > limite:
                self.detener()
                pytest.fail("El servidor de prueba no arrancó")
            time.sleep(0.1)

    def detener(self):
        if self.proceso.poll() is None:
            self.proceso.terminate()
            try:
                self.proceso.wait(15)
            except subprocess.TimeoutExpired:
                self.proceso.kill()

    async def monitor(self, pedido):
        r, w = await asyncio.open_unix_connection(self.socket)
        await enviar_mensaje(w, pedido)
        respuesta = await asyncio.wait_for(recibir_mensaje(r), 10)
        w.close()
        return respuesta

    async def login(self, password="1234"):
        r, w = await asyncio.open_connection("localhost", self.puerto)
        await enviar_mensaje(w, {"accion": "conectar", "nombre": NOMBRE, "password": password})
        return r, w, await asyncio.wait_for(recibir_mensaje(r), 10)


@pytest.fixture(scope="module")
def servidor(sql):
    borrar_sucursal(sql, NOMBRE)
    srv = Servidor(PUERTO, f"/tmp/fn_pytest_{os.getpid()}.sock")
    alta = asyncio.run(srv.monitor({"accion": "registrar_supermercado", "nombre": NOMBRE,
                                    "password": "1234", "umbral_dias": "7"}))
    assert alta["ok"], alta
    yield srv
    srv.detener()
    borrar_sucursal(sql, NOMBRE)


async def respuesta(r):
    """La próxima respuesta, salteando notificaciones que lleguen en el medio."""
    while True:
        m = await asyncio.wait_for(recibir_mensaje(r), 10)
        if m.get("tipo") != "notificacion":
            return m


def test_login_y_sesion_unica(servidor):
    async def caso():
        _, w, rechazo = await servidor.login("mala")
        w.close()
        assert rechazo["tipo"] == "rechazo"

        r, w, bienvenida = await servidor.login()
        assert bienvenida["tipo"] == "bienvenida"

        _, w2, segunda = await servidor.login()
        w2.close()
        assert segunda["tipo"] == "rechazo" and "sesión activa" in segunda["mensaje"]

        estado = await servidor.monitor({"accion": "status"})
        assert NOMBRE in estado["sesiones"]
        w.close()
    asyncio.run(caso())


def test_errores_de_protocolo_no_cortan_la_sesion(servidor):
    async def caso():
        r, w, _ = await servidor.login()
        malos = [
            b"{esto no es json",
            b"[1, 2, 3]",
            b'{"accion": "crear_lote", "codigo": 123}',
            b'{"accion": "listar_lotes", "extra": 1}',
            b'{"accion": "borrar_todo"}',
        ]
        for payload in malos:
            w.write(struct.pack("!I", len(payload)) + payload)
            await w.drain()
            m = await respuesta(r)
            assert m["ok"] is False and m["error"] == "protocolo", (payload, m)

        await enviar_mensaje(w, {"accion": "listar_lotes"})
        assert (await respuesta(r))["ok"] is True
        w.close()
    asyncio.run(caso())


def test_prefijo_imposible_cierra_la_conexion(servidor):
    async def caso():
        r, w = await asyncio.open_connection("localhost", servidor.puerto)
        w.write(struct.pack("!I", 2**31))
        await w.drain()
        m = await asyncio.wait_for(recibir_mensaje(r), 5)
        assert m["error"] == "protocolo"
        assert await asyncio.wait_for(recibir_mensaje(r), 5) is None  # cerró
    asyncio.run(caso())


def test_el_monitor_tambien_valida(servidor):
    async def caso():
        m = await servidor.monitor({"accion": "registrar_supermercado",
                                    "nombre": "X", "password": 1234})
        assert m["ok"] is False and m["error"] == "protocolo"
    asyncio.run(caso())


def test_alta_de_lote_con_notificacion_en_vivo(servidor):
    # El aviso hace todo el recorrido: servidor → Redis → worker-notif →
    # MariaDB + pub/sub → servidor → sucursal
    async def caso():
        r, w, _ = await servidor.login()
        await enviar_mensaje(w, {"accion": "crear_lote", "codigo": "PYT-1", "producto": "Queso",
                                 "categoria": "lacteos", "cantidad": "3",
                                 "fecha_vencimiento": "2027-06-01"})
        tipos = []
        limite = time.monotonic() + 15
        while not {"respuesta", "notificacion"} <= set(tipos) and time.monotonic() < limite:
            tipos.append((await asyncio.wait_for(recibir_mensaje(r), 15))["tipo"])
        w.close()
        assert "respuesta" in tipos
        assert "notificacion" in tipos, "no llegó la 🔔: ¿está corriendo worker-notif?"
    asyncio.run(caso())


def test_apagado_ordenado(servidor):
    # Otro servidor, porque este test lo apaga (la sucursal es la misma)
    socket_path = f"/tmp/fn_pytest_apagado_{os.getpid()}.sock"
    srv = Servidor(PUERTO + 1, socket_path)

    async def caso():
        r, w, bienvenida = await srv.login()
        assert bienvenida["tipo"] == "bienvenida"
        srv.proceso.send_signal(signal.SIGTERM)
        # La sucursal recibe el aviso y después el servidor cierra la conexión
        aviso = await asyncio.wait_for(recibir_mensaje(r), 10)
        assert aviso["tipo"] == "notificacion" and "apagando" in aviso["mensaje"]
        assert await asyncio.wait_for(recibir_mensaje(r), 10) is None
        w.close()

    try:
        asyncio.run(caso())
        assert srv.proceso.wait(15) == 0
        assert not os.path.exists(socket_path)
    finally:
        srv.detener()
