"""Framing: prefijo de 4 bytes + JSON, y qué pasa con cada tipo de error."""

import asyncio
import json
import struct

import pytest

from src.shared import protocol
from src.shared.protocol import ErrorProtocolo, MensajeInvalido, enviar_mensaje, recibir_mensaje
from tests.conftest import WriterFalso, lector


def armar(payload: bytes) -> bytes:
    return struct.pack("!I", len(payload)) + payload


def correr(coro):
    return asyncio.run(coro)


def test_ida_y_vuelta_con_acentos():
    async def caso():
        w = WriterFalso()
        mensaje = {"accion": "crear_lote", "producto": "Leche descremada 1L", "nota": "año"}
        await enviar_mensaje(w, mensaje)
        assert await recibir_mensaje(lector(w.datos)) == mensaje
    correr(caso())


def test_dos_mensajes_pegados_se_leen_por_separado():
    # TCP puede juntar dos envíos en una sola lectura
    async def caso():
        r = lector(armar(b'{"n": 1}') + armar(b'{"n": 2}'))
        assert await recibir_mensaje(r) == {"n": 1}
        assert await recibir_mensaje(r) == {"n": 2}
        assert await recibir_mensaje(r) is None  # cerró entre mensajes: fin normal
    correr(caso())


def test_json_roto_no_desincroniza_el_stream():
    # El mensaje llegó entero: se rechaza, pero el siguiente se lee bien
    async def caso():
        r = lector(armar(b"{esto no es json") + armar(b'{"ok": true}'))
        with pytest.raises(MensajeInvalido):
            await recibir_mensaje(r)
        assert await recibir_mensaje(r) == {"ok": True}
    correr(caso())


@pytest.mark.parametrize("payload", [b"[1, 2, 3]", b'"texto"', b"42", b"null"])
def test_json_que_no_es_objeto(payload):
    async def caso():
        with pytest.raises(MensajeInvalido):
            await recibir_mensaje(lector(armar(payload)))
    correr(caso())


def test_utf8_invalido():
    async def caso():
        with pytest.raises(MensajeInvalido):
            await recibir_mensaje(lector(armar(b'{"a": "\xff"}')))
    correr(caso())


def test_prefijo_mas_grande_que_el_maximo():
    async def caso():
        r = lector(struct.pack("!I", protocol.LONGITUD_MAXIMA + 1))
        with pytest.raises(ErrorProtocolo):
            await recibir_mensaje(r)
    correr(caso())


def test_mensaje_a_medias_se_corta_por_tiempo():
    # Llegó el prefijo y parte del JSON, y el otro lado no manda más
    async def caso():
        r = lector(struct.pack("!I", 100) + b'{"accion"', fin=False)
        with pytest.raises(ErrorProtocolo):
            await recibir_mensaje(r, tiempo_maximo=0.2)
    correr(caso())


def test_sin_mensajes_no_hay_tiempo_maximo():
    # Una sucursal que no manda nada no se corta: el reloj arranca con el primer byte
    async def caso():
        r = lector(b"", fin=False)
        lectura = asyncio.create_task(recibir_mensaje(r, tiempo_maximo=0.1))
        await asyncio.sleep(0.3)
        assert not lectura.done()
        r.feed_data(armar(b'{"hola": 1}'))
        assert await lectura == {"hola": 1}
    correr(caso())


def test_conexion_cortada_a_mitad_del_mensaje():
    async def caso():
        r = lector(struct.pack("!I", 100) + b'{"accion"')
        with pytest.raises(asyncio.IncompleteReadError):
            await recibir_mensaje(r)
    correr(caso())


def test_solo_se_mandan_dicts():
    async def caso():
        with pytest.raises(TypeError):
            await enviar_mensaje(WriterFalso(), ["no", "es", "dict"])
    correr(caso())


def test_el_emisor_tambien_respeta_el_maximo(monkeypatch):
    monkeypatch.setattr(protocol, "LONGITUD_MAXIMA", 50)

    async def caso():
        w = WriterFalso()
        with pytest.raises(ErrorProtocolo):
            await enviar_mensaje(w, {"texto": "x" * 100})
        assert w.datos == b""  # no se mandó nada
    correr(caso())


def test_prefijo_big_endian():
    async def caso():
        w = WriterFalso()
        await enviar_mensaje(w, {"a": 1})
        largo = struct.unpack("!I", w.datos[:4])[0]
        assert largo == len(w.datos) - 4
        assert json.loads(w.datos[4:]) == {"a": 1}
    correr(caso())
