"""Validación de cada pedido contra el esquema de su acción."""

import pytest

from src.shared.esquemas import LOGIN, MONITOR, SUCURSAL, validar

LOTE_OK = {"accion": "crear_lote", "codigo": "LAC-9", "producto": "Yogur",
           "cantidad": "5", "fecha_vencimiento": "2027-01-01"}


@pytest.mark.parametrize("mensaje, acciones", [
    (LOTE_OK, SUCURSAL),
    ({**LOTE_OK, "categoria": "lacteos", "cantidad": 5}, SUCURSAL),  # cantidad como int también
    ({"accion": "listar_lotes"}, SUCURSAL),
    ({"accion": "ver_notificaciones", "solo_no_leidas": True}, SUCURSAL),
    ({"accion": "actualizar_lote", "codigo": "LAC-9"}, SUCURSAL),  # el resto es opcional
    ({"accion": "conectar", "nombre": "Super Vea", "password": "1234"}, LOGIN),
    ({"accion": "registrar_supermercado", "nombre": "X", "password": "1234"}, MONITOR),
])
def test_mensajes_validos(mensaje, acciones):
    assert validar(mensaje, acciones) is None


@pytest.mark.parametrize("mensaje, acciones, problema", [
    ({"accion": "borrar_todo"}, SUCURSAL, "no reconocida"),
    ({"producto": "sin acción"}, SUCURSAL, "no reconocida"),
    ({k: v for k, v in LOTE_OK.items() if k != "codigo"}, SUCURSAL, "Falta el campo 'codigo'"),
    ({**LOTE_OK, "cantdad": "5"}, SUCURSAL, "'cantdad' no corresponde"),  # typo del cliente
    ({**LOTE_OK, "codigo": 123}, SUCURSAL, "'codigo' tiene un tipo inválido"),
    ({**LOTE_OK, "cantidad": [5]}, SUCURSAL, "'cantidad' tiene un tipo inválido"),
    ({"accion": "conectar", "nombre": "X", "password": 1234}, LOGIN, "'password' tiene un tipo"),
    # Una acción de sucursal no vale en el monitor y al revés
    ({"accion": "listar_lotes"}, MONITOR, "no reconocida"),
    ({"accion": "estadisticas"}, SUCURSAL, "no reconocida"),
])
def test_mensajes_invalidos(mensaje, acciones, problema):
    error = validar(mensaje, acciones)
    assert error is not None and problema in error


def test_bool_no_pasa_como_numero():
    # En Python True es un int (isinstance(True, int) da True)
    assert "tipo inválido" in validar({**LOTE_OK, "cantidad": True}, SUCURSAL)


def test_numero_no_pasa_como_bool():
    assert "tipo inválido" in validar({"accion": "ver_notificaciones", "solo_no_leidas": 1}, SUCURSAL)
