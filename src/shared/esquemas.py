"""
Qué campos lleva cada acción y de qué tipo.

El servidor revisa cada pedido contra esto antes de ejecutarlo: si falta un
campo, sobra uno o tiene otro tipo, contesta con un error en vez de llegar
a la base con datos raros. Que los valores tengan sentido (fecha válida,
cantidad positiva, categoría existente) lo siguen revisando los repositorios.
"""

TEXTO = (str,)
# Cantidades y umbrales: el cliente manda lo que se tipeó ("5"); el
# repositorio lo convierte a int y revisa el rango.
NUMERO = (int, str)
BOOLEANO = (bool,)

# acción → {campo: (tipos aceptados, obligatorio)}
LOGIN = {
    "conectar": {"nombre": (TEXTO, True), "password": (TEXTO, True)},
}

SUCURSAL = {
    "crear_lote": {
        "codigo": (TEXTO, True),
        "producto": (TEXTO, True),
        "categoria": (TEXTO, False),
        "cantidad": (NUMERO, True),
        "fecha_vencimiento": (TEXTO, True),
    },
    "actualizar_lote": {
        "codigo": (TEXTO, True),
        "producto": (TEXTO, False),
        "categoria": (TEXTO, False),
        "cantidad": (NUMERO, False),
        "fecha_vencimiento": (TEXTO, False),
    },
    "eliminar_lote": {"codigo": (TEXTO, True)},
    "buscar_lote": {"codigo": (TEXTO, True)},
    "listar_lotes": {},
    "ver_notificaciones": {"solo_no_leidas": (BOOLEANO, False)},
    "resumen_estado": {},
}

MONITOR = {
    "registrar_supermercado": {
        "nombre": (TEXTO, True),
        "password": (TEXTO, True),
        "umbral_dias": (NUMERO, False),
        "email": (TEXTO, False),
    },
    "listar_supermercados": {},
    "activar_supermercado": {"nombre": (TEXTO, True)},
    "desactivar_supermercado": {"nombre": (TEXTO, True)},
    "estadisticas": {},
    "status": {},
    "stats_auth": {},
    "run_tarea": {"tarea": (TEXTO, True)},
}


def validar(mensaje: dict, acciones: dict) -> str | None:
    """Devuelve None si el mensaje respeta el esquema de su acción, o un
    texto con el problema para mandarle al que lo envió."""
    accion = mensaje.get("accion")
    if accion not in acciones:
        return f"Acción '{accion}' no reconocida."

    esquema = acciones[accion]
    for campo, valor in mensaje.items():
        if campo == "accion":
            continue
        if campo not in esquema:
            return f"El campo '{campo}' no corresponde a '{accion}'."
        tipos, _ = esquema[campo]
        # bool es subclase de int en Python: True pasaría como cantidad
        if isinstance(valor, bool) and bool not in tipos or not isinstance(valor, tipos):
            return f"El campo '{campo}' tiene un tipo inválido."

    for campo, (_, obligatorio) in esquema.items():
        if obligatorio and campo not in mensaje:
            return f"Falta el campo '{campo}'."
    return None
