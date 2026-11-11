"""
Errores que el servidor le contesta a una sucursal o al monitor.

Cada uno lleva un código ("error") para que el que recibe sepa qué pasó sin
leer el texto, y un mensaje pensado para el usuario. El detalle técnico (la
excepción) va solo al log del servidor: a la sucursal no le sirve y puede
mostrar cosas de la infraestructura (hosts, usuarios, consultas).

Las desconexiones no tienen respuesta: del otro lado ya no hay nadie.
"""

from concurrent.futures.process import BrokenProcessPool

import pymysql.err

PROTOCOLO = "protocolo"            # el mensaje no respeta el formato o el esquema
BASE_DATOS = "base_datos"          # MariaDB caída o se cortó la conexión
AUTENTICACION = "autenticacion"    # el pool que verifica contraseñas no responde
INTERNO = "interno"                # cualquier otra cosa: un bug

MENSAJES = {
    PROTOCOLO: "Mensaje inválido.",
    BASE_DATOS: "No se pudo acceder a la base de datos. Probá de nuevo en un momento.",
    AUTENTICACION: "No se pudo verificar la contraseña en este momento. Probá de nuevo en un momento.",
    INTERNO: "Error interno del servidor.",
}

# Los de "no hay base": no se puede conectar, se cortó la conexión, la
# conexión ya estaba cerrada. Los IntegrityError (código repetido, etc.) no
# entran acá: los manejan los repositorios y contestan algo concreto.
ERRORES_BASE = (pymysql.err.OperationalError, pymysql.err.InterfaceError)

# El pool de procesos se rompe si se muere uno de sus procesos
ERRORES_AUTENTICACION = (BrokenProcessPool,)

# El otro lado cerró o se cayó la red
DESCONEXIONES = (ConnectionResetError, BrokenPipeError)


def respuesta(codigo: str, mensaje: str | None = None, tipo: str = "respuesta") -> dict:
    return {"tipo": tipo, "ok": False, "error": codigo, "mensaje": mensaje or MENSAJES[codigo]}
