"""
Protocolo de mensajes entre cliente, servidor y monitor.

TCP no respeta los límites entre mensajes: dos envíos pueden llegar pegados
o uno puede llegar partido. Por eso cada mensaje va como JSON precedido por
4 bytes con su longitud; el que recibe lee primero esos 4 bytes y después
exactamente esa cantidad.

Funciona igual sobre TCP y sobre sockets Unix (asyncio usa los mismos streams).
"""

import asyncio
import json
import struct

# "!I" = entero sin signo de 4 bytes, big-endian (orden de red)
LONGITUD_PREFIJO = 4

# Tope por si llega un prefijo roto: sin esto se podría intentar leer
# hasta 4 GB. Ningún mensaje real se acerca a este tamaño.
LONGITUD_MAXIMA = 10 * 1024 * 1024  # 10 MB


async def enviar_mensaje(writer: asyncio.StreamWriter, datos: dict) -> None:
    """Manda un dict como JSON con el prefijo de longitud.

    drain() espera si el buffer de salida está lleno, así un cliente lento
    no hace crecer la memoria del servidor."""
    payload = json.dumps(datos, ensure_ascii=False).encode("utf-8")
    prefijo = struct.pack("!I", len(payload))

    writer.write(prefijo + payload)
    await writer.drain()


async def recibir_mensaje(reader: asyncio.StreamReader) -> dict | None:
    """Lee un mensaje completo y lo devuelve como dict.

    Devuelve None si el otro lado cerró la conexión entre mensajes. Si se
    corta a mitad de un mensaje, se propaga IncompleteReadError."""
    try:
        prefijo = await reader.readexactly(LONGITUD_PREFIJO)
    except asyncio.IncompleteReadError as e:
        if not e.partial:  # cerró sin mandar nada: fin normal
            return None
        raise

    longitud = struct.unpack("!I", prefijo)[0]
    if longitud > LONGITUD_MAXIMA:
        raise ValueError(f"Mensaje de {longitud} bytes excede el máximo permitido")

    payload = await reader.readexactly(longitud)
    return json.loads(payload.decode("utf-8"))
