"""
Protocolo de mensajes entre cliente, servidor y monitor.

TCP no respeta los límites entre mensajes: dos envíos pueden llegar pegados
o uno puede llegar partido. Por eso cada mensaje va como JSON precedido por
4 bytes con su longitud; el que recibe lee primero esos 4 bytes y después
exactamente esa cantidad.

Funciona igual sobre TCP y sobre sockets Unix (asyncio usa los mismos streams).

Hay dos tipos de error, porque no se manejan igual:
- MensajeInvalido: el mensaje llegó entero pero el contenido no sirve (JSON
  roto, no es un objeto). El stream sigue sincronizado, así que se le puede
  contestar con un error y seguir con la conexión.
- ErrorProtocolo: no se sabe dónde termina el mensaje (prefijo enorme o el
  mensaje no terminó de llegar a tiempo). No hay forma de seguir leyendo,
  hay que cerrar.
"""

import asyncio
import json
import struct

# "!I" = entero sin signo de 4 bytes, big-endian (orden de red)
LONGITUD_PREFIJO = 4

# Tope por si llega un prefijo roto: sin esto se podría intentar leer
# hasta 4 GB. Ningún mensaje real se acerca a este tamaño.
LONGITUD_MAXIMA = 10 * 1024 * 1024  # 10 MB

# Una vez que empieza a llegar un mensaje, cuánto puede tardar en llegar
# entero. Entre mensajes no hay límite (la sucursal puede estar un rato sin
# hacer nada), pero alguien que manda medio mensaje y se queda callado no
# puede tener ocupada la conexión para siempre.
TIEMPO_MAXIMO_MENSAJE = 10  # segundos


class ErrorProtocolo(Exception):
    """El stream quedó desincronizado: hay que cerrar la conexión."""


class MensajeInvalido(Exception):
    """El mensaje llegó completo pero no es un objeto JSON válido."""


async def enviar_mensaje(writer: asyncio.StreamWriter, datos: dict) -> None:
    """Manda un dict como JSON con el prefijo de longitud.

    drain() espera si el buffer de salida está lleno, así un cliente lento
    no hace crecer la memoria del servidor."""
    if not isinstance(datos, dict):
        raise TypeError(f"Solo se mandan dicts, llegó {type(datos).__name__}")

    payload = json.dumps(datos, ensure_ascii=False).encode("utf-8")
    # Mejor fallar acá que mandar algo que el otro lado va a rechazar
    if len(payload) > LONGITUD_MAXIMA:
        raise ErrorProtocolo(f"Mensaje de {len(payload)} bytes excede el máximo permitido")

    writer.write(struct.pack("!I", len(payload)) + payload)
    await writer.drain()


async def recibir_mensaje(reader: asyncio.StreamReader,
                          tiempo_maximo: float = TIEMPO_MAXIMO_MENSAJE) -> dict | None:
    """Lee un mensaje completo y lo devuelve como dict.

    Devuelve None si el otro lado cerró la conexión entre mensajes. Si se
    corta a mitad de un mensaje, se propaga IncompleteReadError."""
    # El primer byte puede tardar lo que sea: es esperar el próximo pedido
    try:
        primero = await reader.readexactly(1)
    except asyncio.IncompleteReadError:
        return None  # cerró sin mandar nada: fin normal

    # Desde acá corre el reloj
    try:
        async with asyncio.timeout(tiempo_maximo):
            prefijo = primero + await reader.readexactly(LONGITUD_PREFIJO - 1)
            longitud = struct.unpack("!I", prefijo)[0]
            if longitud > LONGITUD_MAXIMA:
                raise ErrorProtocolo(f"Mensaje de {longitud} bytes excede el máximo permitido")
            payload = await reader.readexactly(longitud)
    except TimeoutError:
        raise ErrorProtocolo(f"El mensaje no terminó de llegar en {tiempo_maximo} s") from None

    try:
        datos = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise MensajeInvalido("El mensaje no es JSON válido.") from None
    if not isinstance(datos, dict):
        raise MensajeInvalido("El mensaje tiene que ser un objeto JSON ({...}).")
    return datos
