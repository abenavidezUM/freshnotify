"""
Cliente de sucursal.

El servidor puede mandar notificaciones en cualquier momento, no solo como
respuesta a un pedido. Por eso hay una corrutina (escuchar_servidor) que lee
todo lo que llega: las notificaciones las muestra en el momento y las
respuestas las deja en una cola para el menú.

input() bloquea, así que el menú corre en un thread con run_in_executor
para no frenar la escucha.
"""

import argparse
import asyncio
import getpass

from src.shared import (
    SERVER_CONNECT_HOST, SERVER_PORT, enviar_mensaje, recibir_mensaje,
    ErrorProtocolo, MensajeInvalido,
)
from src.client import ui


def construir_mensaje(opcion: str) -> dict | None:
    """Opción del menú → mensaje para el servidor (None si la opción no existe).
    Usa input(), por eso se llama con run_in_executor."""
    if opcion == "1":
        return {"accion": "crear_lote", **ui.pedir_datos_lote()}
    if opcion == "2":
        return {"accion": "listar_lotes"}
    if opcion == "3":
        return {"accion": "buscar_lote", "codigo": ui.pedir_texto("Código del lote")}
    if opcion == "4":
        codigo = ui.pedir_texto("Código del lote a modificar")
        cambios = ui.pedir_cambios_lote()
        return {"accion": "actualizar_lote", "codigo": codigo, **cambios}
    if opcion == "5":
        return {"accion": "eliminar_lote", "codigo": ui.pedir_texto("Código del lote a eliminar")}
    if opcion == "6":
        return {"accion": "ver_notificaciones"}
    if opcion == "7":
        return {"accion": "resumen_estado"}
    return None


async def escuchar_servidor(reader, cola_respuestas: asyncio.Queue):
    """Único lector del socket. Si el servidor cierra, mete None en la cola
    para que el menú se entere."""
    try:
        while True:
            mensaje = await recibir_mensaje(reader)
            if mensaje is None:
                await cola_respuestas.put(None)
                break

            if mensaje.get("tipo") == "notificacion":
                # \a hace sonar la terminal; el \n la separa del prompt
                print(f"\a\n  🔔 NOTIFICACIÓN: {mensaje.get('mensaje', '')}\n")
            else:
                await cola_respuestas.put(mensaje)
    except (asyncio.IncompleteReadError, ConnectionResetError, ErrorProtocolo, MensajeInvalido):
        # Si el servidor manda algo que no se entiende, se trata como conexión perdida
        await cola_respuestas.put(None)


async def ejecutar_cliente(host: str, puerto: int, nombre: str, password: str):
    try:
        reader, writer = await asyncio.open_connection(host, puerto)
    except (ConnectionRefusedError, OSError):
        print(f"✗ No se pudo conectar a {host}:{puerto}. ¿Está corriendo el servidor?")
        return

    loop = asyncio.get_running_loop()
    escucha = None

    try:
        # Login (antes de arrancar la escucha)
        await enviar_mensaje(writer, {"accion": "conectar", "nombre": nombre, "password": password})
        respuesta = await recibir_mensaje(reader)

        if respuesta is None or respuesta.get("tipo") == "rechazo":
            motivo = respuesta.get("mensaje") if respuesta else "el servidor cerró la conexión"
            print(f"✗ Conexión rechazada: {motivo}")
            return

        print(f"\n✓ Autenticado como '{respuesta.get('nombre', nombre)}'")
        ui.mostrar_resumen(respuesta)

        # A partir de acá corren el menú y la escucha a la vez
        cola_respuestas: asyncio.Queue = asyncio.Queue()
        escucha = asyncio.create_task(escuchar_servidor(reader, cola_respuestas))

        while True:
            opcion = await loop.run_in_executor(None, ui.mostrar_menu, respuesta.get("nombre", nombre))
            if opcion == "0":
                print("Hasta luego.")
                break

            mensaje = await loop.run_in_executor(None, construir_mensaje, opcion)
            if mensaje is None:
                print("  ⚠ Opción inválida.")
                continue

            await enviar_mensaje(writer, mensaje)
            try:
                resultado = await asyncio.wait_for(cola_respuestas.get(), timeout=10)
            except asyncio.TimeoutError:
                print("✗ El servidor no respondió a tiempo.")
                continue

            if resultado is None:
                print("✗ El servidor cerró la conexión.")
                break
            ui.mostrar_respuesta(resultado)

    except asyncio.IncompleteReadError:
        print("✗ Se perdió la conexión con el servidor.")
    finally:
        if escucha is not None:
            escucha.cancel()
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass


def parsear_argumentos():
    parser = argparse.ArgumentParser(description="FreshNotify — cliente de sucursal")
    parser.add_argument("--host", default=SERVER_CONNECT_HOST,
                        help=f"Host del servidor (default: {SERVER_CONNECT_HOST})")
    parser.add_argument("--puerto", type=int, default=SERVER_PORT,
                        help=f"Puerto del servidor (default: {SERVER_PORT})")
    parser.add_argument("--nombre", default=None,
                        help="Nombre de la sucursal (si no se pasa, se pide por teclado)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parsear_argumentos()
    nombre = args.nombre or input("Nombre del supermercado: ").strip()
    password = getpass.getpass("Contraseña: ")
    try:
        asyncio.run(ejecutar_cliente(args.host, args.puerto, nombre, password))
    except KeyboardInterrupt:
        print("\nHasta luego.")
