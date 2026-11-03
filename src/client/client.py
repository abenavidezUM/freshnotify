"""
Cliente de sucursal.

Se conecta al servidor, se identifica con el nombre de la sucursal y después
manda una operación por cada opción del menú y espera la respuesta. Lo que
se muestra en pantalla está en ui.py.
"""

import argparse
import asyncio

from src.shared import SERVER_CONNECT_HOST, SERVER_PORT, enviar_mensaje, recibir_mensaje
from src.client import ui


def construir_mensaje(opcion: str) -> dict | None:
    """Opción del menú → mensaje para el servidor (None si la opción no existe)."""
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


async def ejecutar_cliente(host: str, puerto: int, nombre: str):
    try:
        reader, writer = await asyncio.open_connection(host, puerto)
    except (ConnectionRefusedError, OSError):
        print(f"✗ No se pudo conectar a {host}:{puerto}. ¿Está corriendo el servidor?")
        return

    try:
        # Handshake: la sucursal se identifica por nombre
        await enviar_mensaje(writer, {"accion": "conectar", "nombre": nombre})
        respuesta = await recibir_mensaje(reader)

        if respuesta is None or respuesta.get("tipo") == "rechazo":
            motivo = respuesta.get("mensaje") if respuesta else "el servidor cerró la conexión"
            print(f"✗ Conexión rechazada: {motivo}")
            return

        print(f"\n✓ Conectado como '{respuesta.get('nombre', nombre)}'")
        ui.mostrar_resumen(respuesta)

        # Una operación por vuelta. input() bloquea, pero por ahora el
        # cliente no tiene nada más que hacer mientras espera.
        while True:
            opcion = ui.mostrar_menu(respuesta.get("nombre", nombre))
            if opcion == "0":
                print("Hasta luego.")
                break

            mensaje = construir_mensaje(opcion)
            if mensaje is None:
                print("  ⚠ Opción inválida.")
                continue

            await enviar_mensaje(writer, mensaje)
            resultado = await recibir_mensaje(reader)
            if resultado is None:
                print("✗ El servidor cerró la conexión.")
                break
            ui.mostrar_respuesta(resultado)

    except asyncio.IncompleteReadError:
        print("✗ Se perdió la conexión con el servidor.")
    finally:
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
    try:
        asyncio.run(ejecutar_cliente(args.host, args.puerto, nombre))
    except KeyboardInterrupt:
        print("\nHasta luego.")
