"""
Monitor de administración.

Sirve para dar de alta supermercados, ver estadísticas y forzar tareas.
Habla con el servidor por un socket Unix en vez de TCP: solo funciona en la
misma máquina, no expone ningún puerto y los permisos los da el sistema de
archivos.

Cada comando abre una conexión, manda el pedido, recibe la respuesta y
cierra. Usa el mismo protocolo que los clientes.
"""

import argparse
import asyncio
import getpass

from src.shared import MONITOR_SOCKET_PATH, enviar_mensaje, recibir_mensaje

MENU = """
─────────── FreshNotify · Monitor administrativo ───────────
 1. Registrar supermercado
 2. Listar supermercados
 3. Activar supermercado
 4. Desactivar supermercado
 5. Estadísticas globales
 6. Estado del servidor (sesiones conectadas)
 7. Estadísticas de autenticación (memoria compartida del pool)
 8. Forzar verificación de vencimientos
 9. Limpiar notificaciones antiguas
 0. Salir
"""


async def enviar_comando(socket_path: str, comando: dict) -> dict | None:
    """Abre la conexión, manda un comando y devuelve la respuesta."""
    try:
        reader, writer = await asyncio.open_unix_connection(socket_path)
    except (ConnectionRefusedError, FileNotFoundError):
        print(f"✗ No se pudo conectar al socket {socket_path}. ¿Está corriendo el servidor?")
        return None

    try:
        await enviar_mensaje(writer, comando)
        return await recibir_mensaje(reader)
    finally:
        writer.close()
        await writer.wait_closed()


def construir_comando(opcion: str) -> dict | None:
    if opcion == "1":
        nombre = input("Nombre de la sucursal: ").strip()
        password = getpass.getpass("Contraseña para la sucursal: ")
        umbral = input("Umbral de alerta en días [7]: ").strip() or "7"
        return {"accion": "registrar_supermercado", "nombre": nombre,
                "password": password, "umbral_dias": umbral}
    if opcion == "2":
        return {"accion": "listar_supermercados"}
    if opcion == "3":
        return {"accion": "activar_supermercado", "nombre": input("Nombre: ").strip()}
    if opcion == "4":
        return {"accion": "desactivar_supermercado", "nombre": input("Nombre: ").strip()}
    if opcion == "5":
        return {"accion": "estadisticas"}
    if opcion == "6":
        return {"accion": "status"}
    if opcion == "7":
        return {"accion": "stats_auth"}
    if opcion == "8":
        return {"accion": "run_tarea", "tarea": "verificar_vencimientos"}
    if opcion == "9":
        return {"accion": "run_tarea", "tarea": "limpiar_notificaciones"}
    return None


def mostrar_respuesta(r: dict) -> None:
    if "supermercados" in r:
        print(f"\n  {'NOMBRE':<28} {'UMBRAL':>7} {'LOTES':>6}  ESTADO")
        print("  " + "─" * 55)
        for s in r["supermercados"]:
            estado = "activo" if s["activo"] else "INACTIVO"
            print(f"  {s['nombre']:<28.28} {s['umbral_dias']:>5} d {s['lotes_activos']:>6}  {estado}")
    elif "estadisticas" in r:
        print("\n  Estadísticas globales")
        print("  ─────────────────────")
        for clave, valor in r["estadisticas"].items():
            print(f"  {clave.replace('_', ' '):<28} {valor}")
    elif "sesiones" in r:
        print(f"\n  Sesiones conectadas: {r['total_sesiones']}")
        for nombre in r["sesiones"]:
            print(f"   · {nombre}")
    elif "auth" in r:
        a = r["auth"]
        print(f"""
  Pool de autenticación (contadores en memoria compartida)
  ─────────────────────────────────────────────────────────
  Procesos del pool:            {a['procesos_pool']}
  Verificaciones simultáneas:   máx. {a['max_simultaneos']} (semáforo)
  Logins exitosos:              {a['logins_exitosos']}
  Logins fallidos:              {a['logins_fallidos']}""")
    else:
        simbolo = "✓" if r.get("ok") else "✗"
        print(f"\n  {simbolo} {r.get('mensaje', '(sin mensaje)')}")


async def main(socket_path: str):
    while True:
        print(MENU)
        opcion = input("Opción: ").strip()
        if opcion == "0":
            print("Hasta luego.")
            break

        comando = construir_comando(opcion)
        if comando is None:
            print("  ⚠ Opción inválida.")
            continue

        respuesta = await enviar_comando(socket_path, comando)
        if respuesta is not None:
            mostrar_respuesta(respuesta)


def parsear_argumentos():
    parser = argparse.ArgumentParser(description="FreshNotify — monitor administrativo")
    parser.add_argument("--socket", default=MONITOR_SOCKET_PATH,
                        help=f"Ruta del Unix Domain Socket (default: {MONITOR_SOCKET_PATH})")
    return parser.parse_args()


if __name__ == "__main__":
    args = parsear_argumentos()
    try:
        asyncio.run(main(args.socket))
    except KeyboardInterrupt:
        print("\nHasta luego.")
