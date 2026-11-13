"""
Carga N sucursales de prueba para ver cómo se reparten las tareas entre workers.

Cada sucursal ("Sucursal Sim 01", "Sucursal Sim 02"…) tiene mail de encargado,
contraseña 1234 y tres lotes: uno que vence hoy, otro en 2 días y otro que
venció ayer. En cada pasada de la revisión eso da 3 avisos por sucursal y un
mail con el resumen. Con 50 sucursales: 150 avisos y 50 mails de una.

Uso:
    python -m src.workers.simular_sucursales crear [--cantidad 50]
    python -m src.workers.simular_sucursales reiniciar   # para repetir la pasada
    python -m src.workers.simular_sucursales borrar
"""

import argparse

import bcrypt

from src.infrastructure import get_sync_connection

PREFIJO = "Sucursal Sim"
PASSWORD = b"1234"

# (código, producto, días hasta el vencimiento)
LOTES = [
    ("SIM-LECHE", "Leche entera", 0),
    ("SIM-YOGUR", "Yogur bebible", 2),
    ("SIM-QUESO", "Queso cremoso", -1),
]


def _ids(cur) -> list[int]:
    cur.execute("SELECT id FROM supermercados WHERE nombre LIKE %s", (f"{PREFIJO} %",))
    return [fila[0] for fila in cur.fetchall()]


def crear(cantidad: int) -> None:
    # Un solo hash para todas: cada bcrypt tarda ~0,2 s y no hace falta que sean distintos
    password_hash = bcrypt.hashpw(PASSWORD, bcrypt.gensalt()).decode()
    conn = get_sync_connection()
    try:
        with conn.cursor() as cur:
            if _ids(cur):
                print("Ya hay sucursales simuladas. Borralas primero con 'borrar'.")
                return
            for n in range(1, cantidad + 1):
                cur.execute(
                    "INSERT INTO supermercados (nombre, password_hash, umbral_dias, email) "
                    "VALUES (%s, %s, 7, %s)",
                    (f"{PREFIJO} {n:02d}", password_hash, f"encargado@sim-{n:02d}.test"),
                )
                sm_id = cur.lastrowid
                cur.executemany(
                    "INSERT INTO lotes (supermercado_id, codigo, producto, cantidad, fecha_vencimiento) "
                    "VALUES (%s, %s, %s, 10, CURDATE() + INTERVAL %s DAY)",
                    [(sm_id, codigo, producto, dias) for codigo, producto, dias in LOTES],
                )
        print(f"{cantidad} sucursales simuladas creadas ({cantidad * len(LOTES)} lotes).")
    finally:
        conn.close()


def reiniciar() -> None:
    """Deja todo como recién creado: borra los avisos y reactiva el lote
    vencido, así la próxima revisión vuelve a generar todo."""
    conn = get_sync_connection()
    try:
        with conn.cursor() as cur:
            ids = _ids(cur)
            if not ids:
                print("No hay sucursales simuladas.")
                return
            marcas = ", ".join(["%s"] * len(ids))
            cur.execute(f"DELETE FROM notificaciones WHERE supermercado_id IN ({marcas})", ids)
            cur.execute(
                f"UPDATE lotes SET activo = TRUE, motivo_baja = NULL WHERE supermercado_id IN ({marcas})",
                ids,
            )
        print(f"{len(ids)} sucursales simuladas reiniciadas.")
    finally:
        conn.close()


def borrar() -> None:
    conn = get_sync_connection()
    try:
        with conn.cursor() as cur:
            ids = _ids(cur)
            if ids:
                marcas = ", ".join(["%s"] * len(ids))
                for tabla in ("notificaciones", "lotes"):
                    cur.execute(f"DELETE FROM {tabla} WHERE supermercado_id IN ({marcas})", ids)
                cur.execute(f"DELETE FROM supermercados WHERE id IN ({marcas})", ids)
        print(f"{len(ids)} sucursales simuladas borradas.")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="FreshNotify — sucursales simuladas")
    parser.add_argument("accion", choices=["crear", "reiniciar", "borrar"])
    parser.add_argument("--cantidad", type=int, default=50, help="cuántas sucursales crear")
    args = parser.parse_args()

    if args.accion == "crear":
        crear(args.cantidad)
    elif args.accion == "reiniciar":
        reiniciar()
    else:
        borrar()


if __name__ == "__main__":
    main()
