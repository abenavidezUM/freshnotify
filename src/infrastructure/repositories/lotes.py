"""
CRUD de lotes.

Todas las funciones reciben la conexión y el supermercado_id de la sesión,
así cada sucursal solo toca sus propios lotes. Devuelven un dict
{"ok": ..., ...} listo para mandar al cliente. Las bajas son lógicas
(activo = FALSE con un motivo), nunca se hace DELETE.
"""

from datetime import date, datetime

import aiomysql
import pymysql.err

CATEGORIAS_VALIDAS = {"lacteos", "carnes", "frescos", "frutas_verduras", "almacen", "congelados", "general"}


def _validar_fecha(fecha_str: str) -> date | None:
    """Parsea YYYY-MM-DD; devuelve None si el formato es inválido."""
    try:
        return datetime.strptime(fecha_str, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _serializar_lote(fila: dict) -> dict:
    """Fila de la BD → dict para mandar como JSON (la fecha va como texto)."""
    return {
        "codigo": fila["codigo"],
        "producto": fila["producto"],
        "categoria": fila["categoria"],
        "cantidad": fila["cantidad"],
        "fecha_vencimiento": fila["fecha_vencimiento"].isoformat(),
        "dias_restantes": int(fila["dias_restantes"]),
    }


async def crear_lote(conn, supermercado_id: int, codigo: str, producto: str,
                     categoria: str, cantidad, fecha_vencimiento: str) -> dict:
    codigo = (codigo or "").strip()
    producto = (producto or "").strip()
    categoria = (categoria or "general").strip().lower()

    if not codigo or not producto:
        return {"ok": False, "mensaje": "El código y el producto son obligatorios."}
    if categoria not in CATEGORIAS_VALIDAS:
        return {"ok": False, "mensaje": f"Categoría inválida. Opciones: {', '.join(sorted(CATEGORIAS_VALIDAS))}."}

    try:
        cantidad = int(cantidad)
    except (ValueError, TypeError):
        return {"ok": False, "mensaje": "La cantidad debe ser un número entero."}
    if cantidad <= 0:
        return {"ok": False, "mensaje": "La cantidad debe ser mayor a cero."}

    fecha = _validar_fecha(fecha_vencimiento)
    if fecha is None:
        return {"ok": False, "mensaje": "Fecha inválida. Formato esperado: YYYY-MM-DD."}

    try:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO lotes (supermercado_id, codigo, producto, categoria, cantidad, fecha_vencimiento)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (supermercado_id, codigo, producto, categoria, cantidad, fecha),
            )
    except pymysql.err.IntegrityError:
        # UNIQUE(supermercado_id, codigo): ese código ya existe en la sucursal
        return {"ok": False, "mensaje": f"Ya existe un lote activo o histórico con el código '{codigo}'."}

    return {"ok": True, "mensaje": f"Lote '{codigo}' ({producto}) dado de alta."}


async def listar_lotes(conn, supermercado_id: int) -> dict:
    """Lotes activos, primero los que vencen antes."""
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT codigo, producto, categoria, cantidad, fecha_vencimiento,
                   DATEDIFF(fecha_vencimiento, CURDATE()) AS dias_restantes
            FROM lotes
            WHERE supermercado_id = %s AND activo = TRUE
            ORDER BY fecha_vencimiento ASC
            """,
            (supermercado_id,),
        )
        filas = await cur.fetchall()

    return {"ok": True, "lotes": [_serializar_lote(f) for f in filas]}


async def buscar_lote(conn, supermercado_id: int, codigo: str) -> dict:
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT codigo, producto, categoria, cantidad, fecha_vencimiento,
                   DATEDIFF(fecha_vencimiento, CURDATE()) AS dias_restantes
            FROM lotes
            WHERE supermercado_id = %s AND codigo = %s AND activo = TRUE
            """,
            (supermercado_id, (codigo or "").strip()),
        )
        fila = await cur.fetchone()

    if fila is None:
        return {"ok": False, "mensaje": f"No hay ningún lote activo con código '{codigo}'."}
    return {"ok": True, "lote": _serializar_lote(fila)}


async def actualizar_lote(conn, supermercado_id: int, codigo: str,
                          producto=None, categoria=None, cantidad=None,
                          fecha_vencimiento=None) -> dict:
    """Actualiza solo los campos que vienen (los None se ignoran).
    El SET se arma según los campos, pero los valores siempre van como
    parámetros, nunca pegados en el SQL."""
    cambios, valores = [], []

    if producto is not None and producto.strip():
        cambios.append("producto = %s")
        valores.append(producto.strip())

    if categoria is not None and categoria.strip():
        cat = categoria.strip().lower()
        if cat not in CATEGORIAS_VALIDAS:
            return {"ok": False, "mensaje": f"Categoría inválida. Opciones: {', '.join(sorted(CATEGORIAS_VALIDAS))}."}
        cambios.append("categoria = %s")
        valores.append(cat)

    if cantidad is not None:
        try:
            cantidad = int(cantidad)
        except (ValueError, TypeError):
            return {"ok": False, "mensaje": "La cantidad debe ser un número entero."}
        if cantidad <= 0:
            return {"ok": False, "mensaje": "La cantidad debe ser mayor a cero."}
        cambios.append("cantidad = %s")
        valores.append(cantidad)

    if fecha_vencimiento is not None:
        fecha = _validar_fecha(fecha_vencimiento)
        if fecha is None:
            return {"ok": False, "mensaje": "Fecha inválida. Formato esperado: YYYY-MM-DD."}
        cambios.append("fecha_vencimiento = %s")
        valores.append(fecha)

    if not cambios:
        return {"ok": False, "mensaje": "No se indicó ningún campo para modificar."}

    valores += [supermercado_id, (codigo or "").strip()]
    async with conn.cursor() as cur:
        await cur.execute(
            f"""
            UPDATE lotes SET {', '.join(cambios)}
            WHERE supermercado_id = %s AND codigo = %s AND activo = TRUE
            """,
            valores,
        )
        modificadas = cur.rowcount

    if modificadas == 0:
        return {"ok": False, "mensaje": f"No hay ningún lote activo con código '{codigo}'."}
    return {"ok": True, "mensaje": f"Lote '{codigo}' actualizado."}


async def eliminar_lote(conn, supermercado_id: int, codigo: str) -> dict:
    """Baja manual: el lote queda en la tabla con motivo 'eliminado_manual'."""
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE lotes SET activo = FALSE, motivo_baja = 'eliminado_manual'
            WHERE supermercado_id = %s AND codigo = %s AND activo = TRUE
            """,
            (supermercado_id, (codigo or "").strip()),
        )
        modificadas = cur.rowcount

    if modificadas == 0:
        return {"ok": False, "mensaje": f"No hay ningún lote activo con código '{codigo}'."}
    return {"ok": True, "mensaje": f"Lote '{codigo}' eliminado del inventario."}
