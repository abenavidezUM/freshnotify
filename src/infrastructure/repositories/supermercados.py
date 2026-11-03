"""Consultas sobre la tabla supermercados que usa el servidor al conectar una sucursal."""

import aiomysql


async def buscar_supermercado_por_nombre(conn, nombre: str) -> dict:
    """Busca la sucursal por nombre (sin importar mayúsculas)."""
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT id, nombre, activo, umbral_dias
            FROM supermercados
            WHERE LOWER(nombre) = LOWER(%s)
            """,
            (nombre,),
        )
        fila = await cur.fetchone()

    if fila is None:
        return {"encontrado": False}

    return {
        "encontrado": True,
        "supermercado_id": fila["id"],
        "nombre": fila["nombre"],
        "activo": bool(fila["activo"]),
        "umbral_dias": fila["umbral_dias"],
    }


async def obtener_resumen_supermercado(conn, supermercado_id: int) -> dict:
    """Lo que se le muestra a la sucursal al conectarse: lotes activos,
    por vencer, vencidos y notificaciones sin leer."""
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                s.umbral_dias,
                COUNT(l.id)                                            AS total_lotes,
                COALESCE(SUM(l.fecha_vencimiento <  CURDATE()), 0)     AS vencidos,
                COALESCE(SUM(l.fecha_vencimiento >= CURDATE()
                    AND l.fecha_vencimiento <= DATE_ADD(CURDATE(), INTERVAL s.umbral_dias DAY)), 0)
                                                                       AS por_vencer
            FROM supermercados s
            LEFT JOIN lotes l ON l.supermercado_id = s.id AND l.activo = TRUE
            WHERE s.id = %s
            GROUP BY s.id
            """,
            (supermercado_id,),
        )
        resumen = await cur.fetchone()

        await cur.execute(
            "SELECT COUNT(*) AS n FROM notificaciones WHERE supermercado_id = %s AND leida = FALSE",
            (supermercado_id,),
        )
        pendientes = await cur.fetchone()

    return {
        "tipo": "resumen_estado",
        "ok": True,
        "umbral_dias": resumen["umbral_dias"],
        "total_lotes": int(resumen["total_lotes"]),
        "por_vencer": int(resumen["por_vencer"]),
        "vencidos": int(resumen["vencidos"]),
        "notificaciones_no_leidas": int(pendientes["n"]),
    }
