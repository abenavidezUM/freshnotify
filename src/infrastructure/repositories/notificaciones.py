"""Historial de notificaciones de cada sucursal."""

import aiomysql


async def ver_notificaciones(conn, supermercado_id: int, solo_no_leidas: bool = False) -> dict:
    """Devuelve las últimas 50 (más nuevas primero) y las marca como leídas."""
    filtro = "AND leida = FALSE" if solo_no_leidas else ""

    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            f"""
            SELECT id, tipo, mensaje, leida, creado_en
            FROM notificaciones
            WHERE supermercado_id = %s {filtro}
            ORDER BY creado_en DESC
            LIMIT 50
            """,
            (supermercado_id,),
        )
        filas = await cur.fetchall()

        if filas:
            ids = [f["id"] for f in filas]
            marcadores = ", ".join(["%s"] * len(ids))
            await cur.execute(
                f"UPDATE notificaciones SET leida = TRUE WHERE id IN ({marcadores})",
                ids,
            )

    return {
        "ok": True,
        "notificaciones": [
            {
                "tipo": f["tipo"],
                "mensaje": f["mensaje"],
                "leida": bool(f["leida"]),
                "fecha": f["creado_en"].strftime("%Y-%m-%d %H:%M"),
            }
            for f in filas
        ],
    }
