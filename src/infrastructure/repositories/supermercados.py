"""
Consultas sobre la tabla supermercados: las que usa el servidor al conectar
una sucursal y las del monitor (alta, listado, activar/desactivar, estadísticas).
"""

import aiomysql
import pymysql.err


async def buscar_supermercado_por_nombre(conn, nombre: str) -> dict:
    """Busca la sucursal por nombre (sin importar mayúsculas). Trae también
    el password_hash para verificarlo en el pool de login."""
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT id, nombre, password_hash, activo, umbral_dias
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
        "password_hash": fila["password_hash"],
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


# --- Operaciones del monitor ---

async def crear_supermercado(conn, nombre: str, password_hash: str, umbral_dias: int = 7,
                             email: str | None = None) -> dict:
    nombre = (nombre or "").strip()
    if not nombre:
        return {"ok": False, "mensaje": "El nombre es obligatorio."}
    try:
        umbral_dias = int(umbral_dias)
        if not 1 <= umbral_dias <= 60:
            raise ValueError
    except (ValueError, TypeError):
        return {"ok": False, "mensaje": "El umbral debe ser un entero entre 1 y 60 días."}

    email = (email or "").strip() or None
    if email is not None and "@" not in email:
        return {"ok": False, "mensaje": "El mail no parece válido."}

    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO supermercados (nombre, password_hash, umbral_dias, email) "
                "VALUES (%s, %s, %s, %s)",
                (nombre, password_hash, umbral_dias, email),
            )
    except pymysql.err.IntegrityError:
        return {"ok": False, "mensaje": f"Ya existe un supermercado llamado '{nombre}'."}

    return {"ok": True, "mensaje": f"Supermercado '{nombre}' registrado (umbral: {umbral_dias} días)."}


async def listar_supermercados(conn) -> dict:
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT s.nombre, s.umbral_dias, s.activo, s.email,
                   COUNT(l.id) AS lotes_activos
            FROM supermercados s
            LEFT JOIN lotes l ON l.supermercado_id = s.id AND l.activo = TRUE
            GROUP BY s.id
            ORDER BY s.nombre
            """
        )
        filas = await cur.fetchall()

    return {
        "ok": True,
        "supermercados": [
            {
                "nombre": f["nombre"],
                "umbral_dias": f["umbral_dias"],
                "activo": bool(f["activo"]),
                "email": f["email"],
                "lotes_activos": int(f["lotes_activos"]),
            }
            for f in filas
        ],
    }


async def cambiar_activo_supermercado(conn, nombre: str, activo: bool) -> dict:
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE supermercados SET activo = %s WHERE LOWER(nombre) = LOWER(%s)",
            (activo, (nombre or "").strip()),
        )
        modificadas = cur.rowcount

    if modificadas == 0:
        return {"ok": False, "mensaje": f"No existe el supermercado '{nombre}' (o ya estaba así)."}
    estado = "activado" if activo else "desactivado"
    return {"ok": True, "mensaje": f"Supermercado '{nombre}' {estado}."}


async def obtener_estadisticas_globales(conn) -> dict:
    """Totales generales para el monitor."""
    async with conn.cursor(aiomysql.DictCursor) as cur:
        await cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM supermercados)                          AS supermercados,
                (SELECT COUNT(*) FROM supermercados WHERE activo = TRUE)      AS supermercados_activos,
                (SELECT COUNT(*) FROM lotes WHERE activo = TRUE)              AS lotes_activos,
                (SELECT COUNT(*) FROM lotes
                  WHERE activo = FALSE AND motivo_baja = 'vencido_automatico') AS bajas_automaticas,
                (SELECT COUNT(*) FROM notificaciones)                         AS notificaciones,
                (SELECT COUNT(*) FROM notificaciones WHERE leida = FALSE)     AS notificaciones_sin_leer
            """
        )
        fila = await cur.fetchone()

    return {"ok": True, "estadisticas": {k: int(v) for k, v in fila.items()}}
