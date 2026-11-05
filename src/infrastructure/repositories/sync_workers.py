"""
Consultas de los workers de Celery: vencimientos, bajas automáticas,
notificaciones y limpieza. Son sincrónicas (PyMySQL) porque los workers
no tienen event loop.
"""

import pymysql.cursors


def supermercados_activos(conn) -> list[dict]:
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("SELECT id, nombre, umbral_dias FROM supermercados WHERE activo = TRUE")
        return list(cur.fetchall())


def lotes_por_vencer(conn, supermercado_id: int, umbral_dias: int) -> list[dict]:
    """Lotes activos que vencen dentro del umbral de la sucursal (incluye hoy)."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT codigo, producto, cantidad, fecha_vencimiento,
                   DATEDIFF(fecha_vencimiento, CURDATE()) AS dias_restantes
            FROM lotes
            WHERE supermercado_id = %s AND activo = TRUE
              AND fecha_vencimiento >= CURDATE()
              AND fecha_vencimiento <= DATE_ADD(CURDATE(), INTERVAL %s DAY)
            ORDER BY fecha_vencimiento
            """,
            (supermercado_id, umbral_dias),
        )
        return list(cur.fetchall())


def dar_baja_lotes_vencidos(conn, supermercado_id: int) -> list[dict]:
    """Da de baja los lotes ya vencidos y devuelve cuáles eran, para avisar de cada uno."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT codigo, producto, cantidad FROM lotes
            WHERE supermercado_id = %s AND activo = TRUE AND fecha_vencimiento < CURDATE()
            """,
            (supermercado_id,),
        )
        vencidos = list(cur.fetchall())

        if vencidos:
            cur.execute(
                """
                UPDATE lotes SET activo = FALSE, motivo_baja = 'vencido_automatico'
                WHERE supermercado_id = %s AND activo = TRUE AND fecha_vencimiento < CURDATE()
                """,
                (supermercado_id,),
            )
    return vencidos


def crear_notificacion(conn, supermercado_id: int, tipo: str, mensaje: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO notificaciones (supermercado_id, tipo, mensaje) VALUES (%s, %s, %s)",
            (supermercado_id, tipo, mensaje),
        )


def ya_notificado_hoy(conn, supermercado_id: int, tipo: str, mensaje: str) -> bool:
    """Para no repetir alertas: Beat corre cada pocos segundos y sin esto el
    mismo lote avisaría en cada pasada. Como el mensaje se arma siempre igual,
    alcanza con buscar el mismo tipo y mensaje en el día."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM notificaciones
            WHERE supermercado_id = %s AND tipo = %s AND mensaje = %s
              AND DATE(creado_en) = CURDATE()
            LIMIT 1
            """,
            (supermercado_id, tipo, mensaje),
        )
        return cur.fetchone() is not None


def limpiar_notificaciones_antiguas(conn, dias_retencion: int) -> int:
    """Borra las notificaciones leídas más viejas que la retención. Devuelve cuántas."""
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM notificaciones
            WHERE leida = TRUE AND creado_en < DATE_SUB(NOW(), INTERVAL %s DAY)
            """,
            (dias_retencion,),
        )
        return cur.rowcount
