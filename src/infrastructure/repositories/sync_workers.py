"""
Consultas de los workers de Celery: vencimientos, bajas automáticas,
notificaciones y limpieza. Son sincrónicas (PyMySQL) porque los workers
no tienen event loop.
"""

import pymysql.cursors
import pymysql.err


def ids_supermercados_activos(conn) -> list[int]:
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM supermercados WHERE activo = TRUE ORDER BY id")
        return [fila[0] for fila in cur.fetchall()]


def supermercado_activo(conn, supermercado_id: int) -> dict | None:
    """None si no existe o si la desactivaron mientras la tarea esperaba en la cola."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            "SELECT id, nombre, umbral_dias FROM supermercados WHERE id = %s AND activo = TRUE",
            (supermercado_id,),
        )
        return cur.fetchone()


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
    """Da de baja los lotes ya vencidos y devuelve los que dio de baja esta llamada.

    Va de a un lote y con "AND activo = TRUE" en el UPDATE: si dos workers
    revisan la misma sucursal a la vez, los dos ven el lote en el SELECT pero
    solo a uno le cambia la fila (rowcount 1). El otro no lo cuenta y no avisa
    dos veces. Lo mismo si la sucursal lo eliminó justo antes.
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, codigo, producto, cantidad FROM lotes
            WHERE supermercado_id = %s AND activo = TRUE AND fecha_vencimiento < CURDATE()
            """,
            (supermercado_id,),
        )
        vencidos = list(cur.fetchall())

        dados_de_baja = []
        for lote in vencidos:
            cur.execute(
                """
                UPDATE lotes SET activo = FALSE, motivo_baja = 'vencido_automatico'
                WHERE id = %s AND activo = TRUE
                """,
                (lote["id"],),
            )
            if cur.rowcount == 1:
                dados_de_baja.append(lote)
    return dados_de_baja


def crear_notificacion(conn, supermercado_id: int, tipo: str, mensaje: str,
                       clave: str | None = None) -> bool:
    """Guarda la notificación. Devuelve False si era repetida.

    La clave es para las alertas que van una vez por día: se le agrega la
    fecha y el UNIQUE (supermercado_id, clave) de la tabla hace el resto. Antes
    se preguntaba primero "¿ya avisé hoy?" y después se insertaba, y con dos
    workers a la vez los dos podían preguntar antes de que el otro insertara.
    Así la base decide sola y no hay hueco entre leer y escribir.
    """
    with conn.cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO notificaciones (supermercado_id, tipo, mensaje, clave)
                VALUES (%s, %s, %s, CONCAT(%s, ':', CURDATE()))
                """,
                (supermercado_id, tipo, mensaje, clave),
            )
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # clave duplicada: ya se avisó hoy
                return False
            raise
    return True


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
