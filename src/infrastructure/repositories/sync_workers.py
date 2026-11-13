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
            "SELECT id, nombre, umbral_dias, email FROM supermercados WHERE id = %s AND activo = TRUE",
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


def lotes_vencidos(conn, supermercado_id: int) -> list[dict]:
    """Lotes activos que ya pasaron su fecha de vencimiento."""
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(
            """
            SELECT id, codigo, producto, cantidad FROM lotes
            WHERE supermercado_id = %s AND activo = TRUE AND fecha_vencimiento < CURDATE()
            """,
            (supermercado_id,),
        )
        return list(cur.fetchall())


def dar_baja_vencido(conn, supermercado_id: int, lote_id: int, mensaje: str) -> bool:
    """Da de baja el lote y guarda el aviso, las dos cosas o ninguna.
    Devuelve False si el lote ya no estaba activo.

    El "AND activo = TRUE" del UPDATE es para cuando dos workers revisan la
    misma sucursal a la vez: los dos ven el lote en el SELECT, pero solo a uno
    le cambia la fila (rowcount 1) y el otro no avisa dos veces. Lo mismo si
    la sucursal lo eliminó justo antes.

    Va en una transacción por los reintentos: si la base se cae entre la baja
    y el aviso, sin transacción quedaría el lote dado de baja y sin aviso, y
    al reintentar ya no estaría activo, así que nadie avisaría nunca.
    """
    conn.begin()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE lotes SET activo = FALSE, motivo_baja = 'vencido_automatico'
                WHERE id = %s AND activo = TRUE
                """,
                (lote_id,),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return False
            cur.execute(
                "INSERT INTO notificaciones (supermercado_id, tipo, mensaje) VALUES (%s, 'lote_vencido', %s)",
                (supermercado_id, mensaje),
            )
        conn.commit()
        return True
    except BaseException:
        # Si la conexión se cortó el rollback también falla, pero da igual:
        # MariaDB descarta sola la transacción que quedó sin commit
        try:
            conn.rollback()
        except pymysql.err.Error:
            pass
        raise


def crear_notificacion(conn, supermercado_id: int, tipo: str, mensaje: str,
                       clave: str | None = None, diaria: bool = False) -> bool:
    """Guarda la notificación. Devuelve False si ya había una con esa clave.

    La clave evita avisos repetidos y el UNIQUE (supermercado_id, clave) de la
    tabla es el que decide. Antes se preguntaba primero "¿ya avisé?" y después
    se insertaba, y con dos workers a la vez los dos podían preguntar antes de
    que el otro insertara. Así no hay hueco entre leer y escribir.

    Con diaria=True se le agrega la fecha a la clave: las alertas de "vence
    pronto" van una vez por día, no una sola vez.
    """
    valor_clave = "CONCAT(%s, ':', CURDATE())" if diaria else "%s"
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"""
                INSERT INTO notificaciones (supermercado_id, tipo, mensaje, clave)
                VALUES (%s, %s, %s, {valor_clave})
                """,
                (supermercado_id, tipo, mensaje, clave),
            )
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1062:  # clave duplicada
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
