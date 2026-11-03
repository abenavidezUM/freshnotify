"""Acceso a datos: conexiones y repositorios. El resto del código importa desde acá y no escribe SQL."""

from .clients import (
    get_async_connection,
    get_sync_connection,
)
from .repositories import (
    buscar_supermercado_por_nombre,
    obtener_resumen_supermercado,
    crear_lote,
    listar_lotes,
    buscar_lote,
    actualizar_lote,
    eliminar_lote,
    ver_notificaciones,
)
