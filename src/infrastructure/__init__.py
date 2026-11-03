"""Acceso a datos: conexiones y repositorios. El resto del código importa desde acá y no escribe SQL."""

from .clients import (
    get_async_connection,
    get_sync_connection,
)
from .repositories import (
    buscar_supermercado_por_nombre,
    obtener_resumen_supermercado,
)
