"""
Sesiones activas, una por sucursal.

Sirve para mandar cada notificación al writer correcto y para que el monitor
vea quién está conectado. No hace falta lock: todo corre en el mismo event
loop y ningún método tiene un await entre leer y escribir el diccionario.
"""

import asyncio


class GestorSesiones:

    def __init__(self):
        self._sesiones: dict[int, dict] = {}  # id → {"nombre": str, "writer": StreamWriter}

    def registrar(self, supermercado_id: int, nombre: str, writer: asyncio.StreamWriter) -> bool:
        """Devuelve False si la sucursal ya tiene una sesión abierta. Revisar
        y guardar van juntos para que no se cuele otro login en el medio."""
        if supermercado_id in self._sesiones:
            return False
        self._sesiones[supermercado_id] = {"nombre": nombre, "writer": writer}
        return True

    def quitar(self, supermercado_id: int) -> None:
        self._sesiones.pop(supermercado_id, None)

    def obtener(self, supermercado_id: int) -> dict | None:
        return self._sesiones.get(supermercado_id)

    def esta_conectada(self, supermercado_id: int) -> bool:
        return supermercado_id in self._sesiones

    def nombres(self) -> list[str]:
        return [s["nombre"] for s in self._sesiones.values()]

    def __len__(self) -> int:
        return len(self._sesiones)
