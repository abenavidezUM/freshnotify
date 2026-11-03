"""Todo lo que el cliente muestra en pantalla o pide por teclado."""

from datetime import datetime

MENU = """
─────────── FreshNotify · {nombre} ───────────
 1. Dar de alta un lote
 2. Listar lotes
 3. Buscar lote por código
 4. Modificar un lote
 5. Eliminar un lote
 6. Ver notificaciones
 7. Resumen de estado
 0. Salir
"""


def mostrar_menu(nombre: str) -> str:
    print(MENU.format(nombre=nombre))
    return input("Opción: ").strip()


def pedir_texto(etiqueta: str, obligatorio: bool = True) -> str:
    while True:
        valor = input(f"{etiqueta}: ").strip()
        if valor or not obligatorio:
            return valor
        print("  ⚠ Este campo es obligatorio.")


def pedir_fecha(etiqueta: str, obligatorio: bool = True) -> str:
    """Valida el formato acá para avisar enseguida (el servidor igual vuelve a validar)."""
    while True:
        valor = input(f"{etiqueta} (YYYY-MM-DD): ").strip()
        if not valor and not obligatorio:
            return ""
        try:
            datetime.strptime(valor, "%Y-%m-%d")
            return valor
        except ValueError:
            print("  ⚠ Fecha inválida. Ejemplo: 2026-09-15")


def pedir_datos_lote() -> dict:
    print("\n— Alta de lote —")
    return {
        "codigo": pedir_texto("Código del lote"),
        "producto": pedir_texto("Producto"),
        "categoria": pedir_texto("Categoría (lacteos/carnes/frescos/frutas_verduras/almacen/congelados/general)"),
        "cantidad": pedir_texto("Cantidad"),
        "fecha_vencimiento": pedir_fecha("Fecha de vencimiento"),
    }


def pedir_cambios_lote() -> dict:
    """Los campos que se dejan vacíos no se modifican."""
    print("\n— Modificación de lote (dejar vacío lo que no cambia) —")
    cambios = {}
    if producto := pedir_texto("Nuevo producto", obligatorio=False):
        cambios["producto"] = producto
    if categoria := pedir_texto("Nueva categoría", obligatorio=False):
        cambios["categoria"] = categoria
    if cantidad := pedir_texto("Nueva cantidad", obligatorio=False):
        cambios["cantidad"] = cantidad
    if fecha := pedir_fecha("Nueva fecha de vencimiento", obligatorio=False):
        cambios["fecha_vencimiento"] = fecha
    return cambios


def _marca_urgencia(dias: int) -> str:
    if dias < 0:
        return "✗ VENCIDO"
    if dias <= 7:
        return f"⚠ vence en {dias} día(s)"
    return f"{dias} días"


def mostrar_lotes(lotes: list) -> None:
    if not lotes:
        print("\n  (no hay lotes activos)")
        return
    print(f"\n  {'CÓDIGO':<10} {'PRODUCTO':<28} {'CATEGORÍA':<16} {'CANT.':>6}  {'VENCE':<12} ESTADO")
    print("  " + "─" * 92)
    for l in lotes:
        print(f"  {l['codigo']:<10} {l['producto']:<28.28} {l['categoria']:<16} "
              f"{l['cantidad']:>6}  {l['fecha_vencimiento']:<12} {_marca_urgencia(l['dias_restantes'])}")


def mostrar_notificaciones(notificaciones: list) -> None:
    if not notificaciones:
        print("\n  (sin notificaciones)")
        return
    print()
    for n in notificaciones:
        estado = " " if n["leida"] else "●"
        print(f"  {estado} [{n['fecha']}] ({n['tipo']}) {n['mensaje']}")


def mostrar_resumen(r: dict) -> None:
    print(f"""
  Estado de la sucursal
  ─────────────────────
  Lotes activos:            {r['total_lotes']}
  Por vencer (≤{r['umbral_dias']} días):    {r['por_vencer']}
  Vencidos:                 {r['vencidos']}
  Notificaciones sin leer:  {r['notificaciones_no_leidas']}""")


def mostrar_respuesta(respuesta: dict) -> None:
    """Muestra la respuesta según lo que traiga."""
    if "lotes" in respuesta:
        mostrar_lotes(respuesta["lotes"])
    elif "lote" in respuesta:
        mostrar_lotes([respuesta["lote"]])
    elif "notificaciones" in respuesta:
        mostrar_notificaciones(respuesta["notificaciones"])
    elif respuesta.get("tipo") == "resumen_estado" or "total_lotes" in respuesta:
        mostrar_resumen(respuesta)
    else:
        simbolo = "✓" if respuesta.get("ok") else "✗"
        print(f"\n  {simbolo} {respuesta.get('mensaje', '(sin mensaje)')}")
