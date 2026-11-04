"""
Demo de condición de carrera con un contador compartido entre procesos.

`valor += 1` parece una sola operación pero son tres: leer, sumar y
escribir. Si dos procesos se intercalan, se pierden incrementos. Es el
mismo tipo de contador que usa el pool de login, por eso ahí va con lock.

Uso:
    python -m src.auth.demo_carrera [--procesos 4] [--incrementos 50000]
"""

import argparse
import multiprocessing


def incrementar_sin_lock(contador, n):
    for _ in range(n):
        # Sin protección: los procesos se pisan.
        # get_obj() accede al valor sin pasar por el lock interno de Value.
        contador.get_obj().value += 1


def incrementar_con_lock(contador, n):
    for _ in range(n):
        # Con el lock, leer-sumar-escribir se hace entero antes de que entre otro
        with contador.get_lock():
            contador.get_obj().value += 1


def correr(funcion, procesos, incrementos):
    contador = multiprocessing.Value("i", 0)
    hijos = [
        multiprocessing.Process(target=funcion, args=(contador, incrementos))
        for _ in range(procesos)
    ]
    for p in hijos:
        p.start()
    for p in hijos:
        p.join()
    return contador.value


def main():
    parser = argparse.ArgumentParser(description="FreshNotify — demo de condición de carrera")
    parser.add_argument("--procesos", type=int, default=4)
    parser.add_argument("--incrementos", type=int, default=50_000)
    args = parser.parse_args()

    esperado = args.procesos * args.incrementos
    print(f"\n{args.procesos} procesos incrementando un contador compartido "
          f"{args.incrementos:,} veces cada uno. Esperado: {esperado:,}\n")

    sin_lock = correr(incrementar_sin_lock, args.procesos, args.incrementos)
    perdidos = esperado - sin_lock
    print(f"  SIN lock:  {sin_lock:>10,}   ✗ se perdieron {perdidos:,} incrementos "
          f"({perdidos / esperado:.0%})")

    con_lock = correr(incrementar_con_lock, args.procesos, args.incrementos)
    print(f"  CON lock:  {con_lock:>10,}   {'✓ exacto' if con_lock == esperado else '✗'}\n")

    print("  El lock serializa la sección crítica: más lento, pero correcto.")
    print("  Este es el mismo lock que protege los contadores de login del pool.\n")


if __name__ == "__main__":
    main()
