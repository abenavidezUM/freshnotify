"""
Benchmark: verificar N contraseñas con bcrypt en serie vs. con un pool de procesos.

Simula varias sucursales logueándose a la vez. Cada verificación es
independiente de las otras, así que se reparte sin sincronizar nada.

Uso:
    python -m src.auth.benchmark [--logins 16] [--procesos 4]
"""

import argparse
import multiprocessing
import time

import bcrypt

# Contraseña de prueba (el hash se genera al arrancar)
PASSWORD = b"1234"


def verificar(password_hash: bytes) -> bool:
    """Una verificación bcrypt (~0,2 s de CPU)."""
    return bcrypt.checkpw(PASSWORD, password_hash)


def main():
    parser = argparse.ArgumentParser(description="FreshNotify — benchmark de autenticación")
    parser.add_argument("--logins", type=int, default=16, help="cuántos logins simular")
    parser.add_argument("--procesos", type=int, default=multiprocessing.cpu_count() // 2,
                        help="tamaño del pool de procesos")
    args = parser.parse_args()

    print(f"\nGenerando hash bcrypt de referencia…")
    password_hash = bcrypt.hashpw(PASSWORD, bcrypt.gensalt())
    trabajos = [password_hash] * args.logins

    # En serie: un login atrás del otro
    t0 = time.perf_counter()
    resultados_serie = [verificar(h) for h in trabajos]
    t_serie = time.perf_counter() - t0

    # En paralelo: Pool.map reparte la lista entre los procesos y junta los resultados
    t0 = time.perf_counter()
    with multiprocessing.Pool(processes=args.procesos) as pool:
        resultados_paralelo = pool.map(verificar, trabajos)
    t_paralelo = time.perf_counter() - t0

    assert resultados_serie == resultados_paralelo  # mismo resultado, distinto tiempo

    speedup = t_serie / t_paralelo
    eficiencia = speedup / args.procesos * 100

    print(f"""
  Benchmark: {args.logins} verificaciones bcrypt
  ─────────────────────────────────────────────
  Secuencial (1 proceso):     {t_serie:6.2f} s
  Paralelo  ({args.procesos} procesos):     {t_paralelo:6.2f} s

  Speedup:     {speedup:.2f}x
  Eficiencia:  {eficiencia:.0f}% (speedup / nº de procesos)

  Por qué no da {args.procesos}.00x exacto: crear procesos y repartir el
  trabajo tiene un costo fijo (ley de Amdahl: la parte no paralelizable
  acota el speedup). Con más trabajo por proceso, la eficiencia sube.
""")


if __name__ == "__main__":
    main()
