"""
Demos de las colas y los workers, para medir y no solo decir.

  cabeza   Por qué dos colas. Encola N mails y atrás un aviso de lote, primero
           todo en la misma cola y después cada uno en la suya, y mide cuánto
           tarda en llegar la 🔔.
  rafaga   Por qué varios contenedores. Encola N mails y mide cuánto tardan en
           salir todos y qué worker mandó cada uno. Se corre con distintas
           cantidades de worker-mail (docker compose up -d --scale worker-mail=4).
  pasada   Todo junto con las sucursales simuladas: fuerza una revisión de
           vencimientos y mide cuándo llegan los avisos en vivo y cuándo los
           mails. Vacía la casilla de Mailpit antes de empezar.

Necesita el compose levantado y, para cabeza y pasada, las sucursales de
src.workers.simular_sucursales.

Uso:
    python -m src.workers.demo_colas cabeza [--mails 50]
    python -m src.workers.demo_colas rafaga [--mails 50]
    python -m src.workers.demo_colas pasada
"""

import argparse
import json
import time
import urllib.request
from collections import Counter

from celery import group

from src.shared import REDIS_NOTIFICATIONS_CHANNEL
from src.infrastructure import get_sync_connection, get_sync_redis_client
from src.workers.celery_app import app
from src.workers.simular_sucursales import PREFIJO, LOTES, reiniciar
from src.workers.tasks import enviar_mail_alertas, notificar_evento, verificar_vencimientos

MAILPIT = "http://localhost:8025/api/v1"
DESTINO_DEMO = "demo@freshnotify.test"


def _sucursales_simuladas() -> list[int]:
    conn = get_sync_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM supermercados WHERE nombre LIKE %s ORDER BY id", (f"{PREFIJO} %",))
            return [fila[0] for fila in cur.fetchall()]
    finally:
        conn.close()


def _workers_de(cola: str) -> list[str]:
    """Nombres de los workers que están escuchando esa cola."""
    respuesta = app.control.inspect(timeout=1).active_queues() or {}
    return sorted(w for w, colas in respuesta.items() if any(c["name"] == cola for c in colas))


def _mail_demo(n: int):
    return (DESTINO_DEMO, "Demo de colas", [f"Mail de prueba {n}"])


# --- cabeza ---------------------------------------------------------------

def _latencia_aviso(sm_id: int, mails: int, cola_mails: str) -> float:
    """Encola los mails en cola_mails, atrás un aviso, y espera a que el
    servidor lo reciba por Redis (lo mismo que escucha el servidor)."""
    pubsub = get_sync_redis_client().pubsub()
    pubsub.subscribe(REDIS_NOTIFICATIONS_CHANNEL)
    texto = f"🔔 Aviso de prueba ({cola_mails}, {time.time()})"
    try:
        for n in range(mails):
            enviar_mail_alertas.apply_async(_mail_demo(n), queue=cola_mails, ignore_result=True)
        t0 = time.perf_counter()
        notificar_evento.apply_async((sm_id, "demo", texto), ignore_result=True)
        while True:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=1)
            if msg and json.loads(msg["data"])["mensaje"] == texto:
                return time.perf_counter() - t0
    finally:
        pubsub.close()


def cabeza(mails: int) -> None:
    ids = _sucursales_simuladas()
    if not ids:
        print("Primero: python -m src.workers.simular_sucursales crear")
        return
    print(f"\n{mails} mails y atrás un aviso de lote…")
    print("  · todo en la cola 'notificaciones'")
    una_cola = _latencia_aviso(ids[0], mails, cola_mails="notificaciones")
    time.sleep(3)  # que termine el último mail antes de la segunda prueba
    print("  · mails en 'mails', aviso en 'notificaciones'")
    separadas = _latencia_aviso(ids[0], mails, cola_mails="mails")

    print(f"""
  Cabeza de cola: {mails} mails delante de un aviso
  ─────────────────────────────────────────────
  Una sola cola:     la 🔔 tarda {una_cola:7.2f} s
  Colas separadas:   la 🔔 tarda {separadas:7.2f} s

  En una cola los workers toman las tareas en orden, y el aviso espera a
  que salgan todos los mails de adelante. En su cola no tiene a nadie
  adelante: lo atiende worker-notif mientras worker-mail sigue con los mails.
""")


# --- rafaga ---------------------------------------------------------------

def rafaga(mails: int) -> None:
    workers = _workers_de("mails")
    if not workers:
        print("No hay ningún worker escuchando la cola 'mails'. ¿Está levantado el compose?")
        return
    print(f"\n{mails} mails para {len(workers)} worker(s) de mails: {', '.join(workers)}")

    t0 = time.perf_counter()
    resultado = group(enviar_mail_alertas.s(*_mail_demo(n)) for n in range(mails)).apply_async()
    # propagate=False: si un mail se queda sin reintentos, se cuenta y se sigue
    resultados = resultado.get(timeout=600, propagate=False)
    total = time.perf_counter() - t0
    por_worker = Counter(r["worker"] for r in resultados if isinstance(r, dict))
    fallidos = mails - sum(por_worker.values())

    print(f"""
  Ráfaga de {mails} mails
  ─────────────────────────────────────────────
  Contenedores de worker-mail: {len(workers)}
  Tiempo total:                {total:6.2f} s  ({mails / total:.1f} mails/s)

  Mails por worker:""")
    for worker, cantidad in sorted(por_worker.items()):
        print(f"    {worker:28} {cantidad:3}")
    if fallidos:
        print(f"    {'sin enviar (sin reintentos)':28} {fallidos:3}")
    print("""
  Cada mail tarda ~1 s esperando al servidor de correo, no usando CPU:
  con el doble de procesos esperando a la vez, sale el doble por segundo.
""")


# --- pasada ---------------------------------------------------------------

def _mails_a_simuladas() -> int:
    with urllib.request.urlopen(f"{MAILPIT}/messages?limit=1000") as r:
        mensajes = json.load(r)["messages"]
    return sum(1 for m in mensajes if any("@sim-" in d["Address"] for d in m["To"]))


def _vaciar_mailpit() -> None:
    urllib.request.urlopen(urllib.request.Request(f"{MAILPIT}/messages", method="DELETE"))


def pasada() -> None:
    ids = set(_sucursales_simuladas())
    if not ids:
        print("Primero: python -m src.workers.simular_sucursales crear")
        return
    avisos_esperados, mails_esperados = len(ids) * len(LOTES), len(ids)
    print(f"Workers: notificaciones {_workers_de('notificaciones')}, mails {_workers_de('mails')}")

    # En este orden por si Beat lanza su revisión justo en el medio: así sus
    # avisos y sus mails también se cuentan
    _vaciar_mailpit()
    pubsub = get_sync_redis_client().pubsub()
    pubsub.subscribe(REDIS_NOTIFICATIONS_CHANNEL)
    reiniciar()
    avisos, t_avisos, t_mails = 0, None, None
    t0 = time.perf_counter()
    verificar_vencimientos.apply_async(ignore_result=True)
    try:
        ultimo_control = 0.0
        while t_avisos is None or t_mails is None:
            msg = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.2)
            if msg and json.loads(msg["data"])["supermercado_id"] in ids:
                avisos += 1
                if avisos == avisos_esperados:
                    t_avisos = time.perf_counter() - t0
            # Mailpit se consulta cada medio segundo, no en cada vuelta
            if t_mails is None and time.perf_counter() - ultimo_control > 0.5:
                ultimo_control = time.perf_counter()
                if _mails_a_simuladas() >= mails_esperados:
                    t_mails = time.perf_counter() - t0
            if time.perf_counter() - t0 > 300:
                print("Pasaron 5 minutos y no llegó todo, ¿están los workers?")
                return
    finally:
        pubsub.close()

    print(f"""
  Revisión de {len(ids)} sucursales simuladas
  ─────────────────────────────────────────────
  {avisos_esperados} avisos en vivo:  {t_avisos:6.2f} s
  {mails_esperados} mails:            {t_mails:6.2f} s

  Las dos cosas avanzan a la vez y una no frena a la otra: los avisos los
  hace worker-notif y los mails worker-mail, cada uno con su cola.
""")


def main():
    parser = argparse.ArgumentParser(description="FreshNotify — demos de colas y workers")
    parser.add_argument("demo", choices=["cabeza", "rafaga", "pasada"])
    parser.add_argument("--mails", type=int, default=50, help="cuántos mails encolar")
    args = parser.parse_args()

    if args.demo == "cabeza":
        cabeza(args.mails)
    elif args.demo == "rafaga":
        rafaga(args.mails)
    else:
        pasada()


if __name__ == "__main__":
    main()
