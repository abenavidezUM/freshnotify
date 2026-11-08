# FreshNotify

Sistema distribuido de gestión de vencimientos de **lotes de alimentos para supermercados**.
Proyecto final de **Computación II (2038)** — Ingeniería en Informática, Universidad de Mendoza.

## El problema

Un supermercado maneja muchos lotes de productos perecederos, cada uno con su fecha de
vencimiento. Controlarlos a mano lleva tiempo y es fácil que se escape alguno, y un lote vencido
en góndola es plata perdida y un riesgo para el cliente.

La idea de FreshNotify es que cada sucursal cargue sus lotes y que el sistema revise los
vencimientos solo, avisando en el momento cuando un lote está por vencer (cada sucursal elige
con cuántos días de anticipación).

## Arquitectura

- **Servidor central** (AsyncIO): acepta múltiples supermercados concurrentes por TCP, despacha
  operaciones CRUD y reenvía notificaciones en tiempo real.
- **Cliente CLI** (uno por sucursal): gestiona el inventario de lotes por TCP.
- **Monitor administrativo**: registra supermercados (con credencial) y consulta estadísticas
  vía Unix Domain Socket.
- **Celery worker + beat** sobre **Redis**: verificación periódica de vencimientos, bajas
  automáticas y pub/sub de notificaciones.
- **Pool de autenticación** (`ProcessPoolExecutor`): verifica hashes bcrypt (CPU-bound) en
  procesos aparte, con semáforo de admisión y contadores en memoria compartida.
- **MariaDB**: persistencia relacional (supermercados → lotes → notificaciones).

## Pendiente

- Dockerizar el servidor, los workers, el monitor y el cliente (hoy corren en la máquina)
- Pruebas de punta a punta con todo levantado en contenedores

## Cómo correr

Requisitos: Docker y Python 3.11+.

```bash
# 1. Infraestructura (MariaDB con el esquema y datos de demo, Redis)
docker compose up -d mariadb redis

# 2. Dependencias
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Cada uno en su terminal (con el venv activado):
python -m src.server.server                                    # servidor
celery -A src.workers.celery_app worker --loglevel=info        # worker
celery -A src.workers.celery_app beat --loglevel=info          # beat (tareas periódicas)

# 4. Cliente (demo: "Super Vea Centro" o "Atomo Guaymallen", contraseña "1234")
python -m src.client.client

# 5. Monitor administrativo (alta de supermercados, estadísticas, forzar tareas)
python -m src.monitor.monitor
```

Demos de concurrencia:

```bash
python -m src.auth.demo_carrera   # contador compartido entre procesos, con y sin lock
python -m src.auth.benchmark      # N logins con bcrypt en serie vs. en el pool de procesos
```

## Estructura

```
src/
├── server/          servidor AsyncIO: TCP + UDS del monitor + suscriptor Redis
├── client/          cliente CLI (menú + escucha de notificaciones push)
├── monitor/         monitor administrativo (UDS, request-response)
├── auth/            pool de autenticación: procesos, semáforo, memoria compartida
├── workers/         Celery: verificar_vencimientos, notificar_evento, limpieza
├── shared/          config, protocolo (framing), logger
└── infrastructure/  conexiones a MariaDB/Redis + repositorios por entidad
```
