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

- **Servidor central** (AsyncIO): acepta múltiples supermercados concurrentes por TCP, registra
  una sesión por sucursal y despacha operaciones CRUD sobre sus lotes.
- **Cliente CLI** (uno por sucursal): gestiona el inventario de lotes por TCP.
- **Protocolo propio**: mensajes JSON con prefijo de longitud de 4 bytes (framing sobre TCP).
- **MariaDB**: persistencia relacional (supermercados → lotes → notificaciones).

## Estado

Funciona la parte cliente-servidor: las sucursales se conectan, cargan y consultan sus lotes.

Falta:
- revisar los vencimientos en segundo plano (Celery + Beat) y avisar en tiempo real
- login con contraseña
- monitor para administrar las sucursales
- levantar todo con Docker Compose

## Cómo correr

Requisitos: Docker y Python 3.11+.

```bash
# 1. Infraestructura (MariaDB con el esquema y datos de demo)
docker compose up -d mariadb

# 2. Dependencias
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 3. Servidor (en su propia terminal, con el venv activado)
python -m src.server.server

# 4. Cliente (sucursales de demo: "Super Vea Centro" o "Atomo Guaymallen")
python -m src.client.client
```

## Estructura

```
src/
├── server/          servidor AsyncIO: handshake, sesiones y despacho CRUD
├── client/          cliente CLI (menú + conexión, separados)
├── shared/          config, protocolo (framing), logger
└── infrastructure/  conexión a MariaDB + repositorios por entidad
```
