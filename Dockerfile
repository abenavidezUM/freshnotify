# Imagen de la app: la misma para el servidor, los workers, beat, el monitor
# y el cliente. Lo que cambia entre uno y otro es el comando del compose.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Primero las dependencias, así no se reinstalan cada vez que cambia el código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

# Sin root (Celery avisa si corre como root). La carpeta del socket del monitor
# se crea acá con dueño fresh: el volumen del compose la copia con esos permisos.
RUN useradd --create-home fresh \
    && mkdir -p /run/freshnotify && chown fresh /run/freshnotify
USER fresh
