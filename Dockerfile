# Imagen de la app. Por ahora la usan los workers y beat del compose; el
# servidor, el monitor y el cliente siguen corriendo en la máquina.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Primero las dependencias, así no se reinstalan cada vez que cambia el código
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src

# Sin root (Celery avisa si corre como root)
RUN useradd --create-home fresh
USER fresh
