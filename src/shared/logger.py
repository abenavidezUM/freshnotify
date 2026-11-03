"""
Logger con el mismo formato para todos los componentes.

Cada uno pide el suyo con obtener_logger("servidor"), obtener_logger("worker"),
etc. El nombre sale en cada línea, así se sabe quién escribió qué.
"""

import logging

FORMATO = "%(asctime)s | %(name)-10s | %(levelname)-8s | %(message)s"


def obtener_logger(nombre: str) -> logging.Logger:
    """Devuelve un logger con formato uniforme, creándolo si no existe."""
    logger = logging.getLogger(nombre)

    # Si ya tiene handler no se agrega otro (si no, cada línea sale repetida)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(FORMATO, datefmt="%H:%M:%S"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
