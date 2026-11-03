-- Esquema de FreshNotify.
-- MariaDB lo ejecuta solo la primera vez que arranca con el volumen vacío
-- (docker-entrypoint-initdb.d). La base y el usuario ya los crea el compose
-- con las variables de entorno; acá van las tablas y los datos de prueba.

USE fresh_db;

-- -----------------------------------------------------------------------------
-- supermercados: cada sucursal que usa el sistema
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS supermercados (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    nombre        VARCHAR(100) NOT NULL UNIQUE,
    -- Hash bcrypt de la contraseña (todavía no se usa)
    password_hash VARCHAR(255) DEFAULT NULL,
    -- Con cuántos días de anticipación avisar
    umbral_dias   INT NOT NULL DEFAULT 7,
    activo        BOOLEAN NOT NULL DEFAULT TRUE,
    creado_en     DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- lotes: la unidad de inventario. Un lote pertenece a un supermercado.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lotes (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    supermercado_id   INT NOT NULL,
    codigo            VARCHAR(50) NOT NULL,
    producto          VARCHAR(100) NOT NULL,
    categoria         VARCHAR(50) NOT NULL DEFAULT 'general',
    cantidad          INT NOT NULL DEFAULT 1,
    fecha_vencimiento DATE NOT NULL,
    -- Baja lógica: los lotes no se borran, se desactivan y se guarda el motivo
    activo            BOOLEAN DEFAULT TRUE,
    motivo_baja       ENUM('eliminado_manual', 'vencido_automatico') DEFAULT NULL,
    creado_en         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (supermercado_id) REFERENCES supermercados(id),
    -- Un mismo código de lote no puede repetirse dentro de una sucursal
    UNIQUE (supermercado_id, codigo)
);

-- -----------------------------------------------------------------------------
-- notificaciones: historial de alertas por supermercado
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notificaciones (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    supermercado_id INT NOT NULL,
    tipo            VARCHAR(50) NOT NULL,
    mensaje         TEXT NOT NULL,
    leida           BOOLEAN NOT NULL DEFAULT FALSE,
    creado_en       DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (supermercado_id) REFERENCES supermercados(id)
);

-- -----------------------------------------------------------------------------
-- Datos de prueba. Las fechas son relativas a CURDATE(), así siempre hay
-- lotes por vencer sin importar el día.
-- -----------------------------------------------------------------------------

INSERT IGNORE INTO supermercados (id, nombre, umbral_dias) VALUES
    (1, 'Super Vea Centro', 7),
    (2, 'Atomo Guaymallen', 5);

INSERT IGNORE INTO lotes (supermercado_id, codigo, producto, categoria, cantidad, fecha_vencimiento) VALUES
    (1, 'LAC-001', 'Yogur bebible 1L',      'lacteos',   48, DATE_ADD(CURDATE(), INTERVAL 3 DAY)),
    (1, 'LAC-002', 'Leche entera 1L',       'lacteos',  120, DATE_ADD(CURDATE(), INTERVAL 5 DAY)),
    (1, 'CAR-001', 'Carne picada x kg',     'carnes',    30, DATE_ADD(CURDATE(), INTERVAL 2 DAY)),
    (1, 'ALM-001', 'Fideos secos 500g',     'almacen',  200, DATE_ADD(CURDATE(), INTERVAL 300 DAY)),
    (2, 'FRE-001', 'Queso cremoso x kg',    'frescos',   15, DATE_ADD(CURDATE(), INTERVAL 6 DAY));
