-- Cambios para una base que ya estaba creada (schema.sql solo corre con el
-- volumen vacío). Se puede correr más de una vez:
--   docker compose exec -T mariadb mariadb -ufresh_user -pfresh_pass fresh_db < docker/actualizar_base.sql

-- Anti-duplicados de alertas en la base
ALTER TABLE notificaciones
    ADD COLUMN IF NOT EXISTS clave VARCHAR(120) DEFAULT NULL,
    ADD UNIQUE INDEX IF NOT EXISTS supermercado_id_clave (supermercado_id, clave);

-- Mail del encargado de cada sucursal
ALTER TABLE supermercados ADD COLUMN IF NOT EXISTS email VARCHAR(150) DEFAULT NULL AFTER umbral_dias;
UPDATE supermercados SET email = 'encargado@supervea-centro.test'  WHERE id = 1 AND email IS NULL;
UPDATE supermercados SET email = 'encargado@atomo-guaymallen.test' WHERE id = 2 AND email IS NULL;
