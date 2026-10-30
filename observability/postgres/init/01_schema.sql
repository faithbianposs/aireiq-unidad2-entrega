CREATE TABLE IF NOT EXISTS lecturas_aire (
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMPTZ NOT NULL,
    event_time TIMESTAMPTZ,
    dispositivo TEXT NOT NULL,
    temperatura_c DOUBLE PRECISION,
    humedad_pct DOUBLE PRECISION,
    presion_hpa DOUBLE PRECISION,
    gas_ohms DOUBLE PRECISION,
    altura_m DOUBLE PRECISION,
    iaq_estimado DOUBLE PRECISION,
    eco2_estimado_ppm DOUBLE PRECISION,
    voc_indice_estimado DOUBLE PRECISION,
    calidad_aire TEXT,
    created_at_origen TEXT,
    insertado_en TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_lecturas_event_time
    ON lecturas_aire (event_time DESC);

CREATE INDEX IF NOT EXISTS idx_lecturas_dispositivo_event_time
    ON lecturas_aire (dispositivo, event_time DESC);