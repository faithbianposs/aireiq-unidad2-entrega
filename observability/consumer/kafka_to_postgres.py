import json
import os
import time
from datetime import datetime, timezone

import psycopg
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aireiq-lecturas")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "aireiq-grafana-sink-v1")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "aireiq_operativo"),
    "user": os.getenv("POSTGRES_USER", "aireiq"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

INSERT_SQL = """
INSERT INTO lecturas_aire (
    kafka_partition, kafka_offset, kafka_timestamp, event_time,
    dispositivo, temperatura_c, humedad_pct, presion_hpa, gas_ohms,
    altura_m, iaq_estimado, eco2_estimado_ppm, voc_indice_estimado,
    calidad_aire, created_at_origen
)
VALUES (
    %(kafka_partition)s, %(kafka_offset)s, %(kafka_timestamp)s, %(event_time)s,
    %(dispositivo)s, %(temperatura_c)s, %(humedad_pct)s, %(presion_hpa)s,
    %(gas_ohms)s, %(altura_m)s, %(iaq_estimado)s, %(eco2_estimado_ppm)s,
    %(voc_indice_estimado)s, %(calidad_aire)s, %(created_at_origen)s
)
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING;
"""


def conectar_postgres():
    for intento in range(1, 31):
        try:
            conexion = psycopg.connect(**DB_CONFIG)
            print("Conectado a PostgreSQL")
            return conexion
        except Exception as error:
            print(f"PostgreSQL no disponible (intento {intento}/30): {error}")
            time.sleep(2)

    raise RuntimeError("No se pudo conectar a PostgreSQL")


def normalizar_evento(evento, mensaje):
    created_at = evento.get("created_at")

    return {
        "kafka_partition": mensaje.partition,
        "kafka_offset": mensaje.offset,
        "kafka_timestamp": datetime.fromtimestamp(
            mensaje.timestamp / 1000, tz=timezone.utc
        ),
        "event_time": created_at,
        "dispositivo": evento.get("dispositivo"),
        "temperatura_c": evento.get("temperatura_c"),
        "humedad_pct": evento.get("humedad_pct"),
        "presion_hpa": evento.get("presion_hpa"),
        "gas_ohms": evento.get("gas_ohms"),
        "altura_m": evento.get("altura_m"),
        "iaq_estimado": evento.get("iaq_estimado"),
        "eco2_estimado_ppm": evento.get("eco2_estimado_ppm"),
        "voc_indice_estimado": evento.get("voc_indice_estimado"),
        "calidad_aire": evento.get("calidad_aire"),
        "created_at_origen": created_at,
    }


def main():
    conexion = conectar_postgres()

    consumidor = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda valor: json.loads(valor.decode("utf-8")),
    )

    print(f"Consumidor activo: Kafka '{KAFKA_TOPIC}' -> PostgreSQL")

    for mensaje in consumidor:
        evento = mensaje.value

        if not evento.get("dispositivo"):
            print("Evento omitido: no contiene dispositivo")
            consumidor.commit()
            continue

        datos = normalizar_evento(evento, mensaje)

        with conexion.cursor() as cursor:
            cursor.execute(INSERT_SQL, datos)

        conexion.commit()
        consumidor.commit()

        print(
            "Lectura guardada en PostgreSQL | "
            f"dispositivo={datos['dispositivo']} "
            f"particion={mensaje.partition} offset={mensaje.offset}"
        )


if __name__ == "__main__":
    main()