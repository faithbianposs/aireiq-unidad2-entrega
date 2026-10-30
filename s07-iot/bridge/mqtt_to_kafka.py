import json
import os

import paho.mqtt.client as mqtt
from kafka import KafkaProducer

MQTT_BROKER = os.getenv("MQTT_BROKER", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "aireiq/frank-ramos/lecturas/v1")
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "emqx")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "public")

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aireiq-lecturas")

CAMPOS_REQUERIDOS = {
    "dispositivo",
    "temperatura_c",
    "humedad_pct",
    "presion_hpa",
    "gas_ohms",
    "altura_m",
    "iaq_estimado",
    "eco2_estimado_ppm",
    "voc_indice_estimado",
    "calidad_aire",
    "created_at",
}

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"Conectado a MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        print(f"Escuchando MQTT topic: {MQTT_TOPIC}")
    else:
        print(f"No se pudo conectar al broker MQTT. Código: {reason_code}")


def on_message(client, userdata, message):
    try:
        lectura = json.loads(message.payload.decode("utf-8"))
    except json.JSONDecodeError:
        print("Mensaje descartado: no es JSON válido")
        return

    faltantes = CAMPOS_REQUERIDOS - set(lectura.keys())
    if faltantes:
        print(f"Mensaje descartado: faltan campos {sorted(faltantes)}")
        return

    try:
        resultado = producer.send(
            KAFKA_TOPIC,
            key=lectura["dispositivo"],
            value=lectura,
        ).get(timeout=10)

        print(
            f"Lectura reenviada a Kafka | "
            f"topic={resultado.topic} "
            f"particion={resultado.partition} "
            f"offset={resultado.offset}"
        )
    except Exception as error:
        print(f"Error al publicar en Kafka: {error}")


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id="aireiq-mqtt-kafka-bridge",
    protocol=mqtt.MQTTv311,
)

client.on_connect = on_connect
client.on_message = on_message
client.reconnect_delay_set(min_delay=1, max_delay=30)

print("Iniciando puente MQTT → Kafka...")
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
client.loop_forever()