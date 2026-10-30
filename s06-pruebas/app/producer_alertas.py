import json
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "aireiq-alertas"

evento = {
    "tipo_evento": "alerta.calidad_aire",
    "dispositivo": "ESP32_AireIQ",
    "nivel": "PRUEBA",
    "origen": "python_s06",
    "mensaje": "Evento de prueba enviado desde productor Python",
    "fecha_evento_utc": datetime.now(timezone.utc).isoformat(),
}

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

resultado = producer.send(
    TOPIC,
    key=evento["dispositivo"],
    value=evento,
).get(timeout=10)

print("Evento enviado correctamente")
print(f"Topic: {resultado.topic}")
print(f"Partición: {resultado.partition}")
print(f"Offset: {resultado.offset}")

producer.close()