import json
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer

TOPIC = "aireiq-ordenes-inspeccion"

evento = {
    "tipo_evento": "inspeccion.creada",
    "orden_id": str(uuid4()),
    "dispositivo": "ESP32_AireIQ",
    "motivo": "Revision programada del ambiente monitoreado",
    "solicitante": "equipo_AireIQ",
    "estado": "PENDIENTE",
    "fecha_evento_utc": datetime.now(timezone.utc).isoformat(),
}

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

resultado = producer.send(
    TOPIC, key=evento["orden_id"], value=evento
).get(timeout=10)

print(f"Orden publicada: {evento['orden_id']}")
print(f"Topic: {resultado.topic} | Particion: {resultado.partition} | Offset: {resultado.offset}")
print(json.dumps(evento, indent=2, ensure_ascii=False))
producer.close()