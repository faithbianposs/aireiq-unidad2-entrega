import json

from kafka import KafkaConsumer

TOPIC = "aireiq-alertas"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="kafka:9092",
    group_id="aireiq-s06-python",
    auto_offset_reset="latest",
    enable_auto_commit=True,
)

print(f"Esperando eventos nuevos en el topic: {TOPIC}")

for mensaje in consumer:
    try:
        evento = json.loads(mensaje.value.decode("utf-8"))
    except json.JSONDecodeError:
        evento = {"contenido_no_json": mensaje.value.decode("utf-8", errors="replace")}

    print("-" * 50)
    print(f"Topic: {mensaje.topic}")
    print(f"Partición: {mensaje.partition}")
    print(f"Offset: {mensaje.offset}")
    print(f"Key: {mensaje.key.decode('utf-8') if mensaje.key else None}")
    print("Evento:")
    print(json.dumps(evento, indent=2, ensure_ascii=False))