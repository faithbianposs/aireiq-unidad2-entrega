import json

from kafka import KafkaConsumer, TopicPartition

TOPIC = "aireiq-ordenes-inspeccion"

consumer = KafkaConsumer(
    bootstrap_servers="kafka:9092",
    enable_auto_commit=False,
    consumer_timeout_ms=60000,
)

particion = TopicPartition(TOPIC, 0)
consumer.assign([particion])
consumer.seek_to_end(particion)

print("Consumidor listo. Esperando una NUEVA orden...", flush=True)

try:
    for mensaje in consumer:
        evento = json.loads(mensaje.value.decode("utf-8"))
        print(f"Topic: {mensaje.topic} | Particion: {mensaje.partition} | Offset: {mensaje.offset}")
        print(f"Key: {mensaje.key.decode('utf-8') if mensaje.key else None}")
        print(json.dumps(evento, indent=2, ensure_ascii=False))
        break
finally:
    consumer.close()