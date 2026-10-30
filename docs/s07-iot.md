---
layout: default
---

# S07 — Ingesta de eventos IoT en tiempo real



## Objetivo



Transmitir mediciones ambientales reales generadas por el dispositivo AireIQ/Snorlax hacia Apache Kafka mediante MQTT y un puente Docker.



## Arquitectura validada



ESP32 con BME680 → MQTT EMQX → puente MQTT a Kafka en Docker → topic Kafka `aireiq-lecturas`.



## Dispositivo y datos



- Dispositivo: `ESP32_AireIQ`

- Sensor: BME680

- Topic MQTT: `aireiq/frank-ramos/lecturas/v1`

- Broker MQTT: `broker.emqx.io:1883`

- Topic Kafka: `aireiq-lecturas`

- Key Kafka: `ESP32_AireIQ`



## Campos transmitidos



- `temperatura_c`

- `humedad_pct`

- `presion_hpa`

- `gas_ohms`

- `altura_m`

- `iaq_estimado`

- `eco2_estimado_ppm`

- `voc_indice_estimado`

- `calidad_aire`

- `created_at`



> Los campos `iaq_estimado`, `eco2_estimado_ppm` y `voc_indice_estimado` son estimaciones derivadas del BME680; no corresponden a un sensor CO₂ dedicado.



## Evidencia de ejecución real



1\. El ESP32 se conectó al Wi‑Fi y publicó lecturas MQTT.

2\. El puente Docker recibió cada lectura y la reenvió al topic Kafka.

3\. Kafka UI mostró lecturas reales en `aireiq-lecturas`.

4\. La lectura validada usa key `ESP32_AireIQ`, partición `1` y contiene el JSON completo con las métricas reales del dispositivo.



## Separación de responsabilidades



- `aireiq-lecturas`: telemetría ambiental real del ESP32.

- `aireiq-alertas`: eventos de prueba y alertas operacionales.

- Supabase permanece como almacenamiento histórico directo del dispositivo.
