\# S07 — Ingesta de eventos IoT en tiempo real



\## Objetivo



Transmitir mediciones ambientales reales generadas por el dispositivo AireIQ/Snorlax hacia Apache Kafka mediante MQTT y un puente Docker.



\## Arquitectura validada



ESP32 con BME680 → MQTT EMQX → puente MQTT a Kafka en Docker → topic Kafka `aireiq-lecturas`.



\## Dispositivo y datos



\- Dispositivo: `ESP32\_AireIQ`

\- Sensor: BME680

\- Topic MQTT: `aireiq/frank-ramos/lecturas/v1`

\- Broker MQTT: `broker.emqx.io:1883`

\- Topic Kafka: `aireiq-lecturas`

\- Key Kafka: `ESP32\_AireIQ`



\## Campos transmitidos



\- `temperatura\_c`

\- `humedad\_pct`

\- `presion\_hpa`

\- `gas\_ohms`

\- `altura\_m`

\- `iaq\_estimado`

\- `eco2\_estimado\_ppm`

\- `voc\_indice\_estimado`

\- `calidad\_aire`

\- `created\_at`



> Los campos `iaq\_estimado`, `eco2\_estimado\_ppm` y `voc\_indice\_estimado` son estimaciones derivadas del BME680; no corresponden a un sensor CO₂ dedicado.



\## Evidencia de ejecución real



1\. El ESP32 se conectó al Wi‑Fi y publicó lecturas MQTT.

2\. El puente Docker recibió cada lectura y la reenvió al topic Kafka.

3\. Kafka UI mostró lecturas reales en `aireiq-lecturas`.

4\. La lectura validada usa key `ESP32\_AireIQ`, partición `1` y contiene el JSON completo con las métricas reales del dispositivo.



\## Separación de responsabilidades



\- `aireiq-lecturas`: telemetría ambiental real del ESP32.

\- `aireiq-alertas`: eventos de prueba y alertas operacionales.

\- Supabase permanece como almacenamiento histórico directo del dispositivo.
