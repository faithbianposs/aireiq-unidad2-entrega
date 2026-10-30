# AireIQ Unidad 2 — Eventos y Streaming

Proyecto de Big Data e IoT para monitorear la calidad ambiental con un ESP32, un sensor BME680, Apache Kafka y procesamiento en tiempo real.

## Objetivo

Capturar mediciones reales del dispositivo AireIQ, procesarlas como eventos y utilizarlas para indicadores, observabilidad e inferencia de temperatura.

## Dispositivo y métricas

El dispositivo `ESP32_AireIQ` registra temperatura, humedad, presión, resistencia de gas, altura y calidad del aire. También envía IAQ, eCO₂ y un índice VOC estimados.

> IAQ y eCO₂ son estimaciones basadas en el BME680; no equivalen a mediciones de sensores dedicados de CO₂.

## Arquitectura implementada

El ESP32 envía lecturas por MQTT. Un puente las publica en Kafka, en el topic `aireiq-lecturas`.

Desde Kafka se utilizan dos rutas:

- **Analítica:** Spark Structured Streaming guarda las lecturas Raw y calcula KPI por ventanas de 5 minutos en Parquet Gold.
- **Visualización operativa:** un consumidor guarda las lecturas en PostgreSQL y Grafana muestra métricas, latencia e inferencia.

Los KPI del dashboard se calculan sobre PostgreSQL. Los KPI Gold calculados por Spark se conservan por separado en Parquet.

## Topics Kafka

| Topic | Tipo | Particiones | Uso |
|---|---|---:|---|
| `aireiq-lecturas` | IoT | 3 | Mediciones del ESP32 |
| `aireiq-alertas` | Operacional | 1 | Alertas de calidad del aire |
| `aireiq-ordenes-inspeccion` | Negocio | 1 | Órdenes de inspección ambiental |

## Contratos de eventos

- `contracts/lectura_aire.v1.json`
- `contracts/alerta.calidad_aire.v1.json`
- `contracts/inspeccion.creada.v1.json`

El evento de negocio `inspeccion.creada` incluye un identificador único de orden, dispositivo, motivo, solicitante, estado y fecha UTC. Su productor y consumidor se encuentran en `s06-pruebas/app/`.

## Procesamiento con Spark

- `spark-streaming/app/stream_lecturas.py`: consume Kafka y guarda lecturas Raw en Parquet.
- `spark-streaming/app/stream_kpis.py`: agrupa por ventanas de 5 minutos, usa un watermark de 1 minuto y guarda KPI Gold en Parquet.
- Ambas consultas emplean directorios de checkpoint para conservar el progreso.

Los archivos generados se guardan bajo `spark-streaming/data/` y no se incluyen en Git.

## Inferencia de temperatura

`spark-streaming/app/modelo_temperatura.py` evalúa una predicción de temperatura a una lectura de distancia con separación cronológica entre entrenamiento y prueba.

En la prueba realizada con 850 filas útiles:

| Método | MAE | RMSE |
|---|---:|---:|
| Regresión lineal Spark ML | 0.0509 °C | 0.0878 °C |
| Lectura anterior como predicción | 0.0112 °C | 0.0200 °C |

La lectura anterior obtuvo menor error en esta prueba. Por ello, el panel de Grafana muestra esa inferencia calculada sobre las lecturas de PostgreSQL. El modelo de Spark y sus predicciones se guardan por separado en `spark-streaming/data/bi_ml/`.

## Dashboard y observabilidad

Grafana está disponible localmente en `http://localhost:43000`. El dashboard **AireIQ — Monitoreo ambiental en tiempo real** incluye temperatura real y predicha, KPI de temperatura por ventana, lecturas recibidas cada 5 minutos, segundos sin nuevas lecturas y latencia desde Kafka hasta PostgreSQL.

PostgreSQL conserva la partición y el offset de Kafka junto con cada lectura, lo que permite identificar el origen de los registros.

## Ejecución

Ejecutar desde la raíz del repositorio, con Docker Desktop iniciado:

```bat
docker compose -f kafka\compose.yml up -d
docker compose -f s06-pruebas\compose.yml up -d --build
docker compose -f spark-streaming\compose.yml up -d --build
docker compose --env-file .env -f observability\compose.example.yml up -d --build
```

Para probar una nueva orden de inspección, iniciar el consumidor en una terminal:

```bat
docker compose -f s06-pruebas\compose.yml exec python-kafka python -u /app/consumer_inspecciones.py
```

Después, publicar una orden desde otra terminal:

```bat
docker compose -f s06-pruebas\compose.yml exec python-kafka python /app/producer_inspeccion.py
```

El identificador de la orden y el offset deben coincidir en el productor y el consumidor. El consumidor espera eventos nuevos; se puede detener con `Ctrl + C`.

## Documentación adicional

- `docs/s06-kafka.md`: Kafka y pruebas de eventos.
- `docs/s07-iot.md`: dispositivo e integración IoT.
- `docs/s10-spark-streaming.md`: procesamiento con Spark.
- `costs/estimacion_costos.md`: almacenamiento, recursos y escenarios de crecimiento.

## Estado

El flujo IoT → MQTT → Kafka, la persistencia Raw/Gold con Spark, el evento de negocio, el dashboard y la evaluación de inferencia se han probado localmente. Para la entrega aún corresponde reunir las evidencias finales, guardar una copia exportable del dashboard y preparar la demostración.