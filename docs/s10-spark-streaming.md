---
layout: default
---

# S10 — Spark Structured Streaming y KPIs temporales

## Objetivo

Consumir telemetría real de AireIQ desde Kafka, guardar las lecturas en Parquet y calcular indicadores por ventanas temporales.

## Flujo validado

ESP32/BME680 → MQTT EMQX → puente Docker → Kafka `aireiq-lecturas` → Spark Structured Streaming → Parquet Raw y Gold.

## Procesamiento Raw

El programa `spark-streaming/app/stream_lecturas.py`:

- Consume `aireiq-lecturas` desde Kafka.
- Convierte el JSON a un esquema tipado.
- Conserva las métricas del ESP32.
- Guarda lecturas en `spark-streaming/data/raw/aireiq_lecturas`.
- Usa checkpoints para recuperar el procesamiento tras un reinicio.

## KPIs Gold

El programa `spark-streaming/app/stream_kpis.py`:

- Agrupa lecturas en ventanas de 5 minutos.
- Usa un watermark de 1 minuto sobre `event_time`.
- Calcula cantidad de lecturas y promedios de temperatura, humedad, presión, IAQ y eCO₂ estimado.
- Calcula temperatura mínima y máxima.
- Guarda resultados en `spark-streaming/data/gold/kpis_5min`, particionados por `fecha_ventana`.

## Ejecutar KPIs en tiempo real

Desde la raíz del repositorio, con Kafka y el contenedor Spark encendidos:

```bat
docker compose -f spark-streaming\compose.yml exec spark-streaming /opt/spark/bin/spark-submit --master local[2] --driver-memory 768m --conf spark.jars.ivy=/tmp/aireiq-ivy --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /opt/aireiq/app/stream_kpis.py
```

Dejar abierta esa ventana mientras se reciben lecturas. Para detener el proceso, presionar `Ctrl+C`.

## Evidencia validada

La consola mostró ventanas reales de `ESP32_AireIQ`. También se comprobaron archivos Parquet recientes en Gold, bajo la partición `fecha_ventana=2026-10-30`.

## Recuperación ante reinicio

Los directorios `checkpoints/` almacenan el progreso y el estado de Spark. Al reiniciar el proceso con los mismos checkpoints, continúa desde el último microbatch confirmado.