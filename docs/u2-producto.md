\# Producto de Unidad II — AireIQ



\## 1. Problema y alcance



AireIQ monitorea las condiciones de un ambiente mediante un ESP32 y un sensor BME680. La pregunta que guía esta dimensión es: ¿cómo recibir las lecturas mientras ocurren, detectar interrupciones del flujo y convertirlas en indicadores útiles para supervisar el ambiente?



En la Unidad I se analizaron lecturas históricas. En la Unidad II se añadió ingesta de eventos, procesamiento en streaming, observabilidad e inferencia de temperatura. El sistema usa mediciones del dispositivo `ESP32\_AireIQ`; IAQ y eCO₂ son estimaciones derivadas del BME680, no mediciones de sensores dedicados.



\## 2. Arquitectura implementada



\- \*\*Telemetría:\*\* ESP32/BME680 → MQTT → puente MQTT-Kafka → topic `aireiq-lecturas`.

\- \*\*Evento empresarial:\*\* productor Python → topic `aireiq-ordenes-inspeccion` → consumidor Python.

\- \*\*Procesamiento:\*\* Kafka → Spark Structured Streaming → Parquet Raw y KPIs Gold.

\- \*\*Consulta operativa:\*\* Kafka → consumidor PostgreSQL → Grafana.

\- \*\*Analítica:\*\* modelo de regresión y baseline de persistencia evaluados con datos de AireIQ; predicción por persistencia mostrada junto a los KPIs en Grafana.



PostgreSQL alimenta los paneles de Grafana. Los archivos Gold de Spark se comprobaron por separado en Parquet; el panel SQL de KPIs no lee directamente esos archivos.



\## 3. Capacidades y evidencia por sesión



| Sesión | Implementación AireIQ | Evidencia |

| --- | --- | --- |

| S6 — Evento empresarial | Órdenes de inspección con `orden\_id`, dispositivo, motivo, solicitante, estado y fecha. Topic, productor, consumidor y contrato `inspeccion.creada.v1.json`. | Productor y consumidor mostraron la misma orden con partición 0 y offset 3. |

| S7 — IoT | Lecturas reales del ESP32 enviadas por MQTT y publicadas en `aireiq-lecturas` mediante el puente. | Contrato de lectura, flujo Kafka y lecturas persistidas en Raw y PostgreSQL. |

| S8 — Spark Streaming | Lecturas Raw y agregaciones Gold en ventanas de 5 minutos, watermark de 1 minuto y checkpoints independientes. | Batch con lecturas reales y archivos Parquet recientes en Gold. |

| S9 — Observabilidad y costos | Dashboard de Grafana con volumen, frescura, latencia y variables ambientales; estimación basada en medidas del sistema. | Panel operativo y `costs/estimacion\_costos.md`. |

| S10 — Series de tiempo | Regresión de temperatura comparada con persistencia mediante una separación cronológica de entrenamiento y prueba. | MAE y RMSE de ambos métodos; predicción a una lectura en Grafana. |

| S11 — BI/ML | Temperatura, humedad, IAQ, eCO₂, KPI por cinco minutos y predicción en el mismo dashboard. | Captura y exportación JSON del tablero. |



\## 4. Resultados medidos



\- En una comprobación, Raw contenía 864 lecturas con 864 tiempos de evento distintos.

\- Spark mostró una ventana de cinco minutos con 7 lecturas del dispositivo. Después se verificaron archivos Parquet recientes en `fecha\_ventana=2026-10-30`.

\- PostgreSQL almacenaba 1176 lecturas; la tabla y sus índices ocupaban 472 kB, aproximadamente 411 bytes por lectura.

\- En 112 lecturas recientes, la latencia Kafka → PostgreSQL tuvo una media de 8,1 ms, un mínimo de 3,7 ms y un máximo de 59,9 ms.

\- El modelo utilizó 850 filas útiles: 680 para entrenamiento y 170 para prueba, en orden temporal.



| Método | MAE de temperatura | RMSE de temperatura |

| --- | ---: | ---: |

| Regresión | 0,0509 °C | 0,0878 °C |

| Persistencia | 0,0112 °C | 0,0200 °C |



La persistencia obtuvo menor error en esta prueba. Por ello, el panel «Temperatura real vs. predicción a una lectura» muestra esa predicción. No se presenta la curva del panel como si fuera la salida del modelo de regresión.



\## 5. Costos, decisiones y límites



La estimación de escalado usa 19 lecturas cada cinco minutos y 411 bytes por lectura en PostgreSQL. Proyecta aproximadamente 67,5 MB mensuales para un sensor, 674,7 MB para diez y 6,75 GB para cien, sin incluir Kafka, Parquet, WAL ni copias de seguridad. El documento de costos separa los valores medidos de los supuestos de consumo eléctrico.



El watermark de un minuto limita cuánto tiempo espera Spark por eventos tardíos. Los checkpoints conservan el progreso de las consultas para reanudarlas después de una interrupción. Los procesos Spark se detienen cuando no se realiza la demostración para cuidar la memoria de la laptop; deben iniciarse de nuevo para la demo en vivo.



El dashboard muestra frescura, volumen y latencia del flujo operativo. Todavía no incluye un panel específico de lag de consumidores ni métricas internas de Spark. La predicción por persistencia usa la lectura previa; no se debe describir como inferencia del modelo de regresión ni como una predicción sin historial.



\## 6. Archivos de reproducción y evidencias



\- Contratos: `contracts/lectura\_aire.v1.json` y `contracts/inspeccion.creada.v1.json`.

\- Evento empresarial: `s06-pruebas/app/producer\_inspeccion.py` y `consumer\_inspecciones.py`.

\- Spark: `spark-streaming/app/stream\_lecturas.py`, `stream\_kpis.py` y `modelo\_temperatura.py`.

\- Observabilidad: `observability/compose.example.yml`, esquema PostgreSQL, consumidor y dashboard exportado en `observability/grafana/dashboards/aireiq-dashboard.json`.

\- Comando verificado para iniciar KPIs y explicación de recuperación: `docs/s10-spark-streaming.md`.

\- Estimación: `costs/estimacion\_costos.md`.

\- Capturas: `evidence/01\_dashboard\_grafana.png`, `02\_inspeccion\_productor.png`, `03\_inspeccion\_consumidor.png`, `evidence04\_spark\_gold\_ventanas.png`, `evidence05\_spark\_streaming\_vivo.png` y `evidence06\_gold\_parquet\_reciente.png`.



\## 7. Guion de verificación en vivo



1\. Mostrar una lectura nueva del ESP32 en Kafka y su actualización en Grafana.

2\. Publicar una orden de inspección y verificar su recepción con el mismo `orden\_id`.

3\. Iniciar Spark KPIs y señalar la ventana, el watermark y los checkpoints en el código y la salida.

4\. Mostrar volumen, frescura y latencia en Grafana; explicar la proyección de costos.

5\. Comparar las métricas de regresión y persistencia, y señalar la predicción junto al KPI de temperatura.
