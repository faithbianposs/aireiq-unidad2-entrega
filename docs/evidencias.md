---

layout: default

title: Evidencias — AireIQ Unidad II

---



# Evidencias de AireIQ — Unidad II



Estas capturas documentan el flujo real utilizado en la evaluación. Los comandos, contratos y resultados se explican en el [producto U2](u2-producto.html).



## Dashboard operativo y BI



Grafana reúne variables ambientales, KPIs por ventanas de cinco minutos, frescura del flujo y predicción de temperatura.



![Dashboard de AireIQ en Grafana](assets/evidencias/01_dashboard_grafana.png)



## Evento empresarial en Kafka



El productor publicó una orden de inspección. El consumidor recibió la misma orden, con la partición y el offset correspondientes.



![Orden publicada por el productor](assets/evidencias/02_inspeccion_productor.png)



![Orden recibida por el consumidor](assets/evidencias/03_inspeccion_consumidor.png)



## Procesamiento con Spark



La consulta Gold mostró ventanas de cinco minutos con lecturas y temperatura promedio del dispositivo.



![KPIs Gold por ventanas](assets/evidencias/evidence04_spark_gold_ventanas.png)



Spark Structured Streaming procesó un batch nuevo mientras llegaban lecturas reales.



![Spark Streaming procesando lecturas](assets/evidencias/evidence05_spark_streaming_vivo.png)



Después se comprobaron archivos Parquet recientes en la salida Gold.



![Archivos Parquet Gold recientes](assets/evidencias/evidence06_gold_parquet_reciente.png)



[Volver al inicio](index.html)
