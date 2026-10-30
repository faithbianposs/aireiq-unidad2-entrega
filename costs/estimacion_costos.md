\# AireIQ — costos y criterios de escalado (Unidad II)



\## Base medida



\- PostgreSQL: 2879 lecturas; tabla e índices: 1 032 192 bytes; aproximadamente 358,52 bytes por lectura.

\- Ingesta observada: 18–19 lecturas por ventana de 5 minutos con un sensor.

\- Latencia Kafka → PostgreSQL: media 8,1 ms, mínimo 3,7 ms y máximo 59,9 ms en 112 lecturas recientes.

\- Memoria de los contenedores: aproximadamente 1,91 GiB en una instantánea con Spark inactivo. Se debe repetir la medición con Spark procesando antes de dimensionar una instalación permanente.



\## Proyección de PostgreSQL



Supuestos: 19 lecturas cada 5 minutos de forma continua, meses de 30 días y 358,52 bytes por lectura. Es una aproximación del tamaño de la tabla y sus índices; no incluye Kafka, Parquet, WAL ni copias de seguridad.



| Sensores equivalentes | Lecturas por mes | PostgreSQL adicional por mes |
| --- | ---: | ---: |
| 1 | 164 160 | 58,85 MB |
| 10 | 1 641 600 | 588,55 MB |
| 100 | 16 416 000 | 5,89 GB |



\## Escenario ilustrativo de operación local



Si la laptop consumiera en promedio 60 W funcionando 24 horas al día, usaría 43,2 kWh en 30 días. Con una tarifa supuesta de S/ 0,80 por kWh, el costo eléctrico sería S/ 34,56 al mes. Estos dos valores son supuestos para comparar escenarios; el costo real requiere medir el consumo eléctrico y consultar el recibo. No incluye internet, depreciación del equipo ni mantenimiento.



\## Decisiones de escalado



\- Vigilar las lecturas por ventana de 5 minutos, los segundos sin nuevas lecturas y la latencia Kafka → PostgreSQL en Grafana.

\- Investigar si la frescura supera 90 segundos o si la latencia aumenta de forma sostenida mientras cae el volumen recibido.

\- Al agregar sensores, volver a medir CPU y RAM con ambos procesos Spark activos; la medición actual de Spark corresponde a un contenedor inactivo.

\- Definir retención para Kafka, Raw y Gold, y medir su espacio junto con PostgreSQL antes de ampliar a 10 o 100 sensores.

\- Separar Kafka, Spark y PostgreSQL en recursos dedicados si la memoria o el disco de la laptop se vuelven el límite.
