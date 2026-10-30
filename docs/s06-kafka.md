\# S06 — Ingesta de eventos empresariales con Kafka



\## Objetivo



Validar la publicación y el consumo de eventos operacionales mediante Apache Kafka y Python.



\## Servicios desplegados



| Servicio | Contenedor | Puerto local |

|---|---|---:|

| Kafka | `aireiq-kafka` | 49092 |

| Kafka UI | `aireiq-kafka-ui` | 48085 |

| Kafka Exporter | `aireiq-kafka-exporter` | 49308 |

| Python de pruebas | `aireiq-s06-python` | Interno Docker |



\## Topics creados



| Topic | Particiones | Propósito |

|---|---:|---|

| `aireiq-lecturas` | 3 | Telemetría IoT real del ESP32 |

| `aireiq-alertas` | 1 | Eventos operacionales de calidad de aire |



\## Prueba manual



Se produjo y consumió un evento JSON en el topic `aireiq-alertas` usando las herramientas de consola de Kafka.



\## Prueba con Python



Se implementaron los siguientes componentes:



\- `s06-pruebas/app/producer\_alertas.py`

\- `s06-pruebas/app/consumer\_alertas.py`



Resultado validado:



1\. El productor Python publicó una alerta de prueba.

2\. Kafka la registró en el topic `aireiq-alertas`.

3\. El consumidor Python recibió el evento con key `ESP32\_AireIQ`.

4\. El evento fue identificado con partición `0` y offset `1`.



\## Separación de datos



Los eventos de prueba usan el campo `"nivel": "PRUEBA"` y se mantienen en `aireiq-alertas`.



El topic `aireiq-lecturas` queda reservado exclusivamente para las mediciones ambientales reales del ESP32/BME680.
