from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, max, min, round,
    to_date, to_timestamp, window
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "aireiq-lecturas"

RUTA_GOLD = "/opt/aireiq/data/gold/kpis_5min"
CHECKPOINT_GOLD = "/opt/aireiq/data/checkpoints/gold_kpis_5min"
CHECKPOINT_CONSOLA = "/opt/aireiq/data/checkpoints/consola_kpis_5min"

schema_lectura = StructType([
    StructField("dispositivo", StringType(), True),
    StructField("temperatura_c", DoubleType(), True),
    StructField("humedad_pct", DoubleType(), True),
    StructField("presion_hpa", DoubleType(), True),
    StructField("gas_ohms", DoubleType(), True),
    StructField("altura_m", DoubleType(), True),
    StructField("iaq_estimado", DoubleType(), True),
    StructField("eco2_estimado_ppm", DoubleType(), True),
    StructField("voc_indice_estimado", DoubleType(), True),
    StructField("calidad_aire", StringType(), True),
    StructField("created_at", StringType(), True)
])

spark = (
    SparkSession.builder
    .appName("AireIQ-KPIs-Streaming")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

eventos = (
    kafka_stream
    .select(from_json(col("value").cast("string"), schema_lectura).alias("lectura"))
    .select("lectura.*")
    .filter(col("dispositivo").isNotNull())
    .withColumn("event_time", to_timestamp("created_at"))
    .filter(col("event_time").isNotNull())
)

kpis = (
    eventos
    .withWatermark("event_time", "1 minute")
    .groupBy(
        window("event_time", "5 minutes").alias("ventana"),
        col("dispositivo"),
        col("calidad_aire")
    )
    .agg(
        count("*").alias("cantidad_lecturas"),
        round(avg("temperatura_c"), 2).alias("temperatura_promedio_c"),
        round(avg("humedad_pct"), 2).alias("humedad_promedio_pct"),
        round(avg("presion_hpa"), 2).alias("presion_promedio_hpa"),
        round(avg("iaq_estimado"), 2).alias("iaq_promedio_estimado"),
        round(avg("eco2_estimado_ppm"), 2).alias("eco2_promedio_estimado_ppm"),
        round(min("temperatura_c"), 2).alias("temperatura_min_c"),
        round(max("temperatura_c"), 2).alias("temperatura_max_c")
    )
    .select(
        col("ventana.start").alias("ventana_inicio"),
        col("ventana.end").alias("ventana_fin"),
        "dispositivo",
        "calidad_aire",
        "cantidad_lecturas",
        "temperatura_promedio_c",
        "humedad_promedio_pct",
        "presion_promedio_hpa",
        "iaq_promedio_estimado",
        "eco2_promedio_estimado_ppm",
        "temperatura_min_c",
        "temperatura_max_c"
    )
    .withColumn("fecha_ventana", to_date("ventana_inicio"))
)

consulta_consola = (
    kpis.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_CONSOLA)
    .start()
)

consulta_gold = (
    kpis.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", RUTA_GOLD)
    .option("checkpointLocation", CHECKPOINT_GOLD)
    .partitionBy("fecha_ventana")
    .start()
)

print("KPIs Streaming activos: ventanas de 5 minutos + watermark de 1 minuto")
spark.streams.awaitAnyTermination()