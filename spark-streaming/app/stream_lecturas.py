from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, to_date, to_timestamp
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "aireiq-lecturas"

RUTA_RAW = "/opt/aireiq/data/raw/aireiq_lecturas"
CHECKPOINT_RAW = "/opt/aireiq/data/checkpoints/raw_aireiq_lecturas"
CHECKPOINT_CONSOLA = "/opt/aireiq/data/checkpoints/consola_aireiq_lecturas"

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
    .appName("AireIQ-Structured-Streaming")
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

lecturas = (
    kafka_stream
    .select(
        from_json(col("value").cast("string"), schema_lectura).alias("lectura"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset")
    )
    .select("lectura.*", "kafka_timestamp", "kafka_partition", "kafka_offset")
    .filter(col("dispositivo").isNotNull())
    .withColumn("event_time", to_timestamp("created_at"))
    .withColumn("event_date", to_date("event_time"))
    .withColumn("ingested_at", current_timestamp())
)

consulta_consola = (
    lecturas
    .select(
        "dispositivo", "event_time", "temperatura_c", "humedad_pct",
        "presion_hpa", "iaq_estimado", "eco2_estimado_ppm",
        "calidad_aire", "kafka_partition", "kafka_offset"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_CONSOLA)
    .start()
)

consulta_raw = (
    lecturas
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", RUTA_RAW)
    .option("checkpointLocation", CHECKPOINT_RAW)
    .partitionBy("event_date")
    .start()
)

print("Spark Streaming activo: Kafka -> consola + Parquet Raw")
spark.streams.awaitAnyTermination()