#!/usr/bin/env python3
"""
TP6 - Missions 3 & 4 : Spark Structured Streaming & Résilience
Projet : ATM Sentinel
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
from pyspark.sql.functions import (
    col, window, sum as _sum, count, when
)

# 1. Initialisation Spark Session
spark = SparkSession.builder \
    .appName("TP6-ATM-Structured-Streaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BASE_DIR = os.path.expanduser("~/TP6_big_data/01_data/streaming")
INCOMING_DIR = os.path.join(BASE_DIR, "incoming")
CHECKPOINT_DIR = os.path.expanduser("~/TP6_big_data/03_streaming/checkpoint")
VALID_OUTPUT_DIR = os.path.expanduser("~/TP6_big_data/03_streaming/output_valid")

os.makedirs(INCOMING_DIR, exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
os.makedirs(VALID_OUTPUT_DIR, exist_ok=True)

print("\n" + "="*80)
print("=== SPARK STRUCTURED STREAMING - ATM SENTINEL ===")
print(f"Écoute du répertoire : {INCOMING_DIR}")
print(f"Dossier Checkpoint  : {CHECKPOINT_DIR}")
print("="*80)

# 2. Définition du Schéma Strict pour le Flux JSON
json_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("atm_id", StringType(), True),
    StructField("cardholder_id", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

# 3. Lecture en Continu depuis le Répertoire incoming/
raw_stream = spark.readStream \
    .schema(json_schema) \
    .option("maxFilesPerTrigger", 2) \
    .json(INCOMING_DIR)

# Conversion du champ event_time en Timestamp
parsed_stream = raw_stream \
    .withColumn("event_timestamp", col("event_time").cast(TimestampType()))

# 4. Filtrage des Événements Valides vs Invalides
valid_events = parsed_stream.filter(
    col("atm_id").isNotNull() & (col("amount") >= 0)
)

# 5. Agrégation Temporelle avec Event-Time, Watermark et Windowing
windowed_aggregates = valid_events \
    .withWatermark("event_timestamp", "10 minutes") \
    .groupBy(
        window(col("event_timestamp"), "5 minutes", "1 minute"),
        col("transaction_type")
    ) \
    .agg(
        count("transaction_id").alias("total_transactions"),
        _sum("amount").alias("volume_financier_total")
    )

# 6. Sinks Streaming

# Sink 1 : Console (Vue agrégée mise à jour)
query_console = windowed_aggregates.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "console")) \
    .trigger(processingTime="2 seconds") \
    .start()

# Sink 2 : Stockage Parquet Append pour la persistance
query_file = valid_events.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", VALID_OUTPUT_DIR) \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "parquet")) \
    .trigger(processingTime="2 seconds") \
    .start()

print("\n[+] Flux Streaming démarré en tâche de fond. En attente de micro-lots...\n")

spark.streams.awaitAnyTermination()
