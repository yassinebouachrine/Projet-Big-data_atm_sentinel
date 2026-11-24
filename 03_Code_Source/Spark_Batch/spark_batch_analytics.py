#!/usr/bin/env python3
"""
TP6 - Mission 2 : Traitements Batch Distribués avec PySpark
Projet : ATM Sentinel
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    date_format, month, hour, stddev, abs as _abs, dense_rank
)
from pyspark.sql.window import Window

# 1. Initialisation de la session Spark
spark = SparkSession.builder \
    .appName("TP6-ATM-Batch-Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

CSV_PATH = os.path.expanduser("~/TP6_big_data/01_data/batch/atm_historical_batch.csv")
OUTPUT_DIR = os.path.expanduser("~/TP6_big_data/02_batch/results")

print("\n" + "="*80)
print("=== SPARK BATCH ANALYTICS - ATM SENTINEL ===")
print("="*80)

# 2. Chargement du dataset historique
df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .csv(CSV_PATH)

print(f"\n[OK] Dataset historique chargé : {df.count()} enregistrements.")
df.printSchema()

# ==============================================================================
# ANALYSE 1 : AGRÉGATION STATISTIQUE ET MÉTIER
# ==============================================================================
print("\n" + "-"*80)
print("--- ANALYSE 1 : BILAN FINANCIER ET STATISTIQUES PAR ÉTAT ET PAR TYPE ---")
print("-"*80)

an1_state_type = df.groupBy("state", "transaction_type") \
    .agg(
        count("transaction_id").alias("nb_transactions"),
        _sum("amount").alias("volume_total_ngn"),
        avg("amount").alias("montant_moyen_ngn"),
        _max("amount").alias("montant_max_ngn")
    ) \
    .orderBy("state", col("volume_total_ngn").desc())

an1_state_type.show(20, truncate=False)

# Export du Top 5 ATM sur les retraits
print("\n[+] Top 5 des GAB les plus sollicités pour les retraits (Withdrawal) :")
top_atms = df.filter(col("transaction_type") == "Withdrawal") \
    .groupBy("atm_id", "state", "city") \
    .agg(_sum("amount").alias("total_retire"), count("*").alias("nb_retraits")) \
    .orderBy(col("total_retire").desc())

top_atms.show(5, truncate=False)

# Save result 1
an1_state_type.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(OUTPUT_DIR, "analyse1_agregations"))


# ==============================================================================
# ANALYSE 2 : ANALYSE TEMPORELLE ET CLASSEMENT PAR FENÊTRE (WINDOW FUNCTIONS)
# ==============================================================================
print("\n" + "-"*80)
print("--- ANALYSE 2 : TENDANCE MENSUELLE & FENÊTRAGE TOP 3 ATM PAR ÉTAT ---")
print("-"*80)

# Enrichissement avec le mois
df_time = df.withColumn("month", month(col("start_time")))

# Calcul du rang des ATMs par État selon leur volume d'activité via Window Spec
window_spec = Window.partitionBy("state").orderBy(col("total_volume").desc())

atm_ranked = df_time.groupBy("state", "atm_id") \
    .agg(_sum("amount").alias("total_volume"), count("*").alias("total_tx")) \
    .withColumn("rang_dans_etat", dense_rank().over(window_spec)) \
    .filter(col("rang_dans_etat") <= 3) \
    .orderBy("state", "rang_dans_etat")

atm_ranked.show(15, truncate=False)

# Save result 2
atm_ranked.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(OUTPUT_DIR, "analyse2_temporelle_window"))


# ==============================================================================
# ANALYSE 3 : QUALITÉ DES DONNÉES ET DÉTECTION D'ANOMALIES (Z-SCORE)
# ==============================================================================
print("\n" + "-"*80)
print("--- ANALYSE 3 : DÉTECTION D'ANOMALIES FINANCIÈRES PAR SCORE Z ---")
print("-"*80)

# Calcul de la moyenne et écart-type des montants par occupation
occ_stats = df.filter(col("amount") > 0) \
    .groupBy("occupation") \
    .agg(
        avg("amount").alias("avg_amount"),
        stddev("amount").alias("stddev_amount")
    )

df_anomalies = df.join(occ_stats, on="occupation") \
    .withColumn("z_score", (col("amount") - col("avg_amount")) / col("stddev_amount"))

# Filtrer les transactions atypiques (Z-Score > 1.5)
anomalies = df_anomalies.filter(col("z_score") > 1.5) \
    .select("transaction_id", "cardholder_id", "occupation", "transaction_type", "amount", "avg_amount", "z_score") \
    .orderBy(col("z_score").desc())

print(f"[!] Nombre de transactions financières atypiques détectées (Z-Score > 1.5) : {anomalies.count()}")
anomalies.show(10, truncate=False)

# Save result 3
anomalies.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(OUTPUT_DIR, "analyse3_anomalies_zscore"))

print("\n=== TOUS LES TRAITEMENTS BATCH SPARK SE SONT EXÉCUTÉS AVEC SUCCÈS ===")
spark.stop()
