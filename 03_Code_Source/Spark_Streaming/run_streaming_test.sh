#!/bin/bash
echo "================================================================="
echo "=== DÉMARRAGE TEST MISSIONS 3 & 4 : STREAMING & RÉSILIENCE ==="
echo "================================================================="

rm -rf ~/TP6_big_data/01_data/streaming/incoming/*
rm -rf ~/TP6_big_data/03_streaming/checkpoint/*
rm -rf ~/TP6_big_data/03_streaming/output_valid/*

echo "[1/3] Lancement du Générateur de Flux en arrière-plan..."
python3 ~/TP6_big_data/01_data/streaming/stream_generator.py > ~/TP6_big_data/05_preuves/generator.log 2>&1 &
GEN_PID=$!
echo "[OK] Générateur démarré avec PID $GEN_PID"

echo "[2/3] Attente de 4 secondes..."
sleep 4

echo "[3/3] Lancement Spark Structured Streaming (Exécution initial 20 sec)..."
timeout 20 spark-submit ~/TP6_big_data/03_streaming/spark_streaming_analytics.py | tee ~/TP6_big_data/05_preuves/streaming_run1.log

echo ""
echo "================================================================="
echo "=== TEST DE RÉSILIENCE : ARRET ET REDÉMARRAGE AVEC CHECKPOINT ==="
echo "================================================================="
echo "[+] Contenu des checkpoints créés :"
ls -lh ~/TP6_big_data/03_streaming/checkpoint/

echo ""
echo "[+] Redémarrage contrôlé (Reprise du flux sur Checkpoint - 15 sec)..."
timeout 15 spark-submit ~/TP6_big_data/03_streaming/spark_streaming_analytics.py | tee ~/TP6_big_data/05_preuves/streaming_run2.log

echo ""
echo "================================================================="
echo "=== BILAN DU STREAMING ET VÉRIFICATION DES RÉSULTATS ==="
echo "================================================================="
echo "Nombre de fichiers JSON générés dans incoming/ : $(ls ~/TP6_big_data/01_data/streaming/incoming/ | wc -l)"
echo "Vérification des événements valides sauvegardés en Parquet :"

cat > /tmp/check_parquet.py << 'PYEOF'
import os
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CheckParquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet(os.path.expanduser("~/TP6_big_data/03_streaming/output_valid"))
print("\n" + "="*60)
print(f"=> TOTAL ÉVÉNEMENTS VALIDES SAUVEGARDÉS VIA STREAMING : {df.count()}")
print("="*60)
df.show(5, truncate=False)
spark.stop()
PYEOF

spark-submit /tmp/check_parquet.py
