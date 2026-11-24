#!/usr/bin/env python3
"""
TP6 - Missions 5 & 6 : Analyse de Graphes Distribuée avec PySpark
Projet : ATM Sentinel
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, lit, broadcast

# 1. Initialisation Spark
spark = SparkSession.builder \
    .appName("TP6-ATM-Graph-Analytics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

VERTICES_PATH = os.path.expanduser("~/TP6_big_data/01_data/graph/vertices.csv")
EDGES_PATH = os.path.expanduser("~/TP6_big_data/01_data/graph/edges.csv")
OUTPUT_DIR = os.path.expanduser("~/TP6_big_data/04_graph/results")

print("\n" + "="*80)
print("=== SPARK GRAPH ANALYTICS - ATM SENTINEL ===")
print("="*80)

# 2. Chargement des Sommets et des Arêtes
vertices = spark.read.option("header", "true").option("inferSchema", "true").csv(VERTICES_PATH)
edges = spark.read.option("header", "true").option("inferSchema", "true").csv(EDGES_PATH)

print(f"\n[OK] Graphe chargé avec succès :")
print(f" => Nombre de Sommets (Vertices) : {vertices.count()} (50 ATMs + 950 Cardholders)")
print(f" => Nombre d'Arêtes (Edges)     : {edges.count()} transactions de relation")

# ==============================================================================
# MISSION 6 - ANALYSE 1 : STRUCTURE & CONNECTIVITÉ (DEGRÉS DU GRAPHE)
# ==============================================================================
print("\n" + "-"*80)
print("--- ANALYSE 1 : CONNECTIVITÉ ET CENTRALITÉ DE DEGRÉ (DEGREES) ---")
print("-"*80)

# Out-Degree : Activité des clients (nombre de transactions émises)
out_degrees = edges.groupBy("src").agg(count("*").alias("out_degree"), _sum("amount").alias("total_depense"))
top_clients = out_degrees.join(vertices, out_degrees.src == vertices.id) \
    .select("id", "name", "state", "detail", "out_degree", "total_depense") \
    .orderBy(col("out_degree").desc())

print("\n[+] Top 5 des Clients les plus actifs (Out-Degree le plus élevé) :")
top_clients.show(5, truncate=False)

# In-Degree : Fréquentation des ATMs (nombre de retraits/dépôts reçus)
in_degrees = edges.groupBy("dst").agg(count("*").alias("in_degree"), _sum("amount").alias("volume_financier_recu"))
top_atms_graph = in_degrees.join(vertices, in_degrees.dst == vertices.id) \
    .select("id", "name", "state", "detail", "in_degree", "volume_financier_recu") \
    .orderBy(col("in_degree").desc())

print("\n[+] Top 5 des GAB les plus fréquentés (In-Degree le plus élevé) :")
top_atms_graph.show(5, truncate=False)


# ==============================================================================
# MISSION 6 - ANALYSE 2 : IMPORTANCE ET CENTRALITÉ (ALGORITHME PAGERANK)
# ==============================================================================
print("\n" + "-"*80)
print("--- ANALYSE 2 : PAGERANK DISTRIBUÉ (DAMPING FACTOR = 0.85, 10 ITÉRATIONS) ---")
print("-"*80)

# Implémentation du PageRank distribué sur DataFrames PySpark
num_vertices = vertices.count()
initial_pagerank = 1.0 / num_vertices
damping_factor = 0.85
num_iterations = 10

# DataFrame initial des rangs
ranks = vertices.select(col("id").alias("vertex_id"), lit(initial_pagerank).alias("rank"))

# Calcul du nombre de liens sortants par sommet
out_counts = edges.groupBy("src").agg(count("*").alias("out_count"))

# Liens normalisés avec poids
links = edges.join(out_counts, "src").select("src", "dst", "out_count")

# Iterations PageRank
for iteration in range(1, num_iterations + 1):
    # Calcul des contributions de rang distribuées
    contributions = links.join(ranks, links.src == ranks.vertex_id) \
        .select(col("dst").alias("vertex_id"), (col("rank") / col("out_count")).alias("contrib"))
    
    # Agrégation des contributions
    sum_contribs = contributions.groupBy("vertex_id").agg(_sum("contrib").alias("sum_contrib"))
    
    # Mise à jour de la formule PageRank : PR(v) = (1 - d)/N + d * sum(PR(u)/Out(u))
    ranks = vertices.select(col("id").alias("vertex_id")) \
        .join(sum_contribs, "vertex_id", "left") \
        .na.fill(0.0) \
        .select(
            col("vertex_id"),
            ((lit(1.0 - damping_factor) / lit(num_vertices)) + (lit(damping_factor) * col("sum_contrib"))).alias("rank")
        )

# Résultat final PageRank joint aux métadonnées des sommets
pagerank_results = ranks.join(vertices, ranks.vertex_id == vertices.id) \
    .select("id", "name", "vertex_type", "state", "detail", col("rank").alias("pagerank_score")) \
    .orderBy(col("pagerank_score").desc())

print(f"\n[+] Top 10 des Sommets (GAB & Clients) les plus Centraux (PageRank Score) :")
pagerank_results.show(10, truncate=False)

# Export des résultats
os.makedirs(OUTPUT_DIR, exist_ok=True)
pagerank_results.coalesce(1).write.mode("overwrite").option("header", "true").csv(os.path.join(OUTPUT_DIR, "pagerank_results"))

print("\n=== ANALYSE DE GRAPHES TERMINÉE AVEC SUCCÈS ===")
spark.stop()
