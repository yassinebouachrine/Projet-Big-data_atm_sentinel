# ATM Sentinel - Plateforme Distribuée Big Data ELK & Spark
**Projet Intégrateur Final - Master SDBDIA (ENSIASD Taroudant)**  
**Binôme :** Yassine BOUACHRINE & Lahcen AHTAT  
**Encadrant :** Pr. L. KARBIL (Année Universitaire 2026/2027)

---

## 1. Description du Projet
Plateforme unifiée de surveillance, de détection de fraudes bancaires (*Cash-Out Fraud*, *Impossible Travel*) et d'analyse prédictive de flux financiers sur un réseau de Guichets Automatiques Bancaires (GAB / ATM).

## 2. Architecture Distribuée (4 Nœuds)
- **VM1 (192.168.1.138) :** Elasticsearch 8.19.23 (Data Node)
- **VM2 (192.168.1.113) :** Elasticsearch 8.19.23 (Master Node Élu) + Logstash 8.19.23 (Moteur d'ingestion & QA)
- **VM3 (192.168.1.127) :** Elasticsearch 8.19.23 (Data Node) + Kibana 8.19.23 (Dashboards interactifs & KPIs)
- **VM4 (192.168.1.124) :** Apache Spark 3.5.3 (Java 17, Scala 2.12) - Moteur de calculs Batch, Streaming et GraphX

## 3. Structure des Livrables

PROJET_FINAL_ELK_SPARK_BOUACHRINE_AHTAT/
├── 01_Rapport/
│ └── Rapport_Final.pdf
├── 02_Video/
│ └── Demonstration_Projet.mp4
├── 03_Code_Source/
│ ├── Data_Generation/
│ ├── Logstash/
│ ├── Elasticsearch/
│ ├── Spark_Batch/
│ ├── Spark_Streaming/
│ └── GraphX/
├── 04_Datasets/
│ ├── Historical_Data/
│ ├── Streaming_Data/
│ └── Graph_Data/
├── 05_Configurations/
└── README.md
## 4. Instructions d'Exécution
- **Batch Analytics :** `./run_batch.sh` (ou dans `03_Code_Source/Spark_Batch/`)
- **Structured Streaming :** `./run_streaming.sh` (ou dans `03_Code_Source/Spark_Streaming/`)
- **GraphX Analytics :** `./run_graph.sh` (ou dans `03_Code_Source/GraphX/`)
