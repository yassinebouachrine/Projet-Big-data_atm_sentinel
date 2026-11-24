#!/usr/bin/env python3
"""
Générateur de Flux Streaming Dynamique pour TP6
Écrit progressivement des micro-lots CSV/JSON dans le dossier incoming/
Génère 10 000+ événements avec présence d'anomalies (dates, montants, doublons)
"""
import os
import sys
import time
import json
import csv
import random
from datetime import datetime, timedelta

TARGET_DIR = os.path.expanduser("~/TP6_big_data/01_data/streaming/incoming")
os.makedirs(TARGET_DIR, exist_ok=True)

BATCH_SIZE = 500  # 500 événements par fichier
NUM_BATCHES = 20  # 20 fichiers = 10 000 événements
INTERVAL_SEC = 2  # Pause de 2 secondes entre les fichiers

TYPES = ["Withdrawal", "Deposit", "Balance Inquiry", "Transfer"]
ATMS = [f"ATM-{st}-{i:03d}" for st in ["LA", "KN", "RI", "EN", "FC"] for i in range(1, 11)]

print(f"=== DEMARRAGE DU GÉNÉRATEUR STREAMING ===")
print(f"Dossier cible : {TARGET_DIR}")
print(f"Total prévu : {NUM_BATCHES * BATCH_SIZE} événements en {NUM_BATCHES} lots")

global_count = 0
for b in range(1, NUM_BATCHES + 1):
    file_path = os.path.join(TARGET_DIR, f"stream_batch_{b:03d}.json")
    events = []
    
    for i in range(1, BATCH_SIZE + 1):
        global_count += 1
        tx_id = f"STRM-TX-{global_count:07d}"
        
        # Injection contrôlée d'anomalies (5% de la masse)
        rand_val = random.random()
        
        if rand_val < 0.02:
            # Anomalie : Montant Négatif
            amount = -5000.0
            status = "INVALID_AMOUNT"
        elif rand_val < 0.04:
            # Anomalie : Champ manquant (ATM nul)
            atm_id = None
            status = "MISSING_ATM"
            amount = 10000.0
        else:
            atm_id = random.choice(ATMS)
            amount = float(random.choice([2000, 5000, 10000, 20000, 50000]))
            status = "VALID"
            
        now = datetime.now() - timedelta(seconds=random.randint(0, 300)) # Event time
        
        event = {
            "transaction_id": tx_id,
            "event_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "atm_id": atm_id,
            "cardholder_id": f"CUST-{random.randint(1001, 1950)}",
            "transaction_type": random.choice(TYPES),
            "amount": amount,
            "status": status
        }
        events.append(event)
        
    with open(file_path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
            
    print(f"[+] Lot {b}/{NUM_BATCHES} écrit : {file_path} ({BATCH_SIZE} événements)")
    time.sleep(INTERVAL_SEC)

print("=== GÉNÉRATION STREAMING TERMINÉE AVEC SUCCÈS ===")
