"""
Orchestration - Pipeline Crypto avec schedule
Remplace Prefect par une solution légère et compatible Windows.
Lance le pipeline complet toutes les heures automatiquement.

Lancer avec :
    python orchestration/pipeline.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import schedule
import time
import requests
import json
import io
import psycopg2
import pandas as pd
from datetime import datetime
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine

from transformation.transform import (
    clean_prices,
    aggregate_prices,
    detect_anomalies,
    save_to_postgres,
)

# ── Configuration ──────────────────────────────────────────────
CRYPTOS  = ["bitcoin", "ethereum", "solana", "binancecoin"]
BASE_URL = "https://api.coingecko.com/api/v3"
DB_URL   = "postgresql://neondb_owner:npg_hwvnSaXg1CO9@ep-damp-dawn-amy2sbg1-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


# ══════════════════════════════════════════════════════════════
# ÉTAPE 1 — Ingestion
# ══════════════════════════════════════════════════════════════
@retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
def step_fetch_prices() -> dict:
    """Appelle CoinGecko — retry 3 fois si l'API est indisponible."""
    logger.info("ÉTAPE 1 — Récupération des prix CoinGecko...")
    url    = f"{BASE_URL}/simple/price"
    params = {
        "ids":                 ",".join(CRYPTOS),
        "vs_currencies":       "usd",
        "include_market_cap":  "true",
        "include_24hr_change": "true",
        "include_24hr_vol":    "true",
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    logger.info(f"  Prix récupérés : {list(data.keys())}")
    return data


# ══════════════════════════════════════════════════════════════
# ÉTAPE 2 — Stockage PostgreSQL
# ══════════════════════════════════════════════════════════════
@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def step_save_postgres(prices_data: dict) -> int:
    """Insère les prix dans PostgreSQL — retry 3 fois si connexion échoue."""
    logger.info("ÉTAPE 2a — Sauvegarde dans PostgreSQL...")
    conn = psycopg2.connect(
        host="localhost", port=5433,
        user="postgres", password="postgres",
        dbname="crypto_db"
    )
    cur  = conn.cursor()
    rows = 0
    for coin, values in prices_data.items():
        cur.execute(
            """INSERT INTO prices
               (coin, price_usd, market_cap_usd, volume_24h_usd, change_24h_pct, source)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                coin,
                values.get("usd"),
                values.get("usd_market_cap"),
                values.get("usd_24h_vol"),
                values.get("usd_24h_change"),
                "coingecko_batch",
            )
        )
        rows += 1
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"  {rows} lignes insérées dans prices")
    return rows


# ══════════════════════════════════════════════════════════════
# ÉTAPE 3 — Stockage MinIO
# ══════════════════════════════════════════════════════════════
def step_save_minio(prices_data: dict):
    """Sauvegarde le JSON brut dans MinIO — non bloquant si indisponible."""
    logger.info("ÉTAPE 2b — Sauvegarde dans MinIO...")
    try:
        from minio import Minio
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        bucket = "raw-prices"
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        payload     = {"ingested_at": datetime.utcnow().isoformat(), "data": prices_data}
        json_bytes  = json.dumps(payload, indent=2).encode("utf-8")
        object_name = f"prices/{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}.json"
        client.put_object(bucket, object_name, io.BytesIO(json_bytes), len(json_bytes))
        logger.info(f"  Sauvegardé dans MinIO : {object_name}")
    except Exception as e:
        logger.warning(f"  MinIO indisponible (pipeline continue) : {e}")


# ══════════════════════════════════════════════════════════════
# ÉTAPE 4 — Transformation
# ══════════════════════════════════════════════════════════════
def step_transform() -> dict:
    """Exécute les 3 transformations métier sur les données récentes."""
    logger.info("ÉTAPE 3 — Transformation des données...")
    engine = create_engine(DB_URL)

    df_raw = pd.read_sql(
        "SELECT * FROM prices ORDER BY ingested_at DESC LIMIT 5000", engine
    )
    logger.info(f"  {len(df_raw)} lignes lues depuis PostgreSQL")

    # Transformation 1 : Nettoyage
    df_clean = clean_prices(df_raw)

    # Transformation 2 : Agrégation
    for period in ["1h", "24h"]:
        stats = aggregate_prices(df_clean, period=period)
        save_to_postgres(stats, "market_stats", engine)

    # Transformation 3 : Anomalies
    alerts = detect_anomalies(df_clean, threshold_pct=5.0)
    save_to_postgres(alerts, "alerts", engine)

    return {
        "lignes_traitees": len(df_clean),
        "alertes":         len(alerts) if not alerts.empty else 0,
    }


# ══════════════════════════════════════════════════════════════
# PIPELINE COMPLET
# ══════════════════════════════════════════════════════════════
def run_pipeline():
    """
    Pipeline complet : ingestion → stockage → transformation.
    Appelé automatiquement toutes les heures par schedule.
    """
    start = datetime.utcnow()
    logger.info("=" * 55)
    logger.info(f"Pipeline démarré : {start.strftime('%Y-%m-%d %H:%M:%S')} UTC")
    logger.info("=" * 55)

    try:
        prices_data = step_fetch_prices()
        nb_inserted = step_save_postgres(prices_data)
        step_save_minio(prices_data)
        result      = step_transform()

        duration = (datetime.utcnow() - start).seconds
        logger.info("=" * 55)
        logger.info("Pipeline terminé avec succès")
        logger.info(f"  Durée              : {duration}s")
        logger.info(f"  Lignes ingérées    : {nb_inserted}")
        logger.info(f"  Lignes transformées: {result['lignes_traitees']}")
        logger.info(f"  Alertes générées   : {result['alertes']}")
        logger.info("=" * 55)

    except Exception as e:
        logger.error(f"Pipeline échoué : {e}")


# ══════════════════════════════════════════════════════════════
# PLANIFICATION — toutes les heures
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":

    logger.info("Orchestrateur démarré — toutes les heures")
    logger.info("Ctrl+C pour arrêter\n")

    # Exécution immédiate au démarrage
    run_pipeline()

    # Puis toutes les heures automatiquement
    schedule.every(1).hours.do(run_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)
