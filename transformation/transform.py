"""
Transformation - Les 3 transformations métier du pipeline crypto
Lit les données brutes depuis PostgreSQL/MinIO,
les nettoie, agrège, détecte les anomalies,
puis sauvegarde les résultats dans PostgreSQL.

Prérequis :
    pip install pandas numpy sqlalchemy psycopg2-binary loguru tenacity
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed
from sqlalchemy import create_engine, text

# ── Configuration ──────────────────────────────────────────────
DB_URL = "postgresql://postgres:postgres@localhost:5433/crypto_db"

ANOMALY_THRESHOLD_PCT = 5.0      # alerte si variation > 5%
AGGREGATION_PERIOD    = "1h"     # période d'agrégation

# ── Connexion PostgreSQL avec retry ───────────────────────────
@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def get_engine():
    """
    Crée la connexion à PostgreSQL.
    Retry automatique 3 fois si la BDD n'est pas encore prête.
    (Exigence du projet : retry sur étape critique)
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    logger.info("Connexion PostgreSQL établie")
    return engine


# ══════════════════════════════════════════════════════════════
# TRANSFORMATION 1 — Nettoyage des données brutes
# ══════════════════════════════════════════════════════════════

def clean_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie le DataFrame des prix bruts :
      - Supprime les lignes avec prix null ou négatif
      - Corrige les types de colonnes
      - Standardise les noms de cryptos
      - Déduplique les entrées identiques

    Args:
        df : DataFrame brut lu depuis PostgreSQL (table prices)

    Returns:
        DataFrame nettoyé, prêt pour l'agrégation
    """
    initial_count = len(df)
    logger.info(f"Nettoyage démarré — {initial_count} lignes en entrée")

    # 1. Supprimer les lignes avec prix manquant ou invalide
    df = df.dropna(subset=["price_usd"])
    df = df[df["price_usd"] > 0]

    # 2. Corriger les types
    df["price_usd"]      = pd.to_numeric(df["price_usd"],      errors="coerce")
    df["market_cap_usd"] = pd.to_numeric(df["market_cap_usd"], errors="coerce")
    df["change_24h_pct"] = pd.to_numeric(df["change_24h_pct"], errors="coerce")
    df["ingested_at"]    = pd.to_datetime(df["ingested_at"],   errors="coerce")

    # 3. Standardiser les noms de cryptos (minuscules, sans espaces)
    df["coin"] = df["coin"].str.lower().str.strip()

    # 4. Supprimer les doublons exacts
    df = df.drop_duplicates(subset=["coin", "ingested_at"])

    # 5. Supprimer les lignes dont le type n'a pas pu être converti
    df = df.dropna(subset=["price_usd", "ingested_at"])

    cleaned_count = len(df)
    removed = initial_count - cleaned_count
    logger.info(f"Nettoyage terminé — {cleaned_count} lignes propres ({removed} supprimées)")

    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════════
# TRANSFORMATION 2 — Agrégation par crypto et par période
# ══════════════════════════════════════════════════════════════

def aggregate_prices(df: pd.DataFrame, period: str = "1h") -> pd.DataFrame:
    """
    Calcule les statistiques de marché pour chaque crypto
    sur la période donnée : min, max, moyenne, volatilité.

    Args:
        df     : DataFrame nettoyé (sortie de clean_prices)
        period : fenêtre temporelle — "1h", "24h", "7d"

    Returns:
        DataFrame avec une ligne par crypto contenant les stats
    """
    logger.info(f"Agrégation démarrée — période : {period}")

    # Filtrer selon la période demandée
    period_map = {"1h": 1, "24h": 24, "7d": 168}
    hours = period_map.get(period, 1)
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    df_period = df[df["ingested_at"] >= cutoff].copy()

    if df_period.empty:
        logger.warning(f"Aucune donnée dans la fenêtre {period}")
        return pd.DataFrame()

    # Calculer les statistiques par crypto
    stats = df_period.groupby("coin").agg(
        price_min   = ("price_usd", "min"),
        price_max   = ("price_usd", "max"),
        price_avg   = ("price_usd", "mean"),
        price_stddev= ("price_usd", "std"),    # volatilité = écart-type
        nb_records  = ("price_usd", "count"),
    ).reset_index()

    # Arrondir pour la lisibilité
    stats["price_min"]    = stats["price_min"].round(2)
    stats["price_max"]    = stats["price_max"].round(2)
    stats["price_avg"]    = stats["price_avg"].round(2)
    stats["price_stddev"] = stats["price_stddev"].round(4).fillna(0)
    stats["period"]       = period
    stats["computed_at"]  = datetime.utcnow()

    logger.info(f"Agrégation terminée — {len(stats)} cryptos traitées")
    return stats


# ══════════════════════════════════════════════════════════════
# TRANSFORMATION 3 — Détection d'anomalies de prix
# ══════════════════════════════════════════════════════════════

def detect_anomalies(df: pd.DataFrame, threshold_pct: float = 5.0) -> pd.DataFrame:
    """
    Détecte les variations de prix anormales pour chaque crypto.
    Si le prix change de plus de `threshold_pct`% entre deux relevés,
    une alerte est générée.

    Args:
        df            : DataFrame nettoyé et trié par date
        threshold_pct : seuil de déclenchement en % (défaut: 5.0)

    Returns:
        DataFrame des alertes à insérer dans la table `alerts`
    """
    logger.info(f"Détection d'anomalies — seuil : {threshold_pct}%")

    alerts = []

    for coin, group in df.groupby("coin"):
        # Trier par date pour calculer la variation correctement
        group = group.sort_values("ingested_at").reset_index(drop=True)

        if len(group) < 2:
            continue    # pas assez de points pour calculer une variation

        # Calculer la variation en % entre chaque relevé consécutif
        group["change_pct"] = group["price_usd"].pct_change() * 100

        # Identifier les variations qui dépassent le seuil
        anomalies = group[group["change_pct"].abs() > threshold_pct]

        for _, row in anomalies.iterrows():
            change  = round(row["change_pct"], 2)
            direction = "price_spike" if change > 0 else "price_drop"
            emoji     = "🚀" if change > 0 else "📉"

            alert = {
                "coin":          coin,
                "alert_type":    direction,
                "price_usd":     row["price_usd"],
                "change_pct":    change,
                "threshold_pct": threshold_pct,
                "message":       (
                    f"{emoji} {coin.upper()} : variation de {change:+.2f}% "
                    f"détectée (prix : ${row['price_usd']:,.2f})"
                ),
                "triggered_at":  row["ingested_at"],
            }
            alerts.append(alert)
            logger.warning(f"ALERTE — {alert['message']}")

    if not alerts:
        logger.info("Aucune anomalie détectée")
        return pd.DataFrame()

    logger.info(f"{len(alerts)} alerte(s) générée(s)")
    return pd.DataFrame(alerts)


# ══════════════════════════════════════════════════════════════
# SAUVEGARDE dans PostgreSQL
# ══════════════════════════════════════════════════════════════

def save_to_postgres(df: pd.DataFrame, table: str, engine):
    """
    Insère un DataFrame dans une table PostgreSQL.
    Utilise 'append' pour ne jamais écraser les données existantes.
    """
    if df.empty:
        logger.info(f"Rien à sauvegarder dans {table}")
        return

    df.to_sql(
        name=table,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info(f"Sauvegardé {len(df)} lignes dans la table '{table}'")


# ══════════════════════════════════════════════════════════════
# PIPELINE PRINCIPAL
# ══════════════════════════════════════════════════════════════

def run_transformations():
    logger.info("=" * 55)
    logger.info("Pipeline de transformation démarré")
    logger.info("=" * 55)

    engine = get_engine()

    # 1. Lire les données brutes depuis PostgreSQL
    df_raw = pd.read_sql("SELECT * FROM prices ORDER BY ingested_at", engine)
    logger.info(f"Données brutes lues : {len(df_raw)} lignes")

    if df_raw.empty:
        logger.error("Aucune donnée dans la table prices — lance d'abord l'ingestion !")
        return

    # 2. Transformation 1 : Nettoyage
    df_clean = clean_prices(df_raw)

    # 3. Transformation 2 : Agrégation (1h et 24h)
    for period in ["1h", "24h"]:
        df_stats = aggregate_prices(df_clean, period=period)
        save_to_postgres(df_stats, "market_stats", engine)

    # 4. Transformation 3 : Détection d'anomalies
    df_alerts = detect_anomalies(df_clean, threshold_pct=ANOMALY_THRESHOLD_PCT)
    save_to_postgres(df_alerts, "alerts", engine)

    logger.info("Pipeline de transformation terminé avec succès")


if __name__ == "__main__":
    run_transformations()
