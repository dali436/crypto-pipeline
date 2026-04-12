"""
Ingestion Batch - Prix Crypto via CoinGecko API
Tourne toutes les heures (via Airflow ou cron)
Sauvegarde les données en JSON dans un dossier local (ou S3/MinIO)
"""

import requests
import json
import os
from datetime import datetime

# ── Configuration ──────────────────────────────────────────────
CRYPTOS       = ["bitcoin", "ethereum", "solana", "binancecoin"]
CURRENCY      = "usd"
OUTPUT_DIR    = "data/raw/batch"          # dossier de sortie local
BASE_URL      = "https://api.coingecko.com/api/v3"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── Fonction 1 : Prix actuels ──────────────────────────────────
def fetch_current_prices() -> dict:
    """
    Appelle CoinGecko pour récupérer le prix actuel,
    la capitalisation boursière et la variation sur 24h.
    """
    url = f"{BASE_URL}/simple/price"
    params = {
        "ids":                  ",".join(CRYPTOS),
        "vs_currencies":        CURRENCY,
        "include_market_cap":   "true",
        "include_24hr_change":  "true",
        "include_24hr_vol":     "true",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()          # lève une erreur si HTTP 4xx/5xx

    data = response.json()

    # Enrichissement : on ajoute un timestamp d'ingestion
    result = {
        "ingested_at": datetime.utcnow().isoformat(),
        "source":      "coingecko",
        "type":        "current_prices",
        "data":        data,
    }
    return result


# ── Fonction 2 : Historique 30 jours ──────────────────────────
def fetch_historical(coin: str = "bitcoin", days: int = 30) -> dict:
    """
    Récupère l'historique de prix d'une crypto sur N jours.
    Retourne une liste de [timestamp_ms, prix].
    """
    url = f"{BASE_URL}/coins/{coin}/market_chart"
    params = {
        "vs_currency": CURRENCY,
        "days":        days,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()

    result = {
        "ingested_at": datetime.utcnow().isoformat(),
        "source":      "coingecko",
        "type":        "historical",
        "coin":        coin,
        "days":        days,
        "data":        data,
    }
    return result


# ── Fonction 3 : Sauvegarde locale ────────────────────────────
def save_to_json(data: dict, filename: str) -> str:
    """
    Sauvegarde un dict Python en fichier JSON.
    Le nom du fichier inclut un timestamp pour éviter les écrasements.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filepath  = os.path.join(OUTPUT_DIR, f"{filename}_{timestamp}.json")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[OK] Sauvegardé : {filepath}")
    return filepath


# ── Pipeline principal ─────────────────────────────────────────
def run_batch_ingestion():
    print(f"\n{'='*50}")
    print(f"Ingestion batch démarrée : {datetime.utcnow().isoformat()}")
    print(f"{'='*50}")

    # 1. Prix actuels
    try:
        prices = fetch_current_prices()
        save_to_json(prices, "current_prices")
        print(f"  Cryptos ingérées : {list(prices['data'].keys())}")
    except requests.RequestException as e:
        print(f"[ERREUR] Prix actuels : {e}")

    # 2. Historique pour chaque crypto
    for coin in CRYPTOS:
        try:
            history = fetch_historical(coin=coin, days=30)
            save_to_json(history, f"historical_{coin}")
            nb_points = len(history["data"].get("prices", []))
            print(f"  Historique {coin} : {nb_points} points de données")
        except requests.RequestException as e:
            print(f"[ERREUR] Historique {coin} : {e}")

    print(f"\nIngestion batch terminée.")


# ── Point d'entrée ─────────────────────────────────────────────
if __name__ == "__main__":
    run_batch_ingestion()
