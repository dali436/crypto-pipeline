"""
Ingestion Streaming - Producteur Kafka
Simule un flux de "ticks" de prix crypto en temps réel
Publie dans un topic Kafka toutes les 5 secondes

Prérequis :
    pip install kafka-python requests
    Kafka doit tourner localement
"""

import json
import time
import random
import requests
from datetime import datetime
from kafka import KafkaProducer

# ── Configuration ──────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"          # adresse de ton broker Kafka
KAFKA_TOPIC   = "crypto_ticks"            # nom du topic
CRYPTOS       = ["bitcoin", "ethereum", "solana"]
INTERVAL_SEC  = 5                         # fréquence d'envoi (secondes)
BASE_URL      = "https://api.coingecko.com/api/v3"


# ── Initialisation du producteur Kafka ─────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",          # attendre la confirmation du broker
    retries=3,           # retry automatique en cas d'échec
)


# ── Récupération du prix actuel ────────────────────────────────
def get_live_price(coin: str) -> dict | None:
    """
    Appelle CoinGecko pour le prix live d'une crypto.
    En cas d'erreur, retourne None (le producteur skip ce tick).
    """
    try:
        url    = f"{BASE_URL}/simple/price"
        params = {
            "ids":                 coin,
            "vs_currencies":       "usd",
            "include_24hr_change": "true",
        }
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        raw  = resp.json().get(coin, {})

        return {
            "coin":        coin,
            "price_usd":   raw.get("usd"),
            "change_24h":  raw.get("usd_24h_change"),
            "timestamp":   datetime.utcnow().isoformat(),
            "source":      "coingecko_live",
        }
    except Exception as e:
        print(f"[WARN] Impossible de récupérer {coin} : {e}")
        return None


# ── Simulation d'un volume de transaction aléatoire ────────────
def add_simulated_volume(tick: dict) -> dict:
    """
    Ajoute un volume de transaction simulé au tick.
    (CoinGecko gratuit ne donne pas le volume tick-by-tick)
    """
    tick["volume_simulated"] = round(random.uniform(100_000, 5_000_000), 2)
    tick["exchange"]         = random.choice(["binance", "coinbase", "kraken"])
    return tick


# ── Envoi vers Kafka ────────────────────────────────────────────
def send_tick(tick: dict):
    """
    Publie un tick dans le topic Kafka.
    La clé du message = nom de la crypto (pour le partitionnement).
    """
    future = producer.send(
        KAFKA_TOPIC,
        key=tick["coin"].encode("utf-8"),
        value=tick,
    )
    record_metadata = future.get(timeout=10)    # bloquant — attend la confirmation
    print(
        f"  [{tick['coin'].upper():12s}] "
        f"${tick['price_usd']:>12,.2f} USD  "
        f"| topic={record_metadata.topic} "
        f"| partition={record_metadata.partition} "
        f"| offset={record_metadata.offset}"
    )


# ── Boucle principale du producteur ────────────────────────────
def run_producer():
    print(f"\n{'='*55}")
    print(f"Producteur Kafka démarré → topic : '{KAFKA_TOPIC}'")
    print(f"Broker : {KAFKA_BROKER}  |  Intervalle : {INTERVAL_SEC}s")
    print(f"{'='*55}\n")

    try:
        while True:
            print(f"--- Tick @ {datetime.utcnow().isoformat()} ---")

            for coin in CRYPTOS:
                tick = get_live_price(coin)
                if tick:
                    tick = add_simulated_volume(tick)
                    send_tick(tick)

            producer.flush()            # s'assurer que tout est envoyé
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n[INFO] Producteur arrêté par l'utilisateur.")
    finally:
        producer.close()
        print("[INFO] Connexion Kafka fermée proprement.")


# ── Point d'entrée ─────────────────────────────────────────────
if __name__ == "__main__":
    run_producer()
