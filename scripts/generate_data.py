"""
Générateur de données historiques réalistes
Simule 30 jours de relevés de prix toutes les heures
pour 4 cryptos → environ 2880 lignes dans PostgreSQL

Lance avec :
    python scripts/generate_data.py
"""

import psycopg2
import random
import math
from datetime import datetime, timedelta

# ── Connexion ──────────────────────────────────────────────────
conn = psycopg2.connect(
    host="localhost", port=5433,
    user="postgres", password="postgres",
    dbname="crypto_db"
)
cur = conn.cursor()

# ── Prix de départ réalistes ───────────────────────────────────
CRYPTOS = {
    "bitcoin":     {"start_price": 60000, "market_cap": 1.18e12, "volatility": 0.02},
    "ethereum":    {"start_price":  3200, "market_cap": 3.84e11, "volatility": 0.025},
    "solana":      {"start_price":   140, "market_cap": 6.44e10, "volatility": 0.035},
    "binancecoin": {"start_price":   550, "market_cap": 8.25e10, "volatility": 0.018},
}

DAYS    = 30
HOURS   = DAYS * 24      # relevé toutes les heures
now     = datetime.utcnow()
start   = now - timedelta(days=DAYS)

print(f"Génération de {HOURS * len(CRYPTOS):,} lignes de données...")
print(f"Période : {start.strftime('%Y-%m-%d')} → {now.strftime('%Y-%m-%d')}")
print()

total_inserted = 0

for coin, config in CRYPTOS.items():
    price     = config["start_price"]
    vol       = config["volatility"]
    rows      = []

    for h in range(HOURS):
        timestamp = start + timedelta(hours=h)

        # Simulation réaliste : mouvement brownien géométrique
        # Le prix monte/descend de façon aléatoire mais cohérente
        trend     = math.sin(h / (HOURS / 3)) * 0.005   # tendance sinusoïdale douce
        noise     = random.gauss(0, vol)                  # bruit aléatoire
        price     = price * (1 + trend + noise)
        price     = max(price, config["start_price"] * 0.3)  # plancher à 30% du prix initial

        market_cap  = price * (config["market_cap"] / config["start_price"])
        volume_24h  = market_cap * random.uniform(0.03, 0.12)
        change_24h  = random.uniform(-8.0, 8.0)

        rows.append((
            coin,
            round(price, 2),
            round(market_cap, 2),
            round(volume_24h, 2),
            round(change_24h, 4),
            "generated",
            timestamp,
        ))

    # Insertion en batch (beaucoup plus rapide qu'un par un)
    cur.executemany(
        """
        INSERT INTO prices (coin, price_usd, market_cap_usd, volume_24h_usd, change_24h_pct, source, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )
    conn.commit()
    total_inserted += len(rows)
    print(f"  {coin:15s} → {len(rows):4d} lignes insérées  "
          f"(prix final : ${price:,.2f})")

# ── Résumé final ───────────────────────────────────────────────
cur.execute("SELECT COUNT(*) FROM prices")
total_in_db = cur.fetchone()[0]

print(f"\n{'='*50}")
print(f"Terminé ! {total_inserted:,} lignes générées")
print(f"Total dans la table prices : {total_in_db:,} lignes")
print(f"{'='*50}")

cur.close()
conn.close()