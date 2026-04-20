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
 
CRYPTOS = {
    "bitcoin": {
        "start_price": 65000,
        "min_price":   60000,   # plancher strict
        "max_price":   90000,   # plafond strict
        "volatility":  0.004,   # 0.4% max par heure
    },
    "ethereum": {
        "start_price": 2200,
        "min_price":   1800,
        "max_price":   3500,
        "volatility":  0.005,
    },
    "solana": {
        "start_price": 80,
        "min_price":   60,
        "max_price":   180,
        "volatility":  0.006,
    },
    "binancecoin": {
        "start_price": 580,
        "min_price":   500,
        "max_price":   750,
        "volatility":  0.004,
    },
}
 
DAYS    = 30
HOURS   = DAYS * 24
now     = datetime.utcnow()
start   = now - timedelta(days=DAYS)
 
print(f"Génération de {HOURS * len(CRYPTOS):,} lignes de données réalistes...")
print(f"Période : {start.strftime('%Y-%m-%d')} → {now.strftime('%Y-%m-%d')}")
print()
 
total_inserted = 0
 
for coin, config in CRYPTOS.items():
    price     = config["start_price"]
    vol       = config["volatility"]
    min_p     = config["min_price"]
    max_p     = config["max_price"]
    rows      = []
 
    for h in range(HOURS):
        timestamp = start + timedelta(hours=h)
 
        # Tendance sinusoïdale douce (cycle de 7 jours)
        # simule les cycles haussiers/baissiers naturels du marché
        trend = math.sin(h / (HOURS / 4)) * 0.002
 
        # Bruit aléatoire très faible
        noise = random.gauss(0, vol)
 
        # Nouveau prix
        price = price * (1 + trend + noise)
 
        # Clamp strict entre min et max — prix ne peut pas sortir
        price = max(min_p, min(max_p, price))
 
        market_cap  = price * (config["start_price"] * 19_000_000 / config["start_price"])
        volume_24h  = market_cap * random.uniform(0.02, 0.08)
        change_24h  = random.uniform(-5.0, 5.0)
 
        rows.append((
            coin,
            round(price, 2),
            round(market_cap, 2),
            round(volume_24h, 2),
            round(change_24h, 4),
            "generated",
            timestamp,
        ))
 
    cur.executemany(
        """
        INSERT INTO prices (coin, price_usd, market_cap_usd, volume_24h_usd, change_24h_pct, source, ingested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        rows
    )
    conn.commit()
    total_inserted += len(rows)
 
    price_min = min(r[1] for r in rows)
    price_max = max(r[1] for r in rows)
    print(f"  {coin:15s} → {len(rows):4d} lignes  "
          f"(fourchette : ${price_min:,.0f} → ${price_max:,.0f})")
 
cur.execute("SELECT COUNT(*) FROM prices")
total_in_db = cur.fetchone()[0]
 
print(f"\n{'='*55}")
print(f"Terminé ! {total_inserted:,} lignes générées")
print(f"Total dans la table prices : {total_in_db:,} lignes")
print(f"{'='*55}")
 
cur.close()
conn.close()
