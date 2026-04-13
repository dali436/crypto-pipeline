-- ============================================================
-- Initialisation de la base de données crypto_db
-- Ce fichier est exécuté automatiquement au premier démarrage
-- du container PostgreSQL (via docker-entrypoint-initdb.d/)
-- ============================================================


-- ── Table 1 : Prix actuels des cryptos ───────────────────────
-- Stocke chaque relevé de prix toutes les heures
CREATE TABLE IF NOT EXISTS prices (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)     NOT NULL,   -- ex: "bitcoin"
    price_usd       NUMERIC(20, 8)  NOT NULL,   -- prix en dollars
    market_cap_usd  NUMERIC(30, 2),             -- capitalisation boursière
    volume_24h_usd  NUMERIC(30, 2),             -- volume échangé sur 24h
    change_24h_pct  NUMERIC(10, 4),             -- variation en % sur 24h
    source          VARCHAR(50)     DEFAULT 'coingecko',
    ingested_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Index pour accélérer les requêtes par coin et par date
CREATE INDEX IF NOT EXISTS idx_prices_coin        ON prices(coin);
CREATE INDEX IF NOT EXISTS idx_prices_ingested_at ON prices(ingested_at DESC);


-- ── Table 2 : Statistiques de marché agrégées ────────────────
-- Résultats des transformations (étape 3 du pipeline)
-- Calculées toutes les heures à partir de la table prices
CREATE TABLE IF NOT EXISTS market_stats (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)     NOT NULL,
    period          VARCHAR(20)     NOT NULL,   -- ex: "1h", "24h", "7d"
    price_min       NUMERIC(20, 8),             -- prix minimum sur la période
    price_max       NUMERIC(20, 8),             -- prix maximum sur la période
    price_avg       NUMERIC(20, 8),             -- prix moyen sur la période
    price_stddev    NUMERIC(20, 8),             -- écart-type (volatilité)
    nb_records      INTEGER,                    -- nombre de relevés
    computed_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stats_coin       ON market_stats(coin);
CREATE INDEX IF NOT EXISTS idx_stats_period     ON market_stats(period);
CREATE INDEX IF NOT EXISTS idx_stats_computed_at ON market_stats(computed_at DESC);


-- ── Table 3 : Alertes de prix (anomalies détectées) ──────────
-- Générée par la transformation métier d'anomalie de prix
CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)     NOT NULL,
    alert_type      VARCHAR(50)     NOT NULL,   -- ex: "price_spike", "price_drop"
    price_usd       NUMERIC(20, 8)  NOT NULL,   -- prix au moment de l'alerte
    change_pct      NUMERIC(10, 4),             -- variation qui a déclenché l'alerte
    threshold_pct   NUMERIC(10, 4),             -- seuil configuré (ex: 5.0 pour 5%)
    message         TEXT,                       -- description lisible
    triggered_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_coin         ON alerts(coin);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at ON alerts(triggered_at DESC);


-- ── Table 4 : Ticks Kafka (streaming) ────────────────────────
-- Sauvegarde des événements temps réel consommés depuis Kafka
CREATE TABLE IF NOT EXISTS kafka_ticks (
    id              SERIAL PRIMARY KEY,
    coin            VARCHAR(50)     NOT NULL,
    price_usd       NUMERIC(20, 8),
    change_24h_pct  NUMERIC(10, 4),
    volume_simulated NUMERIC(20, 2),
    exchange        VARCHAR(50),                -- ex: "binance", "coinbase"
    tick_timestamp  TIMESTAMP       NOT NULL,   -- horodatage du tick original
    consumed_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticks_coin         ON kafka_ticks(coin);
CREATE INDEX IF NOT EXISTS idx_ticks_tick_ts      ON kafka_ticks(tick_timestamp DESC);


-- ── Données de test (optionnel) ───────────────────────────────
-- Insère quelques lignes pour vérifier que tout fonctionne
INSERT INTO prices (coin, price_usd, market_cap_usd, change_24h_pct)
VALUES
    ('bitcoin',      65000.00, 1280000000000, 2.35),
    ('ethereum',      3500.00,  420000000000, 1.80),
    ('solana',          150.00,  65000000000, 3.10),
    ('binancecoin',    580.00,  86000000000, 0.95)
ON CONFLICT DO NOTHING;

-- Confirmation
DO $$
BEGIN
    RAISE NOTICE '✓ Base de données crypto_db initialisée avec succès';
    RAISE NOTICE '  Tables créées : prices, market_stats, alerts, kafka_ticks';
END $$;
