"""
Tests unitaires — Transformations du pipeline crypto
Couvre les 3 transformations métier avec au moins 5 tests
(exigence obligatoire du projet)

Lancer avec :
    pytest tests/test_transform.py -v
    pytest tests/test_transform.py -v --cov=transformation
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# On importe les fonctions à tester
import sys
sys.path.append(".")
from transformation.transform import clean_prices, aggregate_prices, detect_anomalies


# ── Fixtures : données de test réutilisables ──────────────────
@pytest.fixture
def sample_df():
    """
    DataFrame propre de base pour les tests.
    Simule 3 relevés de prix pour bitcoin et ethereum.
    """
    now = datetime.utcnow()
    return pd.DataFrame([
        {"coin": "bitcoin",  "price_usd": 65000.0, "market_cap_usd": 1.28e12, "change_24h_pct":  2.5, "ingested_at": now - timedelta(hours=2)},
        {"coin": "bitcoin",  "price_usd": 66000.0, "market_cap_usd": 1.30e12, "change_24h_pct":  3.0, "ingested_at": now - timedelta(hours=1)},
        {"coin": "bitcoin",  "price_usd": 70000.0, "market_cap_usd": 1.38e12, "change_24h_pct":  7.2, "ingested_at": now},
        {"coin": "ethereum", "price_usd":  3500.0, "market_cap_usd": 4.20e11, "change_24h_pct":  1.8, "ingested_at": now - timedelta(hours=2)},
        {"coin": "ethereum", "price_usd":  3550.0, "market_cap_usd": 4.26e11, "change_24h_pct":  2.0, "ingested_at": now - timedelta(hours=1)},
        {"coin": "ethereum", "price_usd":  3600.0, "market_cap_usd": 4.32e11, "change_24h_pct":  2.8, "ingested_at": now},
    ])

@pytest.fixture
def dirty_df():
    """
    DataFrame avec des données intentionnellement corrompues
    pour tester que le nettoyage les détecte bien.
    """
    now = datetime.utcnow()
    return pd.DataFrame([
        {"coin": "bitcoin",  "price_usd": 65000.0,  "market_cap_usd": 1.28e12, "change_24h_pct": 2.5,  "ingested_at": now},
        {"coin": "bitcoin",  "price_usd": None,      "market_cap_usd": None,    "change_24h_pct": None, "ingested_at": now},   # null → doit être supprimé
        {"coin": "BITCOIN",  "price_usd": 64000.0,  "market_cap_usd": 1.27e12, "change_24h_pct": 1.0,  "ingested_at": now},   # majuscules → standardiser
        {"coin": "ethereum", "price_usd": -100.0,   "market_cap_usd": 4.20e11, "change_24h_pct": 0.5,  "ingested_at": now},   # négatif → doit être supprimé
        {"coin": "  solana ","price_usd": 150.0,    "market_cap_usd": 6.50e10, "change_24h_pct": 3.0,  "ingested_at": now},   # espaces → standardiser
        {"coin": "bitcoin",  "price_usd": 65000.0,  "market_cap_usd": 1.28e12, "change_24h_pct": 2.5,  "ingested_at": now},   # doublon exact → supprimer
    ])


# ══════════════════════════════════════════════════════════════
# TESTS — Transformation 1 : Nettoyage
# ══════════════════════════════════════════════════════════════

def test_clean_removes_null_prices(dirty_df):
    """Les lignes avec prix null doivent être supprimées."""
    result = clean_prices(dirty_df)
    assert result["price_usd"].isnull().sum() == 0, \
        "Il reste des prix null après nettoyage"

def test_clean_removes_negative_prices(dirty_df):
    """Les lignes avec prix négatif ou nul doivent être supprimées."""
    result = clean_prices(dirty_df)
    assert (result["price_usd"] <= 0).sum() == 0, \
        "Il reste des prix négatifs après nettoyage"

def test_clean_standardizes_coin_names(dirty_df):
    """Les noms de cryptos doivent être en minuscules et sans espaces."""
    result = clean_prices(dirty_df)
    for coin in result["coin"]:
        assert coin == coin.lower().strip(), \
            f"Le nom '{coin}' n'est pas standardisé"

def test_clean_removes_duplicates(dirty_df):
    """Les lignes dupliquées (même coin + même timestamp) doivent être supprimées."""
    result = clean_prices(dirty_df)
    duplicates = result.duplicated(subset=["coin", "ingested_at"]).sum()
    assert duplicates == 0, \
        f"{duplicates} doublons trouvés après nettoyage"

def test_clean_preserves_valid_rows(dirty_df):
    """Le nettoyage ne doit pas supprimer les lignes valides."""
    result = clean_prices(dirty_df)
    # Sur 6 lignes : 1 null, 1 négative, 1 doublon = 3 invalides → 3 restantes
    assert len(result) == 3, \
        f"Attendu 3 lignes valides, obtenu {len(result)}"


# ══════════════════════════════════════════════════════════════
# TESTS — Transformation 2 : Agrégation
# ══════════════════════════════════════════════════════════════

def test_aggregate_returns_one_row_per_coin(sample_df):
    """L'agrégation doit retourner exactement une ligne par crypto."""
    result = aggregate_prices(sample_df, period="24h")
    assert len(result) == sample_df["coin"].nunique(), \
        "Nombre de lignes agrégées différent du nombre de cryptos"

def test_aggregate_price_min_less_than_max(sample_df):
    """Le prix minimum doit toujours être inférieur au maximum."""
    result = aggregate_prices(sample_df, period="24h")
    assert (result["price_min"] <= result["price_max"]).all(), \
        "price_min > price_max pour certaines cryptos"

def test_aggregate_average_between_min_and_max(sample_df):
    """La moyenne doit être comprise entre le min et le max."""
    result = aggregate_prices(sample_df, period="24h")
    assert ((result["price_avg"] >= result["price_min"]) &
            (result["price_avg"] <= result["price_max"])).all(), \
        "La moyenne est hors de l'intervalle [min, max]"

def test_aggregate_stddev_non_negative(sample_df):
    """L'écart-type (volatilité) ne peut pas être négatif."""
    result = aggregate_prices(sample_df, period="24h")
    assert (result["price_stddev"] >= 0).all(), \
        "L'écart-type est négatif"

def test_aggregate_empty_returns_empty(sample_df):
    """Si aucune donnée dans la période, retourner un DataFrame vide."""
    # On utilise une période passée très lointaine pour avoir 0 résultats
    old_df = sample_df.copy()
    old_df["ingested_at"] = datetime.utcnow() - timedelta(days=365)
    result = aggregate_prices(old_df, period="1h")
    assert result.empty, \
        "Doit retourner un DataFrame vide si aucune donnée dans la période"


# ══════════════════════════════════════════════════════════════
# TESTS — Transformation 3 : Détection d'anomalies
# ══════════════════════════════════════════════════════════════

def test_detect_spike_above_threshold(sample_df):
    """
    Bitcoin passe de 66000 à 70000 = +6.06% → doit déclencher une alerte.
    Le seuil est à 5%.
    """
    result = detect_anomalies(sample_df, threshold_pct=5.0)
    btc_alerts = result[result["coin"] == "bitcoin"]
    assert len(btc_alerts) > 0, \
        "Aucune alerte générée pour une variation de +6% sur Bitcoin"

def test_detect_no_alert_below_threshold(sample_df):
    """
    Ethereum varie de ~1.4% max → ne doit PAS déclencher d'alerte à 5%.
    """
    result = detect_anomalies(sample_df, threshold_pct=5.0)
    eth_alerts = result[result["coin"] == "ethereum"]
    assert len(eth_alerts) == 0, \
        "Une fausse alerte a été générée pour Ethereum (variation < seuil)"

def test_detect_alert_type_is_spike_or_drop(sample_df):
    """Le type d'alerte doit être 'price_spike' ou 'price_drop' uniquement."""
    result = detect_anomalies(sample_df, threshold_pct=5.0)
    if not result.empty:
        valid_types = {"price_spike", "price_drop"}
        assert set(result["alert_type"]).issubset(valid_types), \
            f"Type d'alerte invalide : {set(result['alert_type']) - valid_types}"

def test_detect_returns_empty_if_no_anomaly():
    """Si les prix sont stables, aucune alerte ne doit être générée."""
    now = datetime.utcnow()
    stable_df = pd.DataFrame([
        {"coin": "bitcoin", "price_usd": 65000.0, "ingested_at": now - timedelta(hours=2)},
        {"coin": "bitcoin", "price_usd": 65100.0, "ingested_at": now - timedelta(hours=1)},
        {"coin": "bitcoin", "price_usd": 65050.0, "ingested_at": now},
    ])
    result = detect_anomalies(stable_df, threshold_pct=5.0)
    assert result.empty, \
        "Des alertes ont été générées alors que les prix sont stables"

def test_detect_message_contains_coin_name(sample_df):
    """Le message d'alerte doit contenir le nom de la crypto en majuscules."""
    result = detect_anomalies(sample_df, threshold_pct=5.0)
    if not result.empty:
        for _, row in result.iterrows():
            assert row["coin"].upper() in row["message"], \
                f"Le nom de la crypto absent du message : {row['message']}"
