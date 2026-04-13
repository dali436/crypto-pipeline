# Crypto Pipeline — Data Engineering End-to-End

Pipeline de données complet pour la surveillance des cryptomonnaies en temps réel.
Ingestion depuis CoinGecko API → Stockage MinIO + PostgreSQL → Transformation → Dashboard Streamlit.

---

## Diagramme d'architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCES DE DONNÉES                           │
│                                                                     │
│   ┌─────────────────┐              ┌──────────────────────────┐     │
│   │  CoinGecko API  │              │   Kafka Producer         │     │
│   │  (Batch / 1h)   │              │   (Streaming / 5s)       │     │
│   └────────┬────────┘              └────────────┬─────────────┘     │
└────────────│───────────────────────────────────│───────────────────┘
             │                                   │
             ▼                                   ▼
┌────────────────────────┐          ┌────────────────────────┐
│   ingestion_batch.py   │          │   Apache Kafka         │
│   requests + pandas    │          │   Topic: crypto_ticks  │
└────────────┬───────────┘          └────────────┬───────────┘
             │                                   │
             └──────────────┬────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          STOCKAGE                                   │
│                                                                     │
│   ┌──────────────────────────┐    ┌──────────────────────────┐     │
│   │   MinIO (Data Lake)      │    │   PostgreSQL             │     │
│   │                          │    │   (Data Warehouse)       │     │
│   │   raw-prices/            │    │                          │     │
│   │   raw-historical/        │    │   prices                 │     │
│   │   raw-streaming/         │    │   market_stats           │     │
│   │                          │    │   alerts                 │     │
│   │   JSON bruts             │    │   kafka_ticks            │     │
│   └──────────────────────────┘    └────────────┬─────────────┘     │
└────────────────────────────────────────────────│────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION                               │
│                                                                     │
│   transform.py                                                      │
│                                                                     │
│   ┌──────────────┐   ┌──────────────────┐   ┌──────────────────┐  │
│   │ 1. Nettoyage │ → │ 2. Agrégation    │ → │ 3. Anomalies     │  │
│   │ Nulls, types │   │ Min/Max/Moy/Std  │   │ Seuil > 5%       │  │
│   │ Doublons     │   │ Par crypto / 1h  │   │ Alertes générées │  │
│   └──────────────┘   └──────────────────┘   └──────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       ORCHESTRATION                                 │
│                                                                     │
│   pipeline.py (schedule)                                            │
│   Exécution automatique toutes les heures                           │
│   Retry automatique sur les étapes critiques (tenacity)             │
│   Logs structurés (loguru)                                          │
└─────────────────────────────────────────────────────────────────────┘
                                                 │
                                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       VISUALISATION                                 │
│                                                                     │
│   Streamlit Dashboard — http://localhost:8501                       │
│                                                                     │
│   • Prix en temps réel (4 cryptos)                                  │
│   • Graphique d'évolution 30 jours                                  │
│   • Statistiques agrégées (min, max, volatilité)                    │
│   • Tableau des alertes de prix                                     │
│   • Volume des échanges (24h)                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Cas d'usage

Surveillance automatique des prix de cryptomonnaies (Bitcoin, Ethereum, Solana, BNB).
Le pipeline détecte les variations anormales de prix (> 5% en 1h), génère des alertes,
et affiche toutes les statistiques dans un dashboard interactif mis à jour toutes les heures.

---

## Sources de données

| Source | Type | Fréquence | Données |
|--------|------|-----------|---------|
| CoinGecko API | Batch (REST) | Toutes les heures | Prix, market cap, volume, variation 24h |
| Kafka Producer | Streaming | Toutes les 5 secondes | Ticks de prix simulés en temps réel |

---

## Stack technologique

| Couche | Technologie | Justification |
|--------|-------------|---------------|
| Ingestion batch | Python + requests | Simple, fiable, CoinGecko gratuit sans clé API |
| Ingestion streaming | Apache Kafka | Standard industrie pour les flux temps réel |
| Data Lake | MinIO | Équivalent S3 local, gratuit, compatible Docker |
| Data Warehouse | PostgreSQL | Robuste, SQL standard, parfait pour l'analytique |
| Transformation | Pandas + SQLAlchemy | Manipulation de données intuitive en Python |
| Orchestration | schedule + tenacity | Léger, compatible Windows, retry automatique |
| Logs | loguru | Logs structurés colorés, simple à configurer |
| Visualisation | Streamlit + Plotly | Dashboard interactif sans frontend complexe |
| Tests | pytest | Framework standard Python, facile à lire |
| Conteneurisation | Docker + docker-compose | Reproductibilité totale de l'environnement |

---

## Structure du projet

```
crypto-pipeline/
├── ingestion/
│   ├── ingestion_batch.py      # Récupère les prix depuis CoinGecko
│   └── kafka_producer.py       # Producteur Kafka (streaming)
├── storage/
│   ├── minio_client.py         # Client MinIO (Data Lake)
│   └── db_init.sql             # Initialisation des tables PostgreSQL
├── transformation/
│   └── transform.py            # 3 transformations métier
├── orchestration/
│   └── pipeline.py             # Pipeline orchestré (schedule)
├── dashboard/
│   └── app.py                  # Dashboard Streamlit
├── scripts/
│   └── generate_data.py        # Générateur de données historiques
├── tests/
│   └── test_transform.py       # 14 tests unitaires
├── docker-compose.yml          # Infrastructure complète
├── requirements.txt            # Dépendances Python
└── README.md                   # Ce fichier
```

---

## Installation et déploiement local

### Prérequis

- Python 3.10+
- Docker Desktop
- Git

### Étapes

**1. Cloner le dépôt :**
```bash
git clone https://github.com/dali436/crypto-pipeline.git
cd crypto-pipeline
```

**2. Créer et activer l'environnement virtuel :**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Mac / Linux
source venv/bin/activate
```

**3. Installer les dépendances :**
```bash
pip install -r requirements.txt
```

**4. Lancer l'infrastructure Docker :**
```bash
docker-compose up -d
```

Attend 15 secondes que PostgreSQL soit prêt, puis vérifie :
```bash
docker-compose ps
```

**5. Générer les données historiques (30 jours) :**
```bash
python scripts/generate_data.py
```

**6. Lancer le pipeline (Terminal 1) :**
```bash
python orchestration/pipeline.py
```

**7. Lancer le dashboard (Terminal 2) :**
```bash
streamlit run dashboard/app.py
```

Ouvre **http://localhost:8501** dans ton navigateur.

---

## Accès aux interfaces

| Interface | URL | Login |
|-----------|-----|-------|
| Dashboard Streamlit | http://localhost:8501 | — |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8080 | — |
| PostgreSQL | localhost:5433 | postgres / postgres |

---

## Lancer les tests

```bash
pytest tests/test_transform.py -v
```

14 tests couvrant les 3 transformations métier :
- 5 tests sur le nettoyage
- 5 tests sur l'agrégation
- 4 tests sur la détection d'anomalies

---

## Fonctionnalités du pipeline

- Ingestion batch planifiée toutes les heures depuis CoinGecko API
- Ingestion streaming temps réel via Apache Kafka (ticks toutes les 5s)
- Stockage brut en JSON dans MinIO (Data Lake)
- Stockage structuré dans PostgreSQL (Data Warehouse)
- Nettoyage automatique des données (nulls, doublons, types)
- Agrégation des statistiques de marché (min, max, moyenne, volatilité)
- Détection d'anomalies de prix avec alertes (seuil configurable)
- Retry automatique sur les étapes critiques (3 tentatives)
- Logs structurés avec loguru
- Dashboard interactif avec rafraîchissement automatique

---

## Auteur

Projet Data Engineering — Pipeline Crypto Surveillance
