"""
Dashboard - Crypto Pipeline
Visualisation interactive des données crypto en temps réel
Affiche : prix live, historique 30j, alertes, statistiques

Lancer avec :
    streamlit run dashboard/app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import datetime


# Configuration de la page


st.set_page_config(
    page_title="Crypto Pipeline Dashboard",
    page_icon="",
    layout="wide",
)

import os
DB_URL = os.getenv("DB_URL", "postgresql+pg8000://postgres:postgres@localhost:5433/crypto_db")

CRYPTO_COLORS = {
    "bitcoin":     "#F7931A",
    "ethereum":    "#627EEA",
    "solana":      "#9945FF",
    "binancecoin": "#F3BA2F",
}

# Connexion PostgreSQL
@st.cache_resource
def get_engine():
    return create_engine(DB_URL)


# Chargement des données
@st.cache_data(ttl=60)
def load_prices():
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM prices ORDER BY ingested_at DESC LIMIT 10000",
        engine
    )

@st.cache_data(ttl=60)
def load_latest_prices():
    engine = get_engine()
    return pd.read_sql(
        """
        SELECT DISTINCT ON (coin)
            coin, price_usd, market_cap_usd, volume_24h_usd, change_24h_pct, ingested_at
        FROM prices
        ORDER BY coin, ingested_at DESC
        """,
        engine
    )

@st.cache_data(ttl=60)
def load_market_stats():
    engine = get_engine()
    return pd.read_sql(
        """
        SELECT DISTINCT ON (coin, period)
            coin, period, price_min, price_max, price_avg, price_stddev, nb_records, computed_at
        FROM market_stats
        ORDER BY coin, period, computed_at DESC
        """,
        engine
    )

@st.cache_data(ttl=60)
def load_alerts():
    engine = get_engine()
    return pd.read_sql(
        "SELECT * FROM alerts ORDER BY triggered_at DESC LIMIT 100",
        engine
    )


# ══════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════

st.title(" Crypto Pipeline Dashboard")
st.caption(f"Dernière mise à jour : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if st.button(" Rafraîchir les données"):
    st.cache_data.clear()
    st.rerun()

st.divider()


# ══════════════════════════════════════════════════════════════
# SECTION 1 — Prix en temps réel
# ══════════════════════════════════════════════════════════════

st.subheader("Prix actuels")

latest = load_latest_prices()

if not latest.empty:
    cols = st.columns(4)
    for i, (_, row) in enumerate(latest.iterrows()):
        with cols[i]:
            change = row.get("change_24h_pct", 0) or 0
            delta_color = "normal" if change >= 0 else "inverse"
            st.metric(
                label=row["coin"].upper(),
                value=f"${row['price_usd']:,.2f}",
                delta=f"{change:+.2f}% (24h)",
                delta_color=delta_color,
            )
else:
    st.warning("Aucune donnée de prix disponible.")

st.divider()


# ══════════════════════════════════════════════════════════════
# SECTION 2 — Graphique d'évolution des prix
# ══════════════════════════════════════════════════════════════

st.subheader("Évolution des prix — 30 derniers jours")

prices_df = load_prices()

if not prices_df.empty:
    prices_df["ingested_at"] = pd.to_datetime(prices_df["ingested_at"])

    cryptos_disponibles = sorted(prices_df["coin"].unique().tolist())
    cryptos_selectionnees = st.multiselect(
        "Sélectionner les cryptos à afficher :",
        options=cryptos_disponibles,
        default=cryptos_disponibles,
    )

    df_filtered = prices_df[prices_df["coin"].isin(cryptos_selectionnees)]

    if not df_filtered.empty:
        fig = px.line(
            df_filtered,
            x="ingested_at",
            y="price_usd",
            color="coin",
            color_discrete_map=CRYPTO_COLORS,
            labels={"ingested_at": "Date", "price_usd": "Prix (USD)", "coin": "Crypto"},
            title="Évolution des prix",
        )
        fig.update_layout(
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            height=450,
        )
        st.plotly_chart(fig, use_container_width=True)

st.divider()


# ══════════════════════════════════════════════════════════════
# SECTION 3 — Statistiques agrégées
# ══════════════════════════════════════════════════════════════

st.subheader("Statistiques de marché")

stats_df = load_market_stats()

if not stats_df.empty:
    periode = st.radio(
        "Période :",
        options=["1h", "24h"],
        horizontal=True,
    )

    stats_filtered = stats_df[stats_df["period"] == periode]

    if not stats_filtered.empty:
        col1, col2 = st.columns(2)

        with col1:
            display_df = stats_filtered[[
                "coin", "price_min", "price_max", "price_avg", "price_stddev", "nb_records"
            ]].copy()
            display_df.columns = ["Crypto", "Min ($)", "Max ($)", "Moyenne ($)", "Volatilité", "Relevés"]
            display_df = display_df.round(2)
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        with col2:
            fig_vol = px.bar(
                stats_filtered,
                x="coin",
                y="price_stddev",
                color="coin",
                color_discrete_map=CRYPTO_COLORS,
                labels={"coin": "Crypto", "price_stddev": "Volatilité (écart-type)"},
                title=f"Volatilité sur {periode}",
            )
            fig_vol.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig_vol, use_container_width=True)

st.divider()


# ══════════════════════════════════════════════════════════════
# SECTION 4 — Alertes détectées
# ══════════════════════════════════════════════════════════════

st.subheader("Alertes de prix détectées")

alerts_df = load_alerts()

if not alerts_df.empty:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total alertes", len(alerts_df))
    with col2:
        spikes = len(alerts_df[alerts_df["alert_type"] == "price_spike"])
        st.metric("Hausses détectées ", spikes)
    with col3:
        drops = len(alerts_df[alerts_df["alert_type"] == "price_drop"])
        st.metric("Baisses détectées ", drops)

    alerts_df["triggered_at"] = pd.to_datetime(alerts_df["triggered_at"])
    display_alerts = alerts_df[[
        "triggered_at", "coin", "alert_type", "price_usd", "change_pct", "message"
    ]].copy()
    display_alerts.columns = ["Heure", "Crypto", "Type", "Prix ($)", "Variation (%)", "Message"]
    display_alerts["Prix ($)"]      = display_alerts["Prix ($)"].round(2)
    display_alerts["Variation (%)"] = display_alerts["Variation (%)"].round(2)

    # Coloration du tableau des alertes
    def color_alert(row):
        """
        Colore uniquement la colonne Variation (%) :
          - Vert gras  → price_spike  (hausse)
          - Rouge gras → price_drop   (baisse)
        Le reste de la ligne reste neutre.
        """
        colors = [""] * len(row)
        variation_idx = row.index.get_loc("Variation (%)")
        type_idx      = row.index.get_loc("Type")

        if row["Type"] == "price_spike":
            # Colonne variation : fond vert clair, texte vert foncé, gras
            colors[variation_idx] = (
                "background-color: #e8f5e9; "
                "color: #1b5e20; "
                "font-weight: bold"
            )
            # Colonne type : fond vert très léger
            colors[type_idx] = "background-color: #f1f8e9; color: #33691e"
        else:
            # Colonne variation : fond rouge clair, texte rouge foncé, gras
            colors[variation_idx] = (
                "background-color: #ffebee; "
                "color: #b71c1c; "
                "font-weight: bold"
            )
            # Colonne type : fond rouge très léger
            colors[type_idx] = "background-color: #fce4ec; color: #880e4f"

        return colors

    st.dataframe(
        display_alerts.style.apply(color_alert, axis=1),
        use_container_width=True,
        hide_index=True,
        height=350,
    )

else:
    st.info("Aucune alerte détectée pour le moment.")

st.divider()


# ══════════════════════════════════════════════════════════════
# SECTION 5 — Volume des échanges
# ══════════════════════════════════════════════════════════════


st.subheader("Volume des échanges (24h)")

if not latest.empty and "volume_24h_usd" in latest.columns:
    latest_clean = latest.dropna(subset=["volume_24h_usd"])
    if not latest_clean.empty:
        fig_pie = px.pie(
            latest_clean,
            names="coin",
            values="volume_24h_usd",
            color="coin",
            color_discrete_map=CRYPTO_COLORS,
            title="Répartition du volume (24h)",
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig_pie, use_container_width=True)


# Footer

st.divider()
st.caption("Crypto Pipeline Dashboard — Data Engineering Project | Sources : CoinGecko API")
