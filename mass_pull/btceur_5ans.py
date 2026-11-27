"""
BTCEUR : 5 ans de bougies journalières Binance, récupérées et traitées en local.

Adaptation en un seul script du projet dbt LioraCrypt (même logique, mêmes colonnes) :
    appel à l'API Binance (rôle de load/load_raw.py) -> btceur_raw_klines.csv
    stg_binance__klines                              -> btceur_stg_klines.csv
    fct_daily_klines                                 -> btceur_fct_daily_klines.csv
    mart_pair_overview                               -> btceur_mart_pair_overview.csv
    tests dbt -> contrôles affichés après chaque étape. Comme avec `dbt build`, un contrôle
                 bloquant en échec arrête le script ; le CSV de l'étape reste écrit.
fct_return_correlations n'est pas repris : il faut au moins deux paires de même devise.

Les CSV sont écrits dans le dossier courant, celui depuis lequel le script est lancé.

Installation : pip install pandas requests
Lancement    : python btceur_5ans.py
"""

import math
import sys
import time
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

# --- Paramètres -------------------------------------------------------------------------

SYMBOL = "BTCEUR"
ANNEES = 5
INTERVAL = "1d"              # fixe : les fenêtres en jours calendaires supposent du journalier
FRESHNESS_MAX_HOURS = 48     # seuil provisoire, comme la variable dbt freshness_max_hours
DOSSIER_SORTIE = Path.cwd()  # dossier d'exécution (Path(__file__).resolve().parent = celui du script)

# Hôte principal, puis hôte Binance réservé aux données de marché publiques, en secours
# (utile si api.binance.com est bloqué là où tourne le script : HTTP 451, 403...).
HOTES_API = ["api.binance.com", "data-api.binance.vision"]
LIMITE_PAR_APPEL = 1000      # maximum accepté par /api/v3/klines

# Seed dim_pairs.csv : symbol -> (base_asset, quote_asset, base_name)
DIM_PAIRS = {
    "BTCUSDT": ("BTC", "USDT", "Bitcoin"),
    "BTCEUR": ("BTC", "EUR", "Bitcoin"),
    "ETHUSDT": ("ETH", "USDT", "Ethereum"),
    "ETHEUR": ("ETH", "EUR", "Ethereum"),
    "SOLUSDT": ("SOL", "USDT", "Solana"),
    "SOLEUR": ("SOL", "EUR", "Solana"),
}

# Les 12 champs d'une bougie Binance, dans l'ordre renvoyé par l'API
CHAMPS_BINANCE = [
    "open_time", "open", "high", "low", "close", "volume", "close_time",
    "quote_asset_volume", "number_of_trades", "taker_buy_base_asset_volume",
    "taker_buy_quote_asset_volume", "ignore",
]

COLONNES_FCT = [
    "symbol", "trade_date", "open_time", "close_time",
    "open_price", "high_price", "low_price", "close_price",
    "base_volume", "quote_volume", "number_of_trades",
    "daily_return", "log_return",
    "ma_7d", "ma_30d", "nb_jours_ma_7d", "nb_jours_ma_30d",
    "volatility_30d_annualized", "nb_rendements_30d",
    "requested_at", "loaded_at",
]


# --- 1. Ingestion : couche RAW ----------------------------------------------------------

def appeler_binance(params):
    """GET /api/v3/klines sur le premier hôte qui répond. Renvoie (bougies, hôte)."""
    erreurs = []
    for hote in HOTES_API:
        try:
            reponse = requests.get(f"https://{hote}/api/v3/klines", params=params, timeout=30)
        except requests.RequestException as exc:
            erreurs.append(f"{hote} : {exc}")
            continue
        if reponse.status_code == 200:
            return reponse.json(), hote
        if reponse.status_code == 400:  # requête refusée (ex. symbole inconnu) : inutile d'insister
            sys.exit(f"Erreur Binance : {reponse.text}")
        erreurs.append(f"{hote} : HTTP {reponse.status_code} {reponse.text[:200]}")
    sys.exit("Impossible de joindre l'API Binance :\n  " + "\n  ".join(erreurs))


def recuperer_raw(debut, run_id):
    """Toutes les bougies depuis `debut`, par appels de 1000 (5 ans = 2 appels)."""
    pages = []
    start_ms = int(debut.timestamp() * 1000)
    while True:
        requested_at = pd.Timestamp.now(tz="UTC")
        bougies, hote = appeler_binance({"symbol": SYMBOL, "interval": INTERVAL,
                                         "startTime": start_ms, "limit": LIMITE_PAR_APPEL})
        if bougies:
            page = pd.DataFrame(bougies, columns=CHAMPS_BINANCE)
            page["requested_at"] = requested_at
            pages.append(page)
            premier = pd.to_datetime(bougies[0][0], unit="ms")
            dernier = pd.to_datetime(bougies[-1][0], unit="ms")
            print(f"  appel {len(pages)} ({hote}) : {len(bougies)} bougies "
                  f"du {premier:%Y-%m-%d} au {dernier:%Y-%m-%d}")
        if len(bougies) < LIMITE_PAR_APPEL:  # page incomplète : on est arrivé à aujourd'hui
            break
        start_ms = bougies[-1][0] + 1  # repartir juste après la dernière bougie reçue
        time.sleep(0.25)
    if not pages:
        sys.exit(f"Aucune bougie reçue pour {SYMBOL} depuis le {debut:%Y-%m-%d}.")

    raw = pd.concat(pages, ignore_index=True)
    raw.insert(0, "run_id", run_id)
    raw.insert(1, "symbol", SYMBOL)
    raw.insert(2, "kline_interval", INTERVAL)
    raw["loaded_at"] = pd.Timestamp.now(tz="UTC")
    return raw


# --- 2. stg_binance__klines -------------------------------------------------------------

def construire_staging(raw):
    """Typage, filtre des bougies non clôturées, dédoublonnage sur (symbol, open_time)."""
    # Binance envoie prix et volumes en texte. Les calculs se font en float ;
    # le CSV raw garde le texte exact (le rôle du NUMERIC dans BigQuery).
    stg = pd.DataFrame({
        "symbol": raw["symbol"],
        "kline_interval": raw["kline_interval"],
        "open_time": pd.to_datetime(raw["open_time"], unit="ms", utc=True),
        "close_time": pd.to_datetime(raw["close_time"], unit="ms", utc=True),
        "open_price": pd.to_numeric(raw["open"]),
        "high_price": pd.to_numeric(raw["high"]),
        "low_price": pd.to_numeric(raw["low"]),
        "close_price": pd.to_numeric(raw["close"]),
        "base_volume": pd.to_numeric(raw["volume"]),
        "quote_volume": pd.to_numeric(raw["quote_asset_volume"]),
        "number_of_trades": raw["number_of_trades"].astype("int64"),
        "taker_buy_base_volume": pd.to_numeric(raw["taker_buy_base_asset_volume"]),
        "taker_buy_quote_volume": pd.to_numeric(raw["taker_buy_quote_asset_volume"]),
        "run_id": raw["run_id"],
        "requested_at": raw["requested_at"],
        "loaded_at": raw["loaded_at"],
    })
    # Bougies clôturées seulement : Binance renvoie aussi la bougie du jour, encore en cours.
    # On compare au moment de l'appel (requested_at), comme dans le modèle dbt.
    stg = stg[stg["close_time"] < stg["requested_at"]]
    # Dédoublonnage : si une bougie revient plusieurs fois, on garde l'appel le plus récent.
    stg = stg.sort_values(["requested_at", "loaded_at"], ascending=False)
    stg = stg.drop_duplicates(["symbol", "open_time"])
    return stg.sort_values("open_time").reset_index(drop=True)


# --- 3. fct_daily_klines ----------------------------------------------------------------

def construire_fct(stg):
    """Une ligne par jour : rendements, moyennes mobiles et volatilité en jours calendaires."""
    fct = stg.copy()
    # Jour de la bougie (date d'ouverture, UTC), gardé en datetime pour les fenêtres glissantes
    fct["trade_date"] = fct["open_time"].dt.tz_localize(None).dt.normalize()

    # Rendements seulement si la veille existe (sinon vide : pas de rendement sur 2 jours)
    veille_presente = fct["trade_date"].diff() == pd.Timedelta(days=1)
    cloture_veille = fct["close_price"].shift(1)
    fct["daily_return"] = ((fct["close_price"] - cloture_veille) / cloture_veille).where(veille_presente)
    fct["log_return"] = np.log(fct["close_price"] / cloture_veille).where(veille_presente)

    # Fenêtres en JOURS CALENDAIRES : rolling("7D") couvre J-6 à J inclus, comme
    # `range between 6 preceding and current row` ; un jour manquant ne décale pas la fenêtre.
    par_jour = fct.set_index("trade_date")
    clotures_7j = par_jour["close_price"].rolling("7D")
    clotures_30j = par_jour["close_price"].rolling("30D")
    rendements_30j = par_jour["log_return"].rolling("30D")
    fct["ma_7d"] = clotures_7j.mean().to_numpy()
    fct["ma_30d"] = clotures_30j.mean().to_numpy()
    fct["nb_jours_ma_7d"] = clotures_7j.count().to_numpy().astype(int)
    fct["nb_jours_ma_30d"] = clotures_30j.count().to_numpy().astype(int)
    # Volatilité = écart-type (échantillon) des rendements log sur 30 jours × racine(365)
    fct["volatility_30d_annualized"] = rendements_30j.std().to_numpy() * math.sqrt(365)
    fct["nb_rendements_30d"] = rendements_30j.count().to_numpy().astype(int)

    fct["trade_date"] = fct["trade_date"].dt.date
    return fct[COLONNES_FCT]


# --- 4. mart_pair_overview --------------------------------------------------------------

def construire_overview(fct):
    """Une ligne : dernière bougie clôturée, rendements 1/7/30 jours, indicateurs, fraîcheur."""
    maintenant = pd.Timestamp.now(tz="UTC")
    derniere = fct.iloc[-1]
    clotures = fct.set_index("trade_date")["close_price"]

    def rendement_depuis(jours):
        # Clôture N jours calendaires avant la dernière bougie ; vide si ce jour manque
        ancienne = clotures.get(derniere["trade_date"] - timedelta(days=jours))
        return np.nan if ancienne is None else (derniere["close_price"] - ancienne) / ancienne

    base_asset, quote_asset, base_name = DIM_PAIRS[SYMBOL]
    return pd.DataFrame([{
        "symbol": SYMBOL,
        "base_asset": base_asset,
        "quote_asset": quote_asset,
        "base_name": base_name,
        "last_trade_date": derniere["trade_date"],
        "last_close_time": derniere["close_time"],
        "last_close_price": derniere["close_price"],
        "return_1d": derniere["daily_return"],
        "return_7d": rendement_depuis(7),
        "return_30d": rendement_depuis(30),
        "ma_7d": derniere["ma_7d"],
        "ma_30d": derniere["ma_30d"],
        "volatility_30d_annualized": derniere["volatility_30d_annualized"],
        "nb_rendements_30d": derniere["nb_rendements_30d"],
        "last_requested_at": derniere["requested_at"],
        "last_loaded_at": derniere["loaded_at"],
        "hours_since_last_close": int((maintenant - derniere["close_time"]).total_seconds() // 3600),
        "is_fresh": derniere["close_time"] >= maintenant - pd.Timedelta(hours=FRESHNESS_MAX_HOURS),
        "computed_at": maintenant,
    }])


# --- Contrôles : l'équivalent des tests dbt ---------------------------------------------

def controler(etape, controles):
    """Chaque contrôle compte les lignes en défaut (0 = OK), comme un test dbt.
    Un contrôle bloquant en échec arrête le script ; un non bloquant affiche un avertissement."""
    print(f"  contrôles {etape} :")
    bloque = False
    for description, nb_en_defaut, bloquant in controles:
        nb_en_defaut = int(nb_en_defaut)
        if nb_en_defaut == 0:
            statut = "OK"
        elif bloquant:
            statut, bloque = "ÉCHEC", True
        else:
            statut = "AVERTISSEMENT"
        detail = f" ({nb_en_defaut} en défaut)" if nb_en_defaut else ""
        print(f"    [{statut}] {description}{detail}")
    if bloque:
        sys.exit(f"Arrêt : contrôle bloquant en échec sur {etape} "
                 "(le CSV de cette étape est écrit pour inspection).")


def controles_staging(stg, debut):
    maintenant = pd.Timestamp.now(tz="UTC")
    premiere = stg["open_time"].min()
    obligatoires = ["symbol", "kline_interval", "open_time", "close_time", "open_price",
                    "high_price", "low_price", "close_price", "base_volume",
                    "run_id", "requested_at", "loaded_at"]
    ohlc_incoherent = (
        (stg["high_price"] < stg[["open_price", "close_price", "low_price"]].max(axis=1))
        | (stg["low_price"] > stg[["open_price", "close_price", "high_price"]].min(axis=1))
        | (stg["low_price"] <= 0)
        | (stg["base_volume"] < 0)
        | (stg["close_time"] <= stg["open_time"])
    )
    return [
        ("colonnes obligatoires renseignées", stg[obligatoires].isna().any(axis=1).sum(), True),
        ("symbol présent dans dim_pairs et intervalle 1d",
         (~stg["symbol"].isin(list(DIM_PAIRS)) | (stg["kline_interval"] != "1d")).sum(), True),
        ("unicité de (symbol, open_time)", stg.duplicated(["symbol", "open_time"]).sum(), True),
        ("cohérence OHLC, prix > 0, volume >= 0, close_time > open_time", ohlc_incoherent.sum(), True),
        (f"fraîcheur : dernière clôture il y a moins de {FRESHNESS_MAX_HOURS} h",
         stg["close_time"].max() < maintenant - pd.Timedelta(hours=FRESHNESS_MAX_HOURS), False),
        (f"historique complet depuis le {debut:%Y-%m-%d} (1re bougie : {premiere:%Y-%m-%d})",
         premiere > debut, False),
    ]


def controles_fct(fct):
    nb_jours_calendaires = (fct["trade_date"].max() - fct["trade_date"].min()).days + 1
    return [
        ("unicité de (symbol, trade_date)", fct.duplicated(["symbol", "trade_date"]).sum(), True),
        ("symbol présent dans dim_pairs", (~fct["symbol"].isin(list(DIM_PAIRS))).sum(), True),
        ("trade_date, close_price et loaded_at renseignés",
         fct[["trade_date", "close_price", "loaded_at"]].isna().any(axis=1).sum(), True),
        ("aucun jour manquant entre la 1re et la dernière bougie",
         nb_jours_calendaires - len(fct), False),
    ]


def controles_overview(overview):
    return [
        ("une ligne pour la paire", len(overview) != 1, True),
        ("dernière bougie renseignée (date, clôture, chargement, is_fresh)",
         overview[["last_trade_date", "last_close_time", "last_close_price",
                   "last_loaded_at", "is_fresh"]].isna().any(axis=1).sum(), True),
    ]


# --- Exécution --------------------------------------------------------------------------

def ecrire_csv(df, nom):
    chemin = DOSSIER_SORTIE / nom
    df.to_csv(chemin, index=False)  # pour Excel en français : ajouter sep=";", decimal=","
    print(f"  -> {chemin} ({len(df)} ligne{'s' if len(df) > 1 else ''})")


def main():
    if SYMBOL not in DIM_PAIRS:
        sys.exit(f"{SYMBOL} n'est pas dans dim_pairs : ajoute-le au dictionnaire DIM_PAIRS.")
    maintenant = pd.Timestamp.now(tz="UTC")
    debut = maintenant.normalize() - pd.DateOffset(years=ANNEES)
    run_id = f"{SYMBOL}_{maintenant:%Y%m%dT%H%M%SZ}"
    prefixe = SYMBOL.lower()

    print(f"1. Ingestion {SYMBOL} {INTERVAL} depuis le {debut:%Y-%m-%d} (run_id {run_id})")
    raw = recuperer_raw(debut, run_id)
    ecrire_csv(raw, f"{prefixe}_raw_klines.csv")

    print("2. stg_binance__klines")
    stg = construire_staging(raw)
    if stg.empty:
        sys.exit("Aucune bougie clôturée : rien à traiter.")
    ecrire_csv(stg, f"{prefixe}_stg_klines.csv")
    controler("staging", controles_staging(stg, debut))

    print("3. fct_daily_klines")
    fct = construire_fct(stg)
    ecrire_csv(fct, f"{prefixe}_fct_daily_klines.csv")
    controler("fct_daily_klines", controles_fct(fct))

    print("4. mart_pair_overview")
    overview = construire_overview(fct)
    ecrire_csv(overview, f"{prefixe}_mart_pair_overview.csv")
    controler("mart_pair_overview", controles_overview(overview))

    ligne = overview.iloc[0]
    print(f"\nTerminé : {len(fct)} jours traités. Dernière bougie clôturée le "
          f"{ligne['last_trade_date']}, clôture {ligne['last_close_price']:.2f} {ligne['quote_asset']}.")


if __name__ == "__main__":
    main()
