"""
config.py
---------
Configuration centrale de l'ingestion du Liora project.

Le registre PAIRS est la source unique des paires à récupérer : pour ajouter
ou retirer une valeur, on modifie uniquement cette liste.
"""

import os

# ---------------------------------------------------------------------------
# Registre des paires (6 paires : BTC, ETH, SOL en USDT et en EUR)
# ---------------------------------------------------------------------------

PAIRS = [
    {"symbol": "BTCUSDT", "asset": "BTC", "quote": "USDT"},
    {"symbol": "BTCEUR",  "asset": "BTC", "quote": "EUR"},
    {"symbol": "ETHUSDT", "asset": "ETH", "quote": "USDT"},
    {"symbol": "ETHEUR",  "asset": "ETH", "quote": "EUR"},
    {"symbol": "SOLUSDT", "asset": "SOL", "quote": "USDT"},
    {"symbol": "SOLEUR",  "asset": "SOL", "quote": "EUR"},
]

# ---------------------------------------------------------------------------
# API Binance (endpoints publics, aucune clé nécessaire)
# ---------------------------------------------------------------------------

# Surchargeable par variable d'environnement. Binance propose aussi un domaine
# réservé aux données de marché publiques : https://data-api.binance.vision
BASE_URL = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")

INTERVAL = os.getenv("KLINES_INTERVAL", "1d")   # bougies journalières
KLINES_LIMIT = 1000                              # maximum par appel pour /klines

# ---------------------------------------------------------------------------
# Robustesse des appels HTTP
# ---------------------------------------------------------------------------

REQUEST_TIMEOUT = 10         # secondes
MAX_RETRIES = 3              # tentatives par appel
MAX_RETRY_AFTER = 60         # on n'attend jamais plus de 60 s sur un 429
PAUSE_BETWEEN_CALLS = 0.25   # pause de courtoisie entre deux appels (secondes)

# ---------------------------------------------------------------------------
# Sortie
# ---------------------------------------------------------------------------

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "data/raw")
