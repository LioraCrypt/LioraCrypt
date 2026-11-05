"""
fetch_crypto.py
---------------
Récupère les bougies (klines) de l'API publique Binance pour les paires du
registre (config.PAIRS) et les enregistre TELLES QUE REÇUES (approche ELT).

Endpoint : GET /api/v3/klines  (sans clé API, un seul symbole par appel)

Principes
- La réponse brute est conservée : les 12 champs de chaque bougie, sans filtre.
  Le nettoyage (typage, bougie en cours, déduplication) se fera plus tard (dbt).
- Un échec sur une paire fait échouer le script (code de sortie 1), sans
  empêcher l'enregistrement des paires qui ont réussi.
- 429 : on respecte l'en-tête Retry-After. 418 (bannissement IP) : on arrête tout.
- Erreurs 4xx (symbole invalide...) : pas de nouvelle tentative.

Utilisation
    python fetch_crypto.py                          # dernières 1000 bougies de chaque paire
    python fetch_crypto.py --since 2017-08-17       # historique complet depuis une date
    python fetch_crypto.py --pairs BTCUSDT ETHEUR   # sous-ensemble du registre
    python fetch_crypto.py --preview                # résumé lisible après récupération

Sortie : un fichier JSON Lines par exécution (une ligne = une paire) dans data/raw/.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

import config

log = logging.getLogger("liora.ingestion")

# Ordre des 12 champs renvoyés par Binance pour chaque bougie.
KLINE_FIELDS = (
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "number_of_trades",
    "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
)


class FetchError(Exception):
    """Échec de récupération pour une paire."""


class RateLimitBan(FetchError):
    """Binance a répondu 418 (IP bannie) : il faut arrêter toute la session."""


# ---------------------------------------------------------------------------
# Appel HTTP robuste
# ---------------------------------------------------------------------------

def _backoff(attempt: int) -> float:
    return 2.0 ** attempt  # 2 s, 4 s, 8 s...


def _retry_after(response: requests.Response, attempt: int) -> float:
    header = response.headers.get("Retry-After")
    try:
        return float(header) if header is not None else _backoff(attempt)
    except ValueError:
        return _backoff(attempt)


def _get(session: requests.Session, url: str, params: dict):
    """GET avec nouvelles tentatives. Renvoie le JSON ou lève FetchError."""
    last_error = "inconnue"

    for attempt in range(1, config.MAX_RETRIES + 1):
        wait = None
        try:
            response = session.get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = f"réseau : {exc}"
            wait = _backoff(attempt)
        else:
            status = response.status_code
            if status == 200:
                return response.json()
            if status == 418:
                raise RateLimitBan(
                    f"HTTP 418 : IP bannie par Binance (Retry-After="
                    f"{response.headers.get('Retry-After')}). Arrêt."
                )
            if status == 429:
                wait = _retry_after(response, attempt)
                last_error = f"HTTP 429 (Retry-After {wait:.0f}s)"
                if wait > config.MAX_RETRY_AFTER:
                    raise FetchError(
                        f"HTTP 429 avec une attente de {wait:.0f}s : trop longue, abandon."
                    )
            elif 500 <= status < 600:
                last_error = f"HTTP {status}"
                wait = _backoff(attempt)
            else:
                # Autres 4xx (ex. 400 symbole invalide) : réessayer ne servirait à rien.
                raise FetchError(f"HTTP {status} : {response.text[:200]}")

        log.warning("Tentative %d/%d échouée (%s)", attempt, config.MAX_RETRIES, last_error)
        if attempt < config.MAX_RETRIES and wait is not None:
            time.sleep(wait)

    raise FetchError(f"{config.MAX_RETRIES} tentatives échouées ({last_error})")


# ---------------------------------------------------------------------------
# Récupération d'une paire
# ---------------------------------------------------------------------------

def fetch_pair(
    session: requests.Session,
    symbol: str,
    interval: str,
    since_ms: int | None = None,
) -> list:
    """Renvoie la liste brute des bougies (chaque bougie = liste de 12 valeurs).

    Sans `since_ms` : un seul appel, les `KLINES_LIMIT` dernières bougies.
    Avec `since_ms` : pagination à partir de cette date (UTC, en millisecondes).
    """
    url = f"{config.BASE_URL}/api/v3/klines"
    limit = config.KLINES_LIMIT

    if since_ms is None:
        klines = _get(session, url, {"symbol": symbol, "interval": interval, "limit": limit})
    else:
        klines, start = [], since_ms
        while True:
            batch = _get(
                session, url,
                {"symbol": symbol, "interval": interval, "startTime": start, "limit": limit},
            )
            if not batch:
                break
            klines.extend(batch)
            if len(batch) < limit:
                break
            start = batch[-1][0] + 1  # open_time de la dernière bougie + 1 ms
            time.sleep(config.PAUSE_BETWEEN_CALLS)

    if not klines:
        raise FetchError("aucune bougie renvoyée")
    bad = [row for row in klines if len(row) != len(KLINE_FIELDS)]
    if bad:
        raise FetchError(f"format inattendu : {len(bad)} bougie(s) sans {len(KLINE_FIELDS)} champs")
    return klines


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def run(symbols: list[str], interval: str, since_ms: int | None,
        out_dir: Path, preview: bool = False) -> int:
    """Récupère les paires demandées, écrit le fichier brut. Renvoie le code de sortie."""
    run_ts = datetime.now(timezone.utc)
    records, failures = [], {}
    banned = False

    with requests.Session() as session:
        for symbol in symbols:
            if banned:
                failures[symbol] = "non tentée (IP bannie)"
                continue
            try:
                klines = fetch_pair(session, symbol, interval, since_ms)
            except RateLimitBan as exc:
                log.error("[%s] %s", symbol, exc)
                failures[symbol] = str(exc)
                banned = True
                continue
            except FetchError as exc:
                log.error("[%s] %s", symbol, exc)
                failures[symbol] = str(exc)
                continue

            log.info("[%s] %d bougies récupérées", symbol, len(klines))
            records.append({
                "source": "binance_spot_klines",
                "symbol": symbol,
                "interval": interval,
                "requested_at": run_ts.isoformat(),
                "since": _iso(since_ms) if since_ms else None,
                "fields": list(KLINE_FIELDS),
                "row_count": len(klines),
                "payload": klines,  # réponse brute, non modifiée
            })
            time.sleep(config.PAUSE_BETWEEN_CALLS)

    if records:
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / f"klines_{interval}_{run_ts:%Y%m%dT%H%M%SZ}.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        log.info("Fichier écrit : %s (%d paire(s))", path, len(records))

    if preview:
        _print_preview(records)

    if failures:
        log.error("ÉCHEC pour %d paire(s) : %s", len(failures), ", ".join(failures))
        return 1
    return 0


def _print_preview(records: list[dict]) -> None:
    """Résumé lisible. Indique aussi si la dernière bougie est encore en cours."""
    now_ms = int(time.time() * 1000)
    print(f"\n{'Paire':<9} {'Bougies':>7}  {'Première':<17} {'Dernière':<17} {'Close':>14}  En cours ?")
    for rec in records:
        first, last = rec["payload"][0], rec["payload"][-1]
        in_progress = int(last[6]) > now_ms  # close_time dans le futur = bougie ouverte
        print(f"{rec['symbol']:<9} {rec['row_count']:>7}  {_iso(first[0])[:16]:<17} "
              f"{_iso(last[0])[:16]:<17} {float(last[4]):>14,.2f}  {'oui' if in_progress else 'non'}")


# ---------------------------------------------------------------------------
# Ligne de commande
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingestion brute des klines Binance.")
    parser.add_argument("--pairs", nargs="+", metavar="SYMBOL",
                        help="sous-ensemble du registre (défaut : toutes les paires)")
    parser.add_argument("--since", metavar="YYYY-MM-DD",
                        help="date de début (UTC) pour récupérer tout l'historique")
    parser.add_argument("--interval", default=config.INTERVAL, help="intervalle (défaut : 1d)")
    parser.add_argument("--out", default=config.OUTPUT_DIR, help="dossier de sortie")
    parser.add_argument("--preview", action="store_true", help="affiche un résumé")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Liste blanche : seuls les symboles du registre sont acceptés.
    registry = [pair["symbol"] for pair in config.PAIRS]
    symbols = [s.upper() for s in args.pairs] if args.pairs else registry
    unknown = [s for s in symbols if s not in registry]
    if unknown:
        parser.error(f"symbole(s) absent(s) du registre : {', '.join(unknown)}")

    since_ms = None
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            parser.error("--since doit être au format YYYY-MM-DD")
        since_ms = int(since.timestamp() * 1000)

    return run(symbols, args.interval, since_ms, Path(args.out), args.preview)


if __name__ == "__main__":
    sys.exit(main())
