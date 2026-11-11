"""
load_raw.py
-----------
Charge les fichiers JSON Lines produits par fetch_crypto.py dans BigQuery
(couche RAW, table raw.klines_raw).

Principes
- RAW reste intact : la réponse Binance est stockée telle quelle (texte JSON dans
  la colonne payload). Aucune règle métier ici : dédoublonnage, bougie en cours
  et typage se font plus tard, dans le staging dbt.
- On ajoute seulement des métadonnées : run_id (nom du fichier) et loaded_at.
- Ajout seul (WRITE_APPEND) par chargement par lot (load job), pas de streaming.
- Garde-fou : un fichier déjà chargé (même run_id) est refusé, sauf --force.
- Aucune clé dans le dépôt : authentification par les identifiants locaux
  (gcloud auth application-default login). Le projet vient de LIORA_GCP_PROJECT.

Utilisation
    export LIORA_GCP_PROJECT=<identifiant-du-projet>
    python load_raw.py --dry-run data/raw/klines_1d_XXXX.jsonl   # ne touche pas BigQuery
    python load_raw.py data/raw/klines_1d_XXXX.jsonl
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("liora.load")

DATASET = "raw"
TABLE = "klines_raw"
LOCATION = "EU"
N_FIELDS = 12  # champs par bougie Binance

# (nom, type, mode, description). "interval" est un mot réservé BigQuery :
# la colonne s'appelle kline_interval.
SCHEMA = [
    ("run_id", "STRING", "REQUIRED", "Nom du fichier source : identifie l'exécution d'ingestion"),
    ("source", "STRING", "REQUIRED", "Origine des données"),
    ("symbol", "STRING", "REQUIRED", "Paire Binance, ex. BTCUSDT"),
    ("kline_interval", "STRING", "REQUIRED", "Intervalle des bougies, ex. 1d"),
    ("requested_at", "TIMESTAMP", "REQUIRED", "Date de l'appel à l'API Binance"),
    ("since", "STRING", "NULLABLE", "Date de début demandée (--since), sinon vide"),
    ("row_count", "INTEGER", "REQUIRED", "Nombre de bougies dans payload"),
    ("fields", "STRING", "REQUIRED", "Noms des champs d'une bougie (texte JSON)"),
    ("payload", "STRING", "REQUIRED", "Réponse brute Binance : tableau de bougies (texte JSON)"),
    ("loaded_at", "TIMESTAMP", "REQUIRED", "Date du chargement dans BigQuery"),
]

REQUIRED_KEYS = ("source", "symbol", "interval", "requested_at", "row_count", "fields", "payload")


def build_rows(path: Path, loaded_at: datetime) -> list[dict]:
    """Lit un fichier JSONL et renvoie les lignes à charger. Lève ValueError si le
    fichier est malformé : mieux vaut échouer que charger un RAW cassé."""
    run_id = path.name
    rows = []
    with path.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name} ligne {number} : JSON invalide ({exc})") from exc
            missing = [k for k in REQUIRED_KEYS if k not in rec]
            if missing:
                raise ValueError(f"{path.name} ligne {number} : clés manquantes {missing}")
            payload = rec["payload"]
            if not isinstance(payload, list) or not payload:
                raise ValueError(f"{path.name} ligne {number} : payload vide ou invalide")
            if rec["row_count"] != len(payload):
                raise ValueError(
                    f"{path.name} ligne {number} : row_count={rec['row_count']} "
                    f"mais {len(payload)} bougies dans payload")
            bad = [c for c in payload if not isinstance(c, list) or len(c) != N_FIELDS]
            if bad:
                raise ValueError(f"{path.name} ligne {number} : {len(bad)} bougie(s) sans {N_FIELDS} champs")
            rows.append({
                "run_id": run_id,
                "source": rec["source"],
                "symbol": rec["symbol"],
                "kline_interval": rec["interval"],
                "requested_at": rec["requested_at"],
                "since": rec.get("since"),
                "row_count": rec["row_count"],
                "fields": json.dumps(rec["fields"], ensure_ascii=False),
                "payload": json.dumps(payload, ensure_ascii=False),
                "loaded_at": loaded_at.isoformat(),
            })
    if not rows:
        raise ValueError(f"{path.name} : fichier vide")
    return rows


# ---------------------------------------------------------------------------
# BigQuery (import tardif : les tests hors ligne n'ont pas besoin de la bibliothèque)
# ---------------------------------------------------------------------------

def _bq_schema():
    from google.cloud import bigquery
    return [bigquery.SchemaField(n, t, mode=m, description=d) for n, t, m, d in SCHEMA]


def ensure_dataset_and_table(client, project: str) -> str:
    from google.cloud import bigquery
    dataset = bigquery.Dataset(f"{project}.{DATASET}")
    dataset.location = LOCATION          # pas d'expiration par défaut des tables
    client.create_dataset(dataset, exists_ok=True)
    table_id = f"{project}.{DATASET}.{TABLE}"
    client.create_table(bigquery.Table(table_id, schema=_bq_schema()), exists_ok=True)
    return table_id


def already_loaded(client, table_id: str, run_id: str) -> bool:
    from google.cloud import bigquery
    config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", run_id)],
        maximum_bytes_billed=100 * 1024 * 1024,   # plafond : la requête échoue au-delà
    )
    sql = f"SELECT COUNT(*) AS n FROM `{table_id}` WHERE run_id = @run_id"
    return next(iter(client.query(sql, job_config=config).result())).n > 0


def load_rows(client, table_id: str, rows: list[dict]) -> None:
    from google.cloud import bigquery
    config = bigquery.LoadJobConfig(
        schema=_bq_schema(),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    client.load_table_from_json(rows, table_id, job_config=config, location=LOCATION).result()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Charge les klines JSONL dans BigQuery (RAW).")
    parser.add_argument("files", nargs="+", type=Path, help="fichier(s) .jsonl produits par fetch_crypto.py")
    parser.add_argument("--project", default=os.getenv("LIORA_GCP_PROJECT"),
                        help="identifiant du projet Google Cloud (défaut : variable LIORA_GCP_PROJECT)")
    parser.add_argument("--dry-run", action="store_true", help="valide les fichiers sans toucher à BigQuery")
    parser.add_argument("--force", action="store_true", help="recharge un fichier déjà chargé (crée des doublons dans RAW)")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    loaded_at = datetime.now(timezone.utc)
    try:
        batches = [(path, build_rows(path, loaded_at)) for path in args.files]
    except (OSError, ValueError) as exc:
        log.error("%s", exc)
        return 1

    for path, rows in batches:
        log.info("%s : %d ligne(s), %d bougies", path.name, len(rows), sum(r["row_count"] for r in rows))
    if args.dry_run:
        log.info("Dry-run : rien n'a été envoyé à BigQuery.")
        return 0

    if not args.project:
        log.error("Projet inconnu : définis LIORA_GCP_PROJECT ou utilise --project.")
        return 1

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project, location=LOCATION)
    table_id = ensure_dataset_and_table(client, args.project)

    for path, rows in batches:
        if not args.force and already_loaded(client, table_id, path.name):
            log.error("%s est déjà chargé dans %s (utilise --force pour forcer).", path.name, table_id)
            return 1
        load_rows(client, table_id, rows)
        log.info("%s chargé dans %s", path.name, table_id)
    return 0


if __name__ == "__main__":
    sys.exit(main())
