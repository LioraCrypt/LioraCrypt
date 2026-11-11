"""Tests hors ligne de load_raw.py : aucun appel BigQuery.

Lancer :  python -m unittest test_load_raw -v
"""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import load_raw as lr

NOW = datetime(2026, 11, 11, 8, 0, tzinfo=timezone.utc)


def candle(open_time):
    return [open_time, "1", "2", "0.5", "1.5", "10", open_time + 86_399_999, "15", 5, "4", "6", "0"]


def record(**over):
    rec = {"source": "binance_spot_klines", "symbol": "BTCUSDT", "interval": "1d",
           "requested_at": "2026-11-11T07:00:00+00:00", "since": None,
           "fields": ["open_time"] * 12, "row_count": 2, "payload": [candle(1), candle(2)]}
    rec.update(over)
    return rec


def write(tmp, *records, name="klines_1d_test.jsonl"):
    path = Path(tmp) / name
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")
    return path


class BuildRowsTests(unittest.TestCase):

    def test_fichier_valide(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = lr.build_rows(write(tmp, record(), record(symbol="ETHEUR")), NOW)
        self.assertEqual([r["symbol"] for r in rows], ["BTCUSDT", "ETHEUR"])
        self.assertEqual(rows[0]["run_id"], "klines_1d_test.jsonl")
        self.assertEqual(rows[0]["loaded_at"], NOW.isoformat())

    def test_payload_conserve_tel_quel(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = lr.build_rows(write(tmp, record()), NOW)
        self.assertEqual(json.loads(rows[0]["payload"]), [candle(1), candle(2)])

    def test_les_cles_correspondent_au_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            rows = lr.build_rows(write(tmp, record()), NOW)
        self.assertEqual(set(rows[0]), {name for name, *_ in lr.SCHEMA})

    def test_row_count_incoherent(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                lr.build_rows(write(tmp, record(row_count=5)), NOW)

    def test_bougie_sans_12_champs(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                lr.build_rows(write(tmp, record(payload=[[1, 2, 3]], row_count=1)), NOW)

    def test_payload_vide(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                lr.build_rows(write(tmp, record(payload=[], row_count=0)), NOW)

    def test_cle_manquante(self):
        rec = record()
        del rec["symbol"]
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                lr.build_rows(write(tmp, rec), NOW)

    def test_json_invalide(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.jsonl"
            path.write_text("pas du json\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                lr.build_rows(path, NOW)

    def test_fichier_vide(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.jsonl"
            path.write_text("", encoding="utf-8")
            with self.assertRaises(ValueError):
                lr.build_rows(path, NOW)


class MainTests(unittest.TestCase):

    def test_dry_run_ne_touche_pas_bigquery(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write(tmp, record())
            self.assertEqual(lr.main(["--dry-run", str(path)]), 0)

    def test_fichier_invalide_donne_code_1(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = write(tmp, record(row_count=9))
            self.assertEqual(lr.main(["--dry-run", str(path)]), 1)

    def test_sans_projet_donne_code_1(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            path = write(tmp, record())
            os.environ.pop("LIORA_GCP_PROJECT", None)
            self.assertEqual(lr.main([str(path)]), 1)

    def test_pas_de_colonne_interval(self):
        self.assertNotIn("interval", [name for name, *_ in lr.SCHEMA])


if __name__ == "__main__":
    unittest.main()
