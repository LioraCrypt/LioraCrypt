"""
Tests hors ligne de fetch_crypto.py : aucun appel réseau, les réponses de
Binance sont simulées.

Lancer :  python -m unittest test_fetch_crypto -v
"""

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import requests

import config
import fetch_crypto as fk


def kline(open_time: int, close: str = "100.0") -> list:
    """Une bougie au format Binance (12 champs)."""
    return [open_time, "99.0", "101.0", "98.0", close, "10.0",
            open_time + 86_399_999, "1000.0", 50, "5.0", "500.0", "0"]


class FakeResponse:
    def __init__(self, status=200, body=None, headers=None):
        self.status_code = status
        self._body = body if body is not None else []
        self.headers = headers or {}
        self.text = json.dumps(self._body)

    def json(self):
        return self._body


class FakeSession:
    """Rejoue une liste de réponses (ou d'exceptions) dans l'ordre."""

    def __init__(self, script):
        self.script = list(script)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(dict(params))
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@mock.patch("fetch_crypto.time.sleep", lambda *_: None)
class FetchTests(unittest.TestCase):

    def test_succes_simple(self):
        session = FakeSession([FakeResponse(200, [kline(1), kline(2)])])
        rows = fk.fetch_pair(session, "BTCUSDT", "1d")
        self.assertEqual(len(rows), 2)
        self.assertEqual(session.calls[0]["limit"], config.KLINES_LIMIT)

    def test_429_respecte_retry_after_puis_reussit(self):
        session = FakeSession([FakeResponse(429, headers={"Retry-After": "5"}),
                               FakeResponse(200, [kline(1)])])
        with mock.patch("fetch_crypto.time.sleep") as sleep:
            rows = fk.fetch_pair(session, "BTCUSDT", "1d")
        self.assertEqual(len(rows), 1)
        sleep.assert_any_call(5.0)

    def test_429_avec_attente_trop_longue_abandonne(self):
        session = FakeSession([FakeResponse(429, headers={"Retry-After": "600"})])
        with self.assertRaises(fk.FetchError):
            fk.fetch_pair(session, "BTCUSDT", "1d")

    def test_418_arrete_tout(self):
        session = FakeSession([FakeResponse(418, headers={"Retry-After": "3600"})])
        with self.assertRaises(fk.RateLimitBan):
            fk.fetch_pair(session, "BTCUSDT", "1d")

    def test_400_pas_de_nouvelle_tentative(self):
        session = FakeSession([FakeResponse(400, {"msg": "Invalid symbol"})])
        with self.assertRaises(fk.FetchError):
            fk.fetch_pair(session, "XXXUSDT", "1d")
        self.assertEqual(len(session.calls), 1)

    def test_erreur_reseau_reessaie_puis_reussit(self):
        session = FakeSession([requests.ConnectionError("boom"),
                               FakeResponse(200, [kline(1)])])
        self.assertEqual(len(fk.fetch_pair(session, "BTCUSDT", "1d")), 1)

    def test_echec_apres_toutes_les_tentatives(self):
        session = FakeSession([FakeResponse(500)] * config.MAX_RETRIES)
        with self.assertRaises(fk.FetchError):
            fk.fetch_pair(session, "BTCUSDT", "1d")
        self.assertEqual(len(session.calls), config.MAX_RETRIES)

    def test_pagination_depuis_une_date(self):
        limit = config.KLINES_LIMIT
        first = [kline(i) for i in range(limit)]          # lot complet
        second = [kline(limit + i) for i in range(10)]    # lot final, plus court
        session = FakeSession([FakeResponse(200, first), FakeResponse(200, second)])
        rows = fk.fetch_pair(session, "BTCUSDT", "1d", since_ms=0)
        self.assertEqual(len(rows), limit + 10)
        self.assertEqual(session.calls[1]["startTime"], first[-1][0] + 1)

    def test_reponse_vide_est_une_erreur(self):
        session = FakeSession([FakeResponse(200, [])])
        with self.assertRaises(fk.FetchError):
            fk.fetch_pair(session, "BTCUSDT", "1d")

    def test_format_inattendu_est_une_erreur(self):
        session = FakeSession([FakeResponse(200, [[1, "2", "3"]])])
        with self.assertRaises(fk.FetchError):
            fk.fetch_pair(session, "BTCUSDT", "1d")


@mock.patch("fetch_crypto.time.sleep", lambda *_: None)
class RunTests(unittest.TestCase):

    def _run(self, script, symbols):
        with tempfile.TemporaryDirectory() as tmp:
            session = FakeSession(script)
            with mock.patch("fetch_crypto.requests.Session") as session_cls:
                session_cls.return_value.__enter__.return_value = session
                code = fk.run(symbols, "1d", None, Path(tmp))
            files = list(Path(tmp).glob("*.jsonl"))
            lines = files[0].read_text().splitlines() if files else []
        return code, lines

    def test_toutes_les_paires_ok(self):
        code, lines = self._run([FakeResponse(200, [kline(1)])] * 2, ["BTCUSDT", "ETHEUR"])
        self.assertEqual(code, 0)
        self.assertEqual(len(lines), 2)
        record = json.loads(lines[0])
        self.assertEqual(record["symbol"], "BTCUSDT")
        self.assertEqual(record["payload"], [kline(1)])  # brut, non modifié
        self.assertEqual(len(record["fields"]), 12)

    def test_une_paire_en_echec_donne_code_1_mais_ecrit_les_autres(self):
        code, lines = self._run([FakeResponse(200, [kline(1)]),
                                 FakeResponse(400, {"msg": "bad"})], ["BTCUSDT", "ETHEUR"])
        self.assertEqual(code, 1)
        self.assertEqual(len(lines), 1)

    def test_ban_418_stoppe_les_paires_suivantes(self):
        script = [FakeResponse(200, [kline(1)]), FakeResponse(418)]
        code, lines = self._run(script, ["BTCUSDT", "ETHEUR", "SOLEUR"])
        self.assertEqual(code, 1)
        self.assertEqual(len(lines), 1)  # SOLEUR n'a même pas été tentée


class RegistryTests(unittest.TestCase):

    def test_les_six_paires_attendues(self):
        symbols = {p["symbol"] for p in config.PAIRS}
        self.assertEqual(symbols, {"BTCUSDT", "BTCEUR", "ETHUSDT", "ETHEUR", "SOLUSDT", "SOLEUR"})

    def test_symbole_inconnu_refuse(self):
        with self.assertRaises(SystemExit):
            fk.main(["--pairs", "DOGEUSDT"])


if __name__ == "__main__":
    unittest.main()
