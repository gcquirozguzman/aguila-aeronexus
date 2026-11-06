"""Pruebas de los reintentos con backoff exponencial (python -m unittest ingesta/test_reintentos.py)."""
import os
import sys
import unittest
from unittest import mock

import requests

sys.path.insert(0, os.path.dirname(__file__))
import main  # noqa: E402


class Respuesta:
    def __init__(self, estado, cabeceras=None):
        self.status_code = estado
        self.headers = cabeceras or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(str(self.status_code))


@mock.patch.object(main.time, "sleep")
class PruebasReintentos(unittest.TestCase):
    def test_reintenta_5xx_y_429_hasta_responder(self, dormir):
        with mock.patch.object(main.requests, "request",
                               side_effect=[Respuesta(503), Respuesta(429, {"Retry-After": "1"}), Respuesta(200)]):
            self.assertEqual(main.pedir("GET", "https://x").status_code, 200)
        self.assertEqual(dormir.call_count, 2)
        self.assertEqual(dormir.call_args_list[1].args[0], 1)  # respeta Retry-After

    def test_backoff_exponencial_y_falla_al_final(self, dormir):
        with mock.patch.object(main.requests, "request", side_effect=requests.ConnectTimeout("t")):
            with self.assertRaises(requests.ConnectTimeout):
                main.pedir("GET", "https://x")
        esperas = [c.args[0] for c in dormir.call_args_list]
        self.assertEqual(len(esperas), main.INTENTOS - 1)
        for n, espera in enumerate(esperas, 1):
            self.assertTrue(2 ** n <= espera < 2 ** n + 1)

    def test_no_espera_si_la_api_pide_demasiado(self, dormir):
        with mock.patch.object(main.requests, "request",
                               side_effect=[Respuesta(429, {"X-Rate-Limit-Retry-After-Seconds": "3600"})]):
            with self.assertRaises(requests.HTTPError):
                main.pedir("GET", "https://x")
        dormir.assert_not_called()

    def test_no_reintenta_errores_del_cliente(self, dormir):
        with mock.patch.object(main.requests, "request", side_effect=[Respuesta(404)]):
            with self.assertRaises(requests.HTTPError):
                main.pedir("GET", "https://x")
        dormir.assert_not_called()


if __name__ == "__main__":
    unittest.main()
