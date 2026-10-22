"""Pruebas del contrato del evento intento_pago (Variante 4 - Riesgo transaccional)."""
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

RAIZ = Path(__file__).resolve().parents[1]
ESQUEMA = json.loads((RAIZ / "samples" / "evento_pago.schema.json").read_text(encoding="utf-8"))
VALIDADOR = Draft202012Validator(ESQUEMA)


def cargar(nombre):
    return json.loads((RAIZ / "samples" / nombre).read_text(encoding="utf-8"))


@pytest.mark.parametrize("archivo", ["evento_pago.json", "evento_pago_anomalo.json"])
def test_ejemplos_cumplen_contrato(archivo):
    errores = list(VALIDADOR.iter_errors(cargar(archivo)))
    assert errores == [], [e.message for e in errores]


def test_falta_campo_obligatorio_es_invalido():
    evento = cargar("evento_pago.json")
    del evento["id_pedido"]
    assert not VALIDADOR.is_valid(evento)


def test_valor_negativo_es_invalido():
    evento = cargar("evento_pago.json")
    evento["valor"] = -1000
    assert not VALIDADOR.is_valid(evento)


def test_medio_de_pago_desconocido_es_invalido():
    evento = cargar("evento_pago.json")
    evento["medio_pago"] = "bitcoin"
    assert not VALIDADOR.is_valid(evento)


def test_eventos_identificables_por_equipo():
    assert cargar("evento_pago.json")["equipo"] == "equipo9-v4"
