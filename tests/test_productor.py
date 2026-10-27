"""Pruebas del generador de eventos del productor (no necesitan Kafka)."""
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from producer.generador import EQUIPO, TIPOS_ANOMALIA, GeneradorPagos, ahora_iso

RAIZ = Path(__file__).resolve().parents[1]
VALIDADOR = Draft202012Validator(json.loads((RAIZ / "samples" / "evento_pago.schema.json").read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def sesiones():
    gen = GeneradorPagos(semilla=42, tasa_anomalias=0.05)
    momento = datetime(2026, 10, 27, 15, 0, tzinfo=timezone.utc)
    return [gen.nueva_sesion(momento) for _ in range(4000)]


def test_todos_los_eventos_cumplen_el_contrato(sesiones):
    for _, intentos in sesiones:
        for intento in intentos:
            evento = dict(intento.evento, fecha_evento=ahora_iso())
            errores = [e.message for e in VALIDADOR.iter_errors(evento)]
            assert errores == [], errores


def test_eventos_identificables_por_equipo(sesiones):
    assert {i.evento["equipo"] for _, intentos in sesiones for i in intentos} == {EQUIPO}


def test_intentos_de_un_pedido_son_consecutivos_y_comparten_pedido(sesiones):
    for _, intentos in sesiones:
        assert [i.evento["numero_intento"] for i in intentos] == list(range(1, len(intentos) + 1))
        assert len({i.evento["id_pedido"] for i in intentos}) == 1
        assert [i.retraso for i in intentos] == sorted(i.retraso for i in intentos)


def test_cada_intento_tiene_id_evento_unico(sesiones):
    ids = [i.evento["id_evento"] for _, intentos in sesiones for i in intentos]
    assert len(ids) == len(set(ids))


def test_pedidos_distintos_en_cada_sesion(sesiones):
    pedidos = [intentos[0].evento["id_pedido"] for _, intentos in sesiones]
    assert len(pedidos) == len(set(pedidos))


def test_tasa_de_anomalias_cercana_al_5_por_ciento(sesiones):
    tipos = Counter(tipo for tipo, _ in sesiones)
    porcentaje = 100 * (len(sesiones) - tipos["normal"]) / len(sesiones)
    assert 3.5 <= porcentaje <= 6.5
    assert set(tipos) - {"normal"} == set(TIPOS_ANOMALIA)


def test_prueba_de_tarjetas_genera_varios_rechazos(sesiones):
    for tipo, intentos in sesiones:
        if tipo == "prueba_tarjetas":
            assert len(intentos) >= 3
            assert all(i.evento["estado"] == "rechazado" for i in intentos)


def test_motivo_de_rechazo_solo_en_rechazos(sesiones):
    for _, intentos in sesiones:
        for i in intentos:
            assert (i.evento["motivo_rechazo"] is not None) == (i.evento["estado"] == "rechazado")


def test_misma_semilla_mismos_eventos():
    momento = datetime(2026, 10, 27, tzinfo=timezone.utc)
    a = [GeneradorPagos(semilla=7).nueva_sesion(momento) for _ in range(1)]
    b = [GeneradorPagos(semilla=7).nueva_sesion(momento) for _ in range(1)]
    assert a[0][1][0].evento == b[0][1][0].evento


def test_formato_de_fecha_iso_con_z():
    assert ahora_iso(datetime(2026, 10, 22, 19, 42, 15, 318000, tzinfo=timezone.utc)) == "2026-10-22T19:42:15.318Z"
