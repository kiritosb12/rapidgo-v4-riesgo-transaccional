"""Pruebas de las reglas de negocio (spark/reglas.py): señales, puntaje y umbrales de alerta."""
import pytest

from spark import reglas as R
from tests.conftest import cargar_ejemplo


def evento(**cambios):
    base = cargar_ejemplo("evento_pago.json")   # pedido normal: 87.500 COP, CO, intento 1, aprobado, 14:42 Bogotá
    base.update(cambios)
    return base


def test_evento_normal_no_tiene_senales():
    assert R.senales(evento()) == []
    assert R.puntaje_riesgo(evento()) == 0


def test_evento_anomalo_de_ejemplo_suma_75():
    anomalo = cargar_ejemplo("evento_pago_anomalo.json")   # 1.250.000 COP, VE, intento 4, 22:12 Bogotá
    assert R.senales(anomalo) == ["S1", "S2", "S3"]
    assert R.puntaje_riesgo(anomalo) == 75
    assert R.evaluar_a2(75, 3) == "MEDIA"


@pytest.mark.parametrize("valor, activa", [(499_999, False), (500_000, True), (3_000_000, True)])
def test_umbral_valor_alto(valor, activa):
    assert ("S1" in R.senales(evento(valor=valor))) is activa


@pytest.mark.parametrize("intento, activa", [(2, False), (3, True)])
def test_umbral_reintento(intento, activa):
    assert ("S3" in R.senales(evento(numero_intento=intento))) is activa


@pytest.mark.parametrize("motivo, activa", [(None, False), ("fondos_insuficientes", False),
                                            ("sospecha_fraude", True), ("otp_incorrecto", True)])
def test_rechazo_sospechoso(motivo, activa):
    assert ("S4" in R.senales(evento(motivo_rechazo=motivo))) is activa


@pytest.mark.parametrize("fecha_utc, hora_bogota, activa", [
    ("2026-10-22T04:59:00.000Z", 23, False),   # 23:59 del día anterior en Bogotá
    ("2026-10-22T05:00:00.000Z", 0, True),     # medianoche en Bogotá (UTC-5)
    ("2026-10-22T09:59:59.000Z", 4, True),
    ("2026-10-22T10:00:00.000Z", 5, False),
])
def test_horario_atipico_usa_hora_de_bogota(fecha_utc, hora_bogota, activa):
    assert R.hora_local(fecha_utc) == hora_bogota
    assert ("S5" in R.senales(evento(fecha_evento=fecha_utc))) is activa


def test_puntaje_maximo_es_100():
    todas = evento(valor=900_000, pais_ip="NG", numero_intento=5, estado="rechazado",
                   motivo_rechazo="sospecha_fraude", fecha_evento="2026-10-22T07:00:00.000Z")
    assert R.senales(todas) == ["S1", "S2", "S3", "S4", "S5"]
    assert R.puntaje_riesgo(todas) == 100


@pytest.mark.parametrize("puntaje, senales, esperado", [
    (59, 3, None),      # no llega al umbral
    (60, 1, None),      # una sola señal nunca alerta (imposible hoy, pero protege el diseño)
    (60, 2, "MEDIA"),
    (79, 3, "MEDIA"),
    (80, 3, "ALTA"),
])
def test_alerta_senales_combinadas(puntaje, senales, esperado):
    assert R.evaluar_a2(puntaje, senales) == esperado


def test_valor_alto_desde_el_exterior_solo_no_alerta():
    """S1 + S2 = 55 < 60: un pedido grande de un viajero no genera A2 por sí solo."""
    ev = evento(valor=800_000, pais_ip="US")
    assert R.puntaje_riesgo(ev) == 55
    assert R.evaluar_a2(R.puntaje_riesgo(ev), len(R.senales(ev))) is None


@pytest.mark.parametrize("intentos, rechazos, esperado", [
    (1, 0, None), (2, 1, None),
    (2, 2, "MEDIA"),    # dos rechazos bastan
    (3, 0, "MEDIA"),    # tres intentos bastan
    (4, 4, "MEDIA"),
    (5, 5, "ALTA"),
])
def test_alerta_multiples_intentos(intentos, rechazos, esperado):
    assert R.evaluar_a1(intentos, rechazos) == esperado
