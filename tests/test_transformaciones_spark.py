"""Pruebas de las transformaciones de Spark (raw → processed) con DataFrames en lote."""
import pytest

from spark import reglas as R
from tests.conftest import cargar_ejemplo

T = pytest.importorskip("spark.transformaciones")


def test_parsear_lee_todos_los_campos_del_contrato(spark, mensajes_kafka):
    df = T.parsear(mensajes_kafka([cargar_ejemplo("evento_pago.json")]))
    fila = df.collect()[0]
    assert fila["id_pedido"] == "PED-20261022-004817"
    assert fila["valor"] == 87500.0
    assert fila["numero_intento"] == 1
    assert fila["particion"] == 0 and fila["offset"] == 0


def test_limpiar_descarta_invalidos_y_otros_equipos(spark, mensajes_kafka):
    bueno = cargar_ejemplo("evento_pago.json")
    otro_equipo = dict(bueno, id_evento="otro-1", equipo="equipo3-v1")
    sin_pedido = dict(bueno, id_evento="otro-2", id_pedido=None)
    valor_cero = dict(bueno, id_evento="otro-3", valor=0)
    fecha_mala = dict(bueno, id_evento="otro-4", fecha_evento="ayer")
    df = T.limpiar(T.parsear(mensajes_kafka([bueno, otro_equipo, sin_pedido, valor_cero, fecha_mala, "{no es json"])))
    assert [f["id_evento"] for f in df.collect()] == [bueno["id_evento"]]


def test_quitar_duplicados_por_id_evento(spark, mensajes_kafka):
    ev = cargar_ejemplo("evento_pago.json")
    df = T.quitar_duplicados(T.limpiar(T.parsear(mensajes_kafka([ev, ev, dict(ev)]))), "10 minutes")
    assert df.count() == 1


def test_enriquecer_coincide_con_las_reglas_en_python(spark, mensajes_kafka):
    normal, anomalo = cargar_ejemplo("evento_pago.json"), cargar_ejemplo("evento_pago_anomalo.json")
    nocturno = dict(normal, id_evento="noct-1", fecha_evento="2026-10-22T07:30:00.000Z", pais_ip="VE",
                    numero_intento=3, estado="rechazado", motivo_rechazo="tarjeta_invalida")
    eventos = [normal, anomalo, nocturno]
    filas = {f["id_evento"]: f for f in T.a_processed(mensajes_kafka(eventos)).collect()}
    for ev in eventos:
        fila = filas[ev["id_evento"]]
        assert fila["puntaje_riesgo"] == R.puntaje_riesgo(ev)
        assert fila["senales_riesgo"] == len(R.senales(ev))
        assert fila["lista_senales"] == ",".join(R.senales(ev))
        assert fila["hora_local"] == R.hora_local(ev["fecha_evento"])
    assert filas[nocturno["id_evento"]]["lista_senales"] == "S2,S3,S4,S5"
    assert filas[anomalo["id_evento"]]["es_evento_riesgo"] is True


def test_fecha_de_particion_es_la_fecha_local(spark, mensajes_kafka):
    # 03:12 UTC del 22 de octubre = 22:12 del 21 de octubre en Bogotá
    fila = T.a_processed(mensajes_kafka([cargar_ejemplo("evento_pago_anomalo.json")])).collect()[0]
    assert str(fila["fecha"]) == "2026-10-21"


def test_raw_conserva_el_mensaje_original(spark, mensajes_kafka):
    df = T.a_raw(mensajes_kafka(["{no es json"], particion=2))
    fila = df.collect()[0]
    assert fila["mensaje_json"] == "{no es json" and fila["particion"] == 2
