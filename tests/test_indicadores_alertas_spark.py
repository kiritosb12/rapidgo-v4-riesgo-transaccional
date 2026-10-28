"""Pruebas de los indicadores por ventana y de las alertas A1 y A2 con DataFrames en lote."""
import pytest

from tests.conftest import cargar_ejemplo

T = pytest.importorskip("spark.transformaciones")
IA = pytest.importorskip("spark.indicadores_alertas")


def intento(n, segundo, estado="rechazado", motivo="tarjeta_invalida", pedido="PED-20261027-000777", **extra):
    base = cargar_ejemplo("evento_pago.json")
    base.update(id_evento=f"{pedido}-{n}", id_pedido=pedido, numero_intento=n, estado=estado,
                motivo_rechazo=motivo if estado == "rechazado" else None,
                fecha_evento=f"2026-10-27T15:00:{segundo:02d}.000Z", **extra)
    return base


def procesar(mensajes_kafka, eventos):
    return T.a_processed(mensajes_kafka(eventos))


def test_a1_detecta_prueba_de_tarjetas(spark, mensajes_kafka):
    eventos = [intento(n, 10 * n) for n in range(1, 5)]                       # 4 rechazos en 40 s
    eventos.append(intento(1, 5, estado="aprobado", pedido="PED-20261027-000001"))  # pedido normal
    alertas = IA.alertas_multiples_intentos(procesar(mensajes_kafka, eventos)).collect()
    assert len(alertas) == 1
    a = alertas[0]
    assert (a["id_pedido"], a["intentos"], a["rechazos"], a["severidad"]) == ("PED-20261027-000777", 4, 4, "MEDIA")
    assert a["detalle"] == "intentos=4, rechazos=4"


def test_a1_severidad_alta_con_cinco_intentos(spark, mensajes_kafka):
    alertas = IA.alertas_multiples_intentos(procesar(mensajes_kafka, [intento(n, 5 * n) for n in range(1, 6)])).collect()
    assert alertas[0]["severidad"] == "ALTA"


def test_a1_no_alerta_con_un_rechazo_y_un_aprobado(spark, mensajes_kafka):
    eventos = [intento(1, 1, motivo="fondos_insuficientes"), intento(2, 30, estado="aprobado")]
    assert IA.alertas_multiples_intentos(procesar(mensajes_kafka, eventos)).count() == 0


def test_a2_solo_con_senales_combinadas(spark, mensajes_kafka):
    anomalo = cargar_ejemplo("evento_pago_anomalo.json")          # 75 puntos
    viajero = dict(cargar_ejemplo("evento_pago.json"), id_evento="v-1", valor=800_000.0, pais_ip="US")  # 55 puntos
    alertas = IA.alertas_senales_combinadas(procesar(mensajes_kafka, [anomalo, viajero])).collect()
    assert [a["id_pedido"] for a in alertas] == [anomalo["id_pedido"]]
    assert alertas[0]["severidad"] == "MEDIA" and alertas[0]["puntaje_riesgo"] == 75
    assert alertas[0]["detalle"].startswith("senales=S1,S2,S3")


def test_indicadores_por_minuto(spark, mensajes_kafka):
    eventos = [intento(n, 10 * n) for n in range(1, 4)]                                         # pedido con 3 rechazos
    eventos += [intento(1, 2 + k, estado="aprobado", pedido=f"PED-20261027-00010{k}") for k in range(3)]
    from pyspark.sql import functions as F
    kpis = IA.indicadores_por_minuto(procesar(mensajes_kafka, eventos))
    fila = kpis.withColumn("inicio_texto", F.date_format("inicio_ventana", "yyyy-MM-dd HH:mm:ss")).collect()[0]
    assert fila["intentos"] == 6 and fila["pedidos"] == 4
    assert fila["k1_intentos_por_pedido"] == 1.5
    assert fila["max_numero_intento"] == 3
    assert fila["k2_valor_promedio"] == 87500.0
    assert fila["valor_promedio_tarjeta_credito"] == 87500.0 and fila["valor_promedio_nequi"] is None
    assert fila["k4_tasa_rechazo"] == 50.0
    assert fila["k3_eventos_riesgo"] == 0          # S3 (20) + S4 (15) = 35 < 60
    assert fila["inicio_texto"] == "2026-10-27 15:00:00"    # en UTC (zona de la sesión)
