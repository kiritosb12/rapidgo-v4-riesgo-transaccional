"""Pruebas de agregar_kpis, consolidar_alertas, validar_curated y generar_reporte (sin Airflow)."""
import json
from pathlib import Path

import pandas as pd
import pytest

from orquestacion import lago, tareas as T
from tests.fabrica_lago import FECHA, crear_lago


def config(raiz):
    return T.Config(ruta_datalake=raiz, kafka_bootstrap="127.0.0.1:1")


def test_agregar_kpis_por_hora(tmp_path):
    cfg = config(crear_lago(tmp_path))
    r = T.agregar_kpis(cfg, FECHA)
    assert (r["eventos"], r["pedidos"], r["horas"]) == (9, 5, 1)
    assert r["eventos_riesgo"] == 2 and r["k4_tasa_rechazo"] == round(100 * 5 / 9, 2)
    hora = lago.leer(cfg.tabla("curated", "resumen_horario"), FECHA).iloc[0]
    assert hora["hora_local"] == 10 and hora["intentos"] == 9 and hora["k1_intentos_por_pedido"] == 1.8


def test_agregar_kpis_es_idempotente(tmp_path):
    cfg = config(crear_lago(tmp_path))
    T.agregar_kpis(cfg, FECHA)
    T.agregar_kpis(cfg, FECHA)
    assert len(lago.leer(cfg.tabla("curated", "resumen_horario"), FECHA)) == 1


def test_consolidar_agrupa_a2_por_pedido(tmp_path):
    cfg = config(crear_lago(tmp_path))
    r = T.consolidar_alertas(cfg, FECHA)
    assert (r["alertas_a1"], r["alertas_a2"], r["pedidos_con_a2"], r["pedidos_con_alerta"]) == (1, 2, 1, 1)
    assert r["reduccion_a2_pct"] == 50.0 and r["pedidos_con_a1_y_a2"] == 1
    fila = lago.leer(cfg.tabla("curated", "alertas_consolidadas"), FECHA).iloc[0]
    assert (fila["alertas_a1"], fila["alertas_a2"], fila["severidad"]) == (1, 2, "MEDIA")
    assert fila["detalle"].startswith("senales=S2,S3,S4, intento=4")


def test_consolidar_severidad_alta_si_alguna_es_alta():
    base = {"id_cliente": "CLI-1", "puntaje_riesgo": 60, "fecha_deteccion": pd.Timestamp("2026-10-27", tz="UTC"), "detalle": "d"}
    a1 = pd.DataFrame([{**base, "id_pedido": "P1", "severidad": "MEDIA"}, {**base, "id_pedido": "P2", "severidad": "MEDIA"}])
    a2 = pd.DataFrame([{**base, "id_pedido": "P1", "severidad": "ALTA", "puntaje_riesgo": 85}])
    c = T.consolidar(a1, a2)
    assert c["id_pedido"].tolist() == ["P1", "P2"]                # ALTA primero
    assert c.iloc[0]["severidad"] == "ALTA" and c.iloc[0]["puntaje_max"] == 85


def test_consolidar_sin_alertas(tmp_path):
    cfg = config(crear_lago(tmp_path, sin_alertas=True))
    r = T.consolidar_alertas(cfg, FECHA)
    assert r["pedidos_con_alerta"] == 0 and r["reduccion_a2_pct"] == 0.0


def preparar(tmp_path, **kw):
    cfg = config(crear_lago(tmp_path, **kw))
    T.consolidar_alertas(cfg, FECHA)
    return cfg


def test_validar_curated_ok(tmp_path):
    r = T.validar_curated(preparar(tmp_path), FECHA)
    assert r["ventanas"] == 2 and r["intentos_kpis"] == r["intentos_processed"] == 9
    assert r["advertencias"] == []


def test_intentos_que_no_cuadran_es_falla_de_calidad(tmp_path):
    cfg = preparar(tmp_path)
    tabla = cfg.tabla("curated", "kpis_1min")
    kpis = lago.leer(tabla)
    kpis.loc[0, "intentos"] += 3
    kpis.to_parquet(lago.archivos_confirmados(tabla)[0], index=False)
    with pytest.raises(T.FalloCalidad, match="intentos de kpis_1min = processed"):
        T.validar_curated(cfg, FECHA)


def test_alerta_de_pedido_inexistente(tmp_path):
    cfg = preparar(tmp_path)
    tabla = cfg.tabla("curated", "alertas_multiples_intentos")
    a1 = lago.leer(tabla)
    a1.loc[0, "id_pedido"] = "PED-NO-EXISTE"
    a1.to_parquet(lago.archivos_confirmados(tabla)[0], index=False)
    T.consolidar_alertas(cfg, FECHA)
    with pytest.raises(T.FalloCalidad, match="alertas con pedido existente"):
        T.validar_curated(cfg, FECHA)


def test_sin_ventanas_cerradas(tmp_path):
    with pytest.raises(T.FalloCalidad, match="no hay ventanas"):
        T.validar_curated(preparar(tmp_path), "2026-01-01")


def _alerta_nueva(cfg, minutos_despues_del_corte):
    """Simula que Spark escribe una alerta A2 de otro pedido real después de consolidar_alertas."""
    tabla = cfg.tabla("curated", "alertas_senales_combinadas")
    a2 = lago.leer(tabla)
    corte = lago.leer(cfg.tabla("curated", "alertas_consolidadas"), FECHA)["ultima_alerta"].max()
    sin_alerta = sorted(set(lago.leer(cfg.tabla("processed", "pagos"), FECHA)["id_pedido"]) - set(a2["id_pedido"])
                        - set(lago.leer(cfg.tabla("curated", "alertas_multiples_intentos"))["id_pedido"]))
    fila = a2.iloc[[0]].assign(id_pedido=sin_alerta[0], fecha_deteccion=corte + pd.Timedelta(minutes=minutos_despues_del_corte))
    pd.concat([a2, fila], ignore_index=True).to_parquet(lago.archivos_confirmados(tabla)[0], index=False)


def test_alertas_que_llegan_despues_de_consolidar_no_son_falla(tmp_path):
    cfg = preparar(tmp_path)
    _alerta_nueva(cfg, 5)                      # Spark sigue escribiendo mientras corre el DAG
    r = T.validar_curated(cfg, FECHA)
    chequeo = next(c for c in r["chequeos"] if c["chequeo"] == "consolidado = pedidos con alerta")
    assert chequeo["ok"] and "1 nuevos después del corte" in str(chequeo["valor"])


def test_pedido_viejo_que_falta_en_el_consolidado_es_falla(tmp_path):
    cfg = preparar(tmp_path)
    _alerta_nueva(cfg, -10)                    # alerta anterior a la foto que no quedó consolidada
    with pytest.raises(T.FalloCalidad, match="consolidado = pedidos con alerta"):
        T.validar_curated(cfg, FECHA)


def test_generar_reporte(tmp_path):
    cfg = preparar(tmp_path)
    resultados = {
        "verificar_fuentes": {"chequeos": [], "advertencias": ["raw recibe datos recientes: valor=70 min (umbral <= 10 min)"]},
        "validar_raw": T.validar_raw(cfg, FECHA),
        "agregar_kpis": T.agregar_kpis(cfg, FECHA),
        "consolidar_alertas": T.consolidar_alertas(cfg, FECHA),
        "validar_curated": T.validar_curated(cfg, FECHA),
    }
    r = T.generar_reporte(cfg, FECHA, resultados, "prueba")
    texto = Path(r["reporte_md"]).read_text(encoding="utf-8")
    assert "# Reporte de riesgo transaccional — 2026-10-27" in texto
    assert "| Pedidos con A2 (agrupadas por pedido) | 1 (−50.0 %) |" in texto
    assert "| PED-20261027-000002 |" in texto and "raw recibe datos recientes" in texto
    datos = json.loads(Path(r["reporte_json"]).read_text(encoding="utf-8"))
    assert datos["fecha"] == FECHA and datos["agregar_kpis"]["eventos"] == 9
    assert r["advertencias"] == 1 + len(resultados["validar_raw"]["advertencias"])
