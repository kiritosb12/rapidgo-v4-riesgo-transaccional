"""Pruebas de la lectura del Data Lake y de las tareas verificar_fuentes y validar_raw (sin Airflow)."""
import os
import socket
import threading
import time

import pytest

from orquestacion import lago, tareas as T
from tests.fabrica_lago import FECHA, crear_lago


@pytest.fixture
def kafka_falso():
    """Un puerto que acepta conexiones, como si Kafka estuviera arriba."""
    servidor = socket.socket()
    servidor.bind(("127.0.0.1", 0))
    servidor.listen()

    def aceptar():
        try:
            while True:
                servidor.accept()[0].close()
        except OSError:          # el socket se cerró al terminar la prueba
            pass

    threading.Thread(target=aceptar, daemon=True).start()
    yield f"127.0.0.1:{servidor.getsockname()[1]}"
    servidor.close()


@pytest.fixture
def puerto_cerrado():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    puerto = s.getsockname()[1]
    s.close()
    return f"127.0.0.1:{puerto}"


def config(raiz, kafka="127.0.0.1:1"):
    return T.Config(ruta_datalake=raiz, kafka_bootstrap=kafka)


# ------------------------------------------------------------------ lago.py
def test_lee_solo_archivos_confirmados_por_spark(tmp_path):
    crear_lago(tmp_path)
    tabla = tmp_path / "raw" / "pagos"
    assert len(list(tabla.rglob("*.parquet"))) == 2              # el confirmado y el huérfano
    assert [a.name for a in lago.archivos_confirmados(tabla)] == ["part-00000.parquet"]
    assert len(lago.leer(tabla, FECHA)) == 9


def test_registro_compactado_y_borrados(tmp_path):
    tabla = tmp_path / "curated" / "t"
    (tabla / "_spark_metadata").mkdir(parents=True)
    for nombre in ("a", "b", "c"):
        (tabla / f"{nombre}.parquet").write_bytes(b"x")
    ruta = lambda n: f'{{"path":"file:///app/datalake/curated/t/{n}.parquet","action":"add"}}'
    (tabla / "_spark_metadata" / "0").write_text("v1\n" + ruta("viejo"))            # anterior al compactado: se ignora
    (tabla / "_spark_metadata" / "9.compact").write_text("v1\n" + ruta("a") + "\n" + ruta("b"))
    (tabla / "_spark_metadata" / "10").write_text("v1\n" + ruta("c") + '\n{"path":"file:///app/datalake/curated/t/a.parquet","action":"delete"}')
    assert [a.name for a in lago.archivos_confirmados(tabla)] == ["b.parquet", "c.parquet"]


def test_ruta_de_spark_se_traduce_a_la_carpeta_local(tmp_path):
    crear_lago(tmp_path)
    archivo = lago.archivos_confirmados(tmp_path / "processed" / "pagos")[0]
    assert archivo.is_relative_to(tmp_path) and archivo.exists()


def test_fechas_quedan_en_utc(tmp_path):
    crear_lago(tmp_path)
    df = lago.leer(tmp_path / "processed" / "pagos", FECHA)
    assert str(df["fecha_evento"].dt.tz) == "UTC"
    assert set(df["fecha"]) == {FECHA}


def test_escribir_reemplaza_la_particion(tmp_path):
    import pandas as pd
    tabla = tmp_path / "curated" / "x"
    lago.escribir(pd.DataFrame({"a": [1, 2, 3]}), tabla, FECHA, "datos")
    lago.escribir(pd.DataFrame({"a": [9]}), tabla, FECHA, "datos")
    assert lago.leer(tabla, FECHA)["a"].tolist() == [9]           # repetir no duplica


def test_resolver_fecha():
    assert T.resolver_fecha("2026-10-27") == "2026-10-27"
    assert len(T.resolver_fecha("")) == 10
    with pytest.raises(ValueError):
        T.resolver_fecha("27/10/2026")


# ------------------------------------------------------------------ verificar_fuentes
def test_verificar_fuentes_ok(tmp_path, kafka_falso):
    r = T.verificar_fuentes(config(crear_lago(tmp_path), kafka_falso), FECHA)
    assert r["archivos_raw"] == 1 and r["archivos_processed"] == 1 and r["advertencias"] == []


def test_kafka_caido_es_falla_de_fuente(tmp_path, puerto_cerrado):
    with pytest.raises(T.FalloFuente, match="Kafka no responde"):
        T.verificar_fuentes(config(crear_lago(tmp_path), puerto_cerrado), FECHA)


def test_raw_vacia_es_falla_de_fuente(tmp_path, kafka_falso):
    with pytest.raises(T.FalloFuente, match="raw no tiene archivos"):
        T.verificar_fuentes(config(crear_lago(tmp_path), kafka_falso), "2026-01-01")


def test_datos_viejos_de_un_dia_pasado_solo_advierten(tmp_path, kafka_falso):
    crear_lago(tmp_path)
    hace_una_hora = time.time() - 3600
    for archivo in (tmp_path / "raw").rglob("*.parquet"):
        os.utime(archivo, (hace_una_hora, hace_una_hora))
    r = T.verificar_fuentes(config(tmp_path, kafka_falso), FECHA)
    assert r["minutos_desde_ultimo_dato"] >= 59
    assert any("datos recientes" in a for a in r["advertencias"])


def test_hoy_sin_datos_nuevos_es_falla_de_fuente(tmp_path, kafka_falso, monkeypatch):
    """Spark o el productor detenidos: hoy no llegan datos a raw → falla (se reintenta)."""
    crear_lago(tmp_path)
    hace_15_min = time.time() - 15 * 60
    for archivo in (tmp_path / "raw").rglob("*.parquet"):
        os.utime(archivo, (hace_15_min, hace_15_min))
    monkeypatch.setattr(T, "_hoy", lambda: FECHA)
    with pytest.raises(T.FalloFuente, match="no recibe datos nuevos hace 15"):
        T.verificar_fuentes(config(tmp_path, kafka_falso), FECHA)


def test_hoy_con_datos_recientes_pasa(tmp_path, kafka_falso, monkeypatch):
    monkeypatch.setattr(T, "_hoy", lambda: FECHA)
    r = T.verificar_fuentes(config(crear_lago(tmp_path), kafka_falso), FECHA)
    assert r["minutos_desde_ultimo_dato"] < 1


# ------------------------------------------------------------------ validar_raw
def test_validar_raw_ok(tmp_path):
    r = T.validar_raw(config(crear_lago(tmp_path)), FECHA)
    assert r["mensajes"] == 9 and r["invalidos"] == 0
    assert sum(r["por_particion"].values()) == 9
    assert all(c["ok"] for c in r["chequeos"] if c["nivel"] == "critico")


def test_hueco_de_offsets_es_falla_de_calidad(tmp_path):
    with pytest.raises(T.FalloCalidad, match="offsets sin huecos"):
        T.validar_raw(config(crear_lago(tmp_path, offsets_con_hueco=True)), FECHA)


def test_mensajes_invalidos_sobre_el_umbral(tmp_path):
    with pytest.raises(T.FalloCalidad, match="inválidos"):
        T.validar_raw(config(crear_lago(tmp_path, mensajes_invalidos=1)), FECHA)   # 1 de 10 = 10 % > 1 %


def test_desbalance_de_particiones_solo_advierte(tmp_path):
    cfg = config(crear_lago(tmp_path))
    cfg.max_desbalance_pct = 0
    r = T.validar_raw(cfg, FECHA)
    assert any("desbalance" in a for a in r["advertencias"])
