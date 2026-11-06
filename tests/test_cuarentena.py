"""Pruebas de la cuarentena de mensajes dañados (fallo controlado de calidad, Semana 3)."""
import pandas as pd
import pytest

from orquestacion import cuarentena as C, tareas as T
from tests.fabrica_lago import FECHA, crear_lago


def config(raiz):
    return T.Config(ruta_datalake=raiz, kafka_bootstrap="127.0.0.1:1")


def test_sin_cuarentena_los_mensajes_danados_fallan(tmp_path):
    with pytest.raises(T.FalloCalidad, match=r"inválidos .*1 de 10"):
        T.validar_raw(config(crear_lago(tmp_path, mensajes_invalidos=1)), FECHA)


def test_lista_los_mensajes_danados(tmp_path):
    danados = C.mensajes_danados(config(crear_lago(tmp_path, mensajes_invalidos=2)), FECHA)
    assert len(danados) == 2 and set(danados["particion"]) == {0}
    assert danados["mensaje"].iloc[0] == "{no es json"


def test_cuarentena_permite_volver_a_validar(tmp_path):
    cfg = config(crear_lago(tmp_path, mensajes_invalidos=2))
    assert C.poner_en_cuarentena(cfg, FECHA, "prueba") == (2, 2)
    r = T.validar_raw(cfg, FECHA)
    assert r["en_cuarentena"] == 2 and r["invalidos"] == 0 and r["mensajes"] == 11
    registro = pd.read_csv(T.ruta_cuarentena(cfg))
    assert list(registro.columns) == ["particion", "offset", "fecha", "motivo", "registrado"]
    assert set(registro["motivo"]) == {"prueba"}


def test_repetir_no_duplica_la_cuarentena(tmp_path):
    cfg = config(crear_lago(tmp_path, mensajes_invalidos=2))
    C.poner_en_cuarentena(cfg, FECHA, "prueba")
    assert C.poner_en_cuarentena(cfg, FECHA, "otra vez") == (0, 2)


def test_raw_no_se_modifica(tmp_path):
    cfg = config(crear_lago(tmp_path, mensajes_invalidos=2))
    from orquestacion import lago
    antes = len(lago.leer(cfg.tabla("raw", "pagos"), FECHA))
    C.poner_en_cuarentena(cfg, FECHA, "prueba")
    assert len(lago.leer(cfg.tabla("raw", "pagos"), FECHA)) == antes


def test_comando(tmp_path, monkeypatch, capsys):
    crear_lago(tmp_path, mensajes_invalidos=3)
    monkeypatch.setenv("RUTA_DATALAKE", str(tmp_path))
    assert C.main(["--fecha", FECHA, "--solo-mostrar"]) == 0
    assert "Mensajes dañados en raw para fecha=2026-10-27: 3" in capsys.readouterr().out
    assert not T.ruta_cuarentena(config(tmp_path)).exists()
    assert C.main(["--fecha", FECHA, "--motivo", "fallo controlado"]) == 0
    assert "Registrados 3 mensajes nuevos" in capsys.readouterr().out
