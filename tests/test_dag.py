from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk", reason="Airflow no está instalado (lo prueba el trabajo validar-dag del CI)")

CARPETA_DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"
DAG_ID = "rapidgo_riesgo_transaccional"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag
    return DagBag(dag_folder=str(CARPETA_DAGS))


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.dags[DAG_ID]


def test_dags_se_importan_sin_errores(dagbag):
    assert dagbag.import_errors == {}
    assert DAG_ID in dagbag.dags


def test_tiene_al_menos_5_tareas(dag):
    assert {t.task_id for t in dag.tasks} == {"verificar_fuentes", "validar_raw", "agregar_kpis",
                                              "consolidar_alertas", "validar_curated", "generar_reporte"}


def test_dependencias(dag):
    abajo = {t.task_id: t.downstream_task_ids for t in dag.tasks}
    assert abajo["verificar_fuentes"] == {"validar_raw"}
    assert abajo["validar_raw"] == {"agregar_kpis", "consolidar_alertas"}
    assert abajo["agregar_kpis"] == abajo["consolidar_alertas"] == {"validar_curated"}
    assert abajo["validar_curated"] == {"generar_reporte"}
    assert abajo["generar_reporte"] == set()


def test_reintentos_y_programacion(dag):
    assert all(t.retries == 2 for t in dag.tasks)
    assert dag.max_active_runs == 1 and dag.catchup is False
    assert "fecha" in dag.params


def test_falla_de_calidad_no_se_reintenta(monkeypatch):
    """FalloCalidad se convierte en AirflowFailException (Airflow no reintenta)."""
    import importlib.util
    from airflow.sdk.exceptions import AirflowFailException
    from orquestacion import tareas as T

    spec = importlib.util.spec_from_file_location("dag_rapidgo", CARPETA_DAGS / "rapidgo_riesgo_transaccional.py")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)

    def falla(cfg, fecha):
        raise T.FalloCalidad("datos malos")

    monkeypatch.setattr(T, "validar_raw", falla)
    with pytest.raises(AirflowFailException, match="datos malos"):
        modulo._ejecutar("validar_raw")(params={"fecha": "2026-10-27"})
