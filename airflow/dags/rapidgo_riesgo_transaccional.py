"""DAG de control y calidad del Data Lake — RapidGo V4 (Riesgo transaccional, Equipo 9).

Este DAG revisa cada hora que las fuentes estén vivas, valida la
calidad de las capas raw y curated, agrega los indicadores por hora, agrupa las alertas por pedido y deja un
reporte en datalake/curated/reportes/fecha=AAAA-MM-DD/.

    verificar_fuentes → validar_raw → ┬→ agregar_kpis ───────┬→ validar_curated → generar_reporte
                                      └→ consolidar_alertas ─┘

- Reintentos: 2, cada 30 s (fallas de infraestructura: Kafka caído, Spark aún sin escribir).
- Una falla de CALIDAD de datos no se reintenta (AirflowFailException): repetir no cambia los datos.
- XCom: cada tarea devuelve un diccionario con conteos; generar_reporte los recoge todos.
- Parámetro `fecha` (AAAA-MM-DD, hora de Bogotá); vacío = hoy.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Param
from airflow.sdk.exceptions import AirflowFailException

from orquestacion import tareas as T

DAG_ID = "rapidgo_riesgo_transaccional"
TAREAS = ["verificar_fuentes", "validar_raw", "agregar_kpis", "consolidar_alertas", "validar_curated"]


def _ejecutar(nombre: str):
    """Envuelve una tarea de orquestacion/tareas.py para usarla en un PythonOperator."""
    def ejecutar(params: dict, **_contexto):
        cfg = T.Config.desde_entorno()
        fecha = T.resolver_fecha(params.get("fecha"))
        try:
            return getattr(T, nombre)(cfg, fecha)          # el valor devuelto queda como XCom
        except T.FalloCalidad as error:
            raise AirflowFailException(str(error)) from error   # datos malos: sin reintentos
        # T.FalloFuente y cualquier otro error: Airflow reintenta (retries=2)
    ejecutar.__name__ = nombre
    return ejecutar


def _generar_reporte(params: dict, ti, run_id: str, **_contexto):
    resultados = {nombre: ti.xcom_pull(task_ids=nombre) for nombre in TAREAS}   # XCom de las 5 tareas
    return T.generar_reporte(T.Config.desde_entorno(), T.resolver_fecha(params.get("fecha")), resultados, run_id)


with DAG(
    dag_id=DAG_ID,
    description="Control y calidad del Data Lake de riesgo transaccional + reporte (Variante 4)",
    schedule="@hourly",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "equipo9-v4",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=10),
    },
    params={"fecha": Param(None, type=["null", "string"], description="AAAA-MM-DD (hora de Bogotá). Vacío = hoy.")},
    tags=["rapidgo", "variante-4", "calidad"],
    doc_md=__doc__,
) as dag:
    operadores = {nombre: PythonOperator(task_id=nombre, python_callable=_ejecutar(nombre)) for nombre in TAREAS}
    reporte = PythonOperator(task_id="generar_reporte", python_callable=_generar_reporte)

    operadores["verificar_fuentes"] >> operadores["validar_raw"]
    operadores["validar_raw"] >> [operadores["agregar_kpis"], operadores["consolidar_alertas"]]
    [operadores["agregar_kpis"], operadores["consolidar_alertas"]] >> operadores["validar_curated"]
    operadores["validar_curated"] >> reporte
