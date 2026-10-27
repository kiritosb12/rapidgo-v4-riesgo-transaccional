import json
import shutil
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]


def cargar_ejemplo(nombre: str) -> dict:
    return json.loads((RAIZ / "samples" / nombre).read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def spark():
    if shutil.which("java") is None:
        pytest.skip("Java no está instalado: se omiten las pruebas de Spark")
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    sesion = (
        SparkSession.builder.master("local[2]").appName("pruebas-rapidgo-v4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    sesion.sparkContext.setLogLevel("ERROR")
    yield sesion
    sesion.stop()


@pytest.fixture
def mensajes_kafka(spark):
    """Convierte una lista de eventos (dict o texto) en un DataFrame con las columnas que entrega Kafka."""
    from datetime import datetime, timezone

    def _crear(eventos, particion=0):
        filas = []
        for i, ev in enumerate(eventos):
            valor = ev if isinstance(ev, str) else json.dumps(ev)
            clave = None if isinstance(ev, str) else ev.get("id_pedido")
            filas.append((clave, valor, "rapidgo.v4.pagos.intentos", particion, i, datetime.now(timezone.utc)))
        return spark.createDataFrame(filas, "key string, value string, topic string, partition int, offset long, timestamp timestamp")

    return _crear
