"""Trabajo de Spark Structured Streaming — RapidGo V4 (Riesgo transaccional).

Lee el topic rapidgo.v4.pagos.intentos y escribe el Data Lake en Parquet:

    datalake/raw/pagos/                         mensaje original + partición/offset
    datalake/processed/pagos/                   evento limpio, sin duplicados, con señales y puntaje
    datalake/curated/kpis_1min/                 indicadores K1–K4 por ventana de 1 minuto
    datalake/curated/alertas_multiples_intentos/  alerta A1
    datalake/curated/alertas_senales_combinadas/  alerta A2
    datalake/_checkpoints/<consulta>/            offsets y estado de cada consulta
    datalake/_metricas/progreso_streaming.jsonl  duración y filas de cada lote (evidencia para ADR-002)

Uso (dentro de Docker Compose):
    docker compose up -d --build spark
    docker compose logs -f spark

Parámetros por variables de entorno (o argumentos, ver --help):
    KAFKA_BOOTSTRAP=kafka:9092  KAFKA_TOPIC=rapidgo.v4.pagos.intentos  RUTA_DATALAKE=/app/datalake
    INICIO_OFFSETS=earliest  INTERVALO_LOTE="10 seconds"  RETRASO_MAXIMO="10 minutes"
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from spark import transformaciones as T
from spark import indicadores_alertas as IA

VERSION_SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"   # debe coincidir con pyspark


@dataclass
class Configuracion:
    bootstrap: str
    topic: str
    ruta_datalake: str
    inicio_offsets: str
    intervalo_lote: str
    retraso_maximo: str
    max_offsets_por_lote: int
    duracion: int

    def ruta(self, *partes: str) -> str:
        return str(Path(self.ruta_datalake, *partes))


def leer_configuracion(argv=None) -> Configuracion:
    e = os.getenv
    p = argparse.ArgumentParser(description="Spark Structured Streaming RapidGo V4")
    p.add_argument("--bootstrap", default=e("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--topic", default=e("KAFKA_TOPIC", "rapidgo.v4.pagos.intentos"))
    p.add_argument("--ruta-datalake", default=e("RUTA_DATALAKE", str(Path(__file__).resolve().parents[1] / "datalake")))
    p.add_argument("--inicio-offsets", default=e("INICIO_OFFSETS", "earliest"), choices=["earliest", "latest"])
    p.add_argument("--intervalo-lote", default=e("INTERVALO_LOTE", "10 seconds"))
    p.add_argument("--retraso-maximo", default=e("RETRASO_MAXIMO", "10 minutes"),
                   help="marca de agua (watermark) sobre fecha_evento")
    p.add_argument("--max-offsets-por-lote", type=int, default=int(e("MAX_OFFSETS_POR_LOTE", "0")))
    p.add_argument("--duracion", type=int, default=int(e("DURACION_SEGUNDOS", "0")), help="0 = sin límite")
    a = p.parse_args(argv)
    return Configuracion(a.bootstrap, a.topic, a.ruta_datalake, a.inicio_offsets, a.intervalo_lote,
                         a.retraso_maximo, a.max_offsets_por_lote, a.duracion)


def crear_sesion(con_kafka: bool = True) -> SparkSession:
    constructor = (
        SparkSession.builder.appName("rapidgo-v4-riesgo-transaccional")
        .master(os.getenv("SPARK_MASTER", "local[3]"))
        .config("spark.sql.session.timeZone", "UTC")         # fechas en UTC; la hora local se calcula aparte
        .config("spark.sql.shuffle.partitions", "3")         # igual al número de particiones del topic
        .config("spark.sql.streaming.stateStore.stateSchemaCheck", "true")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if con_kafka:   # descarga el conector de Kafka desde Maven la primera vez (queda en caché)
        constructor = constructor.config("spark.jars.packages", VERSION_SPARK_KAFKA)
        if os.getenv("SPARK_IVY_DIR"):
            constructor = constructor.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def leer_kafka(spark: SparkSession, cfg: Configuracion) -> DataFrame:
    lector = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.bootstrap)
        .option("subscribe", cfg.topic)
        .option("startingOffsets", cfg.inicio_offsets)
        .option("failOnDataLoss", "false")
    )
    if cfg.max_offsets_por_lote:
        lector = lector.option("maxOffsetsPerTrigger", cfg.max_offsets_por_lote)
    return lector.load()


def _escribir(df: DataFrame, nombre: str, ruta: str, cfg: Configuracion, particion: str | None = None):
    escritor = (
        df.writeStream.queryName(nombre).format("parquet").outputMode("append")
        .option("path", ruta)
        .option("checkpointLocation", cfg.ruta("_checkpoints", nombre))
        .trigger(processingTime=cfg.intervalo_lote)
    )
    if particion:
        escritor = escritor.partitionBy(particion)
    return escritor.start()


def construir_consultas(df_fuente: DataFrame, cfg: Configuracion) -> list:
    """Inicia las consultas continuas a partir de un DataFrame con columnas tipo Kafka
    (key, value, topic, partition, offset, timestamp)."""
    procesado = T.a_processed(df_fuente, cfg.retraso_maximo)
    consultas = [
        _escribir(T.a_raw(df_fuente), "raw_pagos", cfg.ruta("raw", "pagos"), cfg, "fecha"),
        _escribir(procesado, "processed_pagos", cfg.ruta("processed", "pagos"), cfg, "fecha"),
        _escribir(IA.indicadores_por_minuto(procesado), "kpis_1min", cfg.ruta("curated", "kpis_1min"), cfg),
        _escribir(IA.alertas_multiples_intentos(procesado), "alertas_multiples_intentos",
                  cfg.ruta("curated", "alertas_multiples_intentos"), cfg),
        _escribir(IA.alertas_senales_combinadas(procesado), "alertas_senales_combinadas",
                  cfg.ruta("curated", "alertas_senales_combinadas"), cfg),
    ]
    return consultas


class RegistroProgreso:
    """Guarda en JSON Lines la duración y las filas de cada lote de cada consulta."""

    def __init__(self, ruta: str):
        self.ruta = Path(ruta)
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        self.ultimo_lote: dict[str, int] = {}

    def registrar(self, consultas) -> None:
        with self.ruta.open("a", encoding="utf-8") as archivo:
            for q in consultas:
                for p in q.recentProgress:
                    if p["batchId"] <= self.ultimo_lote.get(p["name"], -1):
                        continue
                    self.ultimo_lote[p["name"]] = p["batchId"]
                    estado = p.get("stateOperators") or []
                    fila = {
                        "consulta": p["name"],
                        "lote": p["batchId"],
                        "fecha": p["timestamp"],
                        "filas_entrada": p.get("numInputRows", 0),
                        "filas_por_segundo_entrada": round(p.get("inputRowsPerSecond") or 0, 2),
                        "filas_por_segundo_procesadas": round(p.get("processedRowsPerSecond") or 0, 2),
                        "duracion_lote_ms": p.get("durationMs", {}).get("triggerExecution"),
                        "filas_en_estado": sum(s.get("numRowsTotal", 0) for s in estado),
                        "filas_descartadas_tardias": sum(s.get("numRowsDroppedByWatermark", 0) for s in estado),
                        "marca_agua": p.get("eventTime", {}).get("watermark"),
                    }
                    archivo.write(json.dumps(fila) + "\n")
                    if fila["filas_entrada"]:
                        print(f"[spark] {fila['consulta']:<28} lote {fila['lote']:>4}: "
                              f"{fila['filas_entrada']:>5} filas en {fila['duracion_lote_ms']} ms", flush=True)


def ejecutar(spark: SparkSession, df_fuente: DataFrame, cfg: Configuracion) -> None:
    consultas = construir_consultas(df_fuente, cfg)
    registro = RegistroProgreso(cfg.ruta("_metricas", "progreso_streaming.jsonl"))
    print(f"[spark] {len(consultas)} consultas activas; lote cada {cfg.intervalo_lote}; "
          f"marca de agua {cfg.retraso_maximo}; datalake en {cfg.ruta_datalake}", flush=True)

    detener = {"si": False}
    def al_detener(*_):
        detener["si"] = True
    signal.signal(signal.SIGTERM, al_detener)
    signal.signal(signal.SIGINT, al_detener)

    inicio = time.time()
    try:
        while not detener["si"]:
            spark.streams.awaitAnyTermination(5)     # lanza la excepción si una consulta falla
            registro.registrar(consultas)
            if cfg.duracion and time.time() - inicio >= cfg.duracion:
                break
    finally:
        registro.registrar(consultas)
        for q in consultas:
            q.stop()
        print("[spark] consultas detenidas; el checkpoint permite continuar donde quedó", flush=True)


def main(argv=None) -> int:
    cfg = leer_configuracion(argv)
    spark = crear_sesion(con_kafka=True)
    ejecutar(spark, leer_kafka(spark, cfg), cfg)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
