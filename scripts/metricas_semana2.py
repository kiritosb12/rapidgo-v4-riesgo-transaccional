"""Resume valores REALES de una ejecución del flujo para el ADR-002 y el punto de control de la Semana 2.

Lee el Data Lake (Parquet) y el registro de lotes de Spark, e imprime tablas en Markdown listas
para copiar. También guarda el resultado en datalake/_metricas/resumen_<fecha>.md.

Uso (con el flujo corriendo o después de detenerlo):
    docker compose run --rm --no-deps spark python scripts/metricas_semana2.py
    # o en el computador, si tiene Java 17+:  python scripts/metricas_semana2.py --ruta-datalake datalake
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import datetime
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ))

from pyspark.sql import SparkSession            # noqa: E402
from pyspark.sql import functions as F          # noqa: E402

from spark import transformaciones as T         # noqa: E402


def percentil(valores, p):
    if not valores:
        return None
    valores = sorted(valores)
    return valores[min(len(valores) - 1, int(round(p / 100 * (len(valores) - 1))))]


def tabla(encabezado, filas):
    lineas = ["| " + " | ".join(encabezado) + " |", "|" + "---|" * len(encabezado)]
    lineas += ["| " + " | ".join("" if v is None else str(v) for v in fila) + " |" for fila in filas]
    return "\n".join(lineas)


def leer(spark, ruta: Path):
    if not ruta.exists() or not any(ruta.rglob("*.parquet")):
        return None
    return spark.read.parquet(str(ruta))


def archivos(ruta: Path):
    lista = [f for f in ruta.rglob("*.parquet")] if ruta.exists() else []
    tam = sum(f.stat().st_size for f in lista)
    return len(lista), round(tam / max(len(lista), 1) / 1024, 1)


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ruta-datalake", default=str(RAIZ / "datalake"))
    a = p.parse_args(argv)
    dl = Path(a.ruta_datalake)

    spark = (SparkSession.builder.master("local[2]").appName("metricas-semana2")
             .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    salida = [f"# Métricas reales de ejecución — {datetime.now():%Y-%m-%d %H:%M}", ""]

    # ------------------------------------------------------------ raw
    raw = leer(spark, dl / "raw" / "pagos")
    if raw is None:
        print("No hay datos en datalake/raw/pagos. ¿Está corriendo el productor y Spark?")
        return 1
    total_raw = raw.count()
    por_part = (raw.groupBy("particion").agg(F.count("*").alias("n"), F.min("offset").alias("min"),
                                             F.max("offset").alias("max")).orderBy("particion").collect())
    promedio = total_raw / max(len(por_part), 1)
    salida += ["## 1. Kafka → capa raw", f"Mensajes recibidos: **{total_raw}**", "",
               tabla(["Partición", "Mensajes", "% del total", "Desviación vs. promedio", "Offset mín.", "Offset máx."],
                     [(r["particion"], r["n"], f"{100 * r['n'] / total_raw:.1f} %",
                       f"{100 * (r['n'] - promedio) / promedio:+.1f} %", r["min"], r["max"]) for r in por_part]), ""]

    # ------------------------------------------------------------ validez y duplicados
    como_kafka = raw.select(F.col("mensaje_json").alias("value"), F.col("particion").alias("partition"), "offset")
    validos = T.limpiar(T.parsear(como_kafka))
    n_validos = validos.count()
    n_unicos = validos.select("id_evento").distinct().count()

    proc = leer(spark, dl / "processed" / "pagos")
    salida += ["## 2. Capa processed"]
    if proc is not None:
        n_proc = proc.count()
        retrasos = [r[0] for r in proc.select(
            (F.col("fecha_procesamiento").cast("double") - F.col("fecha_evento").cast("double")).alias("s")).collect()]
        riesgo = proc.filter("es_evento_riesgo").count()
        rango = proc.agg(F.min("fecha_evento").alias("a"), F.max("fecha_evento").alias("b")).collect()[0]
        minutos = max((rango["b"] - rango["a"]).total_seconds() / 60, 1 / 60)
        salida += [tabla(["Medida", "Valor"], [
            ("Mensajes inválidos u de otro equipo descartados", total_raw - n_validos),
            ("Duplicados en raw (mismo id_evento)", n_validos - n_unicos),
            ("Eventos en processed", n_proc),
            ("Pedidos distintos", proc.select("id_pedido").distinct().count()),
            ("Minutos de datos (tiempo del evento)", round(minutos, 1)),
            ("Eventos por segundo (promedio)", round(n_proc / (minutos * 60), 2)),
            ("Eventos de riesgo (puntaje ≥ 60)", f"{riesgo} ({100 * riesgo / max(n_proc, 1):.2f} %)"),
            ("Retraso evento → processed p50 (s)", round(percentil(retrasos, 50), 1)),
            ("Retraso evento → processed p95 (s)", round(percentil(retrasos, 95), 1)),
            ("Retraso evento → processed máximo (s)", round(max(retrasos), 1)),
        ]), ""]
        combinaciones = (proc.filter("senales_riesgo > 0").groupBy("lista_senales", "puntaje_riesgo")
                         .count().orderBy(F.desc("count")).limit(8).collect())
        salida += ["Combinaciones de señales más frecuentes:", "",
                   tabla(["Señales", "Puntaje", "Eventos"], [(r[0], r[1], r[2]) for r in combinaciones]), ""]
    else:
        minutos = None
        salida += ["Sin datos todavía.", ""]

    # ------------------------------------------------------------ curated
    kpis = leer(spark, dl / "curated" / "kpis_1min")
    salida += ["## 3. Capa curated — indicadores por minuto"]
    if kpis is not None:
        demoras = [r[0] for r in kpis.select(
            (F.col("fecha_calculo").cast("double") - F.col("fin_ventana").cast("double")).alias("d")).collect()]
        salida += [f"Ventanas publicadas: **{kpis.count()}**. Demora entre el cierre de la ventana y su escritura: "
                   f"p50 = **{percentil(demoras, 50):.0f} s**, máximo = **{max(demoras):.0f} s**.", ""]
        ultimas = (kpis.orderBy(F.desc("inicio_ventana")).limit(5)
                   .select(F.date_format("inicio_ventana", "HH:mm").alias("v"), "intentos", "pedidos",
                           "k1_intentos_por_pedido", "k2_valor_promedio", "k3_eventos_riesgo",
                           "k3_porcentaje_riesgo", "k4_tasa_rechazo").collect())
        salida += [tabla(["Ventana (UTC)", "Intentos", "Pedidos", "K1", "K2 valor prom.", "K3 riesgo", "K3 %", "K4 rechazo %"],
                         [tuple(r) for r in ultimas]), ""]
    else:
        salida += ["Todavía no hay ventanas cerradas: la marca de agua aún no ha superado el fin de la primera ventana.", ""]

    salida += ["## 4. Alertas"]
    filas_alertas = []
    for nombre in ("alertas_multiples_intentos", "alertas_senales_combinadas"):
        df = leer(spark, dl / "curated" / nombre)
        if df is None:
            filas_alertas.append((nombre, 0, 0, 0, "—"))
            continue
        n = df.count()
        alta = df.filter("severidad = 'ALTA'").count()
        por_min = f"{n / minutos:.2f}" if minutos else "—"
        filas_alertas.append((nombre, n, n - alta, alta, por_min))
    salida += [tabla(["Alerta", "Total", "MEDIA", "ALTA", "Alertas por minuto"], filas_alertas), ""]

    # ------------------------------------------------------------ lotes de Spark
    registro = dl / "_metricas" / "progreso_streaming.jsonl"
    salida += ["## 5. Lotes de Spark (progreso_streaming.jsonl)"]
    if registro.exists():
        por_consulta: dict[str, list[dict]] = {}
        for linea in registro.read_text(encoding="utf-8").splitlines():
            if linea.strip():
                f = json.loads(linea)
                por_consulta.setdefault(f["consulta"], []).append(f)
        filas = []
        for consulta, lotes in sorted(por_consulta.items()):
            con_datos = [l for l in lotes if l["filas_entrada"]]
            dur = [l["duracion_lote_ms"] for l in con_datos if l["duracion_lote_ms"] is not None]
            filas.append((consulta, len(lotes), len(con_datos),
                          round(statistics.mean(dur)) if dur else None, percentil(dur, 95), max(dur) if dur else None,
                          round(statistics.mean(l["filas_entrada"] for l in con_datos)) if con_datos else 0,
                          max(l["filas_en_estado"] for l in lotes),
                          sum(l["filas_descartadas_tardias"] for l in lotes)))
        salida += [tabla(["Consulta", "Lotes", "Con datos", "Duración prom. (ms)", "p95 (ms)", "Máx. (ms)",
                          "Filas por lote", "Filas en estado (máx.)", "Descartadas por tardías"], filas), ""]
    else:
        salida += ["No existe el registro de lotes.", ""]

    salida += ["## 6. Archivos del Data Lake"]
    capas = [("raw/pagos", dl / "raw" / "pagos"), ("processed/pagos", dl / "processed" / "pagos"),
             ("curated/kpis_1min", dl / "curated" / "kpis_1min"),
             ("curated/alertas_multiples_intentos", dl / "curated" / "alertas_multiples_intentos"),
             ("curated/alertas_senales_combinadas", dl / "curated" / "alertas_senales_combinadas")]
    salida += [tabla(["Ruta", "Archivos Parquet", "Tamaño promedio (KB)"], [(n, *archivos(r)) for n, r in capas]), ""]

    texto = "\n".join(salida)
    print(texto)
    destino = dl / "_metricas" / f"resumen_{datetime.now():%Y%m%d_%H%M}.md"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(texto, encoding="utf-8")
    print(f"\n(Guardado en {destino})")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
