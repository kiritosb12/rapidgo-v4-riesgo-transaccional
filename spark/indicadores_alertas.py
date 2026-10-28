"""Indicadores por ventana (K1–K4) y alertas A1/A2 — capa curated.

Reciben el DataFrame de la capa processed (salida de transformaciones.a_processed), que ya
trae la marca de agua sobre fecha_evento. Los umbrales vienen de spark/reglas.py.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark import reglas as R

COLUMNAS_ALERTA = ["tipo_alerta", "severidad", "id_pedido", "id_cliente", "inicio_ventana", "fin_ventana",
                   "intentos", "rechazos", "puntaje_riesgo", "detalle", "fecha_deteccion"]


def indicadores_por_minuto(df: DataFrame) -> DataFrame:
    """K1 intentos por pedido, K2 valor promedio (total y por medio de pago), K3 eventos de riesgo, K4 tasa de rechazo."""
    por_medio = [F.round(F.avg(F.when(F.col("medio_pago") == m, F.col("valor"))), 0).alias(f"valor_promedio_{m}")
                 for m in R.MEDIOS_PAGO]
    return (
        df.groupBy(F.window("fecha_evento", R.VENTANA_KPI).alias("ventana"))
        .agg(
            F.count("*").alias("intentos"),
            F.approx_count_distinct("id_pedido").alias("pedidos"),
            F.max("numero_intento").alias("max_numero_intento"),
            F.round(F.avg("valor"), 0).alias("valor_promedio"),
            *por_medio,
            F.sum(F.col("es_evento_riesgo").cast("int")).alias("eventos_riesgo"),
            F.sum(F.col("es_rechazo").cast("int")).alias("rechazos"),
        )
        .select(
            F.col("ventana.start").alias("inicio_ventana"),
            F.col("ventana.end").alias("fin_ventana"),
            "intentos", "pedidos",
            F.round(F.col("intentos") / F.col("pedidos"), 3).alias("k1_intentos_por_pedido"),
            "max_numero_intento",
            F.col("valor_promedio").alias("k2_valor_promedio"),
            *[f"valor_promedio_{m}" for m in R.MEDIOS_PAGO],
            F.col("eventos_riesgo").alias("k3_eventos_riesgo"),
            F.round(100 * F.col("eventos_riesgo") / F.col("intentos"), 2).alias("k3_porcentaje_riesgo"),
            F.round(100 * F.col("rechazos") / F.col("intentos"), 2).alias("k4_tasa_rechazo"),
            F.current_timestamp().alias("fecha_calculo"),
        )
    )


def alertas_multiples_intentos(df: DataFrame) -> DataFrame:
    """A1 MULTIPLES_INTENTOS: mismo pedido con >= 3 intentos o >= 2 rechazos en una ventana de 5 minutos."""
    agregado = (
        df.groupBy(F.window("fecha_evento", R.VENTANA_A1).alias("ventana"), "id_pedido")
        .agg(
            F.first("id_cliente").alias("id_cliente"),
            F.count("*").alias("intentos"),
            F.sum(F.col("es_rechazo").cast("int")).alias("rechazos"),
            F.max("puntaje_riesgo").alias("puntaje_riesgo"),
        )
        .filter((F.col("intentos") >= R.UMBRAL_A1_INTENTOS) | (F.col("rechazos") >= R.UMBRAL_A1_RECHAZOS))
    )
    return agregado.select(
        F.lit("MULTIPLES_INTENTOS").alias("tipo_alerta"),
        F.when(F.col("intentos") >= R.UMBRAL_A1_ALTA, "ALTA").otherwise("MEDIA").alias("severidad"),
        "id_pedido", "id_cliente",
        F.col("ventana.start").alias("inicio_ventana"),
        F.col("ventana.end").alias("fin_ventana"),
        "intentos", "rechazos", "puntaje_riesgo",
        F.format_string("intentos=%d, rechazos=%d", "intentos", "rechazos").alias("detalle"),
        F.current_timestamp().alias("fecha_deteccion"),
    )


def alertas_senales_combinadas(df: DataFrame) -> DataFrame:
    """A2 SENALES_COMBINADAS: evento con puntaje >= 60 y al menos 2 señales (sin estado)."""
    return (
        df.filter((F.col("puntaje_riesgo") >= R.UMBRAL_EVENTO_RIESGO) & (F.col("senales_riesgo") >= R.MIN_SENALES_A2))
        .select(
            F.lit("SENALES_COMBINADAS").alias("tipo_alerta"),
            F.when(F.col("puntaje_riesgo") >= R.UMBRAL_A2_ALTA, "ALTA").otherwise("MEDIA").alias("severidad"),
            "id_pedido", "id_cliente",
            F.col("fecha_evento").alias("inicio_ventana"),
            F.col("fecha_evento").alias("fin_ventana"),
            F.lit(None).cast("long").alias("intentos"),
            F.col("es_rechazo").cast("long").alias("rechazos"),
            "puntaje_riesgo",
            F.concat(F.lit("senales="), F.col("lista_senales"), F.lit(", intento="), F.col("numero_intento"),
                     F.lit(", valor="), F.col("valor").cast("long"), F.lit(", pais_ip="), F.col("pais_ip")).alias("detalle"),
            F.current_timestamp().alias("fecha_deteccion"),
        )
    )
