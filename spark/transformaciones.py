"""Transformaciones de Spark para los intentos de pago (capas raw y processed).

Todas las funciones reciben y devuelven DataFrames, así que sirven igual para streaming
(Kafka) y para lotes (pruebas con pytest). No se usan UDF de Python: todo son expresiones
nativas de Spark construidas con las constantes de `spark/reglas.py`.

Requisito: la sesión debe tener `spark.sql.session.timeZone = UTC` (ver crear_sesion()).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from spark import reglas as R

EQUIPO = "equipo9-v4"

# Esquema del contrato intento_pago 1.0 (samples/evento_pago.schema.json).
# fecha_evento se lee como texto y se convierte después para poder descartar fechas inválidas.
ESQUEMA_EVENTO = StructType([
    StructField("id_evento", StringType()),
    StructField("tipo_evento", StringType()),
    StructField("version_esquema", StringType()),
    StructField("equipo", StringType()),
    StructField("fecha_evento", StringType()),
    StructField("id_pedido", StringType()),
    StructField("id_cliente", StringType()),
    StructField("valor", DoubleType()),
    StructField("moneda", StringType()),
    StructField("medio_pago", StringType()),
    StructField("id_dispositivo", StringType()),
    StructField("tipo_dispositivo", StringType()),
    StructField("pais_ip", StringType()),
    StructField("ciudad", StringType()),
    StructField("latitud", DoubleType()),
    StructField("longitud", DoubleType()),
    StructField("numero_intento", IntegerType()),
    StructField("estado", StringType()),
    StructField("motivo_rechazo", StringType()),
])


def _fecha_local(col: Column) -> Column:
    return F.from_utc_timestamp(col, R.ZONA_HORARIA)


# ------------------------------------------------------------------ capa raw
def a_raw(df_kafka: DataFrame) -> DataFrame:
    """Mensaje tal como llegó + metadatos de Kafka (partición, offset). Sin interpretar el JSON."""
    return df_kafka.select(
        F.col("key").cast("string").alias("clave"),
        F.col("value").cast("string").alias("mensaje_json"),
        F.col("topic").alias("topic"),
        F.col("partition").alias("particion"),
        F.col("offset").alias("offset"),
        F.col("timestamp").alias("fecha_kafka"),
        F.current_timestamp().alias("fecha_ingesta"),
        F.to_date(_fecha_local(F.col("timestamp"))).alias("fecha"),
    )


# ------------------------------------------------------------------ transformación 1: lectura con esquema
def parsear(df_kafka: DataFrame) -> DataFrame:
    """Interpreta el JSON con el esquema del contrato y conserva partición/offset para trazabilidad."""
    return (
        df_kafka
        .select(
            F.from_json(F.col("value").cast("string"), ESQUEMA_EVENTO).alias("e"),
            F.col("partition").alias("particion"),
            F.col("offset").alias("offset"),
        )
        .select("e.*", "particion", "offset")
    )


# ------------------------------------------------------------------ transformación 2: limpieza y tipos
def limpiar(df: DataFrame) -> DataFrame:
    """Descarta registros inválidos o de otro equipo, convierte tipos y normaliza textos."""
    return (
        df
        .withColumn("fecha_evento", F.try_to_timestamp("fecha_evento"))   # fecha inválida → nulo, no detiene el flujo
        .withColumn("pais_ip", F.upper(F.trim("pais_ip")))
        .withColumn("medio_pago", F.lower(F.trim("medio_pago")))
        .withColumn("estado", F.lower(F.trim("estado")))
        .filter(
            (F.col("equipo") == EQUIPO)
            & (F.col("tipo_evento") == "intento_pago")
            & F.col("id_evento").isNotNull()
            & F.col("id_pedido").isNotNull()
            & F.col("fecha_evento").isNotNull()
            & (F.col("valor") > 0)
            & (F.col("numero_intento") >= 1)
        )
    )


def quitar_duplicados(df: DataFrame, retraso_maximo: str) -> DataFrame:
    """Marca de agua sobre fecha_evento y eliminación de reenvíos con el mismo id_evento.

    En streaming guarda en el estado los id_evento vistos durante `retraso_maximo`;
    en lotes equivale a dropDuplicates.
    """
    df = df.withWatermark("fecha_evento", retraso_maximo)
    if df.isStreaming:
        return df.dropDuplicatesWithinWatermark(["id_evento"])
    return df.dropDuplicates(["id_evento"])


# ------------------------------------------------------------------ transformación 3: señales y puntaje
def expresiones_senales() -> dict[str, Column]:
    hora = F.hour(_fecha_local(F.col("fecha_evento")))
    return {
        "S1": F.col("valor") >= R.UMBRAL_VALOR_ALTO,
        "S2": F.col("pais_ip") != R.PAIS_LOCAL,
        "S3": F.col("numero_intento") >= R.UMBRAL_REINTENTO,
        "S4": F.coalesce(F.col("motivo_rechazo").isin(*R.MOTIVOS_SOSPECHOSOS), F.lit(False)),
        "S5": hora.between(R.HORA_ATIPICA_INICIO, R.HORA_ATIPICA_FIN),
    }


def enriquecer(df: DataFrame) -> DataFrame:
    """Agrega hora local, señales S1–S5, cantidad de señales, puntaje de riesgo y fecha de partición."""
    s = expresiones_senales()
    puntaje = sum(F.when(expr, R.PESOS[codigo]).otherwise(0) for codigo, expr in s.items())
    cantidad = sum(F.when(expr, 1).otherwise(0) for expr in s.values())
    lista = F.concat_ws(",", *[F.when(expr, F.lit(codigo)) for codigo, expr in s.items()])
    return (
        df
        .withColumn("hora_local", F.hour(_fecha_local(F.col("fecha_evento"))))
        .withColumn("senales_riesgo", cantidad.cast("int"))
        .withColumn("lista_senales", lista)
        .withColumn("puntaje_riesgo", puntaje.cast("int"))
        .withColumn("es_evento_riesgo", F.col("puntaje_riesgo") >= R.UMBRAL_EVENTO_RIESGO)
        .withColumn("es_rechazo", F.col("estado") == "rechazado")
        .withColumn("fecha_procesamiento", F.current_timestamp())
        .withColumn("fecha", F.to_date(_fecha_local(F.col("fecha_evento"))))
    )


def a_processed(df_kafka: DataFrame, retraso_maximo: str = "10 minutes") -> DataFrame:
    """Cadena completa raw → processed: parsear → limpiar → quitar duplicados → enriquecer."""
    return enriquecer(quitar_duplicados(limpiar(parsear(df_kafka)), retraso_maximo))
