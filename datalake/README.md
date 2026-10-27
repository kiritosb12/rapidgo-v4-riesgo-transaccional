# Data Lake — RapidGo V4 (Riesgo transaccional)

Lo escribe el trabajo `spark/streaming_pagos.py` (Semana 2). Todas las capas están en Parquet y cada consulta
tiene su propio checkpoint, así que si Spark se detiene continúa desde el último offset confirmado.

| Capa | Ruta | Contenido | Consulta de Spark |
|---|---|---|---|
| Raw (cruda) | `raw/pagos/fecha=AAAA-MM-DD/` | Mensaje JSON tal como llegó + clave, partición, offset y fecha de Kafka | `raw_pagos` |
| Processed (procesada) | `processed/pagos/fecha=AAAA-MM-DD/` | Evento con tipos correctos, sin inválidos ni duplicados (`id_evento`), con `hora_local`, `lista_senales`, `senales_riesgo`, `puntaje_riesgo`, `es_evento_riesgo`, `es_rechazo` | `processed_pagos` |
| Curated (curada) | `curated/kpis_1min/` | Indicadores K1–K4 por ventana fija de 1 minuto (`inicio_ventana`, `fin_ventana`, `fecha_calculo`) | `kpis_1min` |
| Curated | `curated/alertas_multiples_intentos/` | Alerta A1 por pedido y ventana de 5 minutos | `alertas_multiples_intentos` |
| Curated | `curated/alertas_senales_combinadas/` | Alerta A2 por evento | `alertas_senales_combinadas` |
| Curated (Semana 3) | `curated/reportes/` | Reporte generado por Airflow | — |
| Checkpoints | `_checkpoints/<consulta>/` | Offsets de Kafka y estado (ventanas abiertas, id_evento vistos) | todas |
| Métricas | `_metricas/progreso_streaming.jsonl` | Duración y filas de cada lote (lo escribe Spark) | — |
| Métricas | `_metricas/productor_resumen.json` · `_metricas/resumen_*.md` | Resumen del productor y de `scripts/metricas_semana2.py` | — |

- `fecha` es la fecha **local de Bogotá** del evento; las columnas de tiempo se guardan en UTC.
- Las fechas `fecha` de raw salen de la marca de tiempo de Kafka; las de processed, de `fecha_evento`.
- Las alertas A1 y A2 van en carpetas separadas porque en Spark dos consultas continuas no pueden escribir en la misma ruta.

Leer una capa (por ejemplo dentro del contenedor de Spark):

```bash
docker compose exec spark python -c "from pyspark.sql import SparkSession as S; S.builder.getOrCreate().read.parquet('/app/datalake/curated/kpis_1min').orderBy('inicio_ventana').show(truncate=False)"
```

Los datos generados no se suben a Git (ver `.gitignore`); solo se versiona la estructura de carpetas.
Para volver a empezar desde cero: detener los servicios y borrar `raw/`, `processed/`, `curated/`, `_checkpoints/` y `_metricas/`.
