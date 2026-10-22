# Data Lake — RapidGo V4 (Riesgo transaccional)

| Capa | Ruta | Formato | Contenido | Quién escribe |
|---|---|---|---|---|
| Raw (cruda) | `raw/pagos/fecha=AAAA-MM-DD/` | Parquet (mensaje JSON + metadatos de Kafka: partición, offset, marca de tiempo) | El evento tal como llegó, sin modificar | Spark |
| Processed (procesada) | `processed/pagos/fecha=AAAA-MM-DD/` | Parquet | Evento con tipos de dato correctos, sin duplicados por `id_evento`, con `hora_local`, `senales_riesgo` y `puntaje_riesgo` | Spark |
| Curated (curada) | `curated/kpis_1min/` · `curated/alertas/` · `curated/reportes/` | Parquet / CSV / JSON | Indicadores por ventana, alertas A1 y A2, reporte diario | Spark y Airflow |
| Checkpoints | `_checkpoints/<consulta>/` | interno de Spark | Offsets y estado de cada consulta continua | Spark |

Los datos generados no se suben a Git (ver `.gitignore`); solo se versiona la estructura de carpetas.
