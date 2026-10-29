# ADR-002 — Retraso máximo (marca de agua) de las ventanas de Spark

- **Estado:** Aceptada (Semana 2, v0.2.0-streaming)
- **Responsable:** Oscar Becerra · **Revisores:** Karol Perez, Esteban Rueda
- **Issue relacionado:** #12 · **Pull Request:** #35
- **Evidencia:** `docs/evidencias/semana2_metricas_antes.md` y `docs/evidencias/semana2_metricas_despues.md`
  (salida de `scripts/metricas_semana2.py`)

## Contexto / problema
En la Semana 1 definimos una marca de agua de **10 minutos** sobre `fecha_evento` para las ventanas de los
indicadores (1 minuto) y de la alerta A1 (5 minutos por `id_pedido`). Spark escribe una ventana en modo
*append* solo cuando la marca de agua supera su fin, así que el valor elegido decide **cuánto tarda en
aparecer cada resultado** y **cuántos eventos tardíos se aceptan**.

Al ejecutar el flujo completo con Docker el 25 de octubre de 2026 (22:10, hora de Bogotá) durante 15 minutos a 10 eventos
por segundo (9077 mensajes) observamos:

| Medida (antes, marca de agua 10 min) | Valor real |
|---|---|
| Retraso evento → processed p95 / máximo | 9,7 s / 26,6 s |
| Tiempo hasta la primera ventana de indicadores escrita | 10,4 min |
| Demora cierre de ventana → escritura (p50) | 620 s |
| Alertas A1 escritas en los primeros 10 minutos | 0 (la primera apareció a los 14,4 min) |
| Filas en estado de la consulta A1 (máximo) | 15 556 |
| Filas descartadas por llegar tarde | 0 |

El objetivo del proyecto es detectar el riesgo **en minutos**; con 10 minutos de marca de agua un indicador
de 1 minuto aparece unos 11 minutos después y una alerta A1 hasta 15 minutos después, mientras que los
eventos reales llegan con un retraso máximo de 26,6 segundos (p95 de 9,7 s).

## Opciones consideradas
| Opción | A favor | En contra |
|---|---|---|
| Mantener 10 minutos | Acepta eventos con mucho retraso (por ejemplo, un celular sin señal). | Indicadores ~11 min tarde y A1 hasta ~15 min tarde; más filas en el estado. |
| **2 minutos** | Unas 4,5 veces el retraso máximo observado; indicadores ~2–3 min y A1 ≤ 7 min después; menos estado. | Un evento que llegue con más de 2 min de retraso no cuenta en su ventana (sí queda en raw y processed). |
| 30 segundos | Resultados casi inmediatos. | Muy cerca del retraso observado; con un pico de carga o una pausa del productor se perderían eventos de las ventanas. |
| Modo `update` en vez de `append` | Resultados parciales en cada lote. | El sumidero de archivos Parquet solo admite `append`; habría que cambiar de destino. |

## Decisión tomada
Usar **`RETRASO_MAXIMO = 2 minutes`** como valor por defecto (variable de entorno del servicio `spark` en
`docker-compose.yml` y valor por defecto de `spark/streaming_pagos.py`). Se puede cambiar sin tocar el código:

```bash
RETRASO_MAXIMO="5 minutes" docker compose up -d spark
```

La marca de agua es la misma para la eliminación de duplicados por `id_evento`, los indicadores y la alerta A1.

## Evidencia utilizada
- Ejecución antes del cambio: `docs/evidencias/semana2_metricas_antes.md` (tablas 2, 3 y 5).
- Ejecución después del cambio (10 minutos, 6038 mensajes): primera ventana escrita a los 3,2 min, demora p50 de 140 s,
  137 alertas A1 (la primera a los 4,2 min), 0 filas descartadas por llegar tarde, retraso máximo de 11,5 s y
  6046 filas en estado de la consulta A1 como máximo → `docs/evidencias/semana2_metricas_despues.md`.
- Registro de lotes: `datalake/_metricas/progreso_streaming.jsonl` (columna `filas_descartadas_tardias`).

## Consecuencias / compromisos
- Los indicadores llegan unos 8 minutos antes (demora de 620 s a 140 s) y la primera alerta A1 unos 10 minutos antes
  (de 14,4 a 4,2 min). El estado de A1 baja de 15 556 a 6046 filas como máximo.
- Los eventos con más de 2 minutos de retraso se descartan de las ventanas: se vigila
  `filas_descartadas_tardias` en cada ejecución; si deja de ser 0, se revisa este ADR.
- Al cambiar la marca de agua hay que borrar los checkpoints de las consultas con estado
  (`datalake/_checkpoints/processed_pagos`, `kpis_1min`, `alertas_*`) o empezar con un Data Lake limpio.
- Pendiente para la Semana 3: A2 genera unas 27–33 alertas por minuto porque alerta cada intento riesgoso (un pedido de
  prueba de tarjetas produce varias); agruparlas por pedido en el reporte de Airflow. Además, en 15 minutos raw acumuló
  267 archivos Parquet de unos 8 KB: compactarlos en el DAG o subir `INTERVALO_LOTE`.
