# Métricas reales de ejecución — 2026-10-26 03:28

## 1. Kafka → capa raw
Mensajes recibidos: **9077**

| Partición | Mensajes | % del total | Desviación vs. promedio | Offset mín. | Offset máx. |
|---|---|---|---|---|---|
| 0 | 3044 | 33.5 % | +0.6 % | 0 | 3043 |
| 1 | 2999 | 33.0 % | -0.9 % | 0 | 2998 |
| 2 | 3034 | 33.4 % | +0.3 % | 0 | 3033 |

## 2. Capa processed
| Medida | Valor |
|---|---|
| Mensajes inválidos u de otro equipo descartados | 0 |
| Duplicados en raw (mismo id_evento) | 89 |
| Eventos en processed | 8988 |
| Pedidos distintos | 7346 |
| Minutos de datos (tiempo del evento) | 15.0 |
| Eventos por segundo (promedio) | 9.99 |
| Eventos de riesgo (puntaje ≥ 60) | 403 (4.48 %) |
| Retraso evento → processed p50 (s) | 5.1 |
| Retraso evento → processed p95 (s) | 9.7 |
| Retraso evento → processed máximo (s) | 26.6 |

Combinaciones de señales más frecuentes:

| Señales | Puntaje | Eventos |
|---|---|---|
| S3 | 20 | 325 |
| S1 | 30 | 190 |
| S1,S2,S4 | 70 | 187 |
| S2,S3,S4 | 60 | 174 |
| S2,S4 | 40 | 149 |
| S2 | 25 | 141 |
| S3,S4 | 35 | 99 |
| S4 | 15 | 78 |

## 3. Capa curated — indicadores por minuto
Ventanas publicadas: **5**. Demora entre el cierre de la ventana y su escritura: p50 = **620 s**, máximo = **620 s**.

| Ventana (UTC) | Intentos | Pedidos | K1 | K2 valor prom. | K3 riesgo | K3 % | K4 rechazo % |
|---|---|---|---|---|---|---|---|
| 03:14 | 599 | 513 | 1.168 | 123919.0 | 29 | 4.84 | 24.87 |
| 03:13 | 599 | 513 | 1.168 | 102524.0 | 25 | 4.17 | 24.54 |
| 03:12 | 599 | 559 | 1.072 | 128074.0 | 29 | 4.84 | 20.03 |
| 03:11 | 599 | 521 | 1.15 | 101575.0 | 33 | 5.51 | 25.54 |
| 03:10 | 19 | 18 | 1.056 | 193058.0 | 1 | 5.26 | 21.05 |

## 4. Alertas
| Alerta | Total | MEDIA | ALTA | Alertas por minuto |
|---|---|---|---|---|
| alertas_multiples_intentos | 86 | 56 | 30 | 5.73 |
| alertas_senales_combinadas | 403 | 376 | 27 | 26.87 |

## 5. Lotes de Spark (progreso_streaming.jsonl)
| Consulta | Lotes | Con datos | Duración prom. (ms) | p95 (ms) | Máx. (ms) | Filas por lote | Filas en estado (máx.) | Descartadas por tardías |
|---|---|---|---|---|---|---|---|---|
| alertas_multiples_intentos | 91 | 89 | 5600 | 7353 | 12408 | 102 | 15556 | 0 |
| alertas_senales_combinadas | 91 | 89 | 5021 | 7427 | 8786 | 102 | 8988 | 0 |
| kpis_1min | 91 | 89 | 5552 | 7981 | 12440 | 102 | 8999 | 0 |
| processed_pagos | 91 | 89 | 6557 | 8492 | 9776 | 102 | 8988 | 0 |
| raw_pagos | 90 | 89 | 6015 | 8317 | 9735 | 102 | 0 | 0 |

## 6. Archivos del Data Lake
| Ruta | Archivos Parquet | Tamaño promedio (KB) |
|---|---|---|
| raw/pagos | 267 | 8.2 |
| processed/pagos | 267 | 11.6 |
| curated/kpis_1min | 94 | 2.1 |
| curated/alertas_multiples_intentos | 92 | 1.3 |
| curated/alertas_senales_combinadas | 230 | 3.5 |
