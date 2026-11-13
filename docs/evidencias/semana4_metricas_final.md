# Métricas reales de ejecución — 2026-11-13 13:45

## 1. Kafka → capa raw
Mensajes recibidos: **11409**

| Partición | Mensajes | % del total | Desviación vs. promedio | Offset mín. | Offset máx. |
|---|---|---|---|---|---|
| 0 | 3737 | 32.8 % | -1.7 % | 0 | 3736 |
| 1 | 3799 | 33.3 % | -0.1 % | 0 | 3798 |
| 2 | 3873 | 33.9 % | +1.8 % | 0 | 3872 |

## 2. Capa processed
| Medida | Valor |
|---|---|
| Mensajes inválidos u de otro equipo descartados | 0 |
| Duplicados en raw (mismo id_evento) | 119 |
| Eventos en processed | 11981 |
| Pedidos distintos | 9699 |
| Minutos de datos (tiempo del evento) | 20.0 |
| Eventos por segundo (promedio) | 9.99 |
| Eventos de riesgo (puntaje ≥ 60) | 586 (4.89 %) |
| Retraso evento → processed p50 (s) | 5.4 |
| Retraso evento → processed p95 (s) | 12.0 |
| Retraso evento → processed máximo (s) | 31.8 |

Combinaciones de señales más frecuentes:

| Señales | Puntaje | Eventos |
|---|---|---|
| S3 | 20 | 442 |
| S1 | 30 | 312 |
| S2,S3,S4 | 60 | 308 |
| S2,S4 | 40 | 242 |
| S1,S2,S4 | 70 | 219 |
| S2 | 25 | 186 |
| S3,S4 | 35 | 109 |
| S4 | 15 | 90 |

## 3. Capa curated — indicadores por minuto
Ventanas publicadas: **20**. Demora entre el cierre de la ventana y su escritura: p50 = **140 s**, máximo = **149 s**.

| Ventana (UTC) | Intentos | Pedidos | K1 | K2 valor prom. | K3 riesgo | K3 % | K4 rechazo % |
|---|---|---|---|---|---|---|---|
| 13:45 | 599 | 530 | 1.13 | 124186.0 | 23 | 3.84 | 18.03 |
| 13:44 | 599 | 492 | 1.217 | 116073.0 | 27 | 4.51 | 25.54 |
| 13:43 | 600 | 516 | 1.163 | 127911.0 | 41 | 6.83 | 22.5 |
| 13:42 | 599 | 535 | 1.12 | 110675.0 | 25 | 4.17 | 22.37 |
| 13:41 | 599 | 574 | 1.044 | 107584.0 | 27 | 4.51 | 22.54 |

## 4. Alertas
| Alerta | Total | MEDIA | ALTA | Alertas por minuto |
|---|---|---|---|---|
| alertas_multiples_intentos | 412 | 274 | 138 | 20.61 |
| alertas_senales_combinadas | 649 | 621 | 28 | 32.46 |

## 5. Lotes de Spark (progreso_streaming.jsonl)
| Consulta | Lotes | Con datos | Duración prom. (ms) | p95 (ms) | Máx. (ms) | Filas por lote | Filas en estado (máx.) | Descartadas por tardías |
|---|---|---|---|---|---|---|---|---|
| alertas_multiples_intentos | 125 | 125 | 8174 | 18189 | 21525 | 113 | 6106 | 0 |
| alertas_senales_combinadas | 127 | 127 | 8247 | 18032 | 20319 | 112 | 2601 | 0 |
| kpis_1min | 125 | 125 | 8286 | 18041 | 20500 | 112 | 2606 | 0 |
| processed_pagos | 125 | 125 | 9094 | 18082 | 23050 | 113 | 2629 | 0 |
| raw_pagos | 123 | 123 | 8807 | 18572 | 24998 | 114 | 0 | 0 |

## 6. Archivos del Data Lake
| Ruta | Archivos Parquet | Tamaño promedio (KB) |
|---|---|---|
| raw/pagos | 369 | 8.7 |
| processed/pagos | 375 | 12.0 |
| curated/kpis_1min | 142 | 2.4 |
| curated/alertas_multiples_intentos | 134 | 1.5 |
| curated/alertas_senales_combinadas | 321 | 3.6 |


(Guardado en /app/datalake/_metricas/resumen_20261113_1350.md)
