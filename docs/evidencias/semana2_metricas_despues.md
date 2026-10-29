# Métricas reales de ejecución — 2026-10-26 03:50

## 1. Kafka → capa raw
Mensajes recibidos: **6038**

| Partición | Mensajes | % del total | Desviación vs. promedio | Offset mín. | Offset máx. |
|---|---|---|---|---|---|
| 0 | 1965 | 32.5 % | -2.4 % | 0 | 1964 |
| 1 | 2073 | 34.3 % | +3.0 % | 0 | 2072 |
| 2 | 2000 | 33.1 % | -0.6 % | 0 | 1999 |

## 2. Capa processed
| Medida | Valor |
|---|---|
| Mensajes inválidos u de otro equipo descartados | 0 |
| Duplicados en raw (mismo id_evento) | 46 |
| Eventos en processed | 5992 |
| Pedidos distintos | 4907 |
| Minutos de datos (tiempo del evento) | 10.0 |
| Eventos por segundo (promedio) | 9.99 |
| Eventos de riesgo (puntaje ≥ 60) | 331 (5.52 %) |
| Retraso evento → processed p50 (s) | 5.0 |
| Retraso evento → processed p95 (s) | 9.5 |
| Retraso evento → processed máximo (s) | 11.5 |

Combinaciones de señales más frecuentes:

| Señales | Puntaje | Eventos |
|---|---|---|
| S2,S3,S4 | 60 | 157 |
| S3 | 20 | 150 |
| S1,S2,S4 | 70 | 138 |
| S2,S4 | 40 | 126 |
| S2 | 25 | 124 |
| S1 | 30 | 124 |
| S3,S4 | 35 | 63 |
| S4 | 15 | 48 |

## 3. Capa curated — indicadores por minuto
Ventanas publicadas: **8**. Demora entre el cierre de la ventana y su escritura: p50 = **140 s**, máximo = **140 s**.

| Ventana (UTC) | Intentos | Pedidos | K1 | K2 valor prom. | K3 riesgo | K3 % | K4 rechazo % |
|---|---|---|---|---|---|---|---|
| 03:45 | 599 | 516 | 1.161 | 152385.0 | 45 | 7.51 | 27.88 |
| 03:44 | 600 | 510 | 1.176 | 112169.0 | 36 | 6.0 | 22.5 |
| 03:43 | 599 | 518 | 1.156 | 120365.0 | 36 | 6.01 | 24.54 |
| 03:42 | 599 | 550 | 1.089 | 170517.0 | 38 | 6.34 | 23.37 |
| 03:41 | 599 | 535 | 1.12 | 113760.0 | 28 | 4.67 | 22.37 |

## 4. Alertas
| Alerta | Total | MEDIA | ALTA | Alertas por minuto |
|---|---|---|---|---|
| alertas_multiples_intentos | 137 | 96 | 41 | 13.70 |
| alertas_senales_combinadas | 331 | 313 | 18 | 33.10 |

## 5. Lotes de Spark (progreso_streaming.jsonl)
| Consulta | Lotes | Con datos | Duración prom. (ms) | p95 (ms) | Máx. (ms) | Filas por lote | Filas en estado (máx.) | Descartadas por tardías |
|---|---|---|---|---|---|---|---|---|
| alertas_multiples_intentos | 62 | 60 | 5041 | 6689 | 11072 | 101 | 6046 | 0 |
| alertas_senales_combinadas | 62 | 60 | 4407 | 6280 | 7821 | 101 | 2498 | 0 |
| kpis_1min | 62 | 60 | 5330 | 7121 | 11152 | 101 | 2501 | 0 |
| processed_pagos | 62 | 60 | 6086 | 7962 | 9634 | 101 | 2498 | 0 |
| raw_pagos | 61 | 60 | 5280 | 6980 | 8611 | 101 | 0 | 0 |

## 6. Archivos del Data Lake
| Ruta | Archivos Parquet | Tamaño promedio (KB) |
|---|---|---|
| raw/pagos | 180 | 8.1 |
| processed/pagos | 180 | 11.6 |
| curated/kpis_1min | 67 | 2.3 |
| curated/alertas_multiples_intentos | 65 | 1.5 |
| curated/alertas_senales_combinadas | 157 | 3.4 |
