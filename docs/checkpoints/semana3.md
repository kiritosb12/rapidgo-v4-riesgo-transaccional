# Punto de control Semana 3 — v0.3.0-orchestration

**Equipo 9 · Variante 4: Riesgo transaccional** · Esteban Rueda · Oscar Becerra · Karol Perez

## 1. Qué quedó funcionando esta semana
Airflow 3.3.2 en Docker Compose (PostgreSQL, interfaz en el puerto 8081, scheduler y dag-processor) con el DAG
`rapidgo_riesgo_transaccional`: 6 tareas, dos en paralelo, 2 reintentos y XCom; valida fuentes, raw y curated,
resume por hora, agrupa las alertas por pedido y genera un reporte. CI con el trabajo `validar-dag`; 94 pruebas
(82 aprobadas y 12 omitidas en local sin Java ni Airflow; en el CI corren todas). Ejecución de referencia
`manual__evidencia_20261105T134958`: verde en 23.7 s, 3950 mensajes revisados, 200 alertas A2 agrupadas en 90 pedidos (−55.0 %).

## 2–5. Fallos controlados (uno por integrante)
| | Esteban: Kafka detenido | Oscar: Spark detenido | Karol: mensajes dañados |
|---|---|---|---|
| **Tarea que falló** | `verificar_fuentes` (3 intentos) | `verificar_fuentes` (3 intentos, 09:29–09:30) | `validar_raw` (1 intento, sin reintento, 14:46) |
| **Mensaje real del registro** | pendiente: la ficha del PR #42 quedó sin los datos de la ejecución | `FalloFuente: La capa raw no recibe datos nuevos hace 14.7 min (máximo 10.0 min): ¿Spark o el productor están detenidos?` | `AirflowFailException: validar_raw — … % mensajes inválidos o de otro equipo: valor=5.7 % (600 de 10523; …) (umbral <= 1.0 %)` |
| **Tareas afectadas** | las 5 siguientes en upstream_failed | las 5 siguientes en upstream_failed | las 4 siguientes en upstream_failed |
| **Cambio que lo resolvió** | `docker compose start kafka` + Clear | PR #43 (falla si hoy no llegan datos) + `start spark` + Clear | PR #44 (cuarentena) + `orquestacion.cuarentena` + Clear |
| **Verde de nuevo** | pendiente | 09:42:39 (13 min); Spark siguió desde el checkpoint (12 453 mensajes acumulados), 0 huecos | 18:40; 0.0 % inválidos (0 de 78304), 600 en cuarentena |

Detalle y capturas: [fallo 1](../evidencias/semana3_fallo1_kafka.md) · [fallo 2](../evidencias/semana3_fallo2_spark.md) ·
[fallo 3](../evidencias/semana3_fallo3_calidad.md). Estrategia: [ADR-003](../decisions/ADR-003-orquestacion-y-recuperacion.md).
Otros problemas reales de la semana: la consolidación de alertas leía una foto distinta a la del chequeo mientras
Spark escribía (PR #39), y `validar_curated` fallaba con datos correctos por el K1 aproximado de
`approx_count_distinct` y por alertas A2 más recientes que lo último de processed (PR #43).

## 6. Enlaces
- Issues: #13, #14, #15, #16
- Pull Requests: #37 Airflow en Compose · #38 lectura y validar_raw · #39 indicadores, alertas y validar_curated ·
  #40 DAG y reporte · #41 CI con el DAG · #42 fallo 1 · #43 fallo 2 · #44 fallo 3 · #45 ADR-003 y punto de control
- GitHub Actions: [ejecución con los dos trabajos en verde](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/runs/41874627452) · Versión: `v0.3.0-orchestration`
