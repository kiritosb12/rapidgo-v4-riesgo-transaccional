# Evidencias de ejecución

Capturas y salidas reales del equipo que respaldan los puntos de control y los ADR.

| Archivo | Semana | Qué muestra | Cómo se obtiene |
|---|---|---|---|
| `semana2_kafka_ui_particiones.png` | 2 | Topic `rapidgo.v4.pagos.intentos` con 3 particiones y sus offsets | Kafka UI → Topics → el topic → pestaña *Overview* |
| `semana2_kafka_ui_mensaje.png` | 2 | Un mensaje con su clave `id_pedido` | Kafka UI → *Messages* |
| `semana2_spark_ui_streaming.png` | 2 | Las 5 consultas activas y su tasa de entrada | http://localhost:4040 → *Structured Streaming* |
| `semana2_metricas_antes.md` | 2 | Medición con la marca de agua de 10 minutos | `scripts/metricas_semana2.py` (copiar de `datalake/_metricas/`) |
| `semana2_metricas_despues.md` | 2 | Medición después del ADR-002 | igual |
| `semana2_actions.png` | 2 | Un Pull Request con el CI en verde | pestaña *Checks* del PR |
| `semana3_airflow_inicio.png` | 3 | Interfaz de Airflow recién levantada | http://localhost:8081 |
| `semana3_airflow_dag_grafo.png` | 3 | Vista *Graph* del DAG con las 6 tareas en verde | Airflow → DAG → ejecución → *Graph* |
| `semana3_fallo1_kafka.md` + `semana3_fallo1_*.png` | 3 | Fallo controlado 1 (Esteban): Kafka detenido | ficha con horas, mensaje real y recuperación |
| `semana3_fallo2_spark.md` + `semana3_fallo2_*.png` | 3 | Fallo controlado 2 (Oscar): Spark detenido | ídem |
| `semana3_fallo3_calidad.md` + `semana3_fallo3_*.png` | 3 | Fallo controlado 3 (Karol): mensajes dañados y cuarentena | ídem |
| `semana3_reporte.md` | 3 | Un reporte generado por el DAG | copiar de `datalake/curated/reportes/fecha=…/` |
| `semana3_actions.png` | 3 | Un PR con los trabajos `validar` y `validar-dag` en verde | pestaña *Checks* del PR |
