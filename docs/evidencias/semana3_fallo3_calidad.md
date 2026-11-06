# Fallo controlado 3 — Mensajes dañados en el topic

- **Responsable:** Karol Perez · **Fecha y hora:** 2026-11-05 14:46 (hora de Bogotá) · **Ejecución:** `manual__2026-11-05T19:45:25.346796+00:00`
- **Tipo:** falla de **calidad** de datos → Airflow **no** reintenta (AirflowFailException).

## Qué se provocó

Con el flujo corriendo, se publicaron mensajes que no son JSON en el topic, como lo haría un productor
defectuoso. El comando se ejecutó dos veces por un error de comillas en cmd de Windows, así que en total
fueron **600 mensajes** (300 + 300), no 300:

```bash
docker compose exec -T kafka sh -c "i=1; while [ $i -le 300 ]; do echo mensaje-danado-$i; i=$((i+1)); done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic rapidgo.v4.pagos.intentos"
```

Spark los guardó en raw (es la copia fiel) y los descartó en processed. Trigger del DAG a las 14:46:02.

Nota: durante la prueba Airflow también lanzó sola la ejecución programada
`scheduled__2026-11-05T19:00:00+00:00` (14:45:25), que falló en `validar_raw` por los mismos mensajes.
Este informe documenta la ejecución manual.

## Tarea que falló

`validar_raw`, **un solo intento** (14:46:17 a 14:46:23). `verificar_fuentes` pasó en verde.

## Mensaje real del registro

```
AirflowFailException: validar_raw — 1 chequeo(s) crítico(s) fallaron: % mensajes inválidos o de otro equipo:
valor=5.7 % (600 de 10523; p.ej. p2:7919, p2:7920, p2:7921) (umbral <= 1.0 %)
```

## Tareas afectadas

`agregar_kpis`, `consolidar_alertas`, `validar_curated` y `generar_reporte` → **upstream_failed**: no se
generó un reporte con datos contaminados.

## Cambio que resolvió el problema (PR #44)

raw no se borra ni se edita, así que se agregó una **cuarentena**: `orquestacion/cuarentena.py` registra los
mensajes dañados ya revisados en `datalake/_cuarentena/raw_pagos.csv` y `validar_raw` deja de contarlos.

```bash
docker compose restart airflow-scheduler airflow-dag-processor     # cargar el código nuevo
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha 2026-11-05 --solo-mostrar
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha 2026-11-05 --motivo "fallo controlado 3"
```

Salida: 600 mensajes dañados, 300 en la partición 1 (offsets 8314 a 8613) y 300 en la partición 2
(offsets 7919 a 8218). Quedaron registrados 600 mensajes nuevos en cuarentena.

## Recuperación

Clear de `validar_raw` con Downstream a las 18:40:08. `validar_raw` pasó en verde en el intento 2
(18:40:08 a 18:40:35) y ya no cuenta los 600 mensajes en cuarentena como inválidos.

La ejecución `manual__2026-11-05T23:28:53.277630+00:00` terminó con las 6 tareas en verde y generó el reporte.
En `validar_raw`: **0.0 % (0 de 78304)** mensajes inválidos y **600** mensajes en cuarentena
("Mensajes en cuarentena (dañados ya revisados)"). `raw` no se modificó: los mensajes dañados siguen ahí, solo aislados.

## Capturas

`semana3_fallo3_tareas.png` (grafo con `validar_raw` en rojo y 4 tareas en upstream_failed) ·
`semana3_fallo3_log.png` (el error de `validar_raw`) · `semana3_fallo3_recuperado.png` (la misma ejecución
en verde después del Clear)
