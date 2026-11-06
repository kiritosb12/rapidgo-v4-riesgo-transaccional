# Fallo controlado 3 — Mensajes dañados en el topic

- **Responsable:** Karol Perez · **Fecha y hora:** ⟨AAAA-MM-DD hh:mm⟩ · **Ejecución:** ⟨run_id⟩
- **Tipo:** falla de **calidad** de datos → Airflow **no** reintenta (AirflowFailException).

## Qué se provocó
Con el flujo corriendo, se publicaron 300 mensajes que no son JSON en el topic, como lo haría un productor
defectuoso:

```bash
docker compose exec -T kafka sh -c 'i=1; while [ $i -le 300 ]; do echo "mensaje danado $i"; i=$((i+1)); done | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic rapidgo.v4.pagos.intentos'
```
Spark los guardó en raw (es la copia fiel) y los descartó en processed. Trigger del DAG a las ⟨hh:mm:ss⟩.

## Tarea que falló
`validar_raw`, **un solo intento** (⟨hh:mm:ss⟩). `verificar_fuentes` pasó en verde.

## Mensaje real del registro
```
⟨AirflowFailException: validar_raw — 1 chequeo(s) crítico(s) fallaron: % mensajes inválidos o de otro equipo:
valor=__ % (___ de ____; p.ej. p_:____, …) (umbral <= 1.0 %)⟩
```

## Tareas afectadas
`agregar_kpis`, `consolidar_alertas`, `validar_curated` y `generar_reporte` → **upstream_failed**: no se
generó un reporte con datos contaminados.

## Cambio que resolvió el problema (PR ⟨#N⟩)
raw no se borra ni se edita, así que se agregó una **cuarentena**: `orquestacion/cuarentena.py` registra los
mensajes dañados ya revisados en `datalake/_cuarentena/raw_pagos.csv` y `validar_raw` deja de contarlos.

```bash
docker compose restart airflow-scheduler airflow-dag-processor     # cargar el código nuevo
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha ⟨AAAA-MM-DD⟩ --solo-mostrar
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha ⟨AAAA-MM-DD⟩ --motivo "fallo controlado 3"
```
Salida: ⟨300⟩ mensajes en las particiones ⟨…⟩ (offsets ⟨…⟩). Clear de `validar_raw` con Downstream a las
⟨hh:mm⟩ → verde; el reporte muestra **Mensajes en cuarentena: ⟨300⟩**.

## Capturas
`semana3_fallo3_tareas.png` · `semana3_fallo3_log.png` · `semana3_fallo3_cuarentena.png` (salida del comando) ·
`semana3_fallo3_recuperado.png`
