# Fallo controlado 1 — Kafka detenido

- **Responsable:** Esteban Rueda · **Fecha y hora:** ⟨AAAA-MM-DD hh:mm⟩ · **Ejecución:** ⟨run_id⟩
- **Tipo:** falla de **fuente** (infraestructura) → Airflow reintenta.

## Qué se provocó
Con el flujo corriendo (productor + Spark) y una ejecución previa en verde, se detuvo el broker:

```bash
docker compose stop kafka        # ⟨hh:mm:ss⟩
```
Y se ejecutó el DAG (Trigger, fecha vacía) a las ⟨hh:mm:ss⟩.

## Tarea que falló
`verificar_fuentes`, en sus 3 intentos (⟨hh:mm:ss⟩, ⟨hh:mm:ss⟩, ⟨hh:mm:ss⟩; 2 reintentos cada 30 s).

## Mensaje real del registro (intento 3)
```
⟨FalloFuente: Kafka no responde en kafka:9092 (gaierror: …)⟩
```

## Tareas afectadas
`validar_raw`, `agregar_kpis`, `consolidar_alertas`, `validar_curated` y `generar_reporte` → **upstream_failed**.
Spark: ⟨siguió corriendo sin datos / se detuvo con …⟩ · Productor: ⟨…⟩

## Recuperación
```bash
docker compose start kafka                 # ⟨hh:mm:ss⟩
docker compose start spark productor       # solo si aparecían Exited
```
Clear de `verificar_fuentes` con Downstream a las ⟨hh:mm⟩ → intento 4 en verde y reporte `⟨reporte_HHMMSS.md⟩`.
Tiempo desde el fallo hasta el reporte: ⟨__ min⟩.

## Capturas
`semana3_fallo1_tareas.png` (grafo con la falla) · `semana3_fallo1_log.png` (registro del intento 3) ·
`semana3_fallo1_recuperado.png` (la misma ejecución en verde)
