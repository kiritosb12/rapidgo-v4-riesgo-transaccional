# Fallo controlado 2 — Spark detenido

- **Responsable:** Oscar Becerra · **Fecha y hora:** 2026-11-05 09:16 (hora de Bogotá) · **Ejecución:** `manual__fallo2_spark_detenido_142923`
- **Tipo:** falla de **fuente**: Kafka responde, pero a raw no llegan datos → Airflow reintenta.

## Problema encontrado
Con el código del PR del DAG, al detener Spark el DAG **terminaba en verde** y solo dejaba una advertencia
(`raw recibe datos recientes: valor=11.4 min (umbral <= 10.0 min)`) en el reporte: la caída de Spark no se notaba.
Se comprobó el 2026-11-05: Spark detenido a las 09:16:27, Trigger a las 09:27:28 (11 min después) → ejecución
`manual__antes_spark_detenido_142728` **en verde** (las 6 tareas) y el reporte `reporte_092816.md` con esa advertencia.

**Cambio que lo resolvió (PR #43):** si se revisa el día de hoy y raw lleva más de `MAX_MINUTOS_SIN_DATOS`
(10 min por defecto) sin datos nuevos, `verificar_fuentes` falla con `FalloFuente` (se reintenta). Para días
pasados sigue siendo solo una advertencia.

## Qué se provocó
```bash
docker compose stop spark          # 09:16:27
```
Se cargó el código de este PR en Airflow (09:29:17) y, con Spark todavía detenido, se ejecutó el DAG (Trigger, fecha
vacía) a las 09:29:24, **13 min** después de detener Spark. Se usó el máximo por defecto de 10 minutos.

## Tarea que falló
`verificar_fuentes`, 3 intentos (09:29:27, 09:30:11, 09:30:45; 2 reintentos con 30 s de espera).

## Mensaje real del registro (intento 3)
```
FalloFuente: La capa raw no recibe datos nuevos hace 14.7 min (máximo 10.0 min): ¿Spark o el productor están detenidos?
```
(Intento 1: hace 13.5 min · intento 2: hace 14.1 min.)

## Tareas afectadas
Las 5 siguientes (`validar_raw`, `agregar_kpis`, `consolidar_alertas`, `validar_curated`, `generar_reporte`) en
**upstream_failed**. Mientras tanto el productor siguió enviando a Kafka: **12 453 mensajes en espera**
(28 670 en el topic, 16 217 leídos por Spark antes de detenerse).

## Recuperación
```bash
docker compose start spark          # 09:36:46
docker compose logs --tail 20 spark # primer lote después del reinicio: lote 128 → processed 12 902 filas, raw 12 942 (lote 129)
```
Spark continuó desde su checkpoint (lote 128): procesó de una vez los mensajes acumulados en Kafka. Clear de
`verificar_fuentes` (con Downstream) → intento 4 a las 09:41:58 y la ejecución completa **en verde** a las 09:42:39,
reporte `reporte_094238.md`: 31 762 mensajes en raw, `validar_raw` con **0 huecos y 0 offsets repetidos**.
Tiempo desde el fallo hasta el reporte: 13 min (09:29:24 → 09:42:39).

## Otros dos problemas reales encontrados en la misma sesión
Al preparar la ejecución de referencia, `validar_curated` falló dos veces aunque los datos estaban bien. Se corrigió en
este mismo PR (con 4 pruebas nuevas):
1. `indicadores dentro de rango: valor=1` — en la ventana de las 13:59 UTC hubo 174 intentos y 175 pedidos, porque
   `pedidos` sale de `approx_count_distinct` (conteo aproximado, error cercano al 5 %). Ahora se acepta K1 ≥ 0.95.
2. `alertas con pedido existente: valor=1` — dos alertas A2 de las 14:11:01 cuyo pedido aún no estaba en processed
   (última fila 14:10:38): A2 y processed son consultas distintas de Spark. Ahora una A2 posterior a lo último que
   escribió processed (menos 1 minuto) se informa como reciente y no como falla.

## Capturas
`semana3_fallo2_tareas.png` (grafo con `verificar_fuentes` en rojo y 5 tareas en upstream_failed) ·
`semana3_fallo2_log.png` (los 3 intentos y el mensaje del intento 3) · `semana3_fallo2_recuperado.png` (la misma
ejecución en verde después del Clear)
