# ADR-003 — Orquestación con Airflow y estrategia de recuperación ante fallos

- **Estado:** Aceptada (Semana 3, v0.3.0-orchestration)
- **Responsable:** Oscar Becerra · **Revisores:** Karol Perez, Esteban Rueda
- **Issue relacionado:** #16 · **Pull Request:** #45
- **Evidencia:** los 3 fallos controlados (uno por integrante) en `docs/evidencias/semana3_fallo1_kafka.md`,
  `semana3_fallo2_spark.md` y `semana3_fallo3_calidad.md`, con sus capturas, y `docs/evidencias/semana3_reporte.md`

## Contexto / problema
Spark procesa los pagos de forma continua (Semana 2), pero nadie revisaba si las fuentes estaban vivas ni si los
datos del lago eran coherentes, y las alertas A2 llegaban una por intento (≈ 27 por minuto, ADR-002). Necesitamos
una capa de **control por lotes** que valide, resuma y avise, y una forma clara de **recuperarnos** cuando algo
se cae, sin duplicar resultados.

## Opciones consideradas
| Opción | A favor | En contra |
|---|---|---|
| Hacer las validaciones dentro de Spark (más consultas continuas) | Un solo componente. | Mezcla el flujo continuo con tareas de control; un error de calidad detendría el streaming; no hay reintentos ni historial por tarea. |
| Script con `cron` | Muy simple. | Sin dependencias entre pasos, sin reintentos, sin interfaz ni registros por tarea. |
| **DAG de Airflow que lee el lago con pyarrow** | Dependencias visibles, reintentos, XCom, historial y registros por tarea; no toca el streaming; las tareas son Python puro probadas con pytest. | Más servicios en Docker (PostgreSQL + 3 de Airflow, ≈ 0.9 GB de memoria; todo el entorno quedó en 3.1 GB). |

## Decisión tomada
1. **Airflow 3.3.2 con LocalExecutor y PostgreSQL**, DAG `rapidgo_riesgo_transaccional` cada hora con 6 tareas:
   `verificar_fuentes → validar_raw → (agregar_kpis ‖ consolidar_alertas) → validar_curated → generar_reporte`.
2. **Dos tipos de falla:**
   - *De fuente* (`FalloFuente`: Kafka no responde; raw sin archivos de la fecha; **hoy** raw sin datos nuevos hace
     más de `MAX_MINUTOS_SIN_DATOS`, es decir Spark o el productor detenidos): **2 reintentos cada 30 s**, porque
     suelen arreglarse solas (un servicio que se está reiniciando, Spark que aún no escribe).
   - *De calidad* (`FalloCalidad`: offsets con huecos, > 1 % de mensajes inválidos, indicadores que no cuadran
     con processed, alertas sin pedido): **falla de inmediato** (`AirflowFailException`); reintentar no cambia
     los datos y solo retrasa el aviso.
3. **Tareas idempotentes:** cada tarea reemplaza su partición `fecha=…`; volver a ejecutar (Clear) no duplica.
4. **Solo datos confirmados:** se leen los archivos registrados en `_spark_metadata/`, no los que deja un lote fallido.
5. **raw no se borra: cuarentena.** Los mensajes dañados ya revisados se registran en
   `datalake/_cuarentena/raw_pagos.csv` (`python -m orquestacion.cuarentena`) y `validar_raw` deja de contarlos.
6. **Procedimiento de recuperación** (probado en los 3 fallos controlados):
   1. Arreglar la causa: levantar el servicio caído (`docker compose start kafka` / `spark`) o, si son datos
      dañados, revisarlos y ponerlos en cuarentena.
   2. Spark continúa desde su checkpoint, sin perder ni repetir mensajes.
   3. En Airflow: tarea fallida → **Clear** (con Downstream) → la ejecución termina en verde.

## Evidencia utilizada
Un fallo controlado por integrante, con el flujo corriendo:

| # | Responsable | Qué se provocó | Tarea que falló (intentos) | Mensaje real (resumen) | Recuperación | Tiempo hasta verde |
|---|---|---|---|---|---|---|
| 1 | Esteban | `docker compose stop kafka` | `verificar_fuentes` (3) | pendiente: la ficha del PR #42 quedó sin los datos de la ejecución | `start kafka` + Clear | pendiente |
| 2 | Oscar | `docker compose stop spark` | `verificar_fuentes` (3) | raw no recibe datos nuevos hace 14.7 min (máximo 10.0) | `start spark` (checkpoint) + Clear | 13 min |
| 3 | Karol | 600 mensajes no JSON al topic (el comando se ejecutó dos veces) | `validar_raw` (1, sin reintento) | 5.7 % inválidos (600 de 10523) | cuarentena + Clear | ≈ 3 h 54 min (incluye programar la cuarentena) |

En los fallos 2 y 3 las tareas siguientes quedaron en `upstream_failed` y no se generó un reporte con datos
incompletos o contaminados. Detalle, horas y capturas en `docs/evidencias/semana3_fallo*.md`.

Problemas que encontramos y cambiamos por la evidencia:
- Fallo 2: con Spark detenido el DAG terminaba **en verde** con solo una advertencia → `verificar_fuentes` ahora
  falla si hoy no llegan datos (PR #43).
- Fallo 3: no había forma de seguir sin borrar raw → cuarentena (PR #44).
- Consolidación de alertas: leía una foto de los datos distinta a la del chequeo mientras Spark escribía → se
  compara contra el corte de la foto (PR #39).
- `validar_curated`: K1 sale de `approx_count_distinct` (174 intentos y 175 pedidos en una ventana) → se acepta
  K1 ≥ 0.95; y alertas A2 más recientes que lo último de processed se informan como recientes, no como falla (PR #43).

Ejecución normal de referencia (`manual__evidencia_20261105T134958`): 23.7 s en total; reporte con 3950 mensajes,
200 alertas A2 agrupadas en 90 pedidos (−55.0 %), todos los chequeos críticos en verde.

## Consecuencias / compromisos
- Si Kafka o Spark caen, el aviso llega en ≈ 1 minuto (3 intentos × 30 s) y nada de lo que sigue se ejecuta con datos viejos.
- Si el productor termina (por ejemplo con `DURACION_PRODUCTOR`), las ejecuciones de hoy también fallan pasados
  10 minutos: es correcto (no están llegando pagos); para revisar un día pasado se usa el parámetro `fecha`.
- Un problema de calidad se ve de inmediato en rojo en la interfaz, con el chequeo y el valor en el mensaje.
- El DAG se crea **pausado**: hay que activarlo en la interfaz (o `airflow dags unpause`).
- La revisión es por hora; para detectar caídas más rápido habría que bajar el `schedule` (más ejecuciones).
- Pendiente para la Semana 4: compactar los archivos pequeños que deja Spark en el lago y enviar un aviso
  (correo o Slack) cuando una ejecución falle; completar la evidencia del fallo 1.
