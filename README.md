# RapidGo · Variante 4 — Riesgo transaccional

[![CI RapidGo](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/workflows/ci.yml/badge.svg)](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/workflows/ci.yml)

Flujo de datos Big Data de extremo a extremo para detectar **comportamientos atípicos durante los pagos y pedidos** de RapidGo.
Proyecto final — Línea de Profundización I (Big Data) · Ingeniería de Sistemas y Computación · Universidad de Cundinamarca, Facatativá.

| | |
|---|---|
| **Equipo** | 9 |
| **Integrantes** | Esteban Rueda ([@jers120506-gif](https://github.com/jers120506-gif)) · Oscar Becerra ([@kiritosb12](https://github.com/kiritosb12)) · Karol Perez ([@Karitoperez](https://github.com/Karitoperez)) |
| **Variante** | 4 — Riesgo transaccional |
| **Versión actual** | `v0.3.0-orchestration` (Semana 3 · Airflow, calidad y resiliencia) |
| **Tablero Kanban** | [RapidGo V4 – Kanban](https://github.com/users/kiritosb12/projects/2) (20 Issues, Semanas 1 a 4) |

## Video
▶️ [Video final del proyecto (8:21 min)](https://www.youtube.com/watch?v=KqqaC_MNZF4) — problema, arquitectura, demostración, GitHub, resultados y aprendizajes.

## Problema de negocio
En una plataforma de domicilios, los pagos fallidos repetidos, los valores fuera de lo normal y las combinaciones
de señales sospechosas (IP extranjera, muchos reintentos, horario inusual) suelen indicar **prueba de tarjetas
robadas, fraude o fallas de la pasarela de pagos**. Detectarlo en minutos —y no al cierre del día— permite
bloquear pedidos, avisar al equipo antifraude y reducir las devoluciones de cargos.

**Valor de la solución:** indicadores en ventanas de 1 minuto y alertas automáticas por pedido y por evento,
con trazabilidad completa en un Data Lake por capas (raw → processed → curated).

## Arquitectura
![Arquitectura](docs/arquitectura.png)

| Componente | Función en la variante |
|---|---|
| Productor (Python) | Genera intentos de pago simulados con cerca de 5 % de anomalías insertadas |
| Kafka (modo KRaft) | Transporta los eventos en el topic `rapidgo.v4.pagos.intentos` (clave `id_pedido`, 3 particiones) |
| Spark Structured Streaming | Lee, limpia, calcula señales y puntaje de riesgo, indicadores por ventana y alertas |
| Data Lake (Parquet) | Capas raw → processed → curated (ver [datalake/README.md](datalake/README.md)) |
| Airflow | Valida la calidad, agrega, genera el reporte; reintentos y fallo controlado (Semana 3) |
| Docker, GitHub y GitHub Actions | Ejecución local, trazabilidad, integración continua y versiones |

## Contrato del evento
Evento `intento_pago` — esquema en [`samples/evento_pago.schema.json`](samples/evento_pago.schema.json);
ejemplos en [`samples/evento_pago.json`](samples/evento_pago.json) (normal) y
[`samples/evento_pago_anomalo.json`](samples/evento_pago_anomalo.json) (anómalo).

```json
{ "id_evento": "8f3c2a1e-5b7d-4c9a-9e21-3f6a0b7c1d42", "tipo_evento": "intento_pago", "version_esquema": "1.0",
  "equipo": "equipo9-v4", "fecha_evento": "2026-10-22T19:42:15.318Z", "id_pedido": "PED-20261022-004817",
  "id_cliente": "CLI-001523", "valor": 87500.0, "moneda": "COP", "medio_pago": "tarjeta_credito",
  "id_dispositivo": "DIS-7f21a9", "tipo_dispositivo": "android", "pais_ip": "CO", "ciudad": "Bogota",
  "latitud": 4.6533, "longitud": -74.0836, "numero_intento": 1, "estado": "aprobado", "motivo_rechazo": null }
```

## Kafka
| Topic | Clave | Particiones | Replicación | Retención |
|---|---|---|---|---|
| `rapidgo.v4.pagos.intentos` | `id_pedido` | 3 | 1 | 7 días |

Justificación en el [ADR-001](docs/decisions/ADR-001-topic-clave-particiones.md).

## Indicadores y alertas (umbrales iniciales)
Detalle completo en [docs/kpis_y_alertas.md](docs/kpis_y_alertas.md).

| Indicador | Definición |
|---|---|
| K1 Intentos por pedido | intentos / pedidos distintos por ventana de 1 minuto, y máximo de `numero_intento` |
| K2 Valor promedio | promedio de `valor`, total y por medio de pago |
| K3 Eventos de riesgo | cantidad y porcentaje de eventos con `puntaje_riesgo >= 60` |
| K4 Tasa de rechazo | porcentaje de intentos `rechazado` |

| Alerta | Regla |
|---|---|
| A1 `MULTIPLES_INTENTOS` | mismo `id_pedido` con 3 o más intentos, o 2 o más rechazos, en 5 minutos |
| A2 `SENALES_COMBINADAS` | `puntaje_riesgo >= 60` con al menos 2 señales (valor ≥ 500.000, IP fuera de Colombia, intento ≥ 3, rechazo sospechoso, entre las 00 y las 05 h) |

## Ejecución local
Requisitos: Docker Desktop con **al menos 6 GB de memoria** asignados (8 GB recomendados con Airflow), Git y, para correr las pruebas en el
computador, Python 3.12+ y Java 17+ (opcional: sin Java las pruebas de Spark se omiten).

```bash
git clone https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional.git
cd rapidgo-v4-riesgo-transaccional
docker compose up -d --build          # Kafka + topic + Kafka UI + Spark + productor + PostgreSQL + Airflow
docker compose logs kafka-init        # debe mostrar PartitionCount: 3
docker compose logs -f spark          # lotes de cada consulta (la 1.ª vez descarga el conector de Kafka)
```

Prueba completa de punta a punta: levanta todo, espera datos en el lago, ejecuta el DAG y termina con error si algo
no queda en verde ([`scripts/prueba_e2e.sh`](scripts/prueba_e2e.sh); en Windows, desde Git Bash):

```bash
bash scripts/prueba_e2e.sh --limpio   # prueba completa desde cero (≈ 5 min)
bash scripts/prueba_e2e.sh            # usa lo que ya esté corriendo
```

| Servicio | Qué hace | Acceso |
|---|---|---|
| `kafka` / `kafka-init` | Broker KRaft y creación del topic con 3 particiones | `localhost:29092` |
| `kafka-ui` | Ver mensajes, particiones y offsets | http://localhost:8080 |
| `spark` | 5 consultas continuas → `datalake/` | http://localhost:4040 (pestaña *Structured Streaming*) |
| `productor` | ~10 eventos/s; termina si se define `DURACION_PRODUCTOR` | `docker compose logs productor` |
| `postgres` + `airflow-init` | Base de datos de Airflow (se prepara sola) | — |
| `airflow-apiserver` / `airflow-scheduler` / `airflow-dag-processor` | Interfaz, ejecución de tareas y lectura de DAG | http://localhost:8081 |

Parámetros (variables de entorno al levantar): `EVENTOS_POR_SEGUNDO` (10), `DURACION_PRODUCTOR` (0 = sin
límite), `INTERVALO_LOTE` (`10 seconds`), `RETRASO_MAXIMO` (marca de agua, ver ADR-002). Ejemplo:

```bash
EVENTOS_POR_SEGUNDO=20 DURACION_PRODUCTOR=900 docker compose up -d --build   # 15 minutos a 20 eventos/s
```

Ver los resultados y medir la ejecución:

```bash
docker compose run --rm --no-deps spark python scripts/metricas_semana2.py   # tablas Markdown con valores reales
docker compose stop productor spark                                 # detener (el checkpoint guarda el avance)
docker compose down                                                 # apagar todo
```

## Airflow: control de calidad y reporte (Semana 3)
`docker compose up -d --build` también levanta PostgreSQL y Airflow 3.3.2 (api-server, scheduler y dag-processor).
La primera vez tarda unos minutos en construir la imagen.

| Qué | Dónde |
|---|---|
| Interfaz de Airflow (sin usuario ni contraseña, solo uso local) | http://localhost:8081 |
| DAG | `rapidgo_riesgo_transaccional` ([airflow/dags/](airflow/dags/rapidgo_riesgo_transaccional.py)) — cada hora, pausado al crearse |
| Lógica de las tareas (Python puro, probada con pytest) | [orquestacion/tareas.py](orquestacion/tareas.py) |
| Reportes | `datalake/curated/reportes/fecha=AAAA-MM-DD/` |

```
verificar_fuentes → validar_raw → ┬→ agregar_kpis ───────┬→ validar_curated → generar_reporte
                                  └→ consolidar_alertas ─┘
```

| Tarea | Qué hace | Si falla |
|---|---|---|
| `verificar_fuentes` | Kafka responde; raw y processed tienen archivos de la fecha; si es hoy, raw recibió datos en los últimos 10 min (`MAX_MINUTOS_SIN_DATOS`) | se reintenta 2 veces (30 s) |
| `validar_raw` | offsets sin huecos ni repetidos por partición, ≤ 1 % de mensajes inválidos, desbalance entre particiones | falla sin reintentar |
| `agregar_kpis` | indicadores por hora → `curated/resumen_horario/` | falla sin reintentar |
| `consolidar_alertas` | A1 + A2 agrupadas por pedido → `curated/alertas_consolidadas/` | falla sin reintentar |
| `validar_curated` | rangos de K1–K4, ventanas continuas, intentos de `kpis_1min` = processed, alertas con pedido real | falla sin reintentar |
| `generar_reporte` | reúne los XCom de las 5 tareas y escribe el reporte Markdown + JSON | — |

Ejecutar: en la interfaz, activar el DAG (interruptor) y pulsar **Trigger**; el parámetro `fecha` (AAAA-MM-DD,
hora de Bogotá) es opcional (vacío = hoy). Por consola:

```bash
docker compose exec airflow-scheduler airflow dags unpause rapidgo_riesgo_transaccional
docker compose exec airflow-scheduler airflow dags trigger rapidgo_riesgo_transaccional
```

**Mensajes dañados en raw.** raw no se borra; si `validar_raw` falla por mensajes inválidos y ya se revisó su
origen, se ponen en cuarentena y se hace Clear de `validar_raw`:

```bash
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha AAAA-MM-DD --solo-mostrar
docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha AAAA-MM-DD --motivo "explicación"
```

Por qué unas fallas se reintentan y otras no, y los 3 fallos controlados (uno por integrante) con su recuperación:
[ADR-003](docs/decisions/ADR-003-orquestacion-y-recuperacion.md) y [punto de control de la Semana 3](docs/checkpoints/semana3.md).

## Pruebas e integración continua
```bash
pip install -r requirements.txt
pytest -v                                                   # contrato, reglas, productor y Spark
python -m producer.productor_pagos --modo-seco --max-eventos 5   # productor sin Kafka
```

El flujo [`.github/workflows/ci.yml`](.github/workflows/ci.yml) se ejecuta en cada push a `main`, en cada
Pull Request y a mano (*Actions → CI RapidGo → Run workflow*) con dos trabajos en paralelo:

- **`validar`**: sintaxis, `pytest` (contrato, reglas, productor, Spark y tareas de orquestación), productor en
  modo seco y `docker compose config`.
- **`validar-dag`**: instala Airflow 3.3.2 con sus restricciones oficiales, importa el DAG y comprueba sus
  6 tareas, dependencias y reintentos (`tests/test_dag.py`).

`main` exige que los dos pasen antes de integrar. Sin Airflow instalado, `tests/test_dag.py` aparece como *skipped*.

## Resultados (Semana 2)
Ejecución local de 10 minutos a 10 eventos/s con la marca de agua de 2 minutos
([`docs/evidencias/semana2_metricas_despues.md`](docs/evidencias/semana2_metricas_despues.md)):

| Medida | Valor |
|---|---|
| Mensajes por partición | 6038 en total: 32,5 % / 34,3 % / 33,1 % |
| Primera ventana de indicadores escrita | a los 3,2 min (antes, con 10 min de marca de agua: 10,4 min) |
| Demora entre el cierre de la ventana y su escritura | 140 s (antes: 620 s) |
| Eventos de riesgo (puntaje ≥ 60) | 331 (5,52 %) |
| Alertas por minuto | A1: 13,7 · A2: 33,1 |
| Eventos descartados por llegar tarde | 0 |

Detalle y decisión en el [punto de control de la Semana 2](docs/checkpoints/semana2.md) y el
[ADR-002](docs/decisions/ADR-002-marca-de-agua.md).

## Resultados (Semana 3)
Ejecución de referencia del DAG (`manual__evidencia_20261105T134958`, 2026-11-05): **verde en 23.7 s**.

| Medida | Valor |
|---|---|
| Mensajes revisados en raw | 3950 (p0 1342 · p1 1264 · p2 1344), 0 huecos ni offsets repetidos |
| Eventos en processed / pedidos | 4011 / 3268 |
| Eventos de riesgo (K3) / tasa de rechazo (K4) | 200 (4.99 %) / 24.36 % |
| Alertas A2 por intento → agrupadas por pedido | 200 → 90 (−55.0 %) |
| Pedidos con alguna alerta / con A1 y A2 / ALTA | 105 / 10 / 10 |

Fallos controlados: con Spark detenido `verificar_fuentes` falló en sus 3 intentos y la ejecución volvió a verde en
13 min sin perder mensajes; con 600 mensajes dañados `validar_raw` falló sin reintentar y, tras la cuarentena,
pasó con 0.0 % inválidos. Ver el [punto de control de la Semana 3](docs/checkpoints/semana3.md).

## Plan de versiones
| Versión | Semana | Contenido |
|---|---|---|
| `v0.1.0-foundation` | 1 | Arquitectura, contrato, indicadores y alertas, Kanban, ADR-001 |
| `v0.2.0-streaming` | 2 | Productor + Kafka + Spark + Data Lake + integración continua |
| `v0.3.0-orchestration` | 3 | DAG de Airflow, calidad, fallo controlado, integración continua ampliada |
| `v1.0.0` | 4 | Control de calidad final, README final, video |

## Documentación
- Indicadores y alertas: [`docs/kpis_y_alertas.md`](docs/kpis_y_alertas.md)
- Data Lake: [`datalake/README.md`](datalake/README.md)
- Registros de decisiones de arquitectura: [ADR-001](docs/decisions/ADR-001-topic-clave-particiones.md) · [ADR-002](docs/decisions/ADR-002-marca-de-agua.md) · [ADR-003](docs/decisions/ADR-003-orquestacion-y-recuperacion.md)
- Puntos de control semanales: [Semana 1](docs/checkpoints/semana1.md) · [Semana 2](docs/checkpoints/semana2.md) · [Semana 3](docs/checkpoints/semana3.md)

## Contribuciones (Semana 1)
| Integrante | Trabajo realizado | Pull Requests propios | Revisiones aprobadas |
|---|---|---|---|
| Esteban Rueda | Indicadores y reglas de alerta (#3), topic, clave, particiones y ADR-001 (#4), contrato JSON y ejemplos (#2) | #23, #24, #26 | #22, #25, #27 |
| Oscar Becerra | Estructura del repositorio (#1), docker-compose preliminar (#6), arquitectura y README (#5), corrección del esquema y prueba del contrato (#2), punto de control y versión (#7) | #22, #25, #27, #28, #29 | #23, #24, #26 |
| Karol Perez | — | — | — |

## Contribuciones (Semana 2)
| Integrante | Trabajo realizado | Pull Requests propios | Revisiones aprobadas |
|---|---|---|---|
| Esteban Rueda | Integración continua `ci.yml`, dependencias y configuración de pytest (#11), productor de intentos de pago (#8) | #30, #31 | #36 |
| Oscar Becerra | Spark Structured Streaming raw/processed, Dockerfile y servicios de Docker Compose (#9), protección de `main` con el CI, ADR-002, punto de control y versión (#12) | #32, #35, #36 | #30, #31, #33, #34 |
| Karol Perez | Indicadores K1–K4 y alertas A1/A2 en Spark (#10), pruebas de reglas y script de métricas (#11) | #33, #34 | #32, #35 |

## Contribuciones (Semana 3)
| Integrante | Trabajo realizado | Pull Requests propios | Revisiones aprobadas |
|---|---|---|---|
| Esteban Rueda | Lectura del Data Lake, `verificar_fuentes` y `validar_raw` (#13); DAG con reintentos y XCom y reporte (#13); fallo controlado 1 | #38, #40, #42 | #37, #39, #41 |
| Oscar Becerra | Indicadores por hora, alertas consolidadas por pedido y `validar_curated` (#13); fallo controlado 2; ADR-003, punto de control y versión (#16) | #39, #43, #45 | #38, #40, #42, #44 |
| Karol Perez | Airflow y PostgreSQL en Docker Compose (#15); CI con importación del DAG (#14); fallo controlado 3 y cuarentena | #37, #41, #44 | #38, #43 |

Video final: [ver en YouTube](https://www.youtube.com/watch?v=KqqaC_MNZF4) (8:21 min).
