# RapidGo · Variante 4 — Riesgo transaccional

[![CI RapidGo](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/workflows/ci.yml/badge.svg)](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/workflows/ci.yml)

Flujo de datos Big Data de extremo a extremo para detectar **comportamientos atípicos durante los pagos y pedidos** de RapidGo.
Proyecto final — Línea de Profundización I (Big Data) · Ingeniería de Sistemas y Computación · Universidad de Cundinamarca, Facatativá.

| | |
|---|---|
| **Equipo** | 9 |
| **Integrantes** | Esteban Rueda ([@jers120506-gif](https://github.com/jers120506-gif)) · Oscar Becerra ([@kiritosb12](https://github.com/kiritosb12)) · Karol Perez ([@Karitoperez](https://github.com/Karitoperez)) |
| **Variante** | 4 — Riesgo transaccional |
| **Versión actual** | `v0.2.0-streaming` (Semana 2 · flujo funcional e integración continua) |
| **Tablero Kanban** | [RapidGo V4 – Kanban](https://github.com/users/kiritosb12/projects/2) (20 Issues, Semanas 1 a 4) |

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

## Ejecución local (Semana 2)
Requisitos: Docker Desktop con **al menos 6 GB de memoria** asignados, Git y, para correr las pruebas en el
computador, Python 3.12+ y Java 17+ (opcional: sin Java las pruebas de Spark se omiten).

```bash
git clone https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional.git
cd rapidgo-v4-riesgo-transaccional
docker compose up -d --build          # Kafka + topic + Kafka UI + Spark + productor
docker compose logs kafka-init        # debe mostrar PartitionCount: 3
docker compose logs -f spark          # lotes de cada consulta (la 1.ª vez descarga el conector de Kafka)
```

| Servicio | Qué hace | Acceso |
|---|---|---|
| `kafka` / `kafka-init` | Broker KRaft y creación del topic con 3 particiones | `localhost:29092` |
| `kafka-ui` | Ver mensajes, particiones y offsets | http://localhost:8080 |
| `spark` | 5 consultas continuas → `datalake/` | http://localhost:4040 (pestaña *Structured Streaming*) |
| `productor` | ~10 eventos/s; termina si se define `DURACION_PRODUCTOR` | `docker compose logs productor` |

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

## Pruebas e integración continua
```bash
pip install -r requirements.txt
pytest -v                                                   # contrato, reglas, productor y Spark
python -m producer.productor_pagos --modo-seco --max-eventos 5   # productor sin Kafka
```

El flujo [`.github/workflows/ci.yml`](.github/workflows/ci.yml) se ejecuta en cada push a `main`, en cada
Pull Request y a mano (*Actions → CI RapidGo → Run workflow*): valida la sintaxis, ejecuta `pytest`, prueba el
productor en modo seco y valida `docker-compose.yml`. `main` exige que pase antes de integrar.

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
- Registros de decisiones de arquitectura: [ADR-001](docs/decisions/ADR-001-topic-clave-particiones.md) · [ADR-002](docs/decisions/ADR-002-marca-de-agua.md)
- Puntos de control semanales: [Semana 1](docs/checkpoints/semana1.md) · [Semana 2](docs/checkpoints/semana2.md)

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

_Se actualizará cada semana. Enlace al video final: pendiente (Semana 4)._
