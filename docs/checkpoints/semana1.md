# Punto de control Semana 1 — v0.1.0-foundation

**Equipo 9 · Variante 4: Riesgo transaccional** · Esteban Rueda · Oscar Becerra · Karol Perez

## 1. Qué quedó listo
- Repositorio público con la estructura base y la rama `main` protegida (Pull Request con 1 aprobación obligatoria).
- [Tablero Kanban](https://github.com/users/kiritosb12/projects/2) con 20 Issues (Semanas 1 a 4), etiquetas, hitos y los campos Semana, Prioridad y Componente.
- Contrato JSON del evento `intento_pago` con su esquema, dos ejemplos (normal y anómalo) y su prueba automática (6 pruebas aprobadas).
- 4 indicadores y 2 reglas de alerta con umbrales iniciales (`docs/kpis_y_alertas.md`) y el ADR-001.
- `docker-compose.yml` preliminar (Kafka KRaft, creación del topic con 3 particiones y Kafka UI), validado con `docker compose config --quiet`.
- Diagrama de arquitectura de la variante y README inicial.

## 2. Decisión más importante
Usar **`id_pedido` como clave** del topic `rapidgo.v4.pagos.intentos` con **3 particiones**: la alerta principal
(`MULTIPLES_INTENTOS`) cuenta intentos por pedido y necesita que lleguen en orden a la misma partición.
Detalle en el [ADR-001](../decisions/ADR-001-topic-clave-particiones.md).

## 3. Riesgo principal
Que los umbrales iniciales (valor ≥ 500.000 COP, puntaje de riesgo ≥ 60, 3 o más intentos en 5 minutos)
generen **demasiadas o muy pocas alertas** con los datos simulados.
**Mitigación:** el productor insertará cerca de 5 % de anomalías y en la Semana 2 mediremos las alertas por minuto
reales para ajustar los umbrales (ADR-002).

Riesgo secundario: la distribución del trabajo. En la Semana 1 las tareas #2 y #6 las terminaron otros integrantes;
en la Semana 2 cada uno sube sus propios Issues y revisa al menos un Pull Request de otro.

## 4. Problemas encontrados y cómo se resolvieron
| Problema | Solución |
|---|---|
| El primer Pull Request (#21) mezclaba los Issues #1 y #5 | Se cerró y se dividió en un PR por Issue (#22 y #27) |
| GitHub rechazó subir `.github/workflows/` porque el token no tenía el permiso `workflow` | Se generó un token nuevo con ese permiso |
| La automatización del tablero cerraba los Issues al pasar a Review | Se configuró para cerrar solo al llegar a Done y se reabrieron #5 y #6 |
| Un integrante aceptó tarde la invitación y sus Issues no se podían asignar | Se crearon sin asignar y se asignaron al aceptar |
| El esquema del contrato se subió como `evento_pago.shema.json` y faltaba la prueba del contrato | Se corrigió el nombre y se agregó la prueba en el PR #28 (`pytest -v`: 6 pruebas aprobadas) |

## 5. Plan para la Semana 2 (v0.2.0-streaming)
| Integrante | Entregable |
|---|---|
| Esteban Rueda | #8 productor con clave `id_pedido`; #10 indicadores por ventana y alertas A1 y A2 |
| Oscar Becerra | #9 Spark Streaming hacia raw y processed en Parquet con checkpoint |
| Karol Perez | #11 pruebas de contrato y reglas + `ci.yml`; #12 ADR-002 y punto de control |

## 6. Enlaces
- Issues de la Semana 1: [#1 a #7](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/milestone/1?closed=1)
- Pull Requests: [#22](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/22) estructura ·
  [#23](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/23) indicadores y alertas ·
  [#24](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/24) ADR-001 ·
  [#25](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/25) docker-compose ·
  [#26](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/26) contrato JSON ·
  [#27](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/27) arquitectura y README ·
  [#28](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/28) corrección del contrato ·
  [#29](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/29) punto de control
- Versión: `v0.1.0-foundation`
