# ADR-001 — Topic, clave (key) y particiones de Kafka para los intentos de pago
- **Issue relacionado:** #4

## Contexto / problema
La variante 4 detecta comportamientos atípicos en los pagos. La regla A1 (`MULTIPLES_INTENTOS`) cuenta los
intentos **de un mismo pedido** en una ventana de 5 minutos, y el indicador K1 mide intentos por pedido.
Necesitamos que los intentos de un mismo pedido lleguen en orden y que el diseño permita procesamiento
paralelo en Spark sin complicar el entorno local (un solo broker en Docker).

## Opciones consideradas
| Clave | A favor | En contra |
|---|---|---|
| `id_pedido` | Todos los intentos de un pedido caen en la misma partición, así que el orden por pedido está garantizado; la distribución es pareja porque hay muchos pedidos distintos. | No agrupa por cliente: las reglas por cliente requieren redistribuir datos en Spark. |
| `id_cliente` | Útil para reglas de frecuencia por cliente. | Los clientes frecuentes sobrecargan una sola partición; la regla principal es por pedido. |
| `id_dispositivo` | Útil para detectar un dispositivo usado por muchos clientes. | Pierde la relación con el pedido; mismo problema de sobrecarga. |
| Sin clave (reparto rotativo) | Distribución máxima. | Se pierde el orden de los intentos de cada pedido. |

| Particiones | A favor | En contra |
|---|---|---|
| 1 | Sencillo. | No demuestra paralelismo ni la distribución por clave. |
| **3** | 3 tareas en paralelo en Spark local, suficiente para el volumen esperado (5 a 20 eventos por segundo en la demostración); permite ver la distribución por partición y los offsets. | Con un solo broker no hay tolerancia a fallos (aceptable en local). |
| 6 o más | Más paralelismo. | Sobredimensionado para el volumen y para el computador de un estudiante; genera muchos archivos pequeños en el Data Lake. |

## Decisión tomada
- **Topic:** `rapidgo.v4.pagos.intentos`
- **Clave (key):** `id_pedido`
- **Particiones:** 3 · **Factor de replicación:** 1 (entorno local de un broker) · **Retención:** 7 días
- Creación automática de topics desactivada (`auto.create.topics.enable=false`): el topic se crea explícitamente con el servicio `kafka-init` de `docker-compose.yml`.
- Productor con `acks=all` e idempotencia activada para no duplicar intentos al reenviar mensajes.
- Spark elimina duplicados por `id_evento` dentro de la marca de agua como segunda defensa.

## Evidencia utilizada
- Semana 1: análisis de las reglas A1 y K1 (dependen de `id_pedido`) y del volumen planeado del productor.
- **Pendiente para la Semana 2:** registrar la distribución real de mensajes por partición (Kafka UI o
  `kafka-get-offsets.sh`) y confirmar que ninguna partición se desvía más de un 20 % del promedio.

## Consecuencias / compromisos
- Cambiar el número de particiones después altera la asignación clave → partición; por eso se fija ahora.
- Las reglas futuras por cliente o por dispositivo (por ejemplo, "un dispositivo con 3 o más clientes en 10 minutos")
  requerirán agrupar en Spark redistribuyendo datos, lo cual es aceptable con este volumen.
- Con factor de replicación 1, si el broker cae se detiene el flujo: se usará como **fallo controlado** en la Semana 3.
