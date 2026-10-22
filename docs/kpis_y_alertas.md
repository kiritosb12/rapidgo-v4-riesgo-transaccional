# Indicadores (KPI) y reglas de alerta — Variante 4: Riesgo transaccional

> Umbrales **iniciales** (Semana 1). Se ajustarán en la Semana 2 con los valores reales observados
> en la ejecución y el cambio quedará registrado en el ADR-002.

## Señales de riesgo por evento (Spark las calcula evento por evento, sin estado)

| Código | Señal | Condición | Peso |
|---|---|---|---|
| S1 | Valor alto | `valor >= 500000` COP | 30 |
| S2 | IP extranjera | `pais_ip != "CO"` | 25 |
| S3 | Reintento elevado | `numero_intento >= 3` | 20 |
| S4 | Rechazo sospechoso | `motivo_rechazo` es `sospecha_fraude`, `tarjeta_invalida` u `otp_incorrecto` | 15 |
| S5 | Horario atípico | hora local de Bogotá (UTC-5) entre 00:00 y 04:59 | 10 |

`puntaje_riesgo` = suma de los pesos (0 a 100). `senales_riesgo` = número de señales activas.

**Por qué estos valores:** se asume que un pedido típico de domicilio en RapidGo cuesta entre 20.000 y
150.000 COP, por eso 500.000 COP (unas 3 veces un pedido alto) se considera atípico. La IP extranjera y los
reintentos pesan más porque son las señales más asociadas al uso de tarjetas robadas o a la prueba masiva
de tarjetas. El horario nocturno pesa poco porque, por sí solo, es normal (hay pedidos nocturnos legítimos).

## Indicadores mínimos

| # | Indicador | Cálculo | Ventana |
|---|---|---|---|
| K1 | Intentos por pedido | `count(*) / approx_count_distinct(id_pedido)` y `max(numero_intento)` | fija de 1 min |
| K2 | Valor promedio | `avg(valor)` total y por `medio_pago` | fija de 1 min |
| K3 | Eventos de riesgo | cantidad y porcentaje de eventos con `puntaje_riesgo >= 60` | fija de 1 min |
| K4 (adicional) | Tasa de rechazo | intentos `rechazado` / total de intentos | fija de 1 min |

## Reglas de alerta

### A1 — `MULTIPLES_INTENTOS` (con estado, por ventana)
- **Agrupación:** `id_pedido`, ventana fija de **5 minutos** sobre `fecha_evento`, con marca de agua (*watermark*) de 10 minutos.
- **Se dispara si:** `intentos >= 3` **o** `rechazos >= 2` dentro de la ventana.
- **Severidad:** `ALTA` si `intentos >= 5`; en otro caso `MEDIA`.
- **Justificación:** un cliente legítimo rara vez falla más de 2 veces seguidas; 3 o más intentos en 5 minutos
  sugieren prueba de tarjetas o un error de integración con la pasarela de pagos.

### A2 — `SENALES_COMBINADAS` (sin estado, por evento)
- **Se dispara si:** `puntaje_riesgo >= 60` **y** `senales_riesgo >= 2`.
- **Severidad:** `ALTA` si `puntaje_riesgo >= 80`; en otro caso `MEDIA`.
- **Justificación:** se exige la combinación de al menos dos señales para no alertar por un solo factor
  (por ejemplo, un pedido grande legítimo). Ejemplo: `samples/evento_pago_anomalo.json` → S1 + S2 + S3 = 75 → alerta MEDIA.

## Salida de las alertas
Se guardan en `datalake/curated/alertas/` (Parquet) con: `tipo_alerta, severidad, id_pedido, id_cliente,
inicio_ventana, fin_ventana, puntaje_riesgo, detalle, fecha_deteccion`.
