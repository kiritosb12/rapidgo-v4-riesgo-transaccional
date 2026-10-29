# Punto de control Semana 2 — v0.2.0-streaming

**Equipo 9 · Variante 4: Riesgo transaccional** · Esteban Rueda · Oscar Becerra · Karol Perez

## 1. Qué quedó funcionando esta semana
- Productor Python (`producer/`) que publica intentos de pago en `rapidgo.v4.pagos.intentos` con clave
  `id_pedido`, `acks=all`, idempotencia, ~5 % de pedidos anómalos y ~1 % de reenvíos duplicados.
- Spark Structured Streaming (`spark/`) con 5 consultas continuas y checkpoint: raw → processed
  (lectura con esquema, limpieza y tipos, eliminación de duplicados, hora local, señales y puntaje) →
  curated (indicadores K1–K4 por minuto, alertas A1 y A2), todo en Parquet.
- 55 pruebas automáticas y `ci.yml` ejecutándose en cada Pull Request (sintaxis, pruebas, productor en
  modo seco y validación de Docker Compose); `main` exige que pase antes de integrar.

## 2. Valor real observado en la ejecución
Ejecución del 25 de octubre de 2026, 15 minutos a 10 eventos/s (`docs/evidencias/semana2_metricas_antes.md`):
9077 mensajes repartidos en las 3 particiones como 33,5 % / 33,0 % / 33,4 %; 89 duplicados eliminados;
403 eventos de riesgo (4,48 %); 86 alertas A1 y 403 alertas A2 (5,7 y 26,9 por minuto); lotes de processed
de 6557 ms en promedio (cada 10 s); retraso evento → processed p95 de 9,7 s.

## 3. Decisión técnica tomada y por qué
Bajamos la marca de agua de 10 a 2 minutos ([ADR-002](../decisions/ADR-002-marca-de-agua.md)): la primera
ventana de indicadores tardaba 10,4 minutos en aparecer cuando el retraso máximo real de los eventos era de
26,6 s. Después del cambio aparece a los 3,2 minutos, sin eventos descartados por llegar tarde.

## 4. Problema encontrado y cómo se resolvió
Al correr `pytest -v` en Windows fallaron 17 pruebas de reglas con
`zoneinfo._common.ZoneInfoNotFoundError: 'No time zone found with key America/Bogota'`: `spark/reglas.py` calcula
la hora local con `zoneinfo` y Windows no trae la base de datos de zonas horarias (en Linux y en el CI sí pasaban).
Se resolvió en el PR #32 agregando el paquete `tzdata` a `requirements.txt`. En el PR #31 se corrigió otro
problema de Windows: el productor fallaba con `UnicodeEncodeError` al imprimir "→" con la salida redirigida
(Git Bash); ahora fuerza UTF-8. Además, el PR #31 se abrió antes de integrar el #30 y no tenía el `ci.yml`, así
que no corrió el CI; al activar en `main` la regla que exige el check `validar`, quedó bloqueado hasta actualizar la
rama con `main` ("Update branch"), y ahí el CI corrió y quedó en verde.

## 5. Enlaces
- Issues: #8, #9, #10, #11, #12
- Pull Requests: [#31](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/31) productor · [#32](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/32) Spark · [#33](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/33) indicadores y alertas ·
  [#30](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/30) CI · [#34](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/34) pruebas · [#35](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/pull/35) ADR-002
- GitHub Actions: [primera ejecución en verde (PR #30)](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/runs/40517580911) · [todas las ejecuciones](https://github.com/kiritosb12/rapidgo-v4-riesgo-transaccional/actions/workflows/ci.yml)
- Versión: `v0.2.0-streaming`
