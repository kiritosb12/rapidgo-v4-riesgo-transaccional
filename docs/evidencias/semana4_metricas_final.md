# Métricas de la ejecución final — Semana 4 (v1.0.0)

**Equipo 9 · Variante 4: Riesgo transaccional** · Esteban Rueda · Oscar Becerra · Karol Perez

Medición tomada sobre el lago que dejó la prueba de punta a punta desde cero
(`bash scripts/prueba_e2e.sh --limpio`, ver [semana4_prueba_e2e.txt](semana4_prueba_e2e.txt)).
Ejecución del DAG: `⟨manual__…⟩` · fecha: ⟨AAAA-MM-DD HH:MM⟩ (hora de Bogotá) · duración del productor: ⟨N⟩ min.

Comando usado (con el flujo corriendo o recién detenido):

```bash
docker compose run --rm --no-deps spark python scripts/metricas_semana2.py
```

El script imprime las tablas y las guarda en `datalake/_metricas/resumen_<fecha>.md`; abajo va esa salida tal cual.

## Comparación con las semanas anteriores

| Medida | Semana 2 (después del ADR-002) | Semana 3 (referencia del DAG) | Semana 4 (final) |
|---|---|---|---|
| Mensajes en raw | 6038 | 3950 | ⟨N⟩ |
| Reparto por partición | 32.5 / 34.3 / 33.1 % | p0 1342 · p1 1264 · p2 1344 | ⟨__ / __ / __ %⟩ |
| Huecos de offsets | 0 | 0 | ⟨0⟩ |
| Duplicados en raw (mismo id_evento) | 46 | 39 | ⟨N⟩ |
| Eventos de riesgo (K3) | 331 (5.52 %) | 200 (4.99 %) | ⟨N (__ %)⟩ |
| Tasa de rechazo (K4) | 22–28 % por minuto | 24.36 % | ⟨__ %⟩ |
| Retraso evento → processed p95 | 9.5 s | — | ⟨__ s⟩ |
| Demora cierre de ventana → curated | 140 s | — | ⟨__ s⟩ |
| Alertas A2 / pedidos con A2 | 331 / — | 200 / 90 (−55.0 %) | ⟨N / N (−__ %)⟩ |
| Filas descartadas por tardías | 0 | — | ⟨N⟩ |

## Qué cambió y por qué

- ⟨Una o dos frases con lo que se observa de verdad en los números, por ejemplo si el reparto por partición
  se mantiene parecido o si el retraso subió por correr Airflow al mismo tiempo que Spark.⟩
- ⟨Si algo salió distinto a lo esperado, decirlo aquí con el valor.⟩

## Salida del script

⟨Pegar aquí completo el contenido de `datalake/_metricas/resumen_<fecha>.md`, secciones 1 a 6.⟩
