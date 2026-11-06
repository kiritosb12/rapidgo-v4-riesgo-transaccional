# Reporte de riesgo transaccional — 2026-11-05
Generado 2026-11-05 08:50 (hora de Bogotá) · ejecución `manual__evidencia_20261105T134958` · equipo equipo9-v4

## Resumen
| Medida | Valor |
|---|---|
| Mensajes en raw | 3950 |
| Mensajes por partición | p0: 1342, p1: 1264, p2: 1344 |
| Duplicados (mismo id_evento) en raw | 39 |
| Eventos en processed | 4011 |
| Pedidos | 3268 |
| Valor promedio (COP) | 126.076 |
| Eventos de riesgo (K3) | 200 (4.99 %) |
| Tasa de rechazo (K4) | 24.36 % |
| Alertas A1 MULTIPLES_INTENTOS | 25 |
| Alertas A2 SENALES_COMBINADAS (por intento) | 200 |
| Pedidos con A2 (agrupadas por pedido) | 90 (−55.0 %) |
| Pedidos con alguna alerta / con A1 y A2 / ALTA | 105 / 10 / 10 |
| Ventanas de 1 minuto revisadas | 5 |

## Indicadores por hora (hora de Bogotá)
| Hora | Intentos | Pedidos | K1 | Valor prom. | K3 % | K4 % |
|---|---|---|---|---|---|---|
| 08:00 | 4011 | 3268 | 1.227 | 126.076 | 4.99 | 24.36 |

## Pedidos prioritarios (top 10)
| Pedido | Cliente | Severidad | Puntaje máx. | A1 | A2 | Detalle |
|---|---|---|---|---|---|---|
| PED-20261105-312406 | CLI-001190 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1726000, pais_ip=NG |
| PED-20261105-312522 | CLI-003561 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1502000, pais_ip=NG |
| PED-20261105-312843 | CLI-002500 | ALTA | 90 | 0 | 3 | senales=S1,S2,S4, intento=2, valor=2855000, pais_ip=RU |
| PED-20261105-313167 | CLI-000481 | ALTA | 90 | 0 | 3 | senales=S1,S2,S3,S4, intento=3, valor=740000, pais_ip=US |
| PED-20261105-313576 | CLI-002527 | ALTA | 90 | 0 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2023000, pais_ip=US |
| PED-20261105-313729 | CLI-001667 | ALTA | 90 | 0 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2940000, pais_ip=RU |
| PED-20261105-314325 | CLI-001961 | ALTA | 90 | 0 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2225000, pais_ip=ES |
| PED-20261105-314729 | CLI-003248 | ALTA | 90 | 0 | 3 | senales=S1,S2,S3,S4, intento=3, valor=807000, pais_ip=BR |
| PED-20261105-312577 | CLI-001467 | ALTA | 60 | 1 | 3 | senales=S2,S3,S4, intento=5, valor=58000, pais_ip=RU |
| PED-20261105-312473 | CLI-000979 | ALTA | 35 | 1 | 0 | intentos=5, rechazos=5 |

## Calidad de datos
**verificar_fuentes**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | raw recibe datos recientes | 0.1 min | <= 10.0 min |
| ✅ | processed tiene archivos | 117 | > 0 |

**validar_raw**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | offsets sin huecos por partición | 0 | 0 |
| ✅ | offsets sin repetir | 0 | 0 |
| ✅ | % mensajes inválidos o de otro equipo | 0.0 % | <= 1.0 % |
| ✅ | desbalance entre particiones | 4.0 % | <= 20.0 % |

**validar_curated**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | indicadores dentro de rango | 0 | 0 ventanas fuera de rango |
| ✅ | ventanas sin repetir | 0 | 0 |
| ✅ | intentos de kpis_1min = processed | 2514 vs 2514 (0.0 %) | <= 1.0 % |
| ✅ | alertas con pedido existente | 0 | 0 |
| ✅ | severidades válidas | 0 | 0 |
| ✅ | consolidado = pedidos con alerta | 105 consolidados vs 106 con alerta ahora (faltan 0, sobran 0, 1 nuevos después del corte) | 0 faltantes y 0 sobrantes |
| ✅ | ventanas de 1 minuto continuas | 0 | 0 faltantes |

## Advertencias
Ninguna.
