# Reporte de riesgo transaccional — 2026-11-13
Generado 2026-11-13 08:45 (hora de Bogotá) · ejecución `manual__2026-11-13T13:44:19.546901+00:00` · equipo equipo9-v4

## Resumen
| Medida | Valor |
|---|---|
| Mensajes en raw | 10841 |
| Mensajes por partición | p0: 3583, p1: 3623, p2: 3635 |
| Duplicados (mismo id_evento) en raw | 106 |
| Mensajes en cuarentena (dañados ya revisados) | 0 |
| Eventos en processed | 10750 |
| Pedidos | 8701 |
| Valor promedio (COP) | 119.466 |
| Eventos de riesgo (K3) | 534 (4.97 %) |
| Tasa de rechazo (K4) | 23.94 % |
| Alertas A1 MULTIPLES_INTENTOS | 290 |
| Alertas A2 SENALES_COMBINADAS (por intento) | 541 |
| Pedidos con A2 (agrupadas por pedido) | 240 (−55.6 %) |
| Pedidos con alguna alerta / con A1 y A2 / ALTA | 387 / 138 / 120 |
| Ventanas de 1 minuto revisadas | 16 |

## Indicadores por hora (hora de Bogotá)
| Hora | Intentos | Pedidos | K1 | Valor prom. | K3 % | K4 % |
|---|---|---|---|---|---|---|
| 08:00 | 10750 | 8701 | 1.235 | 119.466 | 4.97 | 23.94 |

## Pedidos prioritarios (top 10)
| Pedido | Cliente | Severidad | Puntaje máx. | A1 | A2 | Detalle |
|---|---|---|---|---|---|---|
| PED-20261113-316580 | CLI-000285 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2215000, pais_ip=US |
| PED-20261113-316793 | CLI-001325 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2765000, pais_ip=VE |
| PED-20261113-316883 | CLI-003798 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1838000, pais_ip=NG |
| PED-20261113-316927 | CLI-004190 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=689000, pais_ip=BR |
| PED-20261113-317246 | CLI-001412 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2312000, pais_ip=NG |
| PED-20261113-317454 | CLI-003856 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=824000, pais_ip=US |
| PED-20261113-318230 | CLI-003474 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1246000, pais_ip=VE |
| PED-20261113-318279 | CLI-002958 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=2317000, pais_ip=RU |
| PED-20261113-318424 | CLI-000007 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1482000, pais_ip=BR |
| PED-20261113-318729 | CLI-000804 | ALTA | 90 | 1 | 3 | senales=S1,S2,S3,S4, intento=3, valor=1064000, pais_ip=NG |

## Calidad de datos
**verificar_fuentes**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | raw recibe datos recientes | 0.2 min | <= 10.0 min |
| ✅ | processed tiene archivos | 303 | > 0 |

**validar_raw**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | offsets sin huecos por partición | 0 | 0 |
| ✅ | offsets sin repetir | 0 | 0 |
| ✅ | % mensajes inválidos o de otro equipo | 0.0 % (0 de 10841) | <= 1.0 % |
| ✅ | desbalance entre particiones | 0.8 % | <= 20.0 % |

**validar_curated**

|  | Chequeo | Valor | Umbral |
|---|---|---|---|
| ✅ | indicadores dentro de rango | 0 | 0 ventanas fuera de rango |
| ✅ | ventanas sin repetir | 0 | 0 |
| ✅ | intentos de kpis_1min = processed | 9337 vs 9337 (0.0 %) | <= 1.0 % |
| ✅ | alertas con pedido existente | 0 | 0 |
| ✅ | severidades válidas | 0 | 0 |
| ✅ | consolidado = pedidos con alerta | 387 consolidados vs 390 con alerta ahora (faltan 0, sobran 0, 3 nuevos después del corte) | 0 faltantes y 0 sobrantes |
| ✅ | ventanas de 1 minuto continuas | 0 | 0 faltantes |

## Advertencias
Ninguna.
