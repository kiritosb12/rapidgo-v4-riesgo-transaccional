"""Reglas de negocio de la Variante 4 (Riesgo transaccional).

Este módulo es Python puro (no necesita Spark) y es la ÚNICA fuente de los umbrales:
- `spark/transformaciones.py` construye con estas constantes las expresiones de Spark.
- `tests/` comprueba las reglas aquí y verifica que Spark dé el mismo resultado.

Si el equipo ajusta un umbral (ADR-002), se cambia SOLO en este archivo.
Detalle de cada regla en docs/kpis_y_alertas.md.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------- señales S1–S5
ZONA_HORARIA = "America/Bogota"
UMBRAL_VALOR_ALTO = 500_000            # S1: valor en COP
PAIS_LOCAL = "CO"                      # S2: IP fuera de Colombia
UMBRAL_REINTENTO = 3                   # S3: numero_intento >= 3
MOTIVOS_SOSPECHOSOS = ("sospecha_fraude", "tarjeta_invalida", "otp_incorrecto")  # S4
HORA_ATIPICA_INICIO = 0                # S5: 00:00 ...
HORA_ATIPICA_FIN = 4                   # ... hasta 04:59 (hora local)

# Medios de pago del contrato (K2 calcula el valor promedio de cada uno)
MEDIOS_PAGO = ("tarjeta_credito", "tarjeta_debito", "pse", "nequi", "daviplata", "efectivo")

PESOS = {"S1": 30, "S2": 25, "S3": 20, "S4": 15, "S5": 10}

# ---------------------------------------------------------------- indicadores y alertas
UMBRAL_EVENTO_RIESGO = 60              # K3 y A2: puntaje_riesgo >= 60
MIN_SENALES_A2 = 2                     # A2: al menos 2 señales activas
UMBRAL_A2_ALTA = 80                    # A2: severidad ALTA si puntaje >= 80

VENTANA_KPI = "1 minute"               # K1–K4
VENTANA_A1 = "5 minutes"               # A1 por id_pedido
UMBRAL_A1_INTENTOS = 3                 # A1: intentos >= 3 ...
UMBRAL_A1_RECHAZOS = 2                 # ... o rechazos >= 2
UMBRAL_A1_ALTA = 5                     # A1: severidad ALTA si intentos >= 5


def hora_local(fecha_evento: str | datetime) -> int:
    """Hora (0–23) en Bogotá de una fecha ISO-8601 en UTC, p. ej. '2026-10-22T03:12:48.902Z'."""
    if isinstance(fecha_evento, str):
        fecha_evento = datetime.fromisoformat(fecha_evento.replace("Z", "+00:00"))
    if fecha_evento.tzinfo is None:
        fecha_evento = fecha_evento.replace(tzinfo=timezone.utc)
    return fecha_evento.astimezone(ZoneInfo(ZONA_HORARIA)).hour


def senales(evento: dict) -> list[str]:
    """Códigos de las señales de riesgo activas en un evento intento_pago."""
    activas = []
    if evento["valor"] >= UMBRAL_VALOR_ALTO:
        activas.append("S1")
    if evento["pais_ip"] != PAIS_LOCAL:
        activas.append("S2")
    if evento["numero_intento"] >= UMBRAL_REINTENTO:
        activas.append("S3")
    if evento.get("motivo_rechazo") in MOTIVOS_SOSPECHOSOS:
        activas.append("S4")
    if HORA_ATIPICA_INICIO <= hora_local(evento["fecha_evento"]) <= HORA_ATIPICA_FIN:
        activas.append("S5")
    return activas


def puntaje_riesgo(evento: dict) -> int:
    """Suma de los pesos de las señales activas (0 a 100)."""
    return sum(PESOS[s] for s in senales(evento))


def evaluar_a2(puntaje: int, cantidad_senales: int) -> str | None:
    """Alerta SENALES_COMBINADAS: devuelve la severidad ('ALTA'/'MEDIA') o None si no hay alerta."""
    if puntaje >= UMBRAL_EVENTO_RIESGO and cantidad_senales >= MIN_SENALES_A2:
        return "ALTA" if puntaje >= UMBRAL_A2_ALTA else "MEDIA"
    return None


def evaluar_a1(intentos: int, rechazos: int) -> str | None:
    """Alerta MULTIPLES_INTENTOS para un pedido dentro de su ventana de 5 minutos."""
    if intentos >= UMBRAL_A1_INTENTOS or rechazos >= UMBRAL_A1_RECHAZOS:
        return "ALTA" if intentos >= UMBRAL_A1_ALTA else "MEDIA"
    return None
