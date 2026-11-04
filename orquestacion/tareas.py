"""Tareas de control y calidad del Data Lake que orquesta Airflow (Semana 3).

Dos tipos de error, porque se tratan distinto en el DAG:
- FalloFuente  → algo de infraestructura que puede arreglarse solo (Kafka caído, Spark aún no escribe):
                 Airflow REINTENTA la tarea.
- FalloCalidad → los datos están mal; reintentar no cambia nada: la tarea falla de inmediato.
"""
from __future__ import annotations

import json
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from orquestacion import lago

ZONA = ZoneInfo("America/Bogota")
EQUIPO = "equipo9-v4"
SEVERIDADES = ("MEDIA", "ALTA")


class FalloFuente(Exception):
    """Fuente no disponible (se reintenta)."""


class FalloCalidad(Exception):
    """Los datos no pasan un chequeo crítico (no se reintenta)."""


@dataclass
class Config:
    ruta_datalake: Path
    kafka_bootstrap: str = "kafka:9092"
    max_minutos_sin_datos: float = 10.0       # advertencia si raw no recibe datos hace más de esto
    max_pct_invalidos: float = 1.0            # crítico: % de mensajes que no son JSON válido del equipo
    max_desbalance_pct: float = 20.0          # advertencia: desviación de una partición vs. el promedio (ADR-001)
    max_dif_consistencia_pct: float = 1.0     # crítico: intentos en kpis_1min vs. processed

    @classmethod
    def desde_entorno(cls) -> "Config":
        e = os.getenv
        return cls(
            ruta_datalake=Path(e("RUTA_DATALAKE", "/opt/airflow/datalake")),
            kafka_bootstrap=e("KAFKA_BOOTSTRAP", "kafka:9092"),
            max_minutos_sin_datos=float(e("MAX_MINUTOS_SIN_DATOS", "10")),
            max_pct_invalidos=float(e("MAX_PCT_INVALIDOS", "1")),
            max_desbalance_pct=float(e("MAX_DESBALANCE_PCT", "20")),
            max_dif_consistencia_pct=float(e("MAX_DIF_CONSISTENCIA_PCT", "1")),
        )

    def tabla(self, *partes: str) -> Path:
        return Path(self.ruta_datalake, *partes)


def resolver_fecha(fecha: str | None) -> str:
    """Fecha a revisar (AAAA-MM-DD, hora de Bogotá). Vacía = hoy."""
    if fecha:
        datetime.strptime(fecha, "%Y-%m-%d")      # valida el formato
        return fecha
    return datetime.now(ZONA).strftime("%Y-%m-%d")


# ------------------------------------------------------------------ chequeos
def _chequeo(nombre: str, ok: bool, valor, umbral, nivel: str = "critico") -> dict:
    return {"chequeo": nombre, "ok": bool(ok), "valor": valor, "umbral": umbral, "nivel": nivel}


def _evaluar(tarea: str, chequeos: list[dict]) -> list[str]:
    """Lanza FalloCalidad si falla un chequeo crítico; devuelve las advertencias."""
    criticos = [c for c in chequeos if not c["ok"] and c["nivel"] == "critico"]
    if criticos:
        detalle = "; ".join(f"{c['chequeo']}: valor={c['valor']} (umbral {c['umbral']})" for c in criticos)
        raise FalloCalidad(f"{tarea} — {len(criticos)} chequeo(s) crítico(s) fallaron: {detalle}")
    return [f"{c['chequeo']}: valor={c['valor']} (umbral {c['umbral']})" for c in chequeos if not c["ok"]]


def _fecha_local(serie: pd.Series) -> pd.Series:
    return serie.dt.tz_convert(ZONA).dt.strftime("%Y-%m-%d")


# ------------------------------------------------------------------ 1. verificar_fuentes
def verificar_fuentes(cfg: Config, fecha: str) -> dict:
    """Kafka responde y la capa raw tiene archivos confirmados para la fecha."""
    errores = []
    for servidor in cfg.kafka_bootstrap.split(","):
        host, puerto = servidor.strip().rsplit(":", 1)
        try:
            socket.create_connection((host, int(puerto)), timeout=5).close()
        except OSError as e:
            errores.append(f"Kafka no responde en {servidor.strip()} ({type(e).__name__}: {e})")
    if errores:
        raise FalloFuente("; ".join(errores))

    raw = lago.archivos_de_fecha(cfg.tabla("raw", "pagos"), fecha)
    if not raw:
        raise FalloFuente(f"La capa raw no tiene archivos confirmados para fecha={fecha} en {cfg.tabla('raw', 'pagos')}")
    processed = lago.archivos_de_fecha(cfg.tabla("processed", "pagos"), fecha)
    minutos = round((time.time() - max(a.stat().st_mtime for a in raw)) / 60, 1)

    chequeos = [
        _chequeo("raw recibe datos recientes", minutos <= cfg.max_minutos_sin_datos, f"{minutos} min",
                 f"<= {cfg.max_minutos_sin_datos} min", "advertencia"),
        _chequeo("processed tiene archivos", len(processed) > 0, len(processed), "> 0"),
    ]
    advertencias = _evaluar("verificar_fuentes", chequeos)
    return {"fecha": fecha, "kafka": cfg.kafka_bootstrap, "archivos_raw": len(raw),
            "archivos_processed": len(processed), "minutos_desde_ultimo_dato": minutos,
            "chequeos": chequeos, "advertencias": advertencias}


# ------------------------------------------------------------------ 2. validar_raw
def validar_raw(cfg: Config, fecha: str) -> dict:
    """Calidad de lo que llegó de Kafka: completitud de offsets, JSON válido y reparto por partición."""
    raw = lago.leer(cfg.tabla("raw", "pagos"), fecha, ["mensaje_json", "particion", "offset"])
    total = len(raw)
    if total == 0:
        raise FalloCalidad(f"validar_raw — la capa raw está vacía para fecha={fecha}")

    def es_valido(texto) -> bool:
        try:
            evento = json.loads(texto)
        except (TypeError, ValueError):
            return False
        return isinstance(evento, dict) and evento.get("equipo") == EQUIPO and bool(evento.get("id_evento"))

    validos = raw["mensaje_json"].map(es_valido)
    invalidos = int((~validos).sum())
    ids = raw.loc[validos, "mensaje_json"].map(lambda t: json.loads(t)["id_evento"])
    duplicados = int(ids.duplicated().sum())

    por_part = raw.groupby("particion")["offset"].agg(["count", "min", "max", "nunique"])
    huecos = int((por_part["max"] - por_part["min"] + 1 - por_part["nunique"]).sum())
    offsets_repetidos = int((por_part["count"] - por_part["nunique"]).sum())
    promedio = total / max(len(por_part), 1)
    desbalance = round(float((por_part["count"] - promedio).abs().max() / promedio * 100), 1)
    pct_invalidos = round(100 * invalidos / total, 2)

    chequeos = [
        _chequeo("offsets sin huecos por partición", huecos == 0, huecos, "0"),
        _chequeo("offsets sin repetir", offsets_repetidos == 0, offsets_repetidos, "0"),
        _chequeo("% mensajes inválidos o de otro equipo", pct_invalidos <= cfg.max_pct_invalidos,
                 f"{pct_invalidos} %", f"<= {cfg.max_pct_invalidos} %"),
        _chequeo("desbalance entre particiones", desbalance <= cfg.max_desbalance_pct, f"{desbalance} %",
                 f"<= {cfg.max_desbalance_pct} %", "advertencia"),
    ]
    advertencias = _evaluar("validar_raw", chequeos)
    return {"mensajes": total, "invalidos": invalidos, "duplicados_id_evento": duplicados,
            "por_particion": {str(p): int(n) for p, n in por_part["count"].items()},
            "desbalance_pct": desbalance, "chequeos": chequeos, "advertencias": advertencias}
