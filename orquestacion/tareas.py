"""Tareas de control y calidad del Data Lake que orquesta Airflow (Semana 3).

Python puro + pandas/pyarrow: no importan Airflow, así se prueban con pytest y el DAG
(`airflow/dags/rapidgo_riesgo_transaccional.py`) solo las conecta. Cada tarea devuelve un diccionario
pequeño que Airflow guarda como XCom; `generar_reporte` los recibe todos.

Dos tipos de error, porque se tratan distinto en el DAG (ver ADR-003):
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


def _hoy() -> str:
    return datetime.now(ZONA).strftime("%Y-%m-%d")


def resolver_fecha(fecha: str | None) -> str:
    """Fecha a revisar (AAAA-MM-DD, hora de Bogotá). Vacía = hoy."""
    if fecha:
        datetime.strptime(fecha, "%Y-%m-%d")      # valida el formato
        return fecha
    return _hoy()


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
    """Kafka responde y la capa raw tiene archivos confirmados para la fecha.

    Si se revisa el día de HOY, raw además debe haber recibido datos en los últimos `max_minutos_sin_datos`:
    si no, Spark o el productor están detenidos (falla de fuente, se reintenta). Para días pasados es solo un aviso.
    """
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
    es_hoy = fecha == _hoy()
    if es_hoy and minutos > cfg.max_minutos_sin_datos:
        raise FalloFuente(f"La capa raw no recibe datos nuevos hace {minutos} min (máximo {cfg.max_minutos_sin_datos} min): "
                          "¿Spark o el productor están detenidos?")

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
def es_mensaje_valido(texto) -> bool:
    """JSON de un intento_pago de este equipo con id_evento."""
    try:
        evento = json.loads(texto)
    except (TypeError, ValueError):
        return False
    return isinstance(evento, dict) and evento.get("equipo") == EQUIPO and bool(evento.get("id_evento"))


def ruta_cuarentena(cfg: Config) -> Path:
    return cfg.tabla("_cuarentena", "raw_pagos.csv")


def leer_cuarentena(cfg: Config) -> set[tuple[int, int]]:
    """(partición, offset) de los mensajes de raw puestos en cuarentena (ver orquestacion/cuarentena.py)."""
    ruta = ruta_cuarentena(cfg)
    if not ruta.exists():
        return set()
    df = pd.read_csv(ruta)
    return set(zip(df["particion"].astype(int), df["offset"].astype(int)))


def validar_raw(cfg: Config, fecha: str) -> dict:
    """Calidad de lo que llegó de Kafka: completitud de offsets, JSON válido y reparto por partición.

    Los mensajes en cuarentena (datalake/_cuarentena/raw_pagos.csv) no cuentan como inválidos: raw no se
    borra nunca, se aíslan los mensajes dañados ya revisados.
    """
    raw = lago.leer(cfg.tabla("raw", "pagos"), fecha, ["mensaje_json", "particion", "offset"])
    if raw.empty:
        raise FalloCalidad(f"validar_raw — la capa raw está vacía para fecha={fecha}")
    cuarentena = leer_cuarentena(cfg)
    aislados = pd.Series([(int(p), int(o)) in cuarentena for p, o in zip(raw["particion"], raw["offset"])], index=raw.index)
    en_cuarentena = int(aislados.sum())
    total = len(raw)

    validos = raw["mensaje_json"].map(es_mensaje_valido)
    revisar = ~validos & ~aislados
    invalidos = int(revisar.sum())
    ids = raw.loc[validos, "mensaje_json"].map(lambda t: json.loads(t)["id_evento"])
    duplicados = int(ids.duplicated().sum())

    por_part = raw.groupby("particion")["offset"].agg(["count", "min", "max", "nunique"])
    huecos = int((por_part["max"] - por_part["min"] + 1 - por_part["nunique"]).sum())
    offsets_repetidos = int((por_part["count"] - por_part["nunique"]).sum())
    promedio = total / max(len(por_part), 1)
    desbalance = round(float((por_part["count"] - promedio).abs().max() / promedio * 100), 1)
    pct_invalidos = round(100 * invalidos / total, 2)
    ejemplos = ", ".join(f"p{p}:{o}" for p, o in raw.loc[revisar, ["particion", "offset"]].head(3).itertuples(index=False))

    chequeos = [
        _chequeo("offsets sin huecos por partición", huecos == 0, huecos, "0"),
        _chequeo("offsets sin repetir", offsets_repetidos == 0, offsets_repetidos, "0"),
        _chequeo("% mensajes inválidos o de otro equipo", pct_invalidos <= cfg.max_pct_invalidos,
                 f"{pct_invalidos} % ({invalidos} de {total}" + (f"; p.ej. {ejemplos})" if ejemplos else ")"),
                 f"<= {cfg.max_pct_invalidos} %"),
        _chequeo("desbalance entre particiones", desbalance <= cfg.max_desbalance_pct, f"{desbalance} %",
                 f"<= {cfg.max_desbalance_pct} %", "advertencia"),
    ]
    advertencias = _evaluar("validar_raw", chequeos)
    return {"mensajes": total, "invalidos": invalidos, "en_cuarentena": en_cuarentena, "duplicados_id_evento": duplicados,
            "por_particion": {str(p): int(n) for p, n in por_part["count"].items()},
            "desbalance_pct": desbalance, "chequeos": chequeos, "advertencias": advertencias}


# ------------------------------------------------------------------ 3. agregar_kpis
def resumen_por_hora(processed: pd.DataFrame) -> pd.DataFrame:
    g = processed.groupby("hora_local")
    resumen = pd.DataFrame({
        "intentos": g.size(),
        "pedidos": g["id_pedido"].nunique(),
        "valor_promedio": g["valor"].mean().round(0),
        "eventos_riesgo": g["es_evento_riesgo"].sum().astype(int),
        "rechazos": g["es_rechazo"].sum().astype(int),
    }).reset_index()
    resumen["k1_intentos_por_pedido"] = (resumen["intentos"] / resumen["pedidos"]).round(3)
    resumen["k3_porcentaje_riesgo"] = (100 * resumen["eventos_riesgo"] / resumen["intentos"]).round(2)
    resumen["k4_tasa_rechazo"] = (100 * resumen["rechazos"] / resumen["intentos"]).round(2)
    return resumen


def agregar_kpis(cfg: Config, fecha: str) -> dict:
    """Resumen por hora (hora de Bogotá) desde processed → curated/resumen_horario/fecha=…"""
    columnas = ["id_pedido", "valor", "hora_local", "es_evento_riesgo", "es_rechazo"]
    processed = lago.leer(cfg.tabla("processed", "pagos"), fecha, columnas)
    if processed.empty:
        raise FalloCalidad(f"agregar_kpis — processed está vacía para fecha={fecha}")
    resumen = resumen_por_hora(processed)
    ruta = lago.escribir(resumen, cfg.tabla("curated", "resumen_horario"), fecha, "resumen_horario")
    eventos = len(processed)
    return {
        "eventos": eventos,
        "pedidos": int(processed["id_pedido"].nunique()),
        "horas": int(len(resumen)),
        "valor_promedio": round(float(processed["valor"].mean()), 0),
        "eventos_riesgo": int(processed["es_evento_riesgo"].sum()),
        "k3_porcentaje_riesgo": round(100 * float(processed["es_evento_riesgo"].mean()), 2),
        "k4_tasa_rechazo": round(100 * float(processed["es_rechazo"].mean()), 2),
        "ruta": str(ruta),
    }


# ------------------------------------------------------------------ 4. consolidar_alertas
def _alertas_de_fecha(tabla: Path, fecha: str) -> pd.DataFrame:
    df = lago.leer(tabla)
    if df.empty:
        return df
    return df[_fecha_local(df["inicio_ventana"]) == fecha].copy()


COLUMNAS_CONSOLIDADO = ["id_pedido", "id_cliente", "alertas_a1", "alertas_a2", "severidad", "puntaje_max",
                         "primera_alerta", "ultima_alerta", "detalle"]


def consolidar(a1: pd.DataFrame, a2: pd.DataFrame) -> pd.DataFrame:
    """Una fila por pedido con alerta. A2 alerta cada intento riesgoso; aquí se agrupa por pedido (ADR-002)."""
    columnas = ["id_pedido", "id_cliente", "severidad", "puntaje_riesgo", "fecha_deteccion", "detalle"]
    partes = [df[columnas].assign(tipo=t) for df, t in ((a1, "A1"), (a2, "A2")) if not df.empty]
    if not partes:
        return pd.DataFrame(columns=COLUMNAS_CONSOLIDADO)
    todas = pd.concat(partes, ignore_index=True).sort_values("fecha_deteccion")
    g = todas.groupby("id_pedido")
    detalle_a2 = todas[todas["tipo"] == "A2"].groupby("id_pedido")["detalle"].last()
    detalle_a1 = todas[todas["tipo"] == "A1"].groupby("id_pedido")["detalle"].last()
    salida = pd.DataFrame({
        "id_cliente": g["id_cliente"].first(),
        "alertas_a1": g["tipo"].agg(lambda s: int((s == "A1").sum())),
        "alertas_a2": g["tipo"].agg(lambda s: int((s == "A2").sum())),
        "severidad": g["severidad"].agg(lambda s: "ALTA" if (s == "ALTA").any() else "MEDIA"),
        "puntaje_max": g["puntaje_riesgo"].max().astype(int),
        "primera_alerta": g["fecha_deteccion"].min(),
        "ultima_alerta": g["fecha_deteccion"].max(),
    })
    salida["detalle"] = detalle_a2.reindex(salida.index).combine_first(detalle_a1.reindex(salida.index))
    salida = salida.reset_index()
    salida["_orden"] = salida["severidad"].map({"ALTA": 0, "MEDIA": 1})
    return (salida.sort_values(["_orden", "puntaje_max", "alertas_a2", "alertas_a1"], ascending=[True, False, False, False])
            .drop(columns="_orden").reset_index(drop=True)[COLUMNAS_CONSOLIDADO])


def consolidar_alertas(cfg: Config, fecha: str) -> dict:
    """Une A1 y A2 de la fecha y agrupa por pedido → curated/alertas_consolidadas/fecha=…"""
    a1 = _alertas_de_fecha(cfg.tabla("curated", "alertas_multiples_intentos"), fecha)
    a2 = _alertas_de_fecha(cfg.tabla("curated", "alertas_senales_combinadas"), fecha)
    consolidadas = consolidar(a1, a2)
    ruta = lago.escribir(consolidadas, cfg.tabla("curated", "alertas_consolidadas"), fecha, "alertas_consolidadas")
    pedidos_a2 = int(a2["id_pedido"].nunique()) if not a2.empty else 0
    return {
        "alertas_a1": len(a1),
        "alertas_a2": len(a2),
        "pedidos_con_a2": pedidos_a2,
        "reduccion_a2_pct": round(100 * (1 - pedidos_a2 / len(a2)), 1) if len(a2) else 0.0,
        "pedidos_con_alerta": len(consolidadas),
        "pedidos_con_a1_y_a2": int(((consolidadas["alertas_a1"] > 0) & (consolidadas["alertas_a2"] > 0)).sum()),
        "pedidos_alta": int((consolidadas["severidad"] == "ALTA").sum()),
        "ruta": str(ruta),
    }


# ------------------------------------------------------------------ 5. validar_curated
MARGEN_LOTE = pd.Timedelta(minutes=1)
# `pedidos` sale de approx_count_distinct (HyperLogLog, error cercano al 5 %): en una ventana casi sin
# reintentos puede contar un pedido más que intentos, así que K1 puede quedar apenas por debajo de 1.
K1_MINIMO = 0.95


def _cobertura_consolidado(alertas: pd.DataFrame, consolidadas: pd.DataFrame) -> tuple[int, int, int]:
    """Compara el consolidado con las alertas actuales. Spark sigue escribiendo alertas mientras corre el DAG,
    así que el consolidado es una foto: solo se exige que estén los pedidos cuya primera alerta es anterior a
    esa foto (con un margen para el lote que estaba en curso). Devuelve (faltan, sobran, nuevos_despues_del_corte)."""
    en_consolidado = set(consolidadas["id_pedido"]) if not consolidadas.empty else set()
    if alertas.empty:
        return 0, len(en_consolidado), 0
    primera = alertas.groupby("id_pedido")["fecha_deteccion"].min()
    corte = consolidadas["ultima_alerta"].max() if not consolidadas.empty else primera.max()
    esperados = set(primera[primera <= corte - MARGEN_LOTE].index)
    faltan = len(esperados - en_consolidado)
    sobran = len(en_consolidado - set(primera.index))
    nuevos = len(set(primera.index) - en_consolidado) - faltan
    return faltan, sobran, nuevos


def _alertas_sin_pedido(alertas: pd.DataFrame, pedidos: set, processed: pd.DataFrame) -> tuple[int, int]:
    """Alertas cuyo pedido no está en processed → (huérfanas, A2 recientes). A2 se calcula evento por evento en
    otra consulta de Spark: su alerta puede guardarse segundos antes que la fila del evento en processed. Por eso
    una A2 posterior a lo último que escribió processed (menos un margen para el lote en curso) no se cuenta."""
    if alertas.empty:
        return 0, 0
    sin_pedido = ~alertas["id_pedido"].isin(pedidos)
    if processed.empty:
        return int(sin_pedido.sum()), 0
    corte = processed["fecha_procesamiento"].max() - MARGEN_LOTE
    reciente = (alertas["tipo_alerta"] == "SENALES_COMBINADAS") & (alertas["fecha_deteccion"] > corte)
    return int((sin_pedido & ~reciente).sum()), int((sin_pedido & reciente).sum())


def validar_curated(cfg: Config, fecha: str) -> dict:
    """Coherencia de la capa curated: rangos de los indicadores, ventanas completas, totales que cuadran
    con processed y alertas que apuntan a pedidos reales."""
    kpis = lago.leer(cfg.tabla("curated", "kpis_1min"))
    if not kpis.empty:
        kpis = kpis[_fecha_local(kpis["inicio_ventana"]) == fecha]
    if kpis.empty:
        raise FalloCalidad(f"validar_curated — no hay ventanas de indicadores cerradas para fecha={fecha}")
    processed = lago.leer(cfg.tabla("processed", "pagos"), fecha, ["id_pedido", "fecha_evento", "fecha_procesamiento"])

    fuera_de_rango = int((~kpis["k3_porcentaje_riesgo"].between(0, 100) | ~kpis["k4_tasa_rechazo"].between(0, 100)
                          | (kpis["k1_intentos_por_pedido"] < K1_MINIMO) | (kpis["intentos"] <= 0)).sum())
    inicio, fin = kpis["inicio_ventana"].min(), kpis["fin_ventana"].max()
    esperadas = int((fin - inicio) / pd.Timedelta(minutes=1))
    faltantes = esperadas - kpis["inicio_ventana"].nunique()
    ventanas_repetidas = int(kpis["inicio_ventana"].duplicated().sum())

    en_rango = int(((processed["fecha_evento"] >= inicio) & (processed["fecha_evento"] < fin)).sum())
    suma_kpis = int(kpis["intentos"].sum())
    diferencia = round(100 * abs(suma_kpis - en_rango) / max(en_rango, 1), 2)

    pedidos = set(processed["id_pedido"])
    a1 = _alertas_de_fecha(cfg.tabla("curated", "alertas_multiples_intentos"), fecha)
    a2 = _alertas_de_fecha(cfg.tabla("curated", "alertas_senales_combinadas"), fecha)
    alertas = pd.concat([a1, a2], ignore_index=True) if not (a1.empty and a2.empty) else pd.DataFrame(columns=["id_pedido", "severidad"])
    huerfanas, recientes = _alertas_sin_pedido(alertas, pedidos, processed)
    severidad_mala = int((~alertas["severidad"].isin(SEVERIDADES)).sum())
    consolidadas = lago.leer(cfg.tabla("curated", "alertas_consolidadas"), fecha)
    pedidos_alerta = alertas["id_pedido"].nunique()
    faltan, sobran, nuevas = _cobertura_consolidado(alertas, consolidadas)

    chequeos = [
        _chequeo("indicadores dentro de rango", fuera_de_rango == 0, fuera_de_rango, "0 ventanas fuera de rango"),
        _chequeo("ventanas sin repetir", ventanas_repetidas == 0, ventanas_repetidas, "0"),
        _chequeo("intentos de kpis_1min = processed", diferencia <= cfg.max_dif_consistencia_pct,
                 f"{suma_kpis} vs {en_rango} ({diferencia} %)", f"<= {cfg.max_dif_consistencia_pct} %"),
        _chequeo("alertas con pedido existente", huerfanas == 0,
                 f"{huerfanas} (más {recientes} A2 recientes cuyo pedido aún no llega a processed)" if recientes else huerfanas,
                 "0"),
        _chequeo("severidades válidas", severidad_mala == 0, severidad_mala, "0"),
        _chequeo("consolidado = pedidos con alerta", faltan == 0 and sobran == 0,
                 f"{len(consolidadas)} consolidados vs {pedidos_alerta} con alerta ahora (faltan {faltan}, "
                 f"sobran {sobran}, {nuevas} nuevos después del corte)", "0 faltantes y 0 sobrantes"),
        _chequeo("ventanas de 1 minuto continuas", faltantes == 0, faltantes, "0 faltantes", "advertencia"),
    ]
    advertencias = _evaluar("validar_curated", chequeos)
    return {"ventanas": int(len(kpis)), "ventana_inicio": inicio.isoformat(), "ventana_fin": fin.isoformat(),
            "ventanas_faltantes": int(faltantes), "intentos_kpis": suma_kpis, "intentos_processed": en_rango,
            "chequeos": chequeos, "advertencias": advertencias}


# ------------------------------------------------------------------ 6. generar_reporte
def _tabla_md(encabezado: list[str], filas: list) -> str:
    lineas = ["| " + " | ".join(encabezado) + " |", "|" + "---|" * len(encabezado)]
    return "\n".join(lineas + ["| " + " | ".join(str(v) for v in f) + " |" for f in filas])


def _chequeos_md(nombre: str, resultado: dict) -> str:
    filas = [("✅" if c["ok"] else ("⚠️" if c["nivel"] == "advertencia" else "❌"), c["chequeo"], c["valor"], c["umbral"])
             for c in resultado.get("chequeos", [])]
    return f"**{nombre}**\n\n" + _tabla_md(["", "Chequeo", "Valor", "Umbral"], filas)


def generar_reporte(cfg: Config, fecha: str, resultados: dict, ejecucion: str = "manual") -> dict:
    """Reporte del día en Markdown y JSON → curated/reportes/fecha=…/ (usa los XCom de las demás tareas)."""
    fuentes, raw, kpis = resultados["verificar_fuentes"], resultados["validar_raw"], resultados["agregar_kpis"]
    alertas, curated = resultados["consolidar_alertas"], resultados["validar_curated"]
    horas = lago.leer(cfg.tabla("curated", "resumen_horario"), fecha)
    top = lago.leer(cfg.tabla("curated", "alertas_consolidadas"), fecha).head(10)
    ahora = datetime.now(ZONA)

    partes = [
        f"# Reporte de riesgo transaccional — {fecha}",
        f"Generado {ahora:%Y-%m-%d %H:%M} (hora de Bogotá) · ejecución `{ejecucion}` · equipo {EQUIPO}", "",
        "## Resumen",
        _tabla_md(["Medida", "Valor"], [
            ("Mensajes en raw", raw["mensajes"]),
            ("Mensajes por partición", ", ".join(f"p{p}: {n}" for p, n in raw["por_particion"].items())),
            ("Duplicados (mismo id_evento) en raw", raw["duplicados_id_evento"]),
            ("Mensajes en cuarentena (dañados ya revisados)", raw.get("en_cuarentena", 0)),
            ("Eventos en processed", kpis["eventos"]),
            ("Pedidos", kpis["pedidos"]),
            ("Valor promedio (COP)", f"{kpis['valor_promedio']:,.0f}".replace(",", ".")),
            ("Eventos de riesgo (K3)", f"{kpis['eventos_riesgo']} ({kpis['k3_porcentaje_riesgo']} %)"),
            ("Tasa de rechazo (K4)", f"{kpis['k4_tasa_rechazo']} %"),
            ("Alertas A1 MULTIPLES_INTENTOS", alertas["alertas_a1"]),
            ("Alertas A2 SENALES_COMBINADAS (por intento)", alertas["alertas_a2"]),
            ("Pedidos con A2 (agrupadas por pedido)", f"{alertas['pedidos_con_a2']} (−{alertas['reduccion_a2_pct']} %)"),
            ("Pedidos con alguna alerta / con A1 y A2 / ALTA",
             f"{alertas['pedidos_con_alerta']} / {alertas['pedidos_con_a1_y_a2']} / {alertas['pedidos_alta']}"),
            ("Ventanas de 1 minuto revisadas", curated["ventanas"]),
        ]), "",
        "## Indicadores por hora (hora de Bogotá)",
        _tabla_md(["Hora", "Intentos", "Pedidos", "K1", "Valor prom.", "K3 %", "K4 %"],
                  [(f"{int(h.hora_local):02d}:00", h.intentos, h.pedidos, h.k1_intentos_por_pedido,
                    f"{h.valor_promedio:,.0f}".replace(",", "."), h.k3_porcentaje_riesgo, h.k4_tasa_rechazo)
                   for h in horas.sort_values("hora_local").itertuples()]), "",
        "## Pedidos prioritarios (top 10)",
        _tabla_md(["Pedido", "Cliente", "Severidad", "Puntaje máx.", "A1", "A2", "Detalle"],
                  [(r.id_pedido, r.id_cliente, r.severidad, r.puntaje_max, r.alertas_a1, r.alertas_a2, r.detalle)
                   for r in top.itertuples()]), "",
        "## Calidad de datos",
        _chequeos_md("verificar_fuentes", fuentes), "",
        _chequeos_md("validar_raw", raw), "",
        _chequeos_md("validar_curated", curated), "",
    ]
    advertencias = fuentes["advertencias"] + raw["advertencias"] + curated["advertencias"]
    partes += ["## Advertencias", *(f"- {a}" for a in advertencias)] if advertencias else ["## Advertencias", "Ninguna."]

    carpeta = cfg.tabla("curated", "reportes", f"fecha={fecha}")
    carpeta.mkdir(parents=True, exist_ok=True)
    base = carpeta / f"reporte_{ahora:%H%M%S}"
    base.with_suffix(".md").write_text("\n".join(partes) + "\n", encoding="utf-8")
    base.with_suffix(".json").write_text(json.dumps({"fecha": fecha, "ejecucion": ejecucion, **resultados},
                                                    ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return {"reporte_md": str(base.with_suffix(".md")), "reporte_json": str(base.with_suffix(".json")),
            "advertencias": len(advertencias)}
