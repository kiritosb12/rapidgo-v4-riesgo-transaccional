"""Crea un Data Lake pequeño, igual en estructura al que escribe Spark, para probar orquestacion/ sin Spark.

Escenario (fecha 2026-10-27, todo a las 10:00–10:01 hora de Bogotá = 15:00–15:01 UTC):
- PED-…-000001 normal, 1 intento aprobado.
- PED-…-000002 prueba de tarjetas: 4 intentos rechazados; genera 1 alerta A1 y 2 alertas A2 (intentos 3 y 4).
- PED-…-000003 normal con un rechazo y luego aprobado.
- 2 pedidos normales más. Total 9 intentos en processed y en kpis_1min.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

FECHA = "2026-10-27"
INICIO = datetime(2026, 10, 27, 15, 0, tzinfo=timezone.utc)
RUTA_SPARK = "file:///app/datalake"          # así escribe Spark dentro de su contenedor


def _evento(n_pedido: int, intento: int, segundo: int, estado="aprobado", motivo=None, puntaje=0, senales=""):
    return {
        "id_evento": f"ev-{n_pedido}-{intento}", "equipo": "equipo9-v4", "tipo_evento": "intento_pago",
        "id_pedido": f"PED-20261027-{n_pedido:06d}", "id_cliente": f"CLI-{n_pedido:06d}",
        "valor": 50_000.0 + 1000 * n_pedido, "numero_intento": intento, "estado": estado, "motivo_rechazo": motivo,
        "fecha_evento": INICIO + timedelta(seconds=segundo), "puntaje_riesgo": puntaje, "lista_senales": senales,
    }


def eventos() -> list[dict]:
    return [
        _evento(1, 1, 5),
        *[_evento(2, i, 10 * i, "rechazado", "tarjeta_invalida", p, s)
          for i, (p, s) in enumerate([(40, "S2,S4"), (40, "S2,S4"), (60, "S2,S3,S4"), (60, "S2,S3,S4")], start=1)],
        _evento(3, 1, 20, "rechazado", "fondos_insuficientes"), _evento(3, 2, 70),
        _evento(4, 1, 65), _evento(5, 1, 80),
    ]


def _escribir_spark(tabla: Path, df: pd.DataFrame, nombre: str, particion: str | None, lote: int = 0) -> Path:
    """Escribe un Parquet y lo registra en _spark_metadata como lo haría Spark."""
    carpeta = tabla / particion if particion else tabla
    carpeta.mkdir(parents=True, exist_ok=True)
    archivo = carpeta / f"{nombre}.parquet"
    df.to_parquet(archivo, index=False)
    registro = tabla / "_spark_metadata"
    registro.mkdir(exist_ok=True)
    relativa = archivo.relative_to(tabla.parent.parent).as_posix()   # p. ej. raw/pagos/fecha=…/part-0.parquet
    entrada = {"path": f"{RUTA_SPARK}/{relativa}", "size": archivo.stat().st_size, "isDir": False, "action": "add"}
    destino = registro / str(lote)
    texto = destino.read_text() if destino.exists() else "v1"
    destino.write_text(texto + "\n" + json.dumps(entrada))
    return archivo


def crear_lago(raiz: Path, *, offsets_con_hueco=False, mensajes_invalidos=0, sin_alertas=False) -> Path:
    evs = eventos()
    particion = f"fecha={FECHA}"

    # ---- raw: un mensaje por evento, offsets 0..n por partición
    filas = []
    for i, e in enumerate(evs):
        p = int(e["id_pedido"][-1]) % 3
        filas.append({"clave": e["id_pedido"], "particion": p,
                      "mensaje_json": json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in e.items()})})
    raw = pd.DataFrame(filas)
    raw["offset"] = raw.groupby("particion").cumcount()
    if offsets_con_hueco:
        raw.loc[raw.index[-1], "offset"] += 5
    for k in range(mensajes_invalidos):
        siguiente = int(raw.loc[raw["particion"] == 0, "offset"].max()) + 1     # continúa la partición 0 sin huecos
        raw.loc[len(raw)] = {"clave": None, "particion": 0, "mensaje_json": "{no es json", "offset": siguiente}
    _escribir_spark(raiz / "raw" / "pagos", raw, "part-00000", particion)
    # archivo de un lote que falló: está en la carpeta pero NO en _spark_metadata → no se debe leer
    raw.head(3).to_parquet(raiz / "raw" / "pagos" / particion / "part-huerfano.parquet", index=False)

    # ---- processed
    proc = pd.DataFrame(evs)
    proc["hora_local"] = 10
    proc["es_evento_riesgo"] = proc["puntaje_riesgo"] >= 60
    proc["es_rechazo"] = proc["estado"] == "rechazado"
    proc["fecha_procesamiento"] = INICIO + timedelta(minutes=2)
    _escribir_spark(raiz / "processed" / "pagos", proc, "part-00000", particion)

    # ---- curated/kpis_1min: ventanas 15:00 y 15:01 (UTC) con los mismos totales que processed
    kpis = []
    for minuto in (0, 1):
        ini = INICIO + timedelta(minutes=minuto)
        v = proc[(proc["fecha_evento"] >= ini) & (proc["fecha_evento"] < ini + timedelta(minutes=1))]
        kpis.append({"inicio_ventana": ini, "fin_ventana": ini + timedelta(minutes=1), "intentos": len(v),
                     "pedidos": v["id_pedido"].nunique(), "k1_intentos_por_pedido": round(len(v) / v["id_pedido"].nunique(), 3),
                     "max_numero_intento": int(v["numero_intento"].max()), "k2_valor_promedio": float(v["valor"].mean()),
                     "k3_eventos_riesgo": int(v["es_evento_riesgo"].sum()),
                     "k3_porcentaje_riesgo": round(100 * v["es_evento_riesgo"].mean(), 2),
                     "k4_tasa_rechazo": round(100 * v["es_rechazo"].mean(), 2), "fecha_calculo": INICIO + timedelta(minutes=4)})
    _escribir_spark(raiz / "curated" / "kpis_1min", pd.DataFrame(kpis), "part-00000", None)

    # ---- alertas (mismas columnas que escribe Spark)
    if not sin_alertas:
        base = {"id_pedido": "PED-20261027-000002", "id_cliente": "CLI-000002"}
        a1 = pd.DataFrame([{**base, "tipo_alerta": "MULTIPLES_INTENTOS", "severidad": "MEDIA", "inicio_ventana": INICIO,
                            "fin_ventana": INICIO + timedelta(minutes=5), "intentos": 4, "rechazos": 4,
                            "puntaje_riesgo": 60, "detalle": "intentos=4, rechazos=4", "fecha_deteccion": INICIO + timedelta(minutes=7)}])
        a2 = pd.DataFrame([{**base, "tipo_alerta": "SENALES_COMBINADAS", "severidad": "MEDIA",
                            "inicio_ventana": INICIO + timedelta(seconds=s), "fin_ventana": INICIO + timedelta(seconds=s),
                            "intentos": None, "rechazos": 1, "puntaje_riesgo": 60,
                            "detalle": f"senales=S2,S3,S4, intento={i}, valor=52000, pais_ip=VE",
                            "fecha_deteccion": INICIO + timedelta(seconds=s + 15)} for i, s in ((3, 30), (4, 40))])
        _escribir_spark(raiz / "curated" / "alertas_multiples_intentos", a1, "part-00000", None)
        _escribir_spark(raiz / "curated" / "alertas_senales_combinadas", a2, "part-00000", None)
    return raiz
