"""Pone en cuarentena los mensajes dañados de la capa raw de una fecha.

La capa raw no se borra ni se edita (es la copia fiel de lo que llegó de Kafka). Cuando `validar_raw` falla
por mensajes inválidos y el equipo ya revisó su origen, este comando los registra en
datalake/_cuarentena/raw_pagos.csv (partición, offset, motivo, fecha). `validar_raw` deja de contarlos como
inválidos y el reporte muestra cuántos hay en cuarentena.

Uso (dentro del contenedor de Airflow):
    docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha 2026-11-03 --motivo "prueba de fallo controlado"
    docker compose exec airflow-scheduler python -m orquestacion.cuarentena --fecha 2026-11-03 --solo-mostrar
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime

import pandas as pd

from orquestacion import lago
from orquestacion import tareas as T


def mensajes_danados(cfg: T.Config, fecha: str) -> pd.DataFrame:
    raw = lago.leer(cfg.tabla("raw", "pagos"), fecha, ["mensaje_json", "particion", "offset"])
    if raw.empty:
        return pd.DataFrame(columns=["particion", "offset", "mensaje"])
    danados = raw[~raw["mensaje_json"].map(T.es_mensaje_valido)]
    return pd.DataFrame({"particion": danados["particion"].astype(int), "offset": danados["offset"].astype(int),
                         "mensaje": danados["mensaje_json"].astype(str).str.slice(0, 80)}).sort_values(["particion", "offset"])


def poner_en_cuarentena(cfg: T.Config, fecha: str, motivo: str) -> tuple[int, int]:
    """Agrega a la cuarentena los mensajes dañados de la fecha. Devuelve (nuevos, total en cuarentena)."""
    danados = mensajes_danados(cfg, fecha)
    ruta = T.ruta_cuarentena(cfg)
    previos = pd.read_csv(ruta) if ruta.exists() else pd.DataFrame(columns=["particion", "offset", "fecha", "motivo", "registrado"])
    ya = set(zip(previos["particion"].astype(int), previos["offset"].astype(int)))
    nuevos = danados[[(p, o) not in ya for p, o in zip(danados["particion"], danados["offset"])]]
    if not nuevos.empty:
        filas = nuevos[["particion", "offset"]].assign(fecha=fecha, motivo=motivo,
                                                       registrado=datetime.now(T.ZONA).strftime("%Y-%m-%d %H:%M:%S"))
        ruta.parent.mkdir(parents=True, exist_ok=True)
        pd.concat([previos, filas], ignore_index=True).to_csv(ruta, index=False)
    return len(nuevos), len(previos) + len(nuevos)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Cuarentena de mensajes dañados de la capa raw")
    p.add_argument("--fecha", required=True, help="AAAA-MM-DD (hora de Bogotá)")
    p.add_argument("--motivo", default="mensaje inválido revisado por el equipo")
    p.add_argument("--solo-mostrar", action="store_true", help="lista los mensajes dañados sin registrarlos")
    a = p.parse_args(argv)
    cfg = T.Config.desde_entorno()
    fecha = T.resolver_fecha(a.fecha)

    danados = mensajes_danados(cfg, fecha)
    print(f"Mensajes dañados en raw para fecha={fecha}: {len(danados)}")
    if not danados.empty:
        print(danados.groupby("particion")["offset"].agg(["count", "min", "max"]).rename(
            columns={"count": "mensajes", "min": "offset_min", "max": "offset_max"}).to_string())
        print("Ejemplos:\n" + danados.head(3).to_string(index=False))
    if a.solo_mostrar or danados.empty:
        return 0
    nuevos, total = poner_en_cuarentena(cfg, fecha, a.motivo)
    print(f"Registrados {nuevos} mensajes nuevos en {T.ruta_cuarentena(cfg)} (total en cuarentena: {total}).")
    print("Ahora haga Clear de validar_raw (con Downstream) en Airflow.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
