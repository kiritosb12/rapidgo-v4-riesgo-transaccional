from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import pyarrow.dataset as ds

_LOTE = re.compile(r"^(\d+)(\.compact)?$")


def _ruta_local(ruta_spark: str, tabla: Path) -> Path:
    """Traduce la ruta guardada por Spark (p. ej. file:///app/datalake/raw/pagos/fecha=…/part-….parquet)
    a la ruta de la misma tabla en este equipo o contenedor (p. ej. /opt/airflow/datalake/raw/pagos/…)."""
    ruta = unquote(ruta_spark.split(":", 1)[1] if ruta_spark.startswith("file:") else ruta_spark)
    marca = f"/{tabla.parent.name}/{tabla.name}/"
    if marca in ruta:
        return tabla / ruta.rsplit(marca, 1)[1]
    return Path(ruta)


def archivos_confirmados(tabla: str | Path) -> list[Path]:
    """Archivos Parquet confirmados por Spark en `tabla` (lee el registro _spark_metadata).

    Si la carpeta no tiene registro (por ejemplo, la escribió pandas), devuelve todos los .parquet.
    """
    tabla = Path(tabla)
    registro = tabla / "_spark_metadata"
    if not registro.is_dir():
        return sorted(p for p in tabla.rglob("*.parquet") if not any(x.startswith((".", "_")) for x in p.relative_to(tabla).parts))

    lotes = {}
    for archivo in registro.iterdir():
        m = _LOTE.match(archivo.name)
        if m:
            lotes[int(m.group(1))] = archivo
    compactos = [n for n, a in lotes.items() if a.name.endswith(".compact")]
    desde = max(compactos) if compactos else -1
    leer = ([lotes[desde]] if desde >= 0 else []) + [lotes[n] for n in sorted(lotes) if n > desde]

    vigentes: dict[str, Path] = {}
    for archivo in leer:
        for linea in archivo.read_text(encoding="utf-8").splitlines()[1:]:   # la 1.ª línea es la versión ("v1")
            if not linea.strip():
                continue
            entrada = json.loads(linea)
            if entrada.get("action", "add") == "add":
                vigentes[entrada["path"]] = _ruta_local(entrada["path"], tabla)
            else:
                vigentes.pop(entrada["path"], None)
    return sorted(p for p in vigentes.values() if p.exists())


def archivos_de_fecha(tabla: str | Path, fecha: str | None) -> list[Path]:
    archivos = archivos_confirmados(tabla)
    if fecha:
        archivos = [a for a in archivos if f"fecha={fecha}" in a.as_posix()]
    return archivos


def _a_utc(df: pd.DataFrame) -> pd.DataFrame:
    """Spark guarda las fechas como instantes UTC; pyarrow a veces las entrega sin zona. Se fijan en UTC."""
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize("UTC") if df[col].dt.tz is None else df[col].dt.tz_convert("UTC")
    return df


def leer(tabla: str | Path, fecha: str | None = None, columnas: list[str] | None = None) -> pd.DataFrame:
    """Lee una tabla del lago como DataFrame de pandas. Con `fecha` filtra la partición fecha=AAAA-MM-DD."""
    tabla = Path(tabla)
    archivos = archivos_de_fecha(tabla, fecha)
    if not archivos:
        return pd.DataFrame(columns=columnas or [])
    particiones = "hive" if any("fecha=" in a.as_posix() for a in archivos) else None
    conjunto = ds.dataset([str(a) for a in archivos], format="parquet", partitioning=particiones,
                          partition_base_dir=str(tabla) if particiones else None)
    df = conjunto.to_table(columns=columnas).to_pandas()
    if "fecha" in df.columns:
        df["fecha"] = df["fecha"].astype(str)
    return _a_utc(df)


def escribir(df: pd.DataFrame, tabla: str | Path, fecha: str, nombre: str) -> Path:
    """Escribe (o reemplaza) la partición fecha=… de una tabla curated. Reescribir da el mismo resultado:
    así una tarea se puede repetir sin duplicar datos (idempotencia)."""
    carpeta = Path(tabla) / f"fecha={fecha}"
    carpeta.mkdir(parents=True, exist_ok=True)
    for viejo in carpeta.glob("*.parquet"):
        viejo.unlink()
    destino = carpeta / f"{nombre}.parquet"
    df.drop(columns=["fecha"], errors="ignore").to_parquet(destino, index=False)   # la fecha va en la carpeta
    return destino
