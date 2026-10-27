"""Productor Kafka de intentos de pago — RapidGo V4 (Equipo 9).

Publica eventos "intento_pago" en el topic rapidgo.v4.pagos.intentos con clave = id_pedido.

Ejemplos:
    # desde el computador (Kafka expuesto en localhost:29092)
    python -m producer.productor_pagos --eventos-por-segundo 10 --duracion 600

    # dentro de Docker Compose (usa kafka:9092)
    DURACION_PRODUCTOR=600 docker compose up -d productor

    # sin Kafka: imprime los eventos en pantalla
    python -m producer.productor_pagos --modo-seco --max-eventos 5
"""
from __future__ import annotations

import argparse
import heapq
import itertools
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from producer.generador import GeneradorPagos, ahora_iso

RAIZ = Path(__file__).resolve().parents[1]
TOPIC_POR_DEFECTO = "rapidgo.v4.pagos.intentos"


def leer_argumentos(argv=None):
    p = argparse.ArgumentParser(description="Productor de intentos de pago RapidGo V4")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", TOPIC_POR_DEFECTO))
    p.add_argument("--eventos-por-segundo", type=float, default=10.0)
    p.add_argument("--duracion", type=int, default=0, help="segundos; 0 = sin límite (Ctrl+C para parar)")
    p.add_argument("--max-eventos", type=int, default=0, help="0 = sin límite")
    p.add_argument("--tasa-anomalias", type=float, default=0.05, help="fracción de pedidos anómalos")
    p.add_argument("--tasa-duplicados", type=float, default=0.01, help="fracción de eventos reenviados")
    p.add_argument("--semilla", type=int, default=None)
    p.add_argument("--sin-validar", action="store_true", help="no validar cada evento contra el esquema JSON")
    p.add_argument("--modo-seco", action="store_true", help="no conecta a Kafka; imprime los eventos")
    p.add_argument("--resumen", default=None, help="ruta de un JSON donde guardar el resumen final")
    return p.parse_args(argv)


def crear_validador():
    from jsonschema import Draft202012Validator
    esquema = json.loads((RAIZ / "samples" / "evento_pago.schema.json").read_text(encoding="utf-8"))
    return Draft202012Validator(esquema)


def crear_productor(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "productor-equipo9-v4",
        "acks": "all",                    # el broker confirma solo cuando la réplica líder guardó el mensaje
        "enable.idempotence": True,       # los reintentos de red no duplican mensajes
        "partitioner": "murmur2_random",  # mismo reparto clave→partición que los clientes Java
        "linger.ms": 20,
        "compression.type": "lz4",
    })


class Estadisticas:
    def __init__(self):
        self.enviados = 0
        self.confirmados = 0
        self.errores = 0
        self.duplicados = 0
        self.por_particion = Counter()
        self.ultimo_offset = {}
        self.sesiones = Counter()
        self.inicio = time.time()

    def al_confirmar(self, err, msg):
        if err is not None:
            self.errores += 1
            print(f"[ERROR] entrega fallida: {err}", file=sys.stderr)
            return
        self.confirmados += 1
        self.por_particion[msg.partition()] += 1
        self.ultimo_offset[msg.partition()] = msg.offset()

    def resumen(self) -> dict:
        segundos = max(time.time() - self.inicio, 1e-9)
        total_pedidos = sum(self.sesiones.values())
        return {
            "fecha_fin": ahora_iso(),
            "segundos": round(segundos, 1),
            "eventos_enviados": self.enviados,
            "eventos_confirmados": self.confirmados,
            "errores_entrega": self.errores,
            "duplicados_simulados": self.duplicados,
            "eventos_por_segundo_real": round(self.enviados / segundos, 2),
            "confirmados_por_particion": {str(k): v for k, v in sorted(self.por_particion.items())},
            "ultimo_offset_por_particion": {str(k): v for k, v in sorted(self.ultimo_offset.items())},
            "pedidos_por_tipo": dict(self.sesiones),
            "porcentaje_pedidos_anomalos": round(100 * (total_pedidos - self.sesiones["normal"]) / max(total_pedidos, 1), 2),
        }


def main(argv=None) -> int:
    # En Windows la salida redirigida (Git Bash, archivos) usa cp1252 y falla con caracteres como "→"
    for flujo in (sys.stdout, sys.stderr):
        flujo.reconfigure(encoding="utf-8", errors="replace")
    args = leer_argumentos(argv)
    generador = GeneradorPagos(semilla=args.semilla, tasa_anomalias=args.tasa_anomalias,
                               tasa_duplicados=args.tasa_duplicados)
    validador = None if args.sin_validar else crear_validador()
    productor = None if args.modo_seco else crear_productor(args.bootstrap)
    est = Estadisticas()

    detener = {"si": False}
    def al_detener(*_):
        detener["si"] = True
    signal.signal(signal.SIGINT, al_detener)
    signal.signal(signal.SIGTERM, al_detener)

    pendientes: list = []           # (momento_programado, consecutivo, evento)
    contador = itertools.count()
    intervalo = 1.0 / args.eventos_por_segundo
    fin = time.time() + args.duracion if args.duracion else None
    ultimo_reporte = time.time()
    destino = "pantalla (modo seco)" if args.modo_seco else f"{args.bootstrap} → {args.topic}"
    print(f"[productor] enviando ~{args.eventos_por_segundo} eventos/s a {destino}; "
          f"anomalías={args.tasa_anomalias:.0%}, duplicados={args.tasa_duplicados:.0%}", flush=True)

    def publicar(evento: dict):
        if validador is not None:
            errores = [e.message for e in validador.iter_errors(evento)]
            if errores:
                raise ValueError(f"Evento no cumple el contrato: {errores}")
        cuerpo = json.dumps(evento, ensure_ascii=False)
        if productor is None:
            print(cuerpo)
        else:
            while True:
                try:
                    productor.produce(args.topic, key=evento["id_pedido"], value=cuerpo.encode("utf-8"),
                                      on_delivery=est.al_confirmar)
                    break
                except BufferError:          # cola local llena: esperar a que Kafka confirme
                    productor.poll(0.5)
            productor.poll(0)
        est.enviados += 1

    while not detener["si"]:
        ahora = time.time()
        if fin and ahora >= fin:
            break
        if args.max_eventos and est.enviados >= args.max_eventos:
            break

        if pendientes and pendientes[0][0] <= ahora:
            _, _, evento = heapq.heappop(pendientes)       # siguiente intento de un pedido en curso
        else:
            tipo, intentos = generador.nueva_sesion(datetime.now(timezone.utc))
            est.sesiones[tipo] += 1
            evento = intentos[0].evento
            for intento in intentos[1:]:
                heapq.heappush(pendientes, (ahora + intento.retraso, next(contador), intento.evento))

        evento["fecha_evento"] = ahora_iso()
        publicar(evento)
        if generador.debe_duplicar():
            est.duplicados += 1
            publicar(dict(evento))   # mismo id_evento: Spark debe eliminarlo

        if ahora - ultimo_reporte >= 10:
            ultimo_reporte = ahora
            print(f"[productor] enviados={est.enviados} confirmados={est.confirmados} "
                  f"por_particion={dict(sorted(est.por_particion.items()))} errores={est.errores}", flush=True)
        time.sleep(max(0.0, intervalo - (time.time() - ahora)))

    if productor is not None:
        print("[productor] esperando confirmaciones pendientes...", flush=True)
        productor.flush(30)
    resumen = est.resumen()
    print("[productor] RESUMEN " + json.dumps(resumen, ensure_ascii=False), flush=True)
    if args.resumen:
        Path(args.resumen).parent.mkdir(parents=True, exist_ok=True)
        Path(args.resumen).write_text(json.dumps(resumen, indent=2, ensure_ascii=False), encoding="utf-8")
    return 1 if est.errores else 0


if __name__ == "__main__":
    sys.exit(main())
