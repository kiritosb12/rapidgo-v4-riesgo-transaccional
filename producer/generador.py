"""Generador de intentos de pago simulados para RapidGo V4 (Riesgo transaccional).

Es Python puro (no necesita Kafka), así se puede probar con pytest. "productor_pagos.py"
lo usa para publicar los eventos en Kafka.

Cada sesión es un pedido con uno o varios intentos de pago. Una fracción de los pedidos
("tasa_anomalias", 5% por defecto) sigue un patrón anómalo:

- prueba_tarjetas ........ 3 a 6 intentos rechazados seguidos, motivos sospechosos, IP casi siempre extranjera.
- valor_alto_extranjero .. valor de 600.000 a 3.000.000 COP desde IP extranjera, 1 a 3 intentos.
- reintentos_banco ....... 3 o 4 rechazos por fondos/tiempo agotado y luego aprobado (falla de pasarela).

Los pedidos normales también traen casos límite realistas: 3% son pedidos grandes legítimos
(solo señal S1) y 2% pagan desde el exterior (viajeros, solo señal S2).
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

EQUIPO = "equipo9-v4"
TIPOS_ANOMALIA = ("prueba_tarjetas", "valor_alto_extranjero", "reintentos_banco")

MEDIOS_PAGO = ["tarjeta_credito", "tarjeta_debito", "pse", "nequi", "daviplata", "efectivo"]
PESOS_MEDIOS = [35, 20, 15, 18, 8, 4]
DISPOSITIVOS = ["android", "ios", "web"]
PESOS_DISPOSITIVOS = [55, 25, 20]
PAISES_EXTRANJEROS = ["VE", "US", "ES", "MX", "BR", "NG", "RU"]
CIUDADES = [  # (ciudad, latitud, longitud)
    ("Bogota", 4.7110, -74.0721), ("Medellin", 6.2442, -75.5812), ("Cali", 3.4516, -76.5320),
    ("Barranquilla", 10.9685, -74.7813), ("Facatativa", 4.8143, -74.3545), ("Bucaramanga", 7.1193, -73.1227),
]
PESOS_CIUDADES = [40, 18, 14, 10, 8, 10]


def ahora_iso(momento: datetime | None = None) -> str:
    """Fecha UTC en ISO-8601 con milisegundos y 'Z', p. ej. 2026-10-22T19:42:15.318Z."""
    momento = momento or datetime.now(timezone.utc)
    return momento.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{momento.microsecond // 1000:03d}Z"


@dataclass
class Intento:
    """Un intento programado: `retraso` son los segundos desde el inicio de la sesión."""
    retraso: float
    evento: dict


@dataclass
class GeneradorPagos:
    semilla: int | None = None
    tasa_anomalias: float = 0.05
    tasa_duplicados: float = 0.01
    rng: random.Random = field(init=False)
    _consecutivo: int = field(init=False)

    def __post_init__(self):
        self.rng = random.Random(self.semilla)
        self._consecutivo = self.rng.randint(1, 400_000)

    # ------------------------------------------------------------------ utilidades
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _nuevo_pedido(self, momento: datetime) -> str:
        self._consecutivo += 1
        return f"PED-{momento:%Y%m%d}-{self._consecutivo % 1_000_000:06d}"

    def _valor_normal(self) -> float:
        # Pedido típico de domicilio: mediana ~45.000 COP, casi siempre entre 10.000 y 200.000.
        valor = self.rng.lognormvariate(10.7, 0.55)
        return float(round(min(max(valor, 8_000), 450_000), -2))

    def _base(self, momento: datetime, id_pedido: str, id_cliente: int) -> dict:
        ciudad, lat, lon = self.rng.choices(CIUDADES, PESOS_CIUDADES)[0]
        return {
            "id_evento": None,  # se asigna en cada intento
            "tipo_evento": "intento_pago",
            "version_esquema": "1.0",
            "equipo": EQUIPO,
            "fecha_evento": None,  # se asigna al publicar
            "id_pedido": id_pedido,
            "id_cliente": f"CLI-{id_cliente:06d}",
            "valor": self._valor_normal(),
            "moneda": "COP",
            "medio_pago": self.rng.choices(MEDIOS_PAGO, PESOS_MEDIOS)[0],
            "id_dispositivo": f"DIS-{(id_cliente * 7919) % 0xFFFFFF:06x}",
            "tipo_dispositivo": self.rng.choices(DISPOSITIVOS, PESOS_DISPOSITIVOS)[0],
            "pais_ip": "CO",
            "ciudad": ciudad,
            "latitud": round(lat + self.rng.uniform(-0.03, 0.03), 4),
            "longitud": round(lon + self.rng.uniform(-0.03, 0.03), 4),
            "numero_intento": 1,
            "estado": "aprobado",
            "motivo_rechazo": None,
        }

    def _intentos(self, base: dict, resultados: list[tuple[str, str | None]], separacion: tuple[float, float]):
        """Crea un intento por cada (estado, motivo) con retrasos crecientes."""
        intentos, retraso = [], 0.0
        for numero, (estado, motivo) in enumerate(resultados, start=1):
            evento = dict(base, id_evento=self._uuid(), numero_intento=numero, estado=estado, motivo_rechazo=motivo)
            intentos.append(Intento(retraso, evento))
            retraso += self.rng.uniform(*separacion)
        return intentos

    # ------------------------------------------------------------------ sesiones
    def nueva_sesion(self, momento: datetime | None = None) -> tuple[str, list[Intento]]:
        """Devuelve (tipo_sesion, intentos). tipo_sesion es 'normal' o uno de TIPOS_ANOMALIA."""
        momento = momento or datetime.now(timezone.utc)
        id_cliente = self.rng.randint(1, 5_000)
        base = self._base(momento, self._nuevo_pedido(momento), id_cliente)
        r = self.rng.random

        if r() < self.tasa_anomalias:
            tipo = self.rng.choice(TIPOS_ANOMALIA)
            if tipo == "prueba_tarjetas":
                base.update(tipo_dispositivo="web", medio_pago="tarjeta_credito",
                            pais_ip=self.rng.choice(PAISES_EXTRANJEROS) if r() < 0.7 else "CO",
                            valor=float(round(self.rng.uniform(30_000, 200_000), -2)))
                n = self.rng.randint(3, 6)
                motivos = ["tarjeta_invalida", "sospecha_fraude", "otp_incorrecto"]
                resultados = [("rechazado", self.rng.choice(motivos)) for _ in range(n)]
                return tipo, self._intentos(base, resultados, (2, 8))
            if tipo == "valor_alto_extranjero":
                base.update(pais_ip=self.rng.choice(PAISES_EXTRANJEROS),
                            valor=float(round(self.rng.uniform(600_000, 3_000_000), -3)))
                n = self.rng.randint(1, 3)
                resultados = [("rechazado", "sospecha_fraude")] * (n - 1)
                resultados.append(("aprobado", None) if r() < 0.5 else ("rechazado", "sospecha_fraude"))
                return tipo, self._intentos(base, resultados, (5, 20))
            # reintentos_banco
            n = self.rng.randint(3, 4)
            motivos = ["fondos_insuficientes", "tiempo_agotado_banco"]
            resultados = [("rechazado", self.rng.choice(motivos)) for _ in range(n)] + [("aprobado", None)]
            return tipo, self._intentos(base, resultados, (10, 30))

        # ---- pedido normal
        if r() < 0.03:   # pedido grande legítimo (solo S1)
            base["valor"] = float(round(self.rng.uniform(500_000, 900_000), -3))
        if r() < 0.02:   # cliente de viaje (solo S2)
            base["pais_ip"] = self.rng.choice(["US", "ES", "MX"])
        u = r()
        if u < 0.86:
            resultados = [("aprobado", None)]
        elif u < 0.96:   # un rechazo y luego aprobado
            resultados = [("rechazado", self.rng.choice(["fondos_insuficientes", "tiempo_agotado_banco"])), ("aprobado", None)]
        elif base["medio_pago"] == "pse":
            resultados = [("pendiente", None)]
        else:
            resultados = [("rechazado", "fondos_insuficientes")]
        return "normal", self._intentos(base, resultados, (15, 60))

    def debe_duplicar(self) -> bool:
        """Simula un reenvío de la aplicación: el mismo evento (mismo id_evento) llega dos veces."""
        return self.rng.random() < self.tasa_duplicados
