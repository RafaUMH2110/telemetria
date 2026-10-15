"""
Cliente síncrono reutilizable (WUPWSClient) para la API Weather Underground /
Earth Network (PWS Observations v2). Encapsula consulta, parseo y una
validación física mínima ("sanity check") de los valores recibidos.

Antecesor: telemtria_wu_v1.py

Variables de entorno:
    WU_STATION_ID   (obligatoria)
    WU_API_KEY      (opcional)

Uso:
    WU_STATION_ID=IXXXX python wu_telemetria_cliente_sincrono.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wu_cliente_sincrono")


class WUPWSClient:
    """Cliente de una estación personal (PWS) contra la API v2 de Weather Underground."""

    BASE_URL = "https://api.weather.com/v2/pws/observations/current"

    def __init__(self, station_id: str, api_key: Optional[str] = None, timeout: float = 5.0):
        self.station_id = station_id
        self.api_key = api_key
        self.timeout = timeout

    def _build_params(self) -> dict[str, str]:
        return {
            "stationId": self.station_id,
            "format": "json",
            "numericPrecision": "decimal",  # mejor precisión para física experimental
            "units": "m",
        }

    def get_observation(self) -> Optional[dict[str, Any]]:
        headers = {"x-api-key": self.api_key} if self.api_key else {}

        try:
            start = time.perf_counter()
            response = requests.get(
                self.BASE_URL, params=self._build_params(), headers=headers, timeout=self.timeout
            )
            latency_ms = (time.perf_counter() - start) * 1000
        except requests.exceptions.Timeout:
            log.error("Timeout (%.1fs) consultando la API.", self.timeout)
            return None
        except requests.exceptions.RequestException as exc:
            log.error("Fallo de red: %s", exc)
            return None

        if response.status_code == 401:
            log.error("401: acceso denegado. ¿Falta o caducó la API Key?")
            return None
        if response.status_code == 404:
            log.error("404: endpoint no encontrado. Verifica el Station ID.")
            return None
        if response.status_code != 200:
            log.error("HTTP %s inesperado.", response.status_code)
            return None

        try:
            data = response.json()
        except ValueError:
            log.error("Respuesta no es JSON válido (posible página de error de Earth Network).")
            return None

        observations = data.get("observations") or []
        if not observations:
            log.info("Sin observaciones recientes en el payload.")
            return None

        obs = observations[0]
        telemetry = self._parse_telemetry(obs, latency_ms)

        if not self.validate_observation(telemetry):
            log.warning("Lectura descartada por fallo de sanity check: %s", telemetry)
            return None

        return telemetry

    def _parse_telemetry(self, obs: dict[str, Any], latency_ms: float) -> dict[str, Any]:
        """Extrae telemetría relevante para sistemas de control."""
        epoch = obs.get("epoch")
        obs_time = datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch else None
        now = datetime.now(timezone.utc)
        staleness = (now - obs_time).total_seconds() if obs_time else float("inf")

        metric = obs.get("metric", {})
        return {
            "latency_api_ms": round(latency_ms, 2),
            "staleness_s": round(staleness, 1),
            "temp_c": metric.get("temp"),
            "humidity_pct": obs.get("humidity"),
            "pressure_hpa": metric.get("pressure"),
            "wind_kmh": metric.get("windSpeed"),
            "gust_kmh": metric.get("windGust"),
            "dewpoint_c": obs.get("dewpoint"),
            # NOTA: "solarradiation"/"uv" son los nombres habituales en payloads
            # de Fine Offset, pero no están confirmados contra una respuesta real
            # de tu estación. Si tu consola tiene sensor solar/UV, vuelca un
            # payload crudo (response.json()) una vez y ajusta estas dos claves
            # si no coinciden; si no, se quedarán siempre en None sin avisar.
            "solarradiation_wm2": obs.get("solarradiation"),
            "uv_index": obs.get("uv"),
            "rainrate_mmh": metric.get("precipRate"),
            "quality_code": obs.get("qualityControlFlag", "N/A"),
        }

    @staticmethod
    def validate_observation(telemetry: dict[str, Any]) -> bool:
        """Rechaza lecturas físicamente imposibles (sensor en mal estado, glitch de transmisión)."""
        temp = telemetry.get("temp_c")
        if temp is not None and not (-60 <= temp <= 60):
            return False
        hum = telemetry.get("humidity_pct")
        if hum is not None and not (0 <= hum <= 100):
            return False
        pres = telemetry.get("pressure_hpa")
        if pres is not None and not (850 <= pres <= 1085):
            return False
        return True


def main() -> int:
    station_id = os.environ.get("WU_STATION_ID")
    if not station_id:
        log.error("Falta la variable de entorno WU_STATION_ID.")
        return 1
    api_key = os.environ.get("WU_API_KEY")

    client = WUPWSClient(station_id=station_id, api_key=api_key)
    telemetry = client.get_observation()

    if telemetry is None:
        return 1

    log.info("--- TELEMETRÍA ESTACIÓN %s ---", station_id)
    for key, value in telemetry.items():
        log.info("%22s: %s", key, value)

    if telemetry["staleness_s"] > 120:
        log.warning("Estación stale (%.1fs > 120s). Revisa enlace WiFi/Brutus.", telemetry["staleness_s"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
