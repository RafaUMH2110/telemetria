"""
Consulta puntual (una sola lectura, síncrona) a la API Weather Underground /
Earth Network (PWS Observations v2) para una estación personal
(p. ej. Vevor YT60234 / Fine Offset).

Antecesor: telemetria_wu_v0.py

Variables de entorno:
    WU_STATION_ID   (obligatoria)  - Station ID de Wunderground (ej. IMADRID123)
    WU_API_KEY      (opcional)     - API Key de The Weather Company / IBM

Uso:
    WU_STATION_ID=IXXXX python wu_telemetria_consulta_simple.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Any, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wu_consulta_simple")

BASE_URL = "https://api.weather.com/v2/pws/observations/current"


def consultar_estacion(station_id: str, api_key: Optional[str] = None) -> Optional[dict[str, Any]]:
    """Realiza una única consulta a la API y devuelve la telemetría parseada (o None si falla)."""
    params = {
        "stationId": station_id,
        "format": "json",
        "numericPrecision": "decimal",  # decimal, no integer: conserva precisión físicamente relevante
        "units": "m",
    }
    headers = {"x-api-key": api_key} if api_key else {}

    start = time.perf_counter()
    try:
        response = requests.get(BASE_URL, params=params, headers=headers, timeout=10)
    except requests.exceptions.Timeout:
        log.error("Timeout al contactar la API de Weather Underground.")
        return None
    except requests.exceptions.RequestException as exc:
        log.error("Fallo de red: %s", exc)
        return None

    latency_ms = (time.perf_counter() - start) * 1000

    if response.status_code == 401:
        log.error("401 No autorizado. ¿Falta WU_API_KEY o es inválida?")
        return None
    if response.status_code == 404:
        log.error("404 Not Found. Verifica el Station ID o si hay un incidente en Earth Network.")
        return None
    if response.status_code != 200:
        log.error("HTTP %s inesperado.", response.status_code)
        return None

    try:
        data = response.json()
    except ValueError:
        log.error("La respuesta no es JSON válido (posible página de error HTML, p. ej. 'Brutus').")
        return None

    observations = data.get("observations") or []
    if not observations:
        log.info("Respuesta 200 OK pero sin observaciones (estación sin datos recientes).")
        return None

    obs = observations[0]
    # OJO: con numericPrecision=decimal/integer y units=m, temp/viento/presión van
    # anidados bajo la clave "metric", NO sueltos en obs. Este era el bug del
    # script original (preguntaba por obs["temperature"], que no existe).
    metric = obs.get("metric", {})

    return {
        "latencia_ms": round(latency_ms, 2),
        "temperatura_c": metric.get("temp"),
        "humedad_pct": obs.get("humidity"),
        "viento_kmh": metric.get("windSpeed"),
        "racha_kmh": metric.get("windGust"),
        "presion_hpa": metric.get("pressure"),
    }


def main() -> int:
    station_id = os.environ.get("WU_STATION_ID")
    if not station_id:
        log.error("Falta la variable de entorno WU_STATION_ID.")
        return 1
    api_key = os.environ.get("WU_API_KEY")  # opcional

    resultado = consultar_estacion(station_id, api_key)
    if resultado is None:
        return 1

    log.info("Latencia API: %.2f ms", resultado["latencia_ms"])
    log.info("Temperatura: %s ºC", resultado["temperatura_c"])
    log.info("Humedad: %s %%", resultado["humedad_pct"])
    log.info("Viento: %s km/h (racha %s km/h)", resultado["viento_kmh"], resultado["racha_kmh"])
    log.info("Presión: %s hPa", resultado["presion_hpa"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
