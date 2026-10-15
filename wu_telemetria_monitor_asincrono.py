"""
Monitor asíncrono de baja latencia para una estación personal en Weather
Underground / Earth Network. El productor (polling HTTP) publica cada lectura
válida en una asyncio.Queue; un consumidor independiente la procesa, de modo
que un callback lento (escritura a BD, etc.) nunca retrasa el siguiente ciclo
de lectura.

Antecesor: Monitor_de_tiempo_real_WU.py
Correcciones respecto al original:
  - El backoff exponencial ahora es real (antes se anunciaba pero el código
    usaba siempre el mismo retardo fijo).
  - Se distinguen errores fatales (401/404 -> no tiene sentido reintentar
    para siempre) de errores transitorios (timeout, 5xx).
  - stop_event ahora se espera de verdad (antes se marcaba pero nada lo leía).
  - print() sustituido por logging.

Variables de entorno:
    WU_STATION_ID     (obligatoria)
    WU_API_KEY        (opcional)
    WU_POLL_INTERVAL  (opcional, segundos; por defecto 60)

Uso:
    WU_STATION_ID=IXXXX python wu_telemetria_monitor_asincrono.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wu_monitor_asincrono")

BASE_URL = "https://api.weather.com/v2/pws/observations/current"


class ErrorFatal(Exception):
    """Error que no se recupera reintentando (credenciales inválidas, estación inexistente)."""


class WURealtimeMonitor:
    def __init__(
        self,
        station_id: str,
        api_key: Optional[str] = None,
        poll_interval: float = 60.0,
        max_backoff: float = 300.0,
    ):
        self.station_id = station_id
        self.api_key = api_key
        self.poll_interval = poll_interval
        self.max_backoff = max_backoff
        self.running = False
        self._base_delay = 2.0

    async def fetch_observation(self, session: aiohttp.ClientSession) -> Optional[dict[str, Any]]:
        params = {
            "stationId": self.station_id,
            "format": "json",
            "numericPrecision": "decimal",
            "units": "m",
        }
        headers = {"x-api-key": self.api_key} if self.api_key else {}

        async with session.get(BASE_URL, params=params, headers=headers, timeout=10) as response:
            if response.status == 401:
                raise ErrorFatal("401: API Key inválida o no autorizada.")
            if response.status == 404:
                raise ErrorFatal("404: Station ID no encontrado o endpoint obsoleto.")
            if response.status != 200:
                raise aiohttp.ClientResponseError(
                    request_info=response.request_info,
                    history=response.history,
                    status=response.status,
                    message=f"HTTP {response.status}",
                )
            try:
                # content_type=None: Earth Network a veces devuelve el JSON con
                # un Content-Type que aiohttp no reconoce; esto evita una
                # ContentTypeError espuria con un body que sí es JSON válido.
                data = await response.json(content_type=None)
            except Exception as exc:
                raise ValueError(f"Respuesta no es JSON válido: {exc}") from exc

        observations = data.get("observations") or []
        return observations[0] if observations else None

    def _parse_telemetry(self, obs: dict[str, Any], latency_ms: float) -> dict[str, Any]:
        epoch = obs.get("epoch")
        obs_time = datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch else None
        now = datetime.now(timezone.utc)
        staleness = (now - obs_time).total_seconds() if obs_time else float("inf")

        metric = obs.get("metric", {})
        return {
            "timestamp_utc": now.isoformat(),
            "latency_ms": round(latency_ms, 2),
            "staleness_s": round(staleness, 1),
            "temp_c": metric.get("temp"),
            "humidity_pct": obs.get("humidity"),
            "pressure_hpa": metric.get("pressure"),
            "wind_kmh": metric.get("windSpeed"),
            "gust_kmh": metric.get("windGust"),
            "dewpoint_c": obs.get("dewpoint"),
            "rainrate_mmh": metric.get("precipRate"),
            "quality_flag": obs.get("qualityControlFlag", "N/A"),
        }

    async def run(self, queue: "asyncio.Queue[dict[str, Any]]") -> None:
        """Bucle de adquisición. Solo encola; no procesa nada pesado aquí."""
        self.running = True
        retry_count = 0

        async with aiohttp.ClientSession() as session:
            while self.running:
                try:
                    start = time.perf_counter()
                    obs = await self.fetch_observation(session)
                    latency = (time.perf_counter() - start) * 1000
                    retry_count = 0  # éxito: resetea el backoff

                    if obs:
                        telemetry = self._parse_telemetry(obs, latency)
                        await queue.put(telemetry)
                        if telemetry["staleness_s"] > 120:
                            log.warning("Estación stale: %.1fs", telemetry["staleness_s"])
                    else:
                        log.info("Sin observaciones en el payload.")

                    await asyncio.sleep(self.poll_interval)

                except ErrorFatal as exc:
                    log.error("Error fatal, deteniendo el monitor: %s", exc)
                    self.running = False

                except Exception as exc:
                    retry_count += 1
                    delay = min(self._base_delay * (2 ** (retry_count - 1)), self.max_backoff)
                    log.warning(
                        "Error transitorio (%s). Reintento %d en %.1fs.", exc, retry_count, delay
                    )
                    await asyncio.sleep(delay)

    def stop(self) -> None:
        self.running = False


async def consumidor(queue: "asyncio.Queue[dict[str, Any]]") -> None:
    """Procesa cada lectura de forma independiente del bucle de adquisición."""
    while True:
        telemetry = await queue.get()
        log.info(
            "TELEMETRÍA | lat=%.1fms stale=%.1fs temp=%s°C hum=%s%% pres=%shPa",
            telemetry["latency_ms"],
            telemetry["staleness_s"],
            telemetry["temp_c"],
            telemetry["humidity_pct"],
            telemetry["pressure_hpa"],
        )
        queue.task_done()


async def main_async() -> None:
    station_id = os.environ.get("WU_STATION_ID")
    if not station_id:
        log.error("Falta la variable de entorno WU_STATION_ID.")
        sys.exit(1)
    api_key = os.environ.get("WU_API_KEY")
    poll_interval = float(os.environ.get("WU_POLL_INTERVAL", "60"))

    monitor = WURealtimeMonitor(station_id=station_id, api_key=api_key, poll_interval=poll_interval)
    queue: "asyncio.Queue[dict[str, Any]]" = asyncio.Queue()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        monitor.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    consumer_task = asyncio.create_task(consumidor(queue))
    monitor_task = asyncio.create_task(monitor.run(queue))

    await stop_event.wait()
    monitor.stop()
    await monitor_task
    consumer_task.cancel()
    log.info("Monitor detenido limpiamente.")


if __name__ == "__main__":
    asyncio.run(main_async())
