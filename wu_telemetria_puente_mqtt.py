"""
Puente HTTP -> MQTT: publica la telemetría de una estación Weather Underground
en un topic MQTT con retain=True, para que un suscriptor (app iOS, dashboard,
etc.) reciba de inmediato la última lectura conocida.

Antecesor: Crear_puente_WUMQTT.py
Correcciones respecto al original:
  - El original importaba "paho.mqtt.asyncio.AsyncClient", que no existe en
    paho-mqtt; se usa el cliente real "paho.mqtt.client.Client" (con hilo de
    red en background vía loop_start(), igual que ya hacía el resto del código).
  - Añade usuario/contraseña y TLS opcionales para el broker.
  - QoS configurable (por defecto 1) y comprobación del valor de retorno de
    publish() en vez de ignorarlo.
  - Mismo patrón de backoff exponencial real y distinción fatal/transitorio
    que el monitor asíncrono.

Requiere: aiohttp, paho-mqtt>=2.0

Variables de entorno:
    WU_STATION_ID      (obligatoria)
    WU_API_KEY         (opcional)
    WU_POLL_INTERVAL   (opcional, segundos; por defecto 60)
    MQTT_BROKER_HOST    (opcional; por defecto "localhost")
    MQTT_BROKER_PORT    (opcional; por defecto 1883)
    MQTT_TOPIC         (opcional; por defecto "weather/vevor/telemetry")
    MQTT_USER / MQTT_PASSWORD  (opcionales)
    MQTT_TLS           (opcional; "true"/"false", por defecto "false")
    MQTT_QOS           (opcional; por defecto 1)

Uso:
    WU_STATION_ID=IXXXX MQTT_BROKER_HOST=localhost python wu_telemetria_puente_mqtt.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
import paho.mqtt.client as mqtt

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("wu_puente_mqtt")

BASE_URL = "https://api.weather.com/v2/pws/observations/current"


class ErrorFatal(Exception):
    """Error que no se recupera reintentando (credenciales inválidas, estación inexistente)."""


class WUMQTTBridge:
    def __init__(
        self,
        station_id: str,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        topic: str = "weather/vevor/telemetry",
        api_key: Optional[str] = None,
        mqtt_user: Optional[str] = None,
        mqtt_password: Optional[str] = None,
        use_tls: bool = False,
        qos: int = 1,
        poll_interval: float = 60.0,
        max_backoff: float = 300.0,
    ):
        self.station_id = station_id
        self.api_key = api_key
        self.topic = topic
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.qos = qos
        self.poll_interval = poll_interval
        self.max_backoff = max_backoff
        self._base_delay = 2.0
        self.running = False
        self.last_data: Optional[dict[str, Any]] = None

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._mqtt_connected = asyncio.Event()

        self.mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"wu-bridge-{station_id}",
        )
        if mqtt_user:
            self.mqtt_client.username_pw_set(mqtt_user, mqtt_password)
        if use_tls:
            self.mqtt_client.tls_set()

        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log.info("Conectado al broker MQTT %s:%s", self.broker_host, self.broker_port)
            if self._loop:
                self._loop.call_soon_threadsafe(self._mqtt_connected.set)
        else:
            log.error("Fallo de conexión MQTT: %s", reason_code)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties=None):
        log.warning("Desconectado del broker MQTT (reason_code=%s).", reason_code)
        if self._loop:
            self._loop.call_soon_threadsafe(self._mqtt_connected.clear)

    async def fetch_wu_data(self, session: aiohttp.ClientSession) -> Optional[dict[str, Any]]:
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
                raise RuntimeError(f"HTTP {response.status} inesperado desde WU.")
            try:
                data = await response.json(content_type=None)
            except Exception as exc:
                raise ValueError(f"JSON inválido: {exc}") from exc

        observations = data.get("observations") or []
        return observations[0] if observations else None

    def _build_payload(self, obs: dict[str, Any], latency_ms: float) -> dict[str, Any]:
        metric = obs.get("metric", {})
        now = datetime.now(timezone.utc)
        epoch = obs.get("epoch")
        obs_time = datetime.fromtimestamp(epoch, tz=timezone.utc) if epoch else now
        staleness = (now - obs_time).total_seconds()

        return {
            "station": self.station_id,
            "ts_utc": now.isoformat(),
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

    def _publish(self, payload: dict[str, Any]) -> None:
        if not self._mqtt_connected.is_set():
            log.warning("MQTT no conectado; se omite esta publicación.")
            return
        info = self.mqtt_client.publish(self.topic, json.dumps(payload), qos=self.qos, retain=True)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error("Fallo al publicar en MQTT (rc=%s).", info.rc)
        else:
            log.info("Publicado en %s: %s", self.topic, payload)

    async def run(self) -> None:
        self.running = True
        self._loop = asyncio.get_running_loop()

        try:
            self.mqtt_client.connect_async(self.broker_host, self.broker_port, keepalive=60)
            self.mqtt_client.loop_start()
        except Exception as exc:
            log.error("Error inicial de conexión MQTT: %s", exc)
            return

        retry_count = 0
        async with aiohttp.ClientSession() as session:
            try:
                while self.running:
                    try:
                        start = time.perf_counter()
                        obs = await self.fetch_wu_data(session)
                        latency = (time.perf_counter() - start) * 1000
                        retry_count = 0

                        if obs:
                            payload = self._build_payload(obs, latency)
                            self.last_data = payload
                            self._publish(payload)
                        else:
                            log.info("Sin datos de WU en este ciclo.")

                        await asyncio.sleep(self.poll_interval)

                    except ErrorFatal as exc:
                        log.error("Error fatal, deteniendo el puente: %s", exc)
                        self.running = False

                    except Exception as exc:
                        retry_count += 1
                        delay = min(self._base_delay * (2 ** (retry_count - 1)), self.max_backoff)
                        log.warning(
                            "Error transitorio (%s). Reintento %d en %.1fs.", exc, retry_count, delay
                        )
                        await asyncio.sleep(delay)
            finally:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()

    def stop(self) -> None:
        self.running = False


async def main_async() -> None:
    station_id = os.environ.get("WU_STATION_ID")
    if not station_id:
        log.error("Falta la variable de entorno WU_STATION_ID.")
        sys.exit(1)

    bridge = WUMQTTBridge(
        station_id=station_id,
        broker_host=os.environ.get("MQTT_BROKER_HOST", "localhost"),
        broker_port=int(os.environ.get("MQTT_BROKER_PORT", "1883")),
        topic=os.environ.get("MQTT_TOPIC", "weather/vevor/telemetry"),
        api_key=os.environ.get("WU_API_KEY"),
        mqtt_user=os.environ.get("MQTT_USER"),
        mqtt_password=os.environ.get("MQTT_PASSWORD"),
        use_tls=os.environ.get("MQTT_TLS", "false").lower() == "true",
        qos=int(os.environ.get("MQTT_QOS", "1")),
        poll_interval=float(os.environ.get("WU_POLL_INTERVAL", "60")),
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        bridge.stop()
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    run_task = asyncio.create_task(bridge.run())
    await stop_event.wait()
    await run_task
    log.info("Puente MQTT detenido limpiamente.")


if __name__ == "__main__":
    asyncio.run(main_async())
