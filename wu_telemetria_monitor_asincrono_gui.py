"""
Ventana gráfica (Tkinter) que muestra en tiempo real la telemetría publicada
por WURealtimeMonitor (wu_telemetria_monitor_asincrono.py). El bucle asyncio
original corre íntegro en un hilo en segundo plano; cada lectura que su
consumidor recibe se reenvía a una queue.Queue segura entre hilos, que la
interfaz sondea periódicamente — así Tkinter (que no es thread-safe) nunca
se toca desde el hilo asyncio.

Requiere: aiohttp, tkinter.
  En Linux, si "import tkinter" falla, instala el paquete del sistema
  correspondiente a tu versión de Python (p. ej. "sudo apt install python3-tk").

Este fichero importa WURealtimeMonitor desde wu_telemetria_monitor_asincrono.py,
que debe estar en el mismo directorio (o en PYTHONPATH).

Variables de entorno:
    WU_STATION_ID     (obligatoria)
    WU_API_KEY        (opcional)
    WU_POLL_INTERVAL  (opcional, segundos; por defecto 60)

Uso:
    WU_STATION_ID=IXXXX python wu_telemetria_monitor_asincrono_gui.py
"""

from __future__ import annotations

import asyncio
import os
import queue
import sys
import threading
import tkinter as tk
from datetime import datetime
from tkinter import messagebox, ttk
from typing import Any, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from wu_telemetria_monitor_asincrono import WURealtimeMonitor  # noqa: E402

FIELDS = [
    ("temp_c", "Temperatura", "°C"),
    ("humidity_pct", "Humedad", "%"),
    ("pressure_hpa", "Presión", "hPa"),
    ("wind_kmh", "Viento", "km/h"),
    ("gust_kmh", "Racha", "km/h"),
    ("dewpoint_c", "Punto de rocío", "°C"),
    ("rainrate_mmh", "Lluvia", "mm/h"),
    ("quality_flag", "Calidad", ""),
]

STALE_THRESHOLD_S = 120.0


class TelemetriaGUI:
    def __init__(self, root: tk.Tk, station_id: str, api_key: Optional[str], poll_interval: float):
        self.root = root
        self.station_id = station_id
        self.api_key = api_key
        self.poll_interval = poll_interval

        self.gui_queue: "queue.Queue[dict[str, Any]]" = queue.Queue()
        self.stop_event = threading.Event()

        self._build_widgets()

        self.worker = threading.Thread(target=self._run_async_loop, daemon=True)
        self.worker.start()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(200, self._process_queue)

    def _build_widgets(self) -> None:
        self.root.title(f"Telemetría WU — {self.station_id} (monitor asíncrono)")
        self.root.geometry("520x520")

        header = ttk.Frame(self.root, padding=10)
        header.pack(fill="x")
        ttk.Label(header, text=f"Estación: {self.station_id}", font=("Helvetica", 12, "bold")).pack(side="left")
        self.status_var = tk.StringVar(value="Iniciando…")
        self.status_label = ttk.Label(header, textvariable=self.status_var, foreground="gray")
        self.status_label.pack(side="right")

        grid = ttk.Frame(self.root, padding=10)
        grid.pack(fill="x")
        self.value_vars: dict[str, tk.StringVar] = {}
        for row, (key, label, unit) in enumerate(FIELDS):
            ttk.Label(grid, text=f"{label}:").grid(row=row, column=0, sticky="w", pady=2)
            var = tk.StringVar(value="—")
            self.value_vars[key] = var
            ttk.Label(grid, textvariable=var, font=("Helvetica", 11, "bold")).grid(
                row=row, column=1, sticky="w", padx=10
            )
            ttk.Label(grid, text=unit, foreground="gray").grid(row=row, column=2, sticky="w")

        meta = ttk.Frame(self.root, padding=(10, 0))
        meta.pack(fill="x")
        self.latency_var = tk.StringVar(value="Latencia: —")
        self.staleness_var = tk.StringVar(value="Frescura: —")
        ttk.Label(meta, textvariable=self.latency_var).pack(side="left")
        ttk.Label(meta, textvariable=self.staleness_var).pack(side="right")

        ttk.Separator(self.root).pack(fill="x", pady=6)
        ttk.Label(self.root, text="Histórico de lecturas", padding=(10, 0)).pack(anchor="w")

        log_frame = ttk.Frame(self.root, padding=10)
        log_frame.pack(fill="both", expand=True)
        self.log_text = tk.Text(log_frame, height=10, state="disabled", wrap="none")
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        self.log_text.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

    def _run_async_loop(self) -> None:
        """Corre en un hilo aparte: lanza WURealtimeMonitor.run() tal cual
        (su propio backoff exponencial y manejo de errores fatales/transitorios
        no se tocan) y reenvía cada lectura de su cola interna a self.gui_queue
        (thread-safe) para que la interfaz la recoja."""

        async def consumidor_gui(async_queue: "asyncio.Queue[dict[str, Any]]") -> None:
            while True:
                telemetry = await async_queue.get()
                self.gui_queue.put(telemetry)
                async_queue.task_done()

        async def runner() -> None:
            monitor = WURealtimeMonitor(
                station_id=self.station_id, api_key=self.api_key, poll_interval=self.poll_interval
            )
            async_queue: "asyncio.Queue[dict[str, Any]]" = asyncio.Queue()
            consumer_task = asyncio.create_task(consumidor_gui(async_queue))
            monitor_task = asyncio.create_task(monitor.run(async_queue))

            # Espera a lo que ocurra primero: que la ventana se cierre
            # (stop_event, señalado desde el hilo de Tkinter) o que el propio
            # monitor termine solo (p. ej. tras un error fatal 401/404). Usar
            # asyncio.wait en vez de comprobar monitor.running evita una
            # condición de carrera: monitor.run() aún no ha tenido ocasión de
            # ejecutar su primera línea (self.running = True) hasta que este
            # runner cede el control con un await.
            stop_waiter = asyncio.create_task(asyncio.to_thread(self.stop_event.wait))
            done, _ = await asyncio.wait({monitor_task, stop_waiter}, return_when=asyncio.FIRST_COMPLETED)

            if monitor_task not in done:
                monitor.stop()
                await monitor_task
            if stop_waiter not in done:
                stop_waiter.cancel()

            consumer_task.cancel()
            await asyncio.gather(consumer_task, return_exceptions=True)

            # Avisa a la interfaz de que ya no llegarán más lecturas (bien
            # porque el usuario cerró la ventana, bien por un error fatal).
            self.gui_queue.put({"_event": "stopped"})

        asyncio.run(runner())

    def _process_queue(self) -> None:
        try:
            while True:
                item = self.gui_queue.get_nowait()
                if item.get("_event") == "stopped":
                    self.status_var.set("Monitor detenido")
                    self.status_label.configure(foreground="red")
                else:
                    self._update_values(item)
        except queue.Empty:
            pass
        if not self.stop_event.is_set():
            self.root.after(200, self._process_queue)

    def _update_values(self, telemetry: dict[str, Any]) -> None:
        for key, _, _ in FIELDS:
            value = telemetry.get(key)
            self.value_vars[key].set("—" if value is None else str(value))

        self.latency_var.set(f"Latencia: {telemetry.get('latency_ms', '—')} ms")

        staleness = telemetry.get("staleness_s")
        if staleness is None:
            self.staleness_var.set("Frescura: —")
        else:
            self.staleness_var.set(f"Frescura: {staleness:.1f} s")

        if staleness is not None and staleness <= STALE_THRESHOLD_S:
            self.status_var.set("En línea")
            self.status_label.configure(foreground="green")
        else:
            self.status_var.set(f"Stale (>{STALE_THRESHOLD_S:.0f}s)")
            self.status_label.configure(foreground="orange")

        timestamp = datetime.now().strftime("%H:%M:%S")
        line = (
            f"{timestamp}  "
            f"T={telemetry.get('temp_c')}°C  "
            f"H={telemetry.get('humidity_pct')}%  "
            f"P={telemetry.get('pressure_hpa')}hPa  "
            f"V={telemetry.get('wind_kmh')}km/h  "
            f"stale={staleness}\n"
        )
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line)
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _on_close(self) -> None:
        self.stop_event.set()
        self.root.after(300, self.root.destroy)


def main() -> int:
    station_id = os.environ.get("WU_STATION_ID")
    if not station_id:
        print("Falta la variable de entorno WU_STATION_ID.", file=sys.stderr)
        return 1
    api_key = os.environ.get("WU_API_KEY")
    poll_interval = float(os.environ.get("WU_POLL_INTERVAL", "60"))

    root = tk.Tk()
    try:
        TelemetriaGUI(root, station_id=station_id, api_key=api_key, poll_interval=poll_interval)
    except Exception as exc:  # pragma: no cover - solo para mostrar el error en una ventana
        messagebox.showerror("Error", str(exc))
        return 1
    root.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
