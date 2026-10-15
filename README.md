# Telemetría — Estación meteorológica Vevor YT60234 (Weather Underground / Earth Network)

Conjunto de scripts en Python para leer la telemetría de una estación meteorológica
personal (PWS) dada de alta en [Weather Underground](https://www.wunderground.com)
mediante la API pública de **Earth Network** (`api.weather.com/v2/pws/observations/current`),
con distintos niveles de robustez y dos interfaces gráficas para visualizar las
lecturas en tiempo real.

El proyecto nació a raíz de un error `404 "Brutus Servers"` al consultar una
estación **Vevor YT60234** (internamente, un Fine Offset WS-2000 con consola
WH2600/WH2900) desde la nube de Wunderground/Earth Network, y fue creciendo en
iteraciones sucesivas: de una consulta puntual por línea de comandos hasta un
monitor asíncrono con reconexión y un puente que republica los datos por MQTT.

## Índice

- [Arquitectura y evolución](#arquitectura-y-evolución)
- [Ficheros del repositorio](#ficheros-del-repositorio)
- [Requisitos](#requisitos)
- [Variables de entorno](#variables-de-entorno)
- [Uso](#uso)
  - [1. Consulta puntual](#1-wu_telemetria_consulta_simplepy--consulta-puntual)
  - [2. Cliente síncrono reutilizable](#2-wu_telemetria_cliente_sincronopy--cliente-síncrono-reutilizable)
  - [3. Monitor asíncrono continuo](#3-wu_telemetria_monitor_asincronopy--monitor-asíncrono-continuo)
  - [4. Puente HTTP → MQTT](#4-wu_telemetria_puente_mqttpy--puente-http--mqtt)
  - [5 y 6. Ventanas gráficas (Tkinter)](#5-y-6-ventanas-gráficas-tkinter)
- [Formato de los datos](#formato-de-los-datos)
- [Manejo de errores y backoff](#manejo-de-errores-y-backoff)
- [Limitaciones conocidas / pendientes de verificar](#limitaciones-conocidas--pendientes-de-verificar)
- [Histórico de correcciones respecto a las primeras versiones](#histórico-de-correcciones-respecto-a-las-primeras-versiones)
- [Licencia](#licencia)

## Arquitectura y evolución

Los seis ficheros representan capas sucesivas sobre la misma API, cada una
pensada para un caso de uso distinto. No hay dependencias entre los cuatro
scripts base entre sí (cada uno es autocontenido y ejecutable por separado);
las dos ventanas gráficas sí importan, respectivamente, al cliente síncrono y
al monitor asíncrono, en vez de duplicar su lógica:

```
wu_telemetria_consulta_simple.py        (síncrono, una sola lectura, por línea de comandos)
wu_telemetria_cliente_sincrono.py        (síncrono, reutilizable, con sanity checks)
        └── wu_telemetria_cliente_sincrono_gui.py    (ventana Tkinter sobre el cliente anterior)

wu_telemetria_monitor_asincrono.py       (asíncrono, continuo, con backoff exponencial)
        └── wu_telemetria_monitor_asincrono_gui.py   (ventana Tkinter sobre el monitor anterior)

wu_telemetria_puente_mqtt.py             (asíncrono, continuo, republica cada lectura por MQTT)
```

Elige el fichero según lo que necesites:

| Necesidad | Fichero recomendado |
|---|---|
| Una lectura rápida para comprobar que la estación responde | `wu_telemetria_consulta_simple.py` |
| Integrar la lectura en otro programa Python (síncrono) | `wu_telemetria_cliente_sincrono.py` (clase `WUPWSClient`) |
| Un daemon de monitorización continua en segundo plano | `wu_telemetria_monitor_asincrono.py` |
| Publicar los datos para que otro sistema (p. ej. una app iOS) los consuma en tiempo real | `wu_telemetria_puente_mqtt.py` |
| Ver los datos en una ventana mientras trabajas, sin terminal | cualquiera de las dos `_gui.py` |

## Ficheros del repositorio

| Fichero | Lenguaje/deps extra | Qué hace |
|---|---|---|
| `wu_telemetria_consulta_simple.py` | `requests` | Una única consulta síncrona a la API, imprime los valores por consola y termina. Pensado para comprobar rápidamente que la estación responde. |
| `wu_telemetria_cliente_sincrono.py` | `requests` | Clase `WUPWSClient` reutilizable: encapsula la consulta, el parseo de la respuesta y una validación física mínima (*sanity check*) de los valores recibidos (rechaza lecturas imposibles, p. ej. una temperatura de 200 °C). |
| `wu_telemetria_monitor_asincrono.py` | `aiohttp` | Monitor `asyncio` de bajo acoplamiento: un productor sondea la API y encola cada lectura en una `asyncio.Queue`; un consumidor independiente la procesa, de modo que un callback lento (p. ej. escribir a una base de datos) nunca retrasa el siguiente ciclo de sondeo. Incluye backoff exponencial real con tope y distingue errores fatales (401/404, credenciales inválidas) de errores transitorios (timeout, 5xx). |
| `wu_telemetria_puente_mqtt.py` | `aiohttp`, `paho-mqtt>=2.0` | Igual que el monitor anterior, pero en vez de solo loggear cada lectura la publica en un topic MQTT (por defecto `weather/vevor/telemetry`) con `retain=True`, para que cualquier suscriptor (una app iOS, un dashboard, otro proceso) reciba al instante la última lectura conocida al conectarse. Soporta usuario/contraseña y TLS opcionales para el broker. |
| `wu_telemetria_cliente_sincrono_gui.py` | `requests`, `tkinter` | Ventana gráfica que envuelve `WUPWSClient` (del fichero anterior, debe estar en la misma carpeta): sondea en un hilo en segundo plano y muestra los valores actuales, un indicador de estado y un histórico de lecturas con scroll. |
| `wu_telemetria_monitor_asincrono_gui.py` | `aiohttp`, `tkinter` | Ventana gráfica que envuelve `WURealtimeMonitor` (del fichero anterior, debe estar en la misma carpeta): el bucle `asyncio` original corre íntegro en un hilo aparte y reenvía cada lectura a la interfaz mediante una cola segura entre hilos. Detecta y muestra cuando el monitor se detiene solo por un error fatal. |

## Requisitos

- Python 3.9 o superior (se usan anotaciones `dict[str, Any]` con
  `from __future__ import annotations`, y `asyncio.to_thread` en el monitor con GUI).
- Dependencias de terceros, instalables con `pip`:

  ```bash
  pip install requests aiohttp "paho-mqtt>=2.0"
  ```

  (`requests` solo la necesitan los dos scripts síncronos y su GUI;
  `aiohttp` solo el monitor asíncrono, el puente MQTT y la GUI del monitor;
  `paho-mqtt` solo el puente MQTT.)

- Para las dos ventanas gráficas, el módulo `tkinter` de la librería estándar.
  En muchas distribuciones Linux **no viene incluido con el intérprete** y hay
  que instalarlo aparte del sistema operativo:

  ```bash
  sudo apt install python3-tk        # Debian/Ubuntu
  sudo dnf install python3-tkinter   # Fedora
  ```

  Si tienes varias versiones de Python instaladas, asegúrate de instalar el
  paquete `tk` correspondiente a la versión concreta que vayas a usar para
  ejecutar estos scripts (un `python3-tk` genérico puede quedar emparejado con
  una versión de Python distinta a la que resuelve `python3` en tu `PATH`).

## Variables de entorno

Todos los scripts se configuran por variables de entorno (ninguno pide datos
por `input()` ni lleva credenciales de ejemplo hardcodeadas en el código):

| Variable | Obligatoria | Scripts que la usan | Descripción |
|---|---|---|---|
| `WU_STATION_ID` | **Sí** | todos | Identificador de tu estación en Wunderground (formato típico `IXXXXXXX`). |
| `WU_API_KEY` | No | todos | API Key de The Weather Company / IBM, si tu cuenta la exige. Sin ella, la consulta se hace en modo anónimo. |
| `WU_POLL_INTERVAL` | No (por defecto `60`) | `..._monitor_asincrono*.py`, `..._puente_mqtt.py` | Segundos entre cada sondeo a la API. |
| `MQTT_BROKER_HOST` | No (por defecto `localhost`) | `wu_telemetria_puente_mqtt.py` | Host del broker MQTT. |
| `MQTT_BROKER_PORT` | No (por defecto `1883`) | `wu_telemetria_puente_mqtt.py` | Puerto del broker MQTT. |
| `MQTT_TOPIC` | No (por defecto `weather/vevor/telemetry`) | `wu_telemetria_puente_mqtt.py` | Topic donde se publica cada lectura. |
| `MQTT_USER` / `MQTT_PASSWORD` | No | `wu_telemetria_puente_mqtt.py` | Credenciales del broker, si las requiere. |
| `MQTT_TLS` | No (por defecto `false`) | `wu_telemetria_puente_mqtt.py` | `"true"` para activar TLS en la conexión al broker. |
| `MQTT_QOS` | No (por defecto `1`) | `wu_telemetria_puente_mqtt.py` | Calidad de servicio MQTT de la publicación. |

## Uso

### 1. `wu_telemetria_consulta_simple.py` — consulta puntual

```bash
WU_STATION_ID=IXXXXXXX python wu_telemetria_consulta_simple.py
```

Hace una única petición, imprime temperatura, humedad, viento, racha y
presión por consola (vía `logging`), y termina con código de salida `0` si
obtuvo una lectura válida, o `1` si falló (estación offline, credenciales
inválidas, timeout, JSON inválido, etc. — cada caso queda registrado en el
log con el motivo).

### 2. `wu_telemetria_cliente_sincrono.py` — cliente síncrono reutilizable

Como script:

```bash
WU_STATION_ID=IXXXXXXX python wu_telemetria_cliente_sincrono.py
```

Como librería en otro programa:

```python
from wu_telemetria_cliente_sincrono import WUPWSClient

client = WUPWSClient(station_id="IXXXXXXX", api_key=None)  # api_key opcional
telemetria = client.get_observation()  # dict o None si falla/lectura inválida

if telemetria:
    print(telemetria["temp_c"], telemetria["humidity_pct"])
```

`get_observation()` ya aplica una validación física de rango (temperatura
entre -60 y 60 °C, humedad entre 0 y 100 %, presión entre 850 y 1085 hPa) y
descarta silenciosamente (con un aviso en el log) cualquier lectura que la
incumpla, antes de devolverla.

### 3. `wu_telemetria_monitor_asincrono.py` — monitor asíncrono continuo

```bash
WU_STATION_ID=IXXXXXXX WU_POLL_INTERVAL=60 python wu_telemetria_monitor_asincrono.py
```

Corre indefinidamente hasta `Ctrl+C` (o `SIGTERM`), sondeando cada
`WU_POLL_INTERVAL` segundos. Cada lectura se encola y un consumidor aparte la
loggea; puedes sustituir ese consumidor por tu propia lógica (escritura a
base de datos, alertas, etc.) reutilizando la clase `WURealtimeMonitor` igual
que en el ejemplo anterior.

### 4. `wu_telemetria_puente_mqtt.py` — puente HTTP → MQTT

```bash
WU_STATION_ID=IXXXXXXX \
MQTT_BROKER_HOST=localhost MQTT_BROKER_PORT=1883 \
python wu_telemetria_puente_mqtt.py
```

Cada lectura válida se publica en `MQTT_TOPIC` (por defecto
`weather/vevor/telemetry`) como un JSON con `retain=True`, de forma que
cualquier cliente MQTT que se suscriba después recibe inmediatamente la
última lectura conocida sin esperar al siguiente ciclo. Ejemplo de suscriptor
de prueba con `mosquitto_sub`:

```bash
mosquitto_sub -h localhost -p 1883 -t weather/vevor/telemetry -v
```

### 5 y 6. Ventanas gráficas (Tkinter)

```bash
WU_STATION_ID=IXXXXXXX python wu_telemetria_cliente_sincrono_gui.py
WU_STATION_ID=IXXXXXXX WU_POLL_INTERVAL=30 python wu_telemetria_monitor_asincrono_gui.py
```

Ambas deben ejecutarse desde la misma carpeta que su script base
correspondiente (lo importan por nombre de módulo). La ventana muestra:

- Los valores actuales (temperatura, humedad, presión, viento, racha, punto
  de rocío, lluvia, calidad de la lectura, y en la versión del cliente
  síncrono también radiación solar e índice UV si tu estación los reporta).
- Un indicador de estado con color: **verde** ("En línea") si la lectura
  tiene menos de 120 segundos de antigüedad, **naranja** ("Stale") si es más
  vieja, **rojo** si hubo un error en el último sondeo o si el monitor se
  detuvo solo por un fallo irrecuperable (credenciales inválidas, estación no
  encontrada).
- La latencia de la última petición HTTP.
- Un histórico con scroll de las últimas lecturas, con marca de hora.

El sondeo de red corre siempre en un hilo en segundo plano (en el caso del
monitor asíncrono, el propio bucle `asyncio` original, sin modificar) y se
comunica con la interfaz mediante una `queue.Queue` segura entre hilos —
Tkinter nunca se toca desde otro hilo distinto al principal.

## Formato de los datos

Todas las funciones de parseo devuelven un diccionario con, al menos, estas
claves (algunas pueden venir a `None` si tu estación no reporta ese sensor):

| Clave | Unidad | Descripción |
|---|---|---|
| `temp_c` | °C | Temperatura exterior |
| `humidity_pct` | % | Humedad relativa |
| `pressure_hpa` | hPa | Presión (reducida al nivel del mar, según reporte la estación) |
| `wind_kmh` | km/h | Velocidad del viento |
| `gust_kmh` | km/h | Racha de viento |
| `dewpoint_c` | °C | Punto de rocío |
| `rainrate_mmh` | mm/h | Intensidad de lluvia |
| `quality_code` / `quality_flag` | — | Indicador de calidad que a veces incluye la API (nombre de la clave según el script) |
| `staleness_s` | s | Antigüedad de la lectura: segundos transcurridos desde el `epoch` que reporta la estación hasta el momento de la consulta |
| `latency_api_ms` / `latency_ms` | ms | Tiempo que tardó la petición HTTP en responder (nombre de la clave según el script) |

`staleness_s > 120` se trata en todos los scripts como indicador de que la
estación puede no estar subiendo datos con normalidad, aunque el endpoint
responda `200 OK`.

**Nota importante sobre el campo correcto de la API**: con
`numericPrecision=decimal` y `units=m`, la temperatura, el viento, la racha y
la presión vienen **anidados bajo la clave `"metric"`** dentro de cada
observación, no sueltos en la raíz del objeto — es un detalle fácil de pasar
por alto y fue, de hecho, un bug real en la primera versión de este proyecto
(ver más abajo).

## Manejo de errores y backoff

`wu_telemetria_monitor_asincrono.py` y `wu_telemetria_puente_mqtt.py`
distinguen dos clases de fallo:

- **Fatales** (HTTP 401 — credenciales inválidas, o 404 — Station ID no
  encontrado o endpoint obsoleto): se asume que reintentar no va a arreglar
  nada, así que el proceso se detiene y lo deja constar en el log en vez de
  insistir indefinidamente.
- **Transitorios** (timeout, error de red, 5xx, JSON inválido): se reintenta
  con backoff exponencial real (el retardo se dobla en cada fallo consecutivo
  hasta un tope configurado internamente), reseteando el contador en cuanto
  una petición vuelve a tener éxito.

Los dos scripts síncronos (`consulta_simple` y `cliente_sincrono`) no llevan
backoff porque no mantienen un bucle propio: cada llamada es una consulta
puntual que devuelve `None` y registra el motivo en el log si falla, dejando
la decisión de reintentar a quien los invoque.

## Limitaciones conocidas / pendientes de verificar

- Los campos `solarradiation_wm2` y `uv_index` en
  `wu_telemetria_cliente_sincrono.py` usan los nombres de clave (`solarradiation`,
  `uv`) habituales en estaciones Fine Offset, pero **no están confirmados
  contra un payload real** de la Vevor YT60234. Si tu estación tiene sensor
  solar/UV y estos campos aparecen siempre a `None`, vuelca una respuesta
  cruda (`print(response.json())`) y ajusta esas dos claves si no coinciden.
- No está verificado si tu cuenta concreta de Weather Underground exige
  obligatoriamente una `WU_API_KEY` para el endpoint `v2/pws/observations/current`,
  o si el acceso anónimo sigue funcionando (todos los scripts la tratan como
  opcional).
- Los scripts no incluyen reintento con *fallback* automático de credenciales
  (anónimo → con API Key); si tu cuenta la requiere, exporta `WU_API_KEY`
  desde el principio.

## Histórico de correcciones respecto a las primeras versiones

Este repositorio es la versión corregida de un primer borrador que tenía
varios problemas reales, detectados y solucionados durante su desarrollo:

- **Bug de parseo de campos**: la primera versión (`telemetria_wu_v0.py`,
  no incluida en este repo) leía `obs["temperature"]`, `obs["relativeHumidity"]`
  y `obs["windSpeed"]` directamente de la raíz del objeto, claves que no
  existen en la respuesta real de la API — siempre devolvía `N/A` sin
  lanzar ningún error. Los campos correctos van anidados bajo `obs["metric"]`.
- **Import de MQTT inexistente**: una versión anterior del puente usaba
  `paho.mqtt.asyncio.AsyncClient`, una clase que no existe en `paho-mqtt`.
  Se sustituyó por el cliente real (`paho.mqtt.client.Client`) con la API de
  callbacks `CallbackAPIVersion.VERSION2` de `paho-mqtt>=2.0`.
- **Backoff exponencial que no era tal**: una versión anterior del monitor
  declaraba `max_retries` y `base_delay` pero siempre esperaba el mismo
  retardo fijo tras cualquier error. El backoff actual dobla el retardo en
  cada fallo consecutivo hasta un tope, y lo resetea en el primer éxito.
- **`stop_event` que nunca se consultaba**: una versión anterior marcaba un
  `asyncio.Event()` al recibir `SIGINT`/`SIGTERM` pero ningún `await` lo
  esperaba realmente, así que el apagado dependía solo de un flag interno.
- Ningún script usaba `timeout` en sus peticiones HTTP ni distinguía errores
  de red de errores de parseo JSON, lo que podía dejar el proceso colgado
  indefinidamente o reventar con una excepción sin capturar ante una
  respuesta HTML de error (p. ej. la página de error "Brutus Servers" de
  Earth Network).

Todas las correcciones anteriores fueron verificadas con pruebas
funcionales reales antes de subirse a este repositorio: un servidor HTTP
local simulando distintas respuestas de la API (200 con datos, 401, 404,
JSON inválido), un broker MQTT real (Mosquitto) para el puente, y una
pantalla virtual (Xvfb) para las dos ventanas gráficas.

## Licencia

Sin licencia explícita — uso personal del autor para su propia estación
meteorológica.
