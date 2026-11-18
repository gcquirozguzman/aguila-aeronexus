"""Ingesta de aviones y clima (METAR) hacia Pub/Sub. Corre en la nube como Cloud Run Job, en contenedor.

Cloud Scheduler la dispara cada 2 min (aviones) y cada 5 min (clima):
  - aviones: prueba las fuentes de AVIONES_FUENTES en orden y usa la primera que responde:
      adsblol  adsb.lol, red abierta de receptores ADS-B (licencia ODbL). Responde desde la nube.
      opensky  OpenSky Network. Bloquea las IP de los grandes proveedores de nube; sirve desde una PC.
    Cada consulta en vivo se guarda en bronce (grabaciones/<fuente>/). Si ninguna fuente responde,
    se reproduce la grabación real más reciente (modo "reproduccion").
  - metar: consulta aviationweather.gov y guarda la respuesta cruda en bronce (metar/).

El colector de la PC (python ingesta/colector_pc.py) es opcional: solo agrega OpenSky en vivo.
Cada ejecución publica UN mensaje con el lote completo (más barato que un mensaje por avión).

Variables de entorno:
    FUENTE              aviones | metar   ("opensky" se acepta como sinónimo de aviones)
    AVIONES_FUENTES     orden de las fuentes de aviones (por defecto "adsblol,opensky")
    PROYECTO            aguila-aeronexus
    OPENSKY_CREDENCIAL  JSON {"clientId", "clientSecret"} (en la nube se monta desde Secret Manager)
"""
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

import requests
from google.cloud import pubsub_v1, storage

PROYECTO = os.environ.get("PROYECTO", "aguila-aeronexus")
BUCKET = "aguila-aeronexus-bronce"
GRABACIONES = "grabaciones/opensky/"
METAR_CRUDO = "metar/"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
ESTADOS_URL = "https://opensky-network.org/api/states/all"
ADSBLOL_URL = "https://api.adsb.lol/v2/lat/{lat}/lon/{lon}/dist/250"  # radio máximo: 250 millas náuticas
# centros de consulta: cubren los 18 aeropuertos de AeroNexus con círculos de 250 mn
CENTROS = [(33.64, -84.43), (28.43, -81.31), (41.98, -87.90), (44.88, -93.22), (32.90, -97.04), (39.86, -104.67),
           (33.94, -118.41), (33.43, -112.01), (37.62, -122.38), (47.45, -122.31), (40.64, -73.78)]
PIE_A_M, NUDO_A_MS, PIE_MIN_A_MS = 0.3048, 0.514444, 0.00508
METAR_URL = "https://aviationweather.gov/api/data/metar"
EEUU = {"lamin": 24.4, "lomin": -125.0, "lamax": 49.4, "lomax": -66.9}  # territorio continental
AEROPUERTOS = ["KATL", "KORD", "KDFW", "KDEN", "KLAX", "KJFK", "KSFO", "KLAS", "KSEA", "KCLT", "KMCO", "KPHX",
               "KMIA", "KIAH", "KEWR", "KMSP", "KDTW", "KBOS"]
CAMPOS = ["icao24", "callsign", "pais_origen", "ts_posicion", "ts_contacto", "longitud", "latitud", "altitud_baro_m",
          "en_tierra", "velocidad_ms", "rumbo", "velocidad_vertical_ms", "sensores", "altitud_geo_m", "squawk",
          "spi", "fuente_posicion"]
INTENTOS = 4          # 1 intento + 3 reintentos
ESPERA_MAXIMA = 60    # segundos; si la API pide más, se usa el respaldo en lugar de esperar


def ahora():
    return int(datetime.now(timezone.utc).timestamp())


def credencial():
    if "OPENSKY_CREDENCIAL" in os.environ:
        return json.loads(os.environ["OPENSKY_CREDENCIAL"])
    from google.cloud import secretmanager
    nombre = f"projects/{PROYECTO}/secrets/opensky-credentials/versions/latest"
    return json.loads(secretmanager.SecretManagerServiceClient().access_secret_version(name=nombre).payload.data)


def pedir(metodo, url, intentos=INTENTOS, **kwargs):
    """HTTP con reintentos y backoff exponencial (2, 4 y 8 s, más un azar de hasta 1 s).

    Reintenta ante caídas de red, tiempo agotado, límite de tasa (429) y errores del servidor (5xx).
    Si la API pide esperar (Retry-After o X-Rate-Limit-Retry-After-Seconds), respeta esa espera; si pide más
    de ESPERA_MAXIMA, no insiste: el llamador pasa a su respaldo.
    """
    for n in range(1, intentos + 1):
        espera = 2 ** n + random.uniform(0, 1)
        try:
            r = requests.request(metodo, url, **kwargs)
        except (requests.ConnectionError, requests.Timeout) as e:
            error = e
        else:
            if r.status_code != 429 and r.status_code < 500:
                r.raise_for_status()  # 4xx distinto de 429: reintentar no lo arregla
                return r
            error = requests.HTTPError(f"HTTP {r.status_code}", response=r)
            pedida = r.headers.get("X-Rate-Limit-Retry-After-Seconds") or r.headers.get("Retry-After")
            if pedida and pedida.isdigit():
                if int(pedida) > ESPERA_MAXIMA:
                    print(f"{url}: la API pide esperar {pedida} s; no se reintenta")
                    raise error
                espera = int(pedida)
        if n == intentos:
            raise error
        print(f"{url}: intento {n} de {intentos} falló ({type(error).__name__}); reintento en {espera:.0f} s")
        time.sleep(espera)


def leer_opensky():
    cred = credencial()
    t = pedir("POST", TOKEN_URL, data={"grant_type": "client_credentials", "client_id": cred["clientId"],
                                        "client_secret": cred["clientSecret"]}, timeout=(10, 20))
    r = pedir("GET", ESTADOS_URL, params=EEUU, headers={"Authorization": f"Bearer {t.json()['access_token']}"},
              timeout=(10, 60))
    datos = r.json()
    return {"fuente": "opensky", "modo": "vivo", "ts_consulta": datos.get("time"),
            "registros": [dict(zip(CAMPOS, fila)) for fila in (datos.get("states") or [])],
            "creditos_restantes": r.headers.get("X-Rate-Limit-Remaining")}


def avion_adsb(a, ts):
    """Convierte un avión de adsb.lol (pies, nudos) al mismo registro que entrega OpenSky (metros, m/s)."""
    def por(v, f):
        return round(v * f, 2) if isinstance(v, (int, float)) else None
    en_tierra = a.get("alt_baro") == "ground"
    return {"icao24": a.get("hex"), "callsign": a.get("flight"), "pais_origen": None,
            "ts_posicion": int(ts - a.get("seen_pos", 0)), "ts_contacto": int(ts - a.get("seen", 0)),
            "longitud": a.get("lon"), "latitud": a.get("lat"),
            "altitud_baro_m": 0.0 if en_tierra else por(a.get("alt_baro"), PIE_A_M), "en_tierra": en_tierra,
            "velocidad_ms": por(a.get("gs"), NUDO_A_MS), "rumbo": a.get("track"),
            "velocidad_vertical_ms": por(a.get("baro_rate"), PIE_MIN_A_MS), "sensores": None,
            "altitud_geo_m": por(a.get("alt_geom"), PIE_A_M), "squawk": a.get("squawk"), "spi": bool(a.get("spi")),
            "fuente_posicion": a.get("type")}


def leer_adsblol():
    """Aviones en vivo de adsb.lol: una consulta por centro, sin repetir aviones, solo EE.UU. continental."""
    aviones, ts, fallas = {}, ahora(), 0
    for lat, lon in CENTROS:
        try:
            r = pedir("GET", ADSBLOL_URL.format(lat=lat, lon=lon), timeout=(10, 30),
                      headers={"User-Agent": "aguila-aeronexus (proyecto academico)"})
        except requests.RequestException as e:
            fallas += 1  # un centro caído no tumba la consulta: se sigue con los demás
            print(f"adsblol ({lat}, {lon}): {type(e).__name__}")
            if fallas == len(CENTROS):
                raise
            continue
        datos = r.json()
        ts = datos.get("now", ts * 1000) / 1000
        for a in datos.get("ac", []):
            if a.get("hex") and a.get("lat") is not None and a.get("lon") is not None \
                    and EEUU["lamin"] <= a["lat"] <= EEUU["lamax"] and EEUU["lomin"] <= a["lon"] <= EEUU["lomax"]:
                aviones[a["hex"]] = avion_adsb(a, ts)
        time.sleep(3)  # cortesía con un servicio gratuito y su límite de consultas
    return {"fuente": "adsblol", "modo": "vivo", "ts_consulta": int(ts), "registros": list(aviones.values())}


LECTORES = {"adsblol": leer_adsblol, "opensky": leer_opensky}


def grabar(lote, prefijo=GRABACIONES):
    """Guarda la consulta en vivo en Cloud Storage (bronce) para reprocesarla o reproducirla después."""
    blob = storage.Client(project=PROYECTO).bucket(BUCKET).blob(f"{prefijo}{lote['ts_consulta']}.json")
    blob.upload_from_string(json.dumps(lote, separators=(",", ":")), content_type="application/json")


def leer_grabacion():
    """Reproduce la grabación real más reciente: cada ejecución toma la siguiente consulta en orden."""
    cliente = storage.Client(project=PROYECTO)
    blobs = sorted((b for f in LECTORES for b in cliente.list_blobs(BUCKET, prefix=f"grabaciones/{f}/")),
                   key=lambda b: int(b.name.rsplit("/", 1)[1][:-5]))
    if not blobs:
        raise RuntimeError("no hay grabaciones de aviones en Cloud Storage")
    if ahora() - int(blobs[-1].name.rsplit("/", 1)[1][:-5]) < 180:
        return None  # el colector de la PC está enviando en vivo: no se duplica
    ultima_sesion = [b for b in blobs if int(b.name.rsplit("/", 1)[1][:-5]) >= int(blobs[-1].name.rsplit("/", 1)[1][:-5]) - 12 * 3600]
    blob = ultima_sesion[(ahora() // 120) % len(ultima_sesion)]  # avanza una consulta cada 2 minutos
    lote = json.loads(blob.download_as_bytes())
    lote["modo"] = "reproduccion"
    return lote


def leer_aviones():
    for fuente in os.environ.get("AVIONES_FUENTES", "adsblol,opensky").split(","):
        try:
            lote = LECTORES[fuente.strip()]()
        except (requests.RequestException, ValueError, KeyError) as e:
            print(f"{fuente}: no responde ({type(e).__name__}); se prueba la siguiente fuente")
            continue
        grabar(lote, f"grabaciones/{lote['fuente']}/")
        return lote
    print("ninguna fuente en vivo responde: se reproduce la grabación real más reciente")
    return leer_grabacion()


def leer_metar():
    r = pedir("GET", METAR_URL, params={"ids": ",".join(AEROPUERTOS), "format": "json"}, timeout=(10, 60))
    return {"fuente": "metar", "modo": "vivo", "ts_consulta": ahora(), "registros": r.json()}


def publicar(lote, fuente):
    lote["ts_publicacion"] = ahora()
    publicador = pubsub_v1.PublisherClient()
    topico = publicador.topic_path(PROYECTO, "metar" if fuente == "metar" else "posiciones")
    cuerpo = json.dumps(lote, separators=(",", ":")).encode()
    id_msg = publicador.publish(topico, cuerpo, fuente=fuente, modo=lote["modo"]).result(timeout=60)
    print(json.dumps({"fuente": fuente, "modo": lote["modo"], "registros": len(lote["registros"]),
                      "bytes": len(cuerpo), "mensaje": id_msg, "creditos_restantes": lote.get("creditos_restantes")}))


def main():
    fuente = os.environ.get("FUENTE", "aviones")
    lote = leer_metar() if fuente == "metar" else leer_aviones()
    if lote is None:
        print("colector de la PC en vivo: esta ejecución no publica")
        return
    if fuente == "metar":
        grabar(lote, METAR_CRUDO)  # copia cruda en bronce, para reprocesar si hace falta
    publicar(lote, "metar" if fuente == "metar" else lote["fuente"])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # el Job queda en error y Cloud Run lo registra
        print(f"ERROR ingesta: {e}", file=sys.stderr)
        raise
