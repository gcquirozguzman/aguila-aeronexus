"""Ingesta de aviones (OpenSky) y clima (METAR) hacia Pub/Sub.

Modos:
  Cloud Run Job (Cloud Scheduler cada 2 min con FUENTE=opensky y cada 5 min con FUENTE=metar):
    - metar: consulta aviationweather.gov en vivo y guarda la respuesta cruda en Cloud Storage (capa bronce).
    - opensky: intenta OpenSky en vivo. OpenSky bloquea las IP de los grandes proveedores de nube, así que
      si no responde, reproduce la grabación real más reciente guardada en Cloud Storage (modo "reproduccion").
  Colector en la PC (python ingesta/colector_pc.py):
    - consulta OpenSky en vivo cada 2 min, publica en Pub/Sub (modo "vivo") y guarda cada consulta en
      Cloud Storage como grabación para la reproducción.

Cada ejecución publica UN mensaje con el lote completo (más barato que un mensaje por avión).

Variables de entorno:
    FUENTE              opensky | metar
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


def grabar(lote, prefijo=GRABACIONES):
    """Guarda la consulta en vivo en Cloud Storage para poder reproducirla después."""
    blob = storage.Client(project=PROYECTO).bucket(BUCKET).blob(f"{prefijo}{lote['ts_consulta']}.json")
    blob.upload_from_string(json.dumps(lote, separators=(",", ":")), content_type="application/json")


def leer_grabacion():
    """Reproduce la grabación real más reciente: cada ejecución toma la siguiente consulta en orden."""
    blobs = sorted(storage.Client(project=PROYECTO).list_blobs(BUCKET, prefix=GRABACIONES), key=lambda b: b.name)
    if not blobs:
        raise RuntimeError("no hay grabaciones de OpenSky en Cloud Storage")
    if ahora() - int(blobs[-1].name.rsplit("/", 1)[1][:-5]) < 180:
        return None  # el colector de la PC está enviando en vivo: no se duplica
    ultima_sesion = [b for b in blobs if int(b.name.rsplit("/", 1)[1][:-5]) >= int(blobs[-1].name.rsplit("/", 1)[1][:-5]) - 12 * 3600]
    blob = ultima_sesion[(ahora() // 120) % len(ultima_sesion)]  # avanza una consulta cada 2 minutos
    lote = json.loads(blob.download_as_bytes())
    lote["modo"] = "reproduccion"
    return lote


def leer_aviones():
    try:
        return leer_opensky()
    except requests.RequestException as e:
        print(f"OpenSky no responde tras {INTENTOS} intentos ({type(e).__name__}): "
              "se reproduce la grabación real más reciente")
        return leer_grabacion()


def leer_metar():
    r = pedir("GET", METAR_URL, params={"ids": ",".join(AEROPUERTOS), "format": "json"}, timeout=(10, 60))
    return {"fuente": "metar", "modo": "vivo", "ts_consulta": ahora(), "registros": r.json()}


def publicar(lote, fuente):
    lote["ts_publicacion"] = ahora()
    publicador = pubsub_v1.PublisherClient()
    topico = publicador.topic_path(PROYECTO, "posiciones" if fuente == "opensky" else "metar")
    cuerpo = json.dumps(lote, separators=(",", ":")).encode()
    id_msg = publicador.publish(topico, cuerpo, fuente=fuente, modo=lote["modo"]).result(timeout=60)
    print(json.dumps({"fuente": fuente, "modo": lote["modo"], "registros": len(lote["registros"]),
                      "bytes": len(cuerpo), "mensaje": id_msg, "creditos_restantes": lote.get("creditos_restantes")}))


def main():
    fuente = os.environ.get("FUENTE", "opensky")
    lote = leer_aviones() if fuente == "opensky" else leer_metar()
    if lote is None:
        print("colector de la PC en vivo: esta ejecución no publica")
        return
    if fuente == "metar":
        grabar(lote, METAR_CRUDO)  # copia cruda en bronce, para reprocesar si hace falta
    publicar(lote, fuente)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # el Job queda en error y Cloud Run lo registra
        print(f"ERROR ingesta: {e}", file=sys.stderr)
        raise
