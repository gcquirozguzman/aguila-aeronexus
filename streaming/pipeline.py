"""Pipeline en tiempo real (Apache Beam): Pub/Sub -> limpieza + aeropuerto más cercano -> BigQuery plata.

Dos ramas:
  posiciones-sub: cada mensaje trae el lote de aviones de una consulta a OpenSky. Se separa avión por avión,
                  se valida, se une con el aeropuerto más cercano y se escribe en plata.posiciones.
                  Los registros inválidos van al tópico de errores.
  metar-sub:      reportes de clima de los 18 aeropuertos -> plata.metar.

Local (gratis):  python streaming/pipeline.py --runner DirectRunner
Nube:            python streaming/pipeline.py --runner DataflowRunner --max_num_workers 1 ...
"""
import argparse
import csv
import io
import json
import math
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions

PROYECTO = "aguila-aeronexus"
AEROPUERTOS_GCS = "gs://aguila-aeronexus-bronce/openflights/openflights_airports.dat"

ESQUEMA_POSICIONES = ",".join([
    "ts_consulta:TIMESTAMP", "ts_posicion:TIMESTAMP", "icao24:STRING", "callsign:STRING", "aerolinea_icao:STRING",
    "latitud:FLOAT", "longitud:FLOAT", "altitud_m:FLOAT", "velocidad_ms:FLOAT", "rumbo:FLOAT", "en_tierra:BOOLEAN",
    "aeropuerto_cercano_iata:STRING", "aeropuerto_cercano_icao:STRING", "distancia_km:FLOAT", "procesado_en:TIMESTAMP",
    "modo:STRING"])
ESQUEMA_METAR = ",".join([
    "icao:STRING", "ts_observacion:TIMESTAMP", "temp_c:FLOAT", "punto_rocio_c:FLOAT", "viento_dir:STRING",
    "viento_nudos:FLOAT", "rafaga_nudos:FLOAT", "visibilidad:STRING", "presion_hg:FLOAT", "fenomenos:STRING",
    "categoria_vuelo:STRING", "metar_crudo:STRING", "procesado_en:TIMESTAMP"])


def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat() if epoch is not None else None


def ahora():
    return datetime.now(timezone.utc).isoformat()


def km(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


def separar_lote(mensaje):
    lote = json.loads(mensaje.decode("utf-8"))
    for registro in lote.get("registros", []):
        registro["_ts_consulta"] = lote.get("ts_consulta")
        registro["_modo"] = lote.get("modo", "vivo")
        yield registro


class EnriquecerAvion(beam.DoFn):
    """Valida el avión y le asigna el aeropuerto de EE.UU. más cercano (catálogo OpenFlights)."""

    ERRORES = "errores"

    def setup(self):
        with FileSystems.open(AEROPUERTOS_GCS) as f:
            filas = csv.reader(io.TextIOWrapper(f, encoding="utf-8"))
            self.aeropuertos = [(r[4], r[5], float(r[6]), float(r[7])) for r in filas
                                if r[3] == "United States" and r[4] not in ("\\N", "") and r[5] not in ("\\N", "")]

    def process(self, a):
        lat, lon, icao24 = a.get("latitud"), a.get("longitud"), a.get("icao24")
        if lat is None or lon is None or not icao24 or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            yield beam.pvalue.TaggedOutput(self.ERRORES, json.dumps(
                {"motivo": "sin posicion o icao24", "registro": a}).encode())
            return
        iata, icao, dist = None, None, None
        for ai, ac, alat, alon in self.aeropuertos:
            d = km(lat, lon, alat, alon)
            if dist is None or d < dist:
                iata, icao, dist = ai, ac, d
        callsign = (a.get("callsign") or "").strip() or None
        yield {
            "ts_consulta": iso(a.get("_ts_consulta")),
            "ts_posicion": iso(a.get("ts_posicion")),
            "icao24": icao24,
            "callsign": callsign,
            "aerolinea_icao": callsign[:3] if callsign and callsign[:3].isalpha() else None,
            "latitud": lat,
            "longitud": lon,
            "altitud_m": a.get("altitud_baro_m"),
            "velocidad_ms": a.get("velocidad_ms"),
            "rumbo": a.get("rumbo"),
            "en_tierra": a.get("en_tierra"),
            "aeropuerto_cercano_iata": iata,
            "aeropuerto_cercano_icao": icao,
            "distancia_km": round(dist, 1) if dist is not None else None,
            "procesado_en": ahora(),
            "modo": a.get("_modo"),
        }


def limpiar_metar(m):
    def num(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    if not m.get("icaoId") or m.get("obsTime") is None:
        return
    yield {
        "icao": m["icaoId"],
        "ts_observacion": iso(m["obsTime"]),
        "temp_c": num(m.get("temp")),
        "punto_rocio_c": num(m.get("dewp")),
        "viento_dir": str(m.get("wdir")) if m.get("wdir") is not None else None,
        "viento_nudos": num(m.get("wspd")),
        "rafaga_nudos": num(m.get("wgst")),
        "visibilidad": str(m.get("visib")) if m.get("visib") is not None else None,
        "presion_hg": num(m.get("altim")),
        "fenomenos": m.get("wxString"),
        "categoria_vuelo": m.get("fltCat"),
        "metar_crudo": m.get("rawOb"),
        "procesado_en": ahora(),
    }


def bq(tabla, esquema):
    return beam.io.WriteToBigQuery(
        f"{PROYECTO}:plata.{tabla}", schema=esquema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS)


def run(argv=None):
    parser = argparse.ArgumentParser()
    _, beam_args = parser.parse_known_args(argv)
    opciones = PipelineOptions(beam_args, project=PROYECTO, save_main_session=True)
    opciones.view_as(StandardOptions).streaming = True
    sub = f"projects/{PROYECTO}/subscriptions"

    with beam.Pipeline(options=opciones) as p:
        aviones = (
            p
            | "Leer posiciones" >> beam.io.ReadFromPubSub(subscription=f"{sub}/posiciones-sub")
            | "Separar aviones" >> beam.FlatMap(separar_lote)
            | "Validar y enriquecer" >> beam.ParDo(EnriquecerAvion()).with_outputs(EnriquecerAvion.ERRORES, main="ok")
        )
        aviones.ok | "Guardar posiciones" >> bq("posiciones", ESQUEMA_POSICIONES)
        aviones.errores | "Enviar errores" >> beam.io.WriteToPubSub(f"projects/{PROYECTO}/topics/errores")

        (
            p
            | "Leer METAR" >> beam.io.ReadFromPubSub(subscription=f"{sub}/metar-sub")
            | "Separar reportes" >> beam.FlatMap(separar_lote)
            | "Limpiar METAR" >> beam.FlatMap(limpiar_metar)
            | "Guardar METAR" >> bq("metar", ESQUEMA_METAR)
        )


if __name__ == "__main__":
    run()
