"""Reprocesa el carril en tiempo real desde la capa bronce (consultas de más de 7 días, fuera de la retención de Pub/Sub).

Lee las consultas crudas guardadas en Cloud Storage (grabaciones/opensky/ y metar/), cuyo nombre es la hora de la
consulta en segundos desde 1970, y vuelve a publicar en Pub/Sub las que caen en el rango pedido. Dataflow debe estar
encendido para procesarlas. Cada mensaje conserva su ts_consulta original; si una consulta ya estaba en plata, la vista
oro.fact_posiciones deja una sola fila por icao24 + ts_consulta, así que relanzar no duplica el modelo.

Uso:
  python batch/republicar.py --desde 2026-11-04T17:00 --hasta 2026-11-04T21:00 --simular   # solo lista
  python batch/republicar.py --desde 2026-11-04T17:00 --hasta 2026-11-04T21:00             # publica aviones
  python batch/republicar.py --fuente metar --desde 2026-11-04 --hasta 2026-11-05           # publica clima

Para reprocesar dentro de los 7 días basta con retroceder la suscripción:
  gcloud pubsub subscriptions seek posiciones-sub --time=2026-11-04T17:00:00Z
"""
import argparse
import json
from datetime import datetime, timezone

from google.cloud import pubsub_v1, storage

PROYECTO = "aguila-aeronexus"
BUCKET = "aguila-aeronexus-bronce"
CARPETAS = {"opensky": ("grabaciones/opensky/", "posiciones"), "metar": ("metar/", "metar")}


def segundos(texto):
    return int(datetime.fromisoformat(texto).replace(tzinfo=timezone.utc).timestamp())


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--fuente", choices=CARPETAS, default="opensky")
    p.add_argument("--desde", required=True, help="fecha y hora UTC, ej. 2026-11-04T17:00")
    p.add_argument("--hasta", required=True, help="fecha y hora UTC (no incluida)")
    p.add_argument("--simular", action="store_true", help="solo lista lo que publicaría")
    a = p.parse_args()

    prefijo, topico = CARPETAS[a.fuente]
    desde, hasta = segundos(a.desde), segundos(a.hasta)
    blobs = []
    for b in storage.Client(project=PROYECTO).list_blobs(BUCKET, prefix=prefijo):
        nombre = b.name[len(prefijo):].removesuffix(".json")
        if nombre.isdigit() and desde <= int(nombre) < hasta:
            blobs.append(b)
    blobs.sort(key=lambda b: b.name)
    print(f"{len(blobs)} consultas de {a.fuente} entre {a.desde} y {a.hasta} (UTC)")

    publicador = None if a.simular else pubsub_v1.PublisherClient()
    for b in blobs:
        lote = json.loads(b.download_as_bytes())
        lote.setdefault("fuente", a.fuente)
        lote["modo"] = "vivo"  # es un dato real que se vuelve a procesar, no la reproducción de respaldo
        if a.simular:
            print(f"  {b.name}: {len(lote.get('registros', []))} registros")
            continue
        cuerpo = json.dumps(lote, separators=(",", ":")).encode()
        id_msg = publicador.publish(publicador.topic_path(PROYECTO, topico), cuerpo,
                                    fuente=a.fuente, modo=lote["modo"]).result(timeout=60)
        print(f"  {b.name}: {len(lote.get('registros', []))} registros, mensaje {id_msg}")


if __name__ == "__main__":
    main()
