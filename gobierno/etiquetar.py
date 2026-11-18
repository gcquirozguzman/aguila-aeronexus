"""Gobierno como código: crea la plantilla «gobierno-aguila» en Knowledge Catalog (Dataplex)
y la pega, con sus valores, a cada tabla y vista de plata y oro.

Uso:  python gobierno/etiquetar.py [--proyecto aguila-aeronexus]
Se puede volver a correr: actualiza los valores sin duplicar nada.
"""
import argparse
import json
import shutil
import subprocess
import tempfile
from pathlib import Path

AQUI = Path(__file__).parent
PLANTILLA = "gobierno-aguila"
UBICACION_DATOS = "us"  # los datasets plata y oro están en la multirregión US


def gcloud(*args, check=True):
    exe = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    r = subprocess.run([exe, *args], capture_output=True, text=True, encoding="utf-8")
    if check and r.returncode != 0:
        raise SystemExit(r.stderr.strip())
    return r


def crear_plantilla(proyecto):
    existe = gcloud("dataplex", "aspect-types", "describe", PLANTILLA, "--location=global",
                    f"--project={proyecto}", check=False).returncode == 0
    if existe:
        print(f"Plantilla {PLANTILLA} ya existe")
        return
    gcloud("dataplex", "aspect-types", "create", PLANTILLA, "--location=global", f"--project={proyecto}",
           "--display-name=Gobierno Águila",
           "--description=Dueño, custodio, clasificación, calidad y retención de cada tabla de Águila",
           f"--metadata-template-file-name={AQUI / 'plantilla.json'}")
    print(f"Plantilla {PLANTILLA} creada")


def etiquetar(proyecto):
    conf = json.loads((AQUI / "tablas.json").read_text(encoding="utf-8"))
    clave = f"{proyecto}.global.{PLANTILLA}"
    for tabla, valores in conf["tablas"].items():
        dataset, nombre = tabla.split(".")
        entrada = f"bigquery.googleapis.com/projects/{proyecto}/datasets/{dataset}/tables/{nombre}"
        datos = {**conf["comun"], **valores}
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as f:
            json.dump({clave: {"data": datos}}, f, ensure_ascii=False)
        gcloud("dataplex", "entries", "update", entrada, f"--location={UBICACION_DATOS}",
               "--entry-group=@bigquery", f"--project={proyecto}", f"--update-aspects={f.name}")
        Path(f.name).unlink()
        print(f"  {tabla}: {valores['capa']} · {valores['dominio']}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--proyecto", default="aguila-aeronexus")
    a = p.parse_args()
    crear_plantilla(a.proyecto)
    etiquetar(a.proyecto)
