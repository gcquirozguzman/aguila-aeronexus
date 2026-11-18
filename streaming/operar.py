"""Enciende y apaga el tiempo real sin dejar mensajes acumulados en Pub/Sub (watermark lag).

  python streaming/operar.py encender   1) descarta lo acumulado mientras estuvo apagado (seek a ahora;
                                           ese dato ya está guardado en bronce y se puede republicar),
                                        2) lanza Dataflow con autoescalado (1 a 2 workers),
                                        3) reanuda los schedulers de ingesta.
  python streaming/operar.py apagar     drena Dataflow (termina lo que tiene en curso) y pausa la ingesta.
  python streaming/operar.py estado     muestra el job activo y los mensajes pendientes.

Los schedulers también se pueden controlar con Terraform: terraform apply -var ingesta_activa=true|false.
"""
import shutil
import subprocess
import sys
from datetime import datetime, timezone

PROYECTO, REGION = "aguila-aeronexus", "us-central1"
BUCKET = f"gs://{PROYECTO}-bronce"
SUSCRIPCIONES = ["posiciones-sub", "metar-sub"]
SCHEDULERS = ["aguila-opensky", "aguila-metar"]
GCLOUD = shutil.which("gcloud") or shutil.which("gcloud.cmd")


def gcloud(*args):
    print("$ gcloud", " ".join(args))
    return subprocess.run([GCLOUD, *args, f"--project={PROYECTO}"], check=True, capture_output=True,
                          text=True, encoding="utf-8").stdout.strip()


def job_activo():
    return gcloud("dataflow", "jobs", "list", f"--region={REGION}", "--status=active", "--format=value(id)")


def encender():
    if job_activo():
        sys.exit("Ya hay un job de Dataflow activo.")
    ahora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for s in SUSCRIPCIONES:
        gcloud("pubsub", "subscriptions", "seek", s, f"--time={ahora}")
    subprocess.run([sys.executable, "streaming/pipeline.py", "--runner=DataflowRunner", f"--region={REGION}",
                    f"--temp_location={BUCKET}/tmp", f"--staging_location={BUCKET}/staging",
                    f"--job_name=aguila-streaming-{datetime.now():%m%d%H%M}", "--num_workers=1", "--max_num_workers=2",
                    "--autoscaling_algorithm=THROUGHPUT_BASED", "--machine_type=n1-standard-2",
                    "--enable_streaming_engine", "--experiments=use_runner_v2",
                    f"--service_account_email=sa-dataflow@{PROYECTO}.iam.gserviceaccount.com"], check=True)
    for s in SCHEDULERS:
        gcloud("scheduler", "jobs", "resume", s, f"--location={REGION}")
    print("Tiempo real encendido. Dataflow tarda unos 4 minutos en procesar el primer mensaje.")


def apagar():
    for j in job_activo().split():
        gcloud("dataflow", "jobs", "drain", j, f"--region={REGION}")
    for s in SCHEDULERS:
        gcloud("scheduler", "jobs", "pause", s, f"--location={REGION}")
    print("Dataflow drenándose y schedulers en pausa: no queda nada generando costo.")


def estado():
    print("Job activo:", job_activo() or "ninguno")
    for s in SCHEDULERS:
        print(s, gcloud("scheduler", "jobs", "describe", s, f"--location={REGION}", "--format=value(state)"))


if __name__ == "__main__":
    {"encender": encender, "apagar": apagar, "estado": estado}[sys.argv[1] if len(sys.argv) > 1 else "estado"]()
