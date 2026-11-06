"""Reprocesamiento masivo (backfill) del historial: Cloud Storage (bronce) -> plata -> oro.

Se usa cuando cambia la lógica de limpieza (Spark) o de modelado (SQL). Todo es idempotente: cada paso
reemplaza su resultado, así que se puede relanzar sin duplicar datos.

Pasos:
  1. Sube batch/limpiar_vuelos.py a gs://.../codigo/ y lo ejecuta en Dataproc Serverless con sa-dataproc.
     Lee el CSV crudo de bronce y reescribe el Parquet de plata (vuelos y cuarentena).
  2. Recarga plata.vuelos desde ese Parquet (reemplaza la tabla, con partición por fecha y clustering).
  3. Ejecuta sql/plata.sql (catálogos, clima, cuarentena y cruce) y sql/oro.sql (modelo estrella y vistas).
  4. Muestra los conteos para comparar con la corrida anterior.

Uso:
  python batch/backfill.py              # todo, incluido Spark (≈ 3 min de Dataproc, unos centavos)
  python batch/backfill.py --sin-spark  # solo pasos 2 a 4 (si cambió solo el SQL)
"""
import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

from google.cloud import bigquery

PROYECTO = "aguila-aeronexus"
REGION = "us-central1"
BUCKET = "gs://aguila-aeronexus-bronce"
CUENTA_DATAPROC = f"sa-dataproc@{PROYECTO}.iam.gserviceaccount.com"
RAIZ = Path(__file__).resolve().parent.parent


def paso(texto):
    print(f"\n== {texto}", flush=True)


def gcloud(*args):
    ejecutable = shutil.which("gcloud") or shutil.which("gcloud.cmd")
    subprocess.run([ejecutable, *args, f"--project={PROYECTO}"], check=True)


def spark():
    paso("1. Spark en Dataproc Serverless: bronce (CSV) -> plata (Parquet)")
    gcloud("storage", "cp", str(RAIZ / "batch" / "limpiar_vuelos.py"), f"{BUCKET}/codigo/")
    lote = f"backfill-{time.strftime('%Y%m%d-%H%M%S')}"
    gcloud("dataproc", "batches", "submit", "pyspark", f"{BUCKET}/codigo/limpiar_vuelos.py",
           f"--batch={lote}", f"--region={REGION}", "--version=2.2",
           f"--service-account={CUENTA_DATAPROC}", f"--staging-bucket={BUCKET[5:]}",
           "--", f"{BUCKET}/flight_delays/2015/flights.csv", f"{BUCKET}/plata")


def recargar_vuelos(bq):
    paso("2. Recarga de plata.vuelos desde el Parquet")
    hive = bigquery.HivePartitioningOptions()
    hive.mode = "AUTO"
    hive.source_uri_prefix = f"{BUCKET}/plata/vuelos/"
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        hive_partitioning=hive,
        time_partitioning=bigquery.TimePartitioning(field="fecha"),
        clustering_fields=["origen_iata", "aerolinea_iata"],
    )
    trabajo = bq.load_table_from_uri(f"{BUCKET}/plata/vuelos/*.parquet", f"{PROYECTO}.plata.vuelos", job_config=config)
    trabajo.result()
    print(f"plata.vuelos: {bq.get_table(f'{PROYECTO}.plata.vuelos').num_rows:,} filas")


def ejecutar_sql(bq, archivo):
    paso(f"3. {archivo}")
    script = (RAIZ / "sql" / archivo).read_text(encoding="utf-8")
    bq.query(script).result()
    print("listo")


def conteos(bq):
    paso("4. Conteos")
    consulta = """
    SELECT 1 AS n, 'plata.vuelos' AS tabla, COUNT(*) AS filas FROM plata.vuelos UNION ALL
    SELECT 2, 'plata.vuelos_cuarentena', COUNT(*) FROM plata.vuelos_cuarentena UNION ALL
    SELECT 3, 'plata.vuelos_enriquecidos', COUNT(*) FROM plata.vuelos_enriquecidos UNION ALL
    SELECT 4, 'oro.fact_vuelos', COUNT(*) FROM oro.fact_vuelos UNION ALL
    SELECT 5, 'oro.mv_otp_ruta', COUNT(*) FROM oro.mv_otp_ruta UNION ALL
    SELECT 6, 'OTP de la red (%)', CAST(ROUND(100 * AVG(CAST(a_tiempo AS INT64)), 1) * 10 AS INT64) FROM oro.fact_vuelos
    ORDER BY n
    """
    for fila in bq.query(consulta).result():
        valor = f"{fila.filas / 10:.1f}" if fila.tabla.startswith("OTP") else f"{fila.filas:,}"
        print(f"{fila.tabla:<28}{valor:>12}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sin-spark", action="store_true", help="no vuelve a ejecutar Spark")
    args = parser.parse_args()
    inicio = time.time()
    if not args.sin_spark:
        spark()
    bq = bigquery.Client(project=PROYECTO)
    recargar_vuelos(bq)
    ejecutar_sql(bq, "plata.sql")
    ejecutar_sql(bq, "oro.sql")
    conteos(bq)
    print(f"\nBackfill terminado en {time.time() - inicio:.0f} s")


if __name__ == "__main__":
    sys.exit(main())
