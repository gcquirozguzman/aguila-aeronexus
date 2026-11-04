# aguila-aeronexus

Plataforma de datos Águila de AeroNexus Continental: pipelines en tiempo real y por lotes en Google Cloud con
arquitectura medallion (bronce, plata y oro). Proyecto del curso de Big Data (UTEC, Grupo 4).

## Arquitectura

- **Tiempo real:** Cloud Scheduler → Cloud Run Job (`ingesta/`) → Pub/Sub → Dataflow (`streaming/pipeline.py`) → BigQuery `plata`
- **Por lotes:** Cloud Storage (bronce) → Dataproc Serverless (`batch/limpiar_vuelos.py`) → BigQuery `plata` → SQL (`sql/`) → `oro`

## Estructura

| Carpeta | Contenido |
|---|---|
| `batch/` | PySpark: limpia los vuelos de 2015, excluye octubre y separa los inválidos a cuarentena |
| `streaming/` | Apache Beam: aviones y METAR desde Pub/Sub, con el aeropuerto más cercano y un tópico de errores |
| `ingesta/` | Cloud Run Job (OpenSky y METAR → Pub/Sub) y colector para la PC |
| `sql/` | `plata.sql` (catálogos, clima NOAA y cruce) y `oro.sql` (modelo estrella y vistas de OTP) |

## Fuentes

OpenSky Network, aviationweather.gov (METAR), Flight Delays 2015 (BTS/Kaggle), NOAA GSOD (BigQuery público) y OpenFlights.

OpenSky bloquea las IP de los grandes proveedores de nube. Por eso el colector de aviones corre en una PC
(`ingesta/colector_pc.py` o el proyecto [radar-colector](https://github.com/gcquirozguzman/radar-colector)) y graba
cada consulta. Si el colector no está activo, el Cloud Run Job reproduce la última grabación real (`modo=reproduccion`).

## Ejecución

```
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install "apache-beam[gcp]==2.61.0" -r ingesta/requirements.txt

# por lotes
gcloud storage cp batch/limpiar_vuelos.py gs://aguila-aeronexus-bronce/codigo/
gcloud dataproc batches submit pyspark gs://aguila-aeronexus-bronce/codigo/limpiar_vuelos.py --region=us-central1 --version=2.2 -- gs://aguila-aeronexus-bronce/flight_delays/2015/flights.csv gs://aguila-aeronexus-bronce/plata

# tiempo real (local, gratis)
.venv\Scripts\python streaming/pipeline.py --runner DirectRunner --temp_location gs://aguila-aeronexus-bronce/tmp
```

## Licencia

MIT
