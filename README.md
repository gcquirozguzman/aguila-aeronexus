# aguila-aeronexus

Plataforma de datos Águila de AeroNexus Continental: pipelines en tiempo real y por lotes en Google Cloud con
arquitectura medallion (bronce, plata y oro). Proyecto del curso de Big Data (UTEC, Grupo 4).

## Arquitectura

- **Tiempo real:** Cloud Scheduler → Cloud Run Job (`ingesta/`) → Pub/Sub → Dataflow (`streaming/pipeline.py`) → BigQuery `plata`
- **Por lotes:** Cloud Storage (bronce) → Dataproc Serverless (`batch/limpiar_vuelos.py`) → BigQuery `plata` → SQL (`sql/`) → `oro`

## Estructura

| Carpeta | Contenido |
|---|---|
| `batch/` | PySpark: limpia los vuelos de 2015, excluye octubre y separa los inválidos a cuarentena; `backfill.py` reprocesa todo el historial |
| `streaming/` | Apache Beam: aviones y METAR desde Pub/Sub, con el aeropuerto más cercano y un tópico de errores |
| `ingesta/` | Cloud Run Job (OpenSky y METAR → Pub/Sub) con reintentos y backoff exponencial, colector para la PC y sus pruebas |
| `sql/` | `plata.sql` (catálogos, clima NOAA y cruce) y `oro.sql` (modelo estrella y vistas de OTP) |

## Tablas principales

| Capa | Tabla o vista | Contenido |
|---|---|---|
| bronce (GCS) | `flight_delays/`, `openflights/`, `metar/`, `grabaciones/opensky/` | Archivos crudos tal como llegan |
| plata | `vuelos`, `vuelos_cuarentena`, `vuelos_enriquecidos`, `clima_diario` | Vuelos 2015 limpios, inválidos y cruzados con clima |
| plata | `posiciones`, `metar` | Tiempo real, particionadas por día y agrupadas por aeropuerto |
| oro | `fact_vuelos`, `dim_*`, `fact_posiciones` | Modelo estrella |
| oro | `mv_otp_ruta` | Vista materializada: totales de puntualidad por ruta ya calculados |
| oro | `otp_por_ruta`, `otp_por_aerolinea`, `causas_retraso`, `clima_actual` | Vistas para el negocio |
| oro | `sli_latencia` | SLI de latencia por consulta: de la emisión ADS-B a BigQuery y de la consulta a BigQuery |

## Seguridad

Cada pipeline usa su propia cuenta de servicio con permisos mínimos:

| Cuenta | Usada por | Permisos |
|---|---|---|
| `sa-ingesta` | Cloud Run Jobs y Cloud Scheduler | Publicar en `posiciones` y `metar`, bucket bronce, leer el secreto de OpenSky, ejecutar los jobs |
| `sa-dataflow` | Dataflow | Leer las suscripciones, publicar en `errores`, escribir en `plata`, bucket bronce |
| `sa-dataproc` | Dataproc Serverless | Bucket bronce |

La credencial de OpenSky vive en Secret Manager (`opensky-credentials`), nunca en el código.

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
gcloud dataproc batches submit pyspark gs://aguila-aeronexus-bronce/codigo/limpiar_vuelos.py --region=us-central1 --version=2.2 --service-account=sa-dataproc@aguila-aeronexus.iam.gserviceaccount.com --staging-bucket=aguila-aeronexus-bronce -- gs://aguila-aeronexus-bronce/flight_delays/2015/flights.csv gs://aguila-aeronexus-bronce/plata

# tiempo real (local, gratis)
.venv\Scripts\python streaming/pipeline.py --runner DirectRunner --temp_location gs://aguila-aeronexus-bronce/tmp

# tiempo real (Dataflow, 1 worker)
.venv\Scripts\python streaming/pipeline.py --runner DataflowRunner --region us-central1 --temp_location gs://aguila-aeronexus-bronce/tmp --staging_location gs://aguila-aeronexus-bronce/staging --job_name aguila-streaming --num_workers 1 --max_num_workers 1 --machine_type n1-standard-2 --enable_streaming_engine --experiments use_runner_v2 --service_account_email sa-dataflow@aguila-aeronexus.iam.gserviceaccount.com
```

## Calidad y resiliencia

- **Reintentos:** `ingesta/main.py` reintenta las llamadas a OpenSky y METAR con backoff exponencial (2, 4 y 8 s, más
  un azar) ante caídas de red, `429` y `5xx`, y respeta `Retry-After`. Si la API pide esperar más de 60 s, o si fallan
  los 4 intentos, OpenSky pasa a la reproducción de la grabación. Pruebas: `python -m unittest ingesta/test_reintentos.py`.
- **Errores:** Dataflow envía los registros inválidos al tópico `errores` (salida secundaria del pipeline). Es el patrón
  que recomienda Google para Dataflow, en lugar de la dead-letter policy de la suscripción.
- **Reprocesamiento (backfill):** si cambia la lógica de Spark o del SQL, `python batch/backfill.py` vuelve a ejecutar
  Spark desde el CSV crudo, recarga `plata.vuelos` y reconstruye plata y oro. Cada paso reemplaza su resultado, así que
  se puede relanzar sin duplicar. Con `--sin-spark` solo recarga y reconstruye con SQL.
- **Reproceso en vivo:** las suscripciones guardan 7 días, incluidos los mensajes ya confirmados. Con
  `gcloud pubsub subscriptions seek posiciones-sub --time=<fecha>` Dataflow vuelve a procesar desde esa fecha.
- **Latencia:** `oro.sli_latencia` mide cada consulta en vivo. Meta (SLO): menos de 3 minutos.

## Licencia

MIT
