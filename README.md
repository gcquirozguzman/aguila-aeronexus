<h1 align="center">🦅 Águila</h1>

<p align="center">
  <b>La plataforma de datos de AeroNexus Continental.</b><br>
  Aviones en vivo, clima de aeropuertos y 5.3 millones de vuelos históricos, listos para responder preguntas del negocio.
</p>

<p align="center">
  ☁️ Google Cloud &nbsp;·&nbsp; 🥉🥈🥇 Bronce, plata y oro &nbsp;·&nbsp; ⚡ Tiempo real y por lotes &nbsp;·&nbsp; 🎓 UTEC Big Data, Grupo 4
</p>

---

## 🎬 ¿Qué hace, en una imagen?

```mermaid
flowchart LR
    A["🛰️<br><b>OpenSky</b><br>aviones en vivo"] --> P["📨<br><b>Pub/Sub</b>"]
    B["🌦️<br><b>METAR</b><br>clima cada 5 min"] --> P
    P --> D["⚙️<br><b>Dataflow</b><br>limpia y cruza"]
    D --> BQ["🗄️<br><b>BigQuery</b><br>plata → oro"]
    C["📦<br><b>Vuelos 2015</b><br>5.3 M filas"] --> S["🔥<br><b>Dataproc</b><br>Spark"] --> BQ
    BQ --> API["🔌<br><b>API</b><br>Cloud Run"]
    BQ --> AG["💬<br><b>Agente</b><br>preguntas en español"]

    style BQ fill:#7c5cff,stroke:#7c5cff,color:#fff
    style API fill:#22a06b,stroke:#22a06b,color:#fff
    style AG fill:#22a06b,stroke:#22a06b,color:#fff
```

Dos caminos llegan al mismo lugar: el **tiempo real** (aviones y clima de hoy) y el **histórico** (puntualidad de 2015).
Todo termina en la capa **oro** de BigQuery, que se consulta con la API o conversando con el agente.

---

## 🧰 Las piezas

|     | Carpeta | Para qué sirve |
| :-: | :------ | :------------- |
| 🛰️ | **`ingesta/`** | Pide aviones a OpenSky y clima a METAR y los envía a Pub/Sub. Reintenta si la fuente falla. |
| ⚙️ | **`streaming/`** | Pipeline de Dataflow (Apache Beam): limpia cada avión, le pone el aeropuerto más cercano y lo guarda en BigQuery. |
| 🔥 | **`batch/`** | Spark limpia los vuelos de 2015; también reprocesa todo (`backfill.py`) o vuelve a publicar días pasados (`republicar.py`). |
| 🗄️ | **`sql/`** | Construye plata y oro: modelo estrella, puntualidad por ruta y aerolínea, clima actual y latencia. |
| 🔌 | **`api/`** | API en Cloud Run que entrega aviones, clima y puntualidad en JSON. |

El colector que corre en la PC está en otro repositorio: [radar-colector](https://github.com/gcquirozguzman/radar-colector).

---

## 🔌 La API

Vive en Cloud Run y es privada: solo responde a quien tiene acceso al proyecto.

| Pregunta del negocio | Endpoint |
| :------------------- | :------- |
| ¿Cómo está hoy un aeropuerto? | `GET /aeropuertos/ATL/panorama` |
| ¿Qué aviones hay cerca? | `GET /aeropuertos/DFW/aviones?radio_km=30` |
| ¿Qué clima tiene ahora? | `GET /aeropuertos/ORD/clima` |
| ¿Qué tan puntual es una ruta? | `GET /rutas/LAX/SFO/otp` |
| ¿Qué tan puntual es una aerolínea, mes a mes? | `GET /aerolineas/AA/otp` |

### Probarla en el navegador

```
gcloud run services proxy aguila-api --region us-central1 --project aguila-aeronexus
```

Abre http://localhost:8080/docs y prueba cada endpoint con el botón **Try it out**.

### Probarla desde la terminal

```
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" https://aguila-api-305567386804.us-central1.run.app/rutas/LAX/SFO/otp
```

> 🟢 Responde `{"origen_id":"LAX","destino_id":"SFO","vuelos":13457,"otp_pct":72.9,...}` → todo bien.
> 🔴 Responde `403` → falta el token o tu cuenta no tiene acceso al proyecto.

---

## 💬 El agente

En BigQuery → **Agentes** está **Águila Analista**. Se le pregunta en español, por ejemplo:

- *¿Cuáles son las 5 rutas con peor puntualidad?*
- *¿Qué aeropuertos tienen hoy condición adversa?*
- *¿Cuántos aviones hay ahora cerca de Atlanta?*

Lee solo la capa oro y muestra la consulta SQL que usó para responder.

---

## 🚀 Primera vez (solo una vez)

### 1️⃣ Ten Python 3.12 y gcloud

`py -3.12 --version` y `gcloud --version` deben responder con un número.

### 2️⃣ Inicia sesión en Google Cloud

```
gcloud auth login
gcloud auth application-default login
gcloud config set project aguila-aeronexus
```

### 3️⃣ Instala las librerías

<details open>
<summary><b>🪟 Windows</b></summary>

```
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install "apache-beam[gcp]==2.61.0" -r ingesta/requirements.txt -r api/requirements.txt
```

</details>

<details>
<summary><b>🍎 Mac / 🐧 Linux</b></summary>

```
python3.12 -m venv .venv
.venv/bin/python -m pip install "apache-beam[gcp]==2.61.0" -r ingesta/requirements.txt -r api/requirements.txt
```

</details>

---

## 🎯 La demo, paso a paso

```mermaid
flowchart LR
    A["⚙️<br>Enciendes<br>Dataflow"] --> B["▶️<br>Reanudas<br>los schedulers"] --> C["🛰️<br>Inicias<br>el colector"] --> D["🔌<br>Consultas la API<br>y el agente"] --> E["⛔<br>Apagas<br>todo"]
    style E fill:#e5484d,stroke:#e5484d,color:#fff
```

**1. Enciende Dataflow** (unos 5 minutos antes; tarda en arrancar):

```
.venv\Scripts\python streaming/pipeline.py --runner DataflowRunner --region us-central1 --temp_location gs://aguila-aeronexus-bronce/tmp --staging_location gs://aguila-aeronexus-bronce/staging --job_name aguila-streaming --num_workers 1 --max_num_workers 1 --machine_type n1-standard-2 --enable_streaming_engine --experiments use_runner_v2 --service_account_email sa-dataflow@aguila-aeronexus.iam.gserviceaccount.com
```

**2. Reanuda la ingesta en la nube:**

```
gcloud scheduler jobs resume aguila-opensky --location us-central1
gcloud scheduler jobs resume aguila-metar --location us-central1
```

**3. Inicia el colector en la PC** ([radar-colector](https://github.com/gcquirozguzman/radar-colector)) y pulsa **Iniciar**.

**4. Muestra los datos llegando:** la API (`/aeropuertos/ATL/panorama`) y el agente.

**5. Apaga todo al terminar.** ⚠️ Dataflow encendido cuesta unos S/ 15 a S/ 20 al día.

```
gcloud dataflow jobs drain $(gcloud dataflow jobs list --region us-central1 --status active --format "value(id)") --region us-central1
gcloud scheduler jobs pause aguila-opensky --location us-central1
gcloud scheduler jobs pause aguila-metar --location us-central1
```

💡 **Tip:** para practicar sin gastar, corre el pipeline en tu PC con `--runner DirectRunner --temp_location gs://aguila-aeronexus-bronce/tmp`.

---

## 🗄️ Lo que hay en BigQuery

| Capa | Qué guarda |
| :--- | :--------- |
| 🥉 **bronce** (Cloud Storage) | Los archivos tal como llegan: CSV de vuelos, OpenFlights, METAR y cada consulta a OpenSky. |
| 🥈 **plata** | Vuelos 2015 limpios (`vuelos`), los inválidos (`vuelos_cuarentena`), clima diario NOAA y lo que llega en vivo (`posiciones`, `metar`). |
| 🥇 **oro** | Modelo estrella (`fact_vuelos`, `fact_posiciones`, `dim_*`) y vistas para el negocio: `otp_por_ruta`, `otp_por_aerolinea`, `causas_retraso`, `clima_actual`, `sli_latencia`. |

<details>
<summary><b>🔥 Cargar el histórico (lotes)</b></summary>

```
gcloud storage cp batch/limpiar_vuelos.py gs://aguila-aeronexus-bronce/codigo/
gcloud dataproc batches submit pyspark gs://aguila-aeronexus-bronce/codigo/limpiar_vuelos.py --region=us-central1 --version=2.2 --service-account=sa-dataproc@aguila-aeronexus.iam.gserviceaccount.com --staging-bucket=aguila-aeronexus-bronce -- gs://aguila-aeronexus-bronce/flight_delays/2015/flights.csv gs://aguila-aeronexus-bronce/plata
```

Después se ejecutan `sql/plata.sql` y `sql/oro.sql` en BigQuery. Para rehacer todo de una vez: `python batch/backfill.py`.

</details>

<details>
<summary><b>🔌 Desplegar la API</b></summary>

```
gcloud run deploy aguila-api --source api --region us-central1 --service-account sa-api@aguila-aeronexus.iam.gserviceaccount.com --no-allow-unauthenticated --max-instances 2
```

</details>

---

## 🛡️ Calidad y seguridad

| | Qué pasa | Cómo lo resolvemos |
| :-: | :------- | :----------------- |
| 🔁 | OpenSky o METAR fallan | Reintenta 4 veces (2, 4 y 8 s) y respeta `Retry-After`; si sigue fallando, reproduce la última grabación real. |
| 🚫 | Llega un dato roto | Dataflow lo manda al tópico `errores` y sigue con el resto. |
| ♻️ | Hay que reprocesar | En los últimos 7 días: `gcloud pubsub subscriptions seek posiciones-sub --time=<fecha>`. Antes: `python batch/republicar.py --desde <fecha> --hasta <fecha>`. |
| 👯 | Llegan duplicados | `oro.fact_posiciones` deja una sola fila por avión y consulta. |
| ⏱️ | ¿Llega a tiempo? | `oro.sli_latencia` mide cada consulta. Meta: menos de 3 minutos. |
| 🔑 | Credenciales | La de OpenSky vive en Secret Manager. Cada pieza usa su propia cuenta con lo mínimo: `sa-ingesta`, `sa-dataflow`, `sa-dataproc`, `sa-api` (solo lectura). |

Pruebas de los reintentos: `python -m unittest ingesta/test_reintentos.py`.

---

## 📚 Fuentes

OpenSky Network · aviationweather.gov (METAR) · Flight Delays 2015 (BTS/Kaggle) · NOAA GSOD · OpenFlights

> OpenSky bloquea las IP de los grandes proveedores de nube. Por eso los aviones los pide un colector en una PC
> ([radar-colector](https://github.com/gcquirozguzman/radar-colector)). Si la PC está apagada, la nube reproduce la
> última grabación real y la marca como `modo=reproduccion`.

## 📄 Licencia

MIT
