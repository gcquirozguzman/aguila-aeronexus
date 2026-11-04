"""Pipeline por lotes: limpia el historial de vuelos 2015 (capa bronce -> plata).

Lee flights.csv desde Cloud Storage, excluye octubre (códigos DOT numéricos en vez de IATA),
tipa columnas, arma fecha y horas, separa los registros inválidos a cuarentena y escribe
Parquet particionado por mes para cargarlo luego en BigQuery.

Uso (Dataproc Serverless):
    gcloud dataproc batches submit pyspark batch/limpiar_vuelos.py --region us-central1 \
        -- gs://aguila-aeronexus-bronce/flight_delays/2015/flights.csv gs://aguila-aeronexus-bronce/plata
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

ENTRADA, SALIDA = sys.argv[1], sys.argv[2]

ENTEROS = ["YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "FLIGHT_NUMBER", "SCHEDULED_DEPARTURE", "DEPARTURE_TIME",
           "DEPARTURE_DELAY", "TAXI_OUT", "WHEELS_OFF", "SCHEDULED_TIME", "ELAPSED_TIME", "AIR_TIME", "DISTANCE",
           "WHEELS_ON", "TAXI_IN", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME", "ARRIVAL_DELAY", "DIVERTED", "CANCELLED",
           "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]
TEXTOS = ["AIRLINE", "TAIL_NUMBER", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT", "CANCELLATION_REASON"]
COLUMNAS = ["YEAR", "MONTH", "DAY", "DAY_OF_WEEK", "AIRLINE", "FLIGHT_NUMBER", "TAIL_NUMBER", "ORIGIN_AIRPORT",
            "DESTINATION_AIRPORT", "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "DEPARTURE_DELAY", "TAXI_OUT",
            "WHEELS_OFF", "SCHEDULED_TIME", "ELAPSED_TIME", "AIR_TIME", "DISTANCE", "WHEELS_ON", "TAXI_IN",
            "SCHEDULED_ARRIVAL", "ARRIVAL_TIME", "ARRIVAL_DELAY", "DIVERTED", "CANCELLED", "CANCELLATION_REASON",
            "AIR_SYSTEM_DELAY", "SECURITY_DELAY", "AIRLINE_DELAY", "LATE_AIRCRAFT_DELAY", "WEATHER_DELAY"]
ESQUEMA = T.StructType([T.StructField(c, T.IntegerType() if c in ENTEROS else T.StringType()) for c in COLUMNAS])

CAUSAS = {"AIR_SYSTEM_DELAY": "sistema_aereo", "SECURITY_DELAY": "seguridad", "AIRLINE_DELAY": "aerolinea",
          "LATE_AIRCRAFT_DELAY": "avion_anterior", "WEATHER_DELAY": "clima"}


def hora(col):
    """HHMM entero (ej. 1745) -> 'HH:MM'; 2400 se trata como 00:00."""
    h = F.lpad((F.col(col) % 2400).cast("string"), 4, "0")
    return F.when(F.col(col).isNull(), None).otherwise(F.concat(F.substring(h, 1, 2), F.lit(":"), F.substring(h, 3, 2)))


spark = SparkSession.builder.appName("aguila-limpiar-vuelos").getOrCreate()
crudo = spark.read.csv(ENTRADA, header=True, schema=ESQUEMA, mode="PERMISSIVE")
total = crudo.count()

# octubre 2015 trae aeropuertos como códigos DOT numéricos: se excluye (decisión del grupo, 23 oct 2026)
sin_octubre = crudo.filter(F.col("MONTH") != 10)

iata = r"^[A-Z0-9]{3}$"
valido = (
    F.col("YEAR").eqNullSafe(2015)
    & F.col("MONTH").between(1, 12) & F.col("DAY").between(1, 31)
    & F.col("AIRLINE").rlike(r"^[A-Z0-9]{2}$")
    & F.col("ORIGIN_AIRPORT").rlike(iata) & F.col("DESTINATION_AIRPORT").rlike(iata)
    & F.col("SCHEDULED_DEPARTURE").between(0, 2400)
    & F.col("CANCELLED").isin(0, 1) & F.col("DIVERTED").isin(0, 1)
)
marcado = sin_octubre.withColumn("_valido", F.coalesce(valido, F.lit(False)))

cuarentena = (marcado.filter(~F.col("_valido")).drop("_valido")
              .withColumn("motivo", F.lit("no cumple reglas de calidad"))
              .withColumn("cargado_en", F.current_timestamp()))

causa_principal = F.when(F.col("ARRIVAL_DELAY") > 15, F.greatest(*[
    F.when(F.coalesce(F.col(c), F.lit(0)) > 0, F.struct(F.col(c).alias("min"), F.lit(n).alias("causa")))
    for c, n in CAUSAS.items()]).getField("causa"))

limpio = (
    marcado.filter(F.col("_valido")).drop("_valido")
    .dropDuplicates(["YEAR", "MONTH", "DAY", "AIRLINE", "FLIGHT_NUMBER", "ORIGIN_AIRPORT", "SCHEDULED_DEPARTURE"])
    .select(
        F.make_date("YEAR", "MONTH", "DAY").alias("fecha"),
        F.col("MONTH").alias("mes"),
        F.col("DAY_OF_WEEK").alias("dia_semana"),
        F.col("AIRLINE").alias("aerolinea_iata"),
        F.col("FLIGHT_NUMBER").alias("numero_vuelo"),
        F.col("TAIL_NUMBER").alias("matricula"),
        F.col("ORIGIN_AIRPORT").alias("origen_iata"),
        F.col("DESTINATION_AIRPORT").alias("destino_iata"),
        hora("SCHEDULED_DEPARTURE").alias("salida_programada"),
        hora("DEPARTURE_TIME").alias("salida_real"),
        F.col("DEPARTURE_DELAY").alias("retraso_salida_min"),
        hora("SCHEDULED_ARRIVAL").alias("llegada_programada"),
        hora("ARRIVAL_TIME").alias("llegada_real"),
        F.col("ARRIVAL_DELAY").alias("retraso_llegada_min"),
        F.col("DISTANCE").alias("distancia_millas"),
        (F.col("CANCELLED") == 1).alias("cancelado"),
        F.col("CANCELLATION_REASON").alias("motivo_cancelacion"),
        (F.col("DIVERTED") == 1).alias("desviado"),
        *[F.col(c).alias(f"retraso_{n}_min") for c, n in CAUSAS.items()],
        causa_principal.alias("causa_principal"),
        ((F.col("CANCELLED") == 0) & (F.col("DIVERTED") == 0) & (F.col("ARRIVAL_DELAY") <= 15)).alias("a_tiempo"),
    )
)

limpio.write.mode("overwrite").partitionBy("mes").parquet(f"{SALIDA}/vuelos")
cuarentena.write.mode("overwrite").parquet(f"{SALIDA}/vuelos_cuarentena")

n_oct = crudo.filter(F.col("MONTH") == 10).count()
n_ok = spark.read.parquet(f"{SALIDA}/vuelos").count()
n_cuar = spark.read.parquet(f"{SALIDA}/vuelos_cuarentena").count()
print(f"RESUMEN total={total} octubre_excluido={n_oct} validos={n_ok} cuarentena={n_cuar}")
spark.stop()
