"""API de Águila: expone la capa oro de BigQuery para el negocio.

Endpoints:
  GET /                                  qué ofrece la API
  GET /aeropuertos/{iata}/aviones        aviones cerca del aeropuerto en la última consulta en vivo
  GET /aeropuertos/{iata}/clima          último METAR del aeropuerto y si hay condición adversa
  GET /aeropuertos/{iata}/panorama       aviones + clima + puntualidad histórica de sus salidas
  GET /rutas/{origen}/{destino}/otp      puntualidad histórica (2015) de una ruta
  GET /aerolineas/{iata}/otp             puntualidad histórica (2015) de una aerolínea, mes a mes

Cada consulta lleva parámetros (nunca se arma el SQL con el texto del usuario) y un tope de bytes facturados.
"""
import os
import re
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Path, Query
from google.cloud import bigquery

PROYECTO = os.environ.get("PROYECTO", "aguila-aeronexus")
TOPE_BYTES = 1_000_000_000  # 1 GB por consulta: oro entera pesa menos de 1 GB
IATA = r"^[A-Za-z0-9]{2,3}$"

app = FastAPI(title="Águila API", version="1.0",
              description="AeroNexus Continental: aviones en vivo, clima y puntualidad (OTP) desde BigQuery.")
_cliente = None


def cliente():
    global _cliente
    if _cliente is None:
        _cliente = bigquery.Client(project=PROYECTO)
    return _cliente


def consultar(sql, **params):
    tipos = {int: "INT64", float: "FLOAT64", str: "STRING"}
    config = bigquery.QueryJobConfig(
        maximum_bytes_billed=TOPE_BYTES,
        query_parameters=[bigquery.ScalarQueryParameter(k, tipos[type(v)], v) for k, v in params.items()],
    )
    filas = cliente().query(sql, job_config=config).result()
    return [dict(f.items()) for f in filas]


def codigo(texto):
    if not re.match(IATA, texto):
        raise HTTPException(400, "Código IATA inválido, ej. ATL")
    return texto.upper()


def minutos_desde(ts):
    return round((datetime.now(timezone.utc) - ts).total_seconds() / 60, 1) if ts else None


SQL_AVIONES = """
WITH ultima AS (SELECT MAX(ts_consulta) AS ts FROM `{p}.oro.fact_posiciones`)
SELECT ts_consulta, ts_posicion, icao24, TRIM(callsign) AS callsign, aerolinea_id,
       ROUND(distancia_km, 1) AS distancia_km, latitud, longitud,
       ROUND(altitud_m) AS altitud_m, ROUND(velocidad_ms * 3.6) AS velocidad_kmh, en_tierra
FROM `{p}.oro.fact_posiciones`
WHERE ts_consulta = (SELECT ts FROM ultima) AND aeropuerto_id = @iata AND distancia_km <= @radio
ORDER BY distancia_km
"""

SQL_CLIMA = """
SELECT aeropuerto_id, icao, nombre, ciudad, ts_observacion, temp_c, viento_nudos, rafaga_nudos,
       visibilidad, fenomenos, categoria_vuelo, condicion_adversa, metar_crudo
FROM `{p}.oro.clima_actual`
WHERE aeropuerto_id = @iata
"""

SQL_OTP_SALIDAS = """
SELECT SUM(vuelos) AS vuelos,
       ROUND(100 * SAFE_DIVIDE(SUM(vuelos_a_tiempo), SUM(vuelos_con_dato)), 1) AS otp_pct,
       ROUND(SAFE_DIVIDE(SUM(suma_retraso_min), SUM(vuelos_con_retraso)), 1) AS retraso_promedio_min
FROM `{p}.oro.mv_otp_ruta`
WHERE origen_id = @iata
"""

SQL_OTP_RUTA = """
SELECT origen_id, destino_id, vuelos, otp_pct, retraso_promedio_min
FROM `{p}.oro.otp_por_ruta`
WHERE origen_id = @origen AND destino_id = @destino
"""

SQL_OTP_AEROLINEA = """
SELECT aerolinea_id, nombre, mes, nombre_mes, vuelos, otp_pct
FROM `{p}.oro.otp_por_aerolinea`
WHERE aerolinea_id = @iata
ORDER BY mes
"""


def aviones(iata, radio):
    filas = consultar(SQL_AVIONES.format(p=PROYECTO), iata=iata, radio=radio)
    ts = filas[0]["ts_consulta"] if filas else None
    return {
        "aeropuerto": iata,
        "radio_km": radio,
        "consulta": ts,
        "minutos_desde_consulta": minutos_desde(ts),
        "total": len(filas),
        "en_tierra": sum(1 for f in filas if f["en_tierra"]),
        "aviones": [{k: v for k, v in f.items() if k != "ts_consulta"} for f in filas],
    }


def clima(iata):
    filas = consultar(SQL_CLIMA.format(p=PROYECTO), iata=iata)
    if not filas:
        raise HTTPException(404, f"Sin METAR para {iata}")
    f = filas[0]
    f["minutos_desde_observacion"] = minutos_desde(f["ts_observacion"])
    return f


@app.get("/")
def inicio():
    return {
        "api": "Águila · AeroNexus Continental",
        "docs": "/docs",
        "ejemplos": ["/aeropuertos/ATL/panorama", "/aeropuertos/DFW/aviones?radio_km=30",
                     "/aeropuertos/ORD/clima", "/rutas/LAX/SFO/otp", "/aerolineas/AA/otp"],
    }


@app.get("/aeropuertos/{iata}/aviones")
def aviones_cercanos(iata: str = Path(examples=["ATL"]),
                     radio_km: float = Query(50, gt=0, le=200, description="Distancia máxima al aeropuerto")):
    return aviones(codigo(iata), float(radio_km))


@app.get("/aeropuertos/{iata}/clima")
def clima_aeropuerto(iata: str = Path(examples=["ORD"])):
    return clima(codigo(iata))


@app.get("/aeropuertos/{iata}/panorama")
def panorama(iata: str = Path(examples=["ATL"])):
    iata = codigo(iata)
    salidas = consultar(SQL_OTP_SALIDAS.format(p=PROYECTO), iata=iata)[0]
    try:
        c = clima(iata)
    except HTTPException:
        c = None
    a = aviones(iata, 50.0)
    return {
        "aeropuerto": iata,
        "clima": c,
        "aviones_a_50_km": {k: a[k] for k in ("consulta", "minutos_desde_consulta", "total", "en_tierra")},
        "puntualidad_salidas_2015": salidas if salidas["vuelos"] else None,
        "alerta": bool(c and c["condicion_adversa"]),
    }


@app.get("/rutas/{origen}/{destino}/otp")
def otp_ruta(origen: str = Path(examples=["LAX"]), destino: str = Path(examples=["SFO"])):
    filas = consultar(SQL_OTP_RUTA.format(p=PROYECTO), origen=codigo(origen), destino=codigo(destino))
    if not filas:
        raise HTTPException(404, "Ruta sin datos o con menos de 300 vuelos en 2015")
    return filas[0]


@app.get("/aerolineas/{iata}/otp")
def otp_aerolinea(iata: str = Path(examples=["AA"])):
    filas = consultar(SQL_OTP_AEROLINEA.format(p=PROYECTO), iata=codigo(iata))
    if not filas:
        raise HTTPException(404, f"Sin vuelos de la aerolínea {iata} en 2015")
    total = sum(f["vuelos"] for f in filas)
    return {
        "aerolinea": filas[0]["aerolinea_id"],
        "nombre": filas[0]["nombre"],
        "vuelos_2015": total,
        "otp_pct_anual": round(sum(f["otp_pct"] * f["vuelos"] for f in filas) / total, 1),
        "por_mes": [{k: f[k] for k in ("mes", "nombre_mes", "vuelos", "otp_pct")} for f in filas],
    }
