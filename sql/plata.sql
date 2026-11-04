-- Capa plata: catálogos, clima diario y cruce vuelo + aeropuerto + clima.
-- Se ejecuta después de cargar plata.vuelos (Parquet del pipeline Spark) y bronce.openflights_aeropuertos.

-- 1. Aeropuertos de EE.UU. con IATA e ICAO (llave maestra OpenFlights)
CREATE OR REPLACE TABLE plata.aeropuertos AS
SELECT
  iata,
  icao,
  nombre,
  ciudad,
  latitud,
  longitud,
  altitud_pies
FROM bronce.openflights_aeropuertos
WHERE pais = 'United States'
  AND iata IS NOT NULL AND iata != '\\N'
  AND icao IS NOT NULL AND icao != '\\N'
QUALIFY ROW_NUMBER() OVER (PARTITION BY iata ORDER BY id) = 1;

-- 2. Aerolíneas (catálogo del dataset de vuelos)
LOAD DATA OVERWRITE plata.aerolineas (aerolinea_iata STRING, nombre STRING)
FROM FILES (format = 'CSV', skip_leading_rows = 1,
            uris = ['gs://aguila-aeronexus-bronce/flight_delays/2015/airlines.csv']);

-- 3. Clima diario 2015 por aeropuerto (NOAA GSOD, estación con el mismo código ICAO)
CREATE OR REPLACE TABLE plata.clima_diario
PARTITION BY fecha
CLUSTER BY icao AS
WITH estaciones AS (
  SELECT DISTINCT s.usaf, s.wban, s.call AS icao
  FROM `bigquery-public-data.noaa_gsod.stations` s
  JOIN plata.aeropuertos a ON s.call = a.icao
  WHERE s.`end` >= '20150101'
)
SELECT
  e.icao,
  DATE(CAST(g.year AS INT64), CAST(g.mo AS INT64), CAST(g.da AS INT64)) AS fecha,
  ROUND(AVG(NULLIF(g.temp, 9999.9)), 1) AS temp_f,
  ROUND(AVG(SAFE_CAST(NULLIF(g.wdsp, '999.9') AS FLOAT64)), 1) AS viento_nudos,
  ROUND(AVG(NULLIF(g.visib, 999.9)), 1) AS visibilidad_millas,
  ROUND(AVG(NULLIF(g.prcp, 99.99)), 2) AS precipitacion_pulg,
  MAX(g.thunder = '1') AS tormenta,
  MAX(g.snow_ice_pellets = '1') AS nieve,
  MAX(g.fog = '1') AS niebla
FROM `bigquery-public-data.noaa_gsod.gsod2015` g
JOIN estaciones e ON g.stn = e.usaf AND g.wban = e.wban
GROUP BY e.icao, fecha;

-- 4. Cruce: vuelo + aeropuertos (IATA -> ICAO) + clima del origen ese día
CREATE OR REPLACE TABLE plata.vuelos_enriquecidos
PARTITION BY fecha
CLUSTER BY origen_iata, aerolinea_iata AS
SELECT
  v.*,
  ao.icao AS origen_icao,
  ao.ciudad AS origen_ciudad,
  ad.icao AS destino_icao,
  ad.ciudad AS destino_ciudad,
  c.temp_f AS origen_temp_f,
  c.viento_nudos AS origen_viento_nudos,
  c.visibilidad_millas AS origen_visibilidad_millas,
  c.precipitacion_pulg AS origen_precipitacion_pulg,
  c.tormenta AS origen_tormenta,
  c.nieve AS origen_nieve
FROM plata.vuelos v
LEFT JOIN plata.aeropuertos ao ON v.origen_iata = ao.iata
LEFT JOIN plata.aeropuertos ad ON v.destino_iata = ad.iata
LEFT JOIN plata.clima_diario c ON c.icao = ao.icao AND c.fecha = v.fecha;
