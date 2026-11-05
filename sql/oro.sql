-- Capa oro: modelo estrella (dimensiones + hechos) y vistas de puntualidad (OTP).

-- Catálogo de aerolíneas de OpenFlights (para traducir el prefijo ICAO del callsign: AAL -> AA)
LOAD DATA OVERWRITE bronce.openflights_aerolineas
  (id INT64, nombre STRING, alias STRING, iata STRING, icao STRING, callsign STRING, pais STRING, activa STRING)
FROM FILES (format = 'CSV', null_marker = '\\N',
            uris = ['gs://aguila-aeronexus-bronce/openflights/openflights_airlines.dat']);

-- dim_aeropuerto
CREATE OR REPLACE TABLE oro.dim_aeropuerto AS
SELECT iata AS aeropuerto_id, icao, nombre, ciudad, latitud, longitud, altitud_pies
FROM plata.aeropuertos;

-- dim_aerolinea
CREATE OR REPLACE TABLE oro.dim_aerolinea AS
SELECT a.aerolinea_iata AS aerolinea_id, o.icao AS aerolinea_icao, a.nombre
FROM plata.aerolineas a
LEFT JOIN bronce.openflights_aerolineas o
  ON o.iata = a.aerolinea_iata AND o.activa = 'Y' AND o.icao IS NOT NULL AND o.pais = 'United States'
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.aerolinea_iata ORDER BY o.id) = 1;

-- dim_fecha (2015 para el historial y 2026 para el dato en vivo)
CREATE OR REPLACE TABLE oro.dim_fecha AS
SELECT
  d AS fecha_id,
  EXTRACT(YEAR FROM d) AS anio,
  EXTRACT(MONTH FROM d) AS mes,
  FORMAT_DATE('%B', d) AS nombre_mes,
  EXTRACT(DAY FROM d) AS dia,
  EXTRACT(DAYOFWEEK FROM d) AS dia_semana,
  EXTRACT(DAYOFWEEK FROM d) IN (1, 7) AS fin_de_semana,
  (EXTRACT(MONTH FROM d) IN (6, 7, 8)
   OR (EXTRACT(MONTH FROM d) = 11 AND EXTRACT(DAY FROM d) BETWEEN 20 AND 30)
   OR (EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) >= 15)
   OR (EXTRACT(MONTH FROM d) = 1 AND EXTRACT(DAY FROM d) <= 5)) AS temporada_alta,
  d IN ('2015-01-01', '2015-01-19', '2015-02-16', '2015-05-25', '2015-07-03', '2015-09-07', '2015-10-12',
        '2015-11-11', '2015-11-26', '2015-12-25') AS feriado
FROM UNNEST(ARRAY_CONCAT(GENERATE_DATE_ARRAY('2015-01-01', '2015-12-31'),
                         GENERATE_DATE_ARRAY('2026-01-01', '2026-12-31'))) AS d;

-- dim_clima (un registro por aeropuerto y día)
CREATE OR REPLACE TABLE oro.dim_clima
PARTITION BY fecha
CLUSTER BY icao AS
SELECT CONCAT(icao, '-', FORMAT_DATE('%Y%m%d', fecha)) AS clima_id, *
FROM plata.clima_diario;

-- fact_vuelos (historial 2015, sin octubre)
CREATE OR REPLACE TABLE oro.fact_vuelos
PARTITION BY fecha_id
CLUSTER BY origen_id, aerolinea_id AS
SELECT
  v.fecha AS fecha_id,
  v.aerolinea_iata AS aerolinea_id,
  v.origen_iata AS origen_id,
  v.destino_iata AS destino_id,
  CONCAT(v.origen_icao, '-', FORMAT_DATE('%Y%m%d', v.fecha)) AS clima_origen_id,
  v.numero_vuelo,
  v.salida_programada,
  v.retraso_salida_min,
  v.retraso_llegada_min,
  v.retraso_sistema_aereo_min,
  v.retraso_seguridad_min,
  v.retraso_aerolinea_min,
  v.retraso_avion_anterior_min,
  v.retraso_clima_min,
  v.causa_principal,
  v.cancelado,
  v.motivo_cancelacion,
  v.desviado,
  v.a_tiempo,
  v.distancia_millas
FROM plata.vuelos_enriquecidos v;

-- fact_posiciones (vista: siempre muestra lo último que llegó por streaming)
CREATE OR REPLACE VIEW oro.fact_posiciones AS
SELECT
  DATE(ts_consulta) AS fecha_id,
  ts_consulta,
  icao24,
  callsign,
  a.aerolinea_id,
  aeropuerto_cercano_iata AS aeropuerto_id,
  distancia_km,
  latitud,
  longitud,
  altitud_m,
  velocidad_ms,
  en_tierra
FROM plata.posiciones p
LEFT JOIN oro.dim_aerolinea a ON a.aerolinea_icao = p.aerolinea_icao;

-- Vistas de puntualidad (OTP = % de vuelos con 15 min de retraso o menos)
CREATE OR REPLACE VIEW oro.otp_por_ruta AS
SELECT origen_id, destino_id, COUNT(*) AS vuelos,
       ROUND(100 * AVG(CAST(a_tiempo AS INT64)), 1) AS otp_pct,
       ROUND(AVG(retraso_llegada_min), 1) AS retraso_promedio_min
FROM oro.fact_vuelos
GROUP BY origen_id, destino_id
HAVING vuelos >= 300;

CREATE OR REPLACE VIEW oro.otp_por_aerolinea AS
SELECT f.aerolinea_id, a.nombre, d.mes, d.nombre_mes, COUNT(*) AS vuelos,
       ROUND(100 * AVG(CAST(f.a_tiempo AS INT64)), 1) AS otp_pct
FROM oro.fact_vuelos f
JOIN oro.dim_aerolinea a USING (aerolinea_id)
JOIN oro.dim_fecha d USING (fecha_id)
GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE VIEW oro.causas_retraso AS
SELECT f.origen_id, f.causa_principal, COUNT(*) AS vuelos_retrasados,
       COUNTIF(c.tormenta) AS con_tormenta, COUNTIF(c.nieve) AS con_nieve
FROM oro.fact_vuelos f
LEFT JOIN oro.dim_clima c ON c.clima_id = f.clima_origen_id
WHERE f.causa_principal IS NOT NULL
GROUP BY 1, 2;

-- Clima actual: último METAR de cada aeropuerto (tiempo real), con su condición de vuelo
CREATE OR REPLACE VIEW oro.clima_actual AS
SELECT
  a.aeropuerto_id,
  m.icao,
  a.nombre,
  a.ciudad,
  m.ts_observacion,
  m.temp_c,
  m.viento_nudos,
  m.rafaga_nudos,
  m.visibilidad,
  m.fenomenos,
  m.categoria_vuelo,
  m.categoria_vuelo IN ('IFR', 'LIFR') OR IFNULL(m.rafaga_nudos, 0) >= 25
    OR REGEXP_CONTAINS(IFNULL(m.fenomenos, ''), r'TS|SN|FZ') AS condicion_adversa,
  m.metar_crudo
FROM plata.metar m
JOIN oro.dim_aeropuerto a ON a.icao = m.icao
WHERE TRUE
QUALIFY ROW_NUMBER() OVER (PARTITION BY m.icao ORDER BY m.ts_observacion DESC) = 1;
