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

-- dim_fecha (2015 para el historial y 2026 para el dato en vivo, cada año con sus feriados)
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
        '2015-11-11', '2015-11-26', '2015-12-25',
        -- feriados federales de EE.UU. 2026 (4 de julio cae sábado: se observa el viernes 3)
        '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25', '2026-06-19', '2026-07-03', '2026-09-07',
        '2026-10-12', '2026-11-11', '2026-11-26', '2026-12-25') AS feriado
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
-- Grano: un avión en una consulta real a OpenSky.
--   * Sin modo 'reproduccion': son grabaciones reenviadas cuando la PC está apagada, no datos nuevos.
--     (modo NULL = filas de las primeras pruebas, anteriores a esa columna; eran en vivo.)
--   * Pub/Sub entrega "al menos una vez" y el seek reenvía mensajes: queda una sola fila por
--     (icao24, ts_consulta), la primera que procesó Dataflow.
--   * ts_posicion muestra si OpenSky repitió la última posición conocida en varias consultas.
CREATE OR REPLACE VIEW oro.fact_posiciones AS
SELECT
  DATE(ts_consulta) AS fecha_id,
  ts_consulta,
  ts_posicion,
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
LEFT JOIN oro.dim_aerolinea a ON a.aerolinea_icao = p.aerolinea_icao
WHERE IFNULL(p.modo, 'vivo') = 'vivo'
QUALIFY ROW_NUMBER() OVER (PARTITION BY p.icao24, p.ts_consulta ORDER BY p.procesado_en) = 1;

-- Vistas de puntualidad (OTP = % de vuelos con 15 min de retraso o menos)

-- Vista materializada: BigQuery guarda los totales por ruta ya calculados y los actualiza solo cuando
-- cambia fact_vuelos; cada consulta de OTP por ruta lee unos miles de filas en lugar de 5.3 millones.
CREATE OR REPLACE MATERIALIZED VIEW oro.mv_otp_ruta AS
SELECT origen_id, destino_id,
       COUNT(*) AS vuelos,
       COUNTIF(a_tiempo) AS vuelos_a_tiempo,
       COUNT(a_tiempo) AS vuelos_con_dato,
       SUM(retraso_llegada_min) AS suma_retraso_min,
       COUNT(retraso_llegada_min) AS vuelos_con_retraso
FROM oro.fact_vuelos
GROUP BY origen_id, destino_id;

CREATE OR REPLACE VIEW oro.otp_por_ruta AS
SELECT origen_id, destino_id, vuelos,
       ROUND(100 * SAFE_DIVIDE(vuelos_a_tiempo, vuelos_con_dato), 1) AS otp_pct,
       ROUND(SAFE_DIVIDE(suma_retraso_min, vuelos_con_retraso), 1) AS retraso_promedio_min
FROM oro.mv_otp_ruta
WHERE vuelos >= 300;

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

-- SLI de latencia del dato en vivo, por consulta (solo modo vivo; la reproducción repite datos viejos).
--   extremo a extremo: desde que el avión emitió su posición (ts_posicion, ADS-B) hasta que Dataflow la
--                      procesó y la envió a BigQuery (procesado_en)
--   plataforma:        desde la consulta a OpenSky (ts_consulta) hasta procesado_en; es lo que controla Águila
--   posición vieja:    aviones cuya última posición ya tenía 150 s o más cuando OpenSky la entregó
-- SLO (meta de la Entrega 1): latencia menor a 3 minutos.
CREATE OR REPLACE VIEW oro.sli_latencia AS
SELECT
  ts_consulta,
  COUNT(*) AS aviones,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts_posicion, SECOND), 100)[OFFSET(50)] AS e2e_p50_s,
  APPROX_QUANTILES(TIMESTAMP_DIFF(procesado_en, ts_posicion, SECOND), 100)[OFFSET(95)] AS e2e_p95_s,
  ROUND(100 * COUNTIF(TIMESTAMP_DIFF(procesado_en, ts_posicion, SECOND) < 180) / COUNT(*), 1) AS e2e_pct_menor_3min,
  MAX(TIMESTAMP_DIFF(procesado_en, ts_consulta, SECOND)) AS plataforma_max_s,
  ROUND(100 * COUNTIF(TIMESTAMP_DIFF(ts_consulta, ts_posicion, SECOND) >= 150) / COUNT(*), 1) AS pct_posicion_vieja
FROM plata.posiciones
WHERE modo = 'vivo' AND ts_posicion IS NOT NULL
GROUP BY ts_consulta;
