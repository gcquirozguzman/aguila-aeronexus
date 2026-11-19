-- Capa de Machine Learning: probabilidad de que un vuelo llegue con más de 15 minutos de retraso.
-- Variables que se conocen antes de despegar: calendario, hora, aerolínea, ruta, distancia y el clima del día en el
-- origen (en producción, el pronóstico). Entrena con enero a septiembre de 2015 y evalúa con noviembre y diciembre,
-- meses que el modelo no vio. Octubre no está en los datos.

CREATE OR REPLACE VIEW `aguila-aeronexus.oro.ml_vuelos` AS
SELECT
  NOT v.a_tiempo                                   AS retrasado,
  f.mes                                             AS mes,
  CAST(f.mes AS STRING)                             AS mes_cat,
  CAST(f.dia_semana AS STRING)                      AS dia_semana,
  f.temporada_alta, f.feriado,
  SAFE_CAST(SUBSTR(LPAD(v.salida_programada, 4, '0'), 1, 2) AS INT64) AS hora_salida,
  v.aerolinea_id, v.origen_id, v.destino_id, v.distancia_millas,
  c.temp_f, c.viento_nudos, c.visibilidad_millas, c.precipitacion_pulg, c.tormenta, c.nieve, c.niebla,
  f.mes >= 11                                       AS es_evaluacion
FROM `aguila-aeronexus.oro.fact_vuelos` v
JOIN `aguila-aeronexus.oro.dim_fecha` f USING (fecha_id)
LEFT JOIN `aguila-aeronexus.oro.dim_clima` c ON c.clima_id = v.clima_origen_id
WHERE NOT v.cancelado AND NOT v.desviado AND v.a_tiempo IS NOT NULL;

CREATE OR REPLACE MODEL `aguila-aeronexus.oro.modelo_retraso`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['retrasado'],
  auto_class_weights = TRUE,          -- hay muchos más vuelos a tiempo que retrasados
  data_split_method = 'CUSTOM',
  data_split_col = 'es_evaluacion',   -- TRUE = noviembre y diciembre, solo para evaluar
  max_iterations = 10,
  enable_global_explain = TRUE
) AS
SELECT * EXCEPT (mes) FROM `aguila-aeronexus.oro.ml_vuelos`;

-- Calidad sobre noviembre y diciembre
SELECT * FROM ML.EVALUATE(MODEL `aguila-aeronexus.oro.modelo_retraso`);

-- Qué variables pesan más
SELECT * FROM ML.GLOBAL_EXPLAIN(MODEL `aguila-aeronexus.oro.modelo_retraso`) ORDER BY attribution DESC LIMIT 10;

-- Ejemplo: los vuelos de O'Hare (ORD) con más riesgo de retraso en diciembre, por ruta, aerolínea y hora
SELECT origen_id, destino_id, aerolinea_id, hora_salida,
       COUNT(*) AS vuelos,
       ROUND(AVG(p.prob) * 100, 1) AS riesgo_retraso_pct,
       ROUND(COUNTIF(retrasado) / COUNT(*) * 100, 1) AS retraso_real_pct
FROM ML.PREDICT(MODEL `aguila-aeronexus.oro.modelo_retraso`,
       (SELECT * EXCEPT (mes) FROM `aguila-aeronexus.oro.ml_vuelos` WHERE origen_id = 'ORD' AND mes = 12)),
     UNNEST(predicted_retrasado_probs) p
WHERE p.label = TRUE
GROUP BY origen_id, destino_id, aerolinea_id, hora_salida
HAVING vuelos >= 20
ORDER BY riesgo_retraso_pct DESC
LIMIT 10;
