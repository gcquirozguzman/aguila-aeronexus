variable "proyecto" {
  description = "ID del proyecto de Google Cloud"
  type        = string
  default     = "aguila-aeronexus"
}

variable "region" {
  description = "Región de Cloud Run, Scheduler, Dataflow y Dataproc"
  type        = string
  default     = "us-central1"
}

variable "ubicacion_bigquery" {
  description = "Ubicación de los datasets de BigQuery"
  type        = string
  default     = "US"
}

variable "ingesta_activa" {
  description = "true reanuda los schedulers de ingesta (demo); false los deja en pausa para no gastar"
  type        = bool
  default     = false
}

variable "fuentes_aviones" {
  description = "Orden de las fuentes de aviones que prueba el job de ingesta"
  type        = string
  default     = "adsblol,opensky"
}

variable "imagen_ingesta" {
  description = "Imagen del job de ingesta (la construye gcloud run jobs deploy --source ingesta)"
  type        = string
  default     = "us-central1-docker.pkg.dev/aguila-aeronexus/cloud-run-source-deploy/aguila-ingesta-opensky"
}

variable "imagen_api" {
  description = "Imagen de la API (la construye gcloud run deploy --source api)"
  type        = string
  default     = "us-central1-docker.pkg.dev/aguila-aeronexus/cloud-run-source-deploy/aguila-api"
}
