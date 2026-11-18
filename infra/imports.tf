# La plataforma se creó primero en la consola. Estos bloques la adoptan en Terraform sin recrear nada;
# en un proyecto nuevo se borra este archivo y `terraform apply` crea todo desde cero.

import {
  for_each = toset(local.apis)
  to       = google_project_service.apis[each.value]
  id       = "${var.proyecto}/${each.value}"
}

import {
  to = google_storage_bucket.bronce
  id = "${var.proyecto}-bronce"
}

import {
  to = google_bigquery_dataset.bronce
  id = "projects/${var.proyecto}/datasets/bronce"
}

import {
  to = google_bigquery_dataset.plata
  id = "projects/${var.proyecto}/datasets/plata"
}

import {
  to = google_bigquery_dataset.oro
  id = "projects/${var.proyecto}/datasets/oro"
}

import {
  for_each = toset(["posiciones", "metar", "errores"])
  to       = google_pubsub_topic.topicos[each.value]
  id       = "projects/${var.proyecto}/topics/${each.value}"
}

import {
  for_each = local.suscripciones
  to       = google_pubsub_subscription.suscripciones[each.key]
  id       = "projects/${var.proyecto}/subscriptions/${each.key}"
}

import {
  for_each = local.cuentas
  to       = google_service_account.cuentas[each.key]
  id       = "projects/${var.proyecto}/serviceAccounts/${each.key}@${var.proyecto}.iam.gserviceaccount.com"
}

import {
  to = google_secret_manager_secret.opensky
  id = "projects/${var.proyecto}/secrets/opensky-credentials"
}

import {
  for_each = local.jobs
  to       = google_cloud_run_v2_job.ingesta[each.key]
  id       = "projects/${var.proyecto}/locations/${var.region}/jobs/${each.key}"
}

import {
  for_each = local.jobs
  to       = google_cloud_scheduler_job.ingesta[each.key]
  id       = "projects/${var.proyecto}/locations/${var.region}/jobs/${each.value.scheduler}"
}

import {
  to = google_cloud_run_v2_service.api
  id = "projects/${var.proyecto}/locations/${var.region}/services/aguila-api"
}

import {
  to = google_dataplex_aspect_type.gobierno
  id = "projects/${var.proyecto}/locations/global/aspectTypes/gobierno-aguila"
}
