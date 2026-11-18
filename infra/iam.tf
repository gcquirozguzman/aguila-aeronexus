# Una cuenta de servicio por pieza y cada permiso sobre el recurso exacto que usa (mínimo privilegio).
# A nivel de proyecto solo quedan los roles que Google no permite dar por recurso.

locals {
  cuentas = {
    "sa-ingesta"  = "Ingesta Cloud Run"
    "sa-dataflow" = "Pipeline streaming Dataflow"
    "sa-dataproc" = "Batch Spark Dataproc"
    "sa-api"      = "Águila API (solo lectura)"
  }
  bronce_objetos = "projects/_/buckets/${google_storage_bucket.bronce.name}/objects/"
}

resource "google_service_account" "cuentas" {
  for_each     = local.cuentas
  account_id   = each.key
  display_name = each.value
}

# ------------------------------------------------------------------ roles de proyecto (no existen por recurso)
locals {
  roles_proyecto = {
    "sa-dataflow-worker" = { cuenta = "sa-dataflow", rol = "roles/dataflow.worker" }
    "sa-dataflow-jobs"   = { cuenta = "sa-dataflow", rol = "roles/bigquery.jobUser" }
    "sa-dataflow-logs"   = { cuenta = "sa-dataflow", rol = "roles/logging.logWriter" }
    "sa-dataproc-worker" = { cuenta = "sa-dataproc", rol = "roles/dataproc.worker" }
    "sa-dataproc-logs"   = { cuenta = "sa-dataproc", rol = "roles/logging.logWriter" }
    "sa-ingesta-logs"    = { cuenta = "sa-ingesta", rol = "roles/logging.logWriter" }
    "sa-api-jobs"        = { cuenta = "sa-api", rol = "roles/bigquery.jobUser" }
  }
}

resource "google_project_iam_member" "proyecto" {
  for_each = local.roles_proyecto
  project  = var.proyecto
  role     = each.value.rol
  member   = google_service_account.cuentas[each.value.cuenta].member
}

# ------------------------------------------------------------------ capa bronce
# sa-ingesta solo escribe y lee en sus carpetas (grabaciones/ y metar/); no puede borrar.
resource "google_storage_bucket_iam_member" "ingesta_escribe" {
  bucket = google_storage_bucket.bronce.name
  role   = "roles/storage.objectCreator"
  member = google_service_account.cuentas["sa-ingesta"].member
  condition {
    title      = "solo-grabaciones-y-metar"
    expression = "resource.name.startsWith(\"${local.bronce_objetos}grabaciones/\") || resource.name.startsWith(\"${local.bronce_objetos}metar/\")"
  }
}

resource "google_storage_bucket_iam_member" "ingesta_lee" {
  bucket = google_storage_bucket.bronce.name
  role   = "roles/storage.objectViewer"
  member = google_service_account.cuentas["sa-ingesta"].member
  condition {
    title      = "listar-y-leer-grabaciones"
    expression = "resource.type == \"storage.googleapis.com/Bucket\" || resource.name.startsWith(\"${local.bronce_objetos}grabaciones/\")"
  }
}

# Dataflow y Dataproc usan la capa bronce como zona de trabajo (temp, staging, código y Parquet de plata).
resource "google_storage_bucket_iam_member" "procesamiento" {
  for_each = toset(["sa-dataflow", "sa-dataproc"])
  bucket   = google_storage_bucket.bronce.name
  role     = "roles/storage.objectAdmin"
  member   = google_service_account.cuentas[each.value].member
}

# ------------------------------------------------------------------ Pub/Sub
resource "google_pubsub_topic_iam_member" "ingesta_publica" {
  for_each = toset(["posiciones", "metar"])
  topic    = google_pubsub_topic.topicos[each.value].name
  role     = "roles/pubsub.publisher"
  member   = google_service_account.cuentas["sa-ingesta"].member
}

resource "google_pubsub_topic_iam_member" "dataflow_errores" {
  topic  = google_pubsub_topic.topicos["errores"].name
  role   = "roles/pubsub.publisher"
  member = google_service_account.cuentas["sa-dataflow"].member
}

resource "google_pubsub_subscription_iam_member" "dataflow_lee" {
  for_each     = toset(["posiciones-sub", "metar-sub"])
  subscription = google_pubsub_subscription.suscripciones[each.value].name
  role         = "roles/pubsub.subscriber"
  member       = google_service_account.cuentas["sa-dataflow"].member
}

resource "google_pubsub_subscription_iam_member" "dataflow_ve" {
  for_each     = toset(["posiciones-sub", "metar-sub"])
  subscription = google_pubsub_subscription.suscripciones[each.value].name
  role         = "roles/pubsub.viewer"
  member       = google_service_account.cuentas["sa-dataflow"].member
}

# ------------------------------------------------------------------ secreto y jobs
resource "google_secret_manager_secret_iam_member" "ingesta_secreto" {
  secret_id = google_secret_manager_secret.opensky.id
  role      = "roles/secretmanager.secretAccessor"
  member    = google_service_account.cuentas["sa-ingesta"].member
}

# Cloud Scheduler dispara los jobs con sa-ingesta: solo puede ejecutar esos dos jobs.
resource "google_cloud_run_v2_job_iam_member" "scheduler_ejecuta" {
  for_each = google_cloud_run_v2_job.ingesta
  name     = each.value.name
  location = var.region
  role     = "roles/run.invoker"
  member   = google_service_account.cuentas["sa-ingesta"].member
}
