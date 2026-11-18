# Infraestructura base de Águila: APIs, capa bronce, datasets, Pub/Sub y secreto.
# Identidades y permisos en iam.tf; ingesta en ingesta.tf; API en api.tf; gobierno en gobierno.tf.

locals {
  apis = [
    "artifactregistry.googleapis.com", "bigquery.googleapis.com", "cloudbuild.googleapis.com",
    "cloudscheduler.googleapis.com", "dataflow.googleapis.com", "dataplex.googleapis.com",
    "dataproc.googleapis.com", "iam.googleapis.com", "pubsub.googleapis.com", "run.googleapis.com",
    "secretmanager.googleapis.com", "storage.googleapis.com",
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.apis)
  service            = each.value
  disable_on_destroy = false
}

# ------------------------------------------------------------------ capa bronce (Cloud Storage)
resource "google_storage_bucket" "bronce" {
  name                        = "${var.proyecto}-bronce"
  location                    = upper(var.region)
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false

  versioning {
    enabled = true
  }

  soft_delete_policy {
    retention_duration_seconds = 604800 # 7 días
  }

  # a los 90 días el dato crudo pasa a almacenamiento más barato
  lifecycle_rule {
    condition {
      age                   = 90
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # las versiones reemplazadas se borran a los 90 días
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 90
      with_state                 = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }
}

# ------------------------------------------------------------------ BigQuery: bronce, plata y oro
resource "google_bigquery_dataset" "bronce" {
  dataset_id                 = "bronce"
  location                   = var.ubicacion_bigquery
  description                = "Capa bronce - Plataforma Aguila"
  max_time_travel_hours      = "168"
  delete_contents_on_destroy = false
}

# plata: la escribe Dataflow; la API no la lee directo, solo a través de las vistas de oro (vista autorizada)
resource "google_bigquery_dataset" "plata" {
  dataset_id                 = "plata"
  location                   = var.ubicacion_bigquery
  description                = "Capa plata - Plataforma Aguila"
  max_time_travel_hours      = "168"
  delete_contents_on_destroy = false

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "WRITER"
    user_by_email = google_service_account.cuentas["sa-dataflow"].email
  }
  access {
    dataset {
      dataset {
        project_id = var.proyecto
        dataset_id = "oro"
      }
      target_types = ["VIEWS"]
    }
  }
}

# oro: lo único que lee la API
resource "google_bigquery_dataset" "oro" {
  dataset_id                 = "oro"
  location                   = var.ubicacion_bigquery
  description                = "Capa oro - Plataforma Aguila"
  max_time_travel_hours      = "168"
  delete_contents_on_destroy = false

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }
  access {
    role          = "READER"
    special_group = "projectReaders"
  }
  access {
    role          = "READER"
    user_by_email = google_service_account.cuentas["sa-api"].email
  }
}

# ------------------------------------------------------------------ Pub/Sub
resource "google_pubsub_topic" "topicos" {
  for_each = toset(["posiciones", "metar", "errores"])
  name     = each.value
}

locals {
  # retención de 7 días y mensajes confirmados retenidos: permite reprocesar con seek
  suscripciones = {
    "posiciones-sub" = { topico = "posiciones", ack = 60, retener = true }
    "metar-sub"      = { topico = "metar", ack = 60, retener = true }
    "errores-sub"    = { topico = "errores", ack = 10, retener = false }
  }
}

resource "google_pubsub_subscription" "suscripciones" {
  for_each                   = local.suscripciones
  name                       = each.key
  topic                      = google_pubsub_topic.topicos[each.value.topico].id
  ack_deadline_seconds       = each.value.ack
  message_retention_duration = "604800s"
  retain_acked_messages      = each.value.retener

  expiration_policy {
    ttl = "" # nunca vence aunque el pipeline esté apagado
  }
}

# ------------------------------------------------------------------ secreto (el valor se carga fuera de Terraform)
resource "google_secret_manager_secret" "opensky" {
  secret_id = "opensky-credentials"
  replication {
    auto {}
  }
}
