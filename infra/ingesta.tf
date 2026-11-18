# Ingesta en la nube: dos Cloud Run Jobs (contenedor de ingesta/) disparados por Cloud Scheduler.

locals {
  jobs = {
    "aguila-ingesta-opensky" = { fuente = "aviones", programa = "*/2 * * * *", scheduler = "aguila-opensky" }
    "aguila-ingesta-metar"   = { fuente = "metar", programa = "*/5 * * * *", scheduler = "aguila-metar" }
  }
}

resource "google_cloud_run_v2_job" "ingesta" {
  for_each            = local.jobs
  name                = each.key
  location            = var.region
  deletion_protection = true

  template {
    task_count = 1
    template {
      service_account       = google_service_account.cuentas["sa-ingesta"].email
      max_retries           = 1
      timeout               = "300s"
      execution_environment = "EXECUTION_ENVIRONMENT_GEN2"

      containers {
        image = var.imagen_ingesta
        resources {
          limits = { cpu = "1", memory = "512Mi" }
        }
        env {
          name  = "FUENTE"
          value = each.value.fuente
        }
        env {
          name  = "PROYECTO"
          value = var.proyecto
        }
        dynamic "env" {
          for_each = each.value.fuente == "aviones" ? [1] : []
          content {
            name  = "AVIONES_FUENTES"
            value = var.fuentes_aviones
          }
        }
        dynamic "env" {
          for_each = each.value.fuente == "aviones" ? [1] : []
          content {
            name = "OPENSKY_CREDENCIAL"
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.opensky.secret_id
                version = "1"
              }
            }
          }
        }
      }
    }
  }

  lifecycle {
    # la imagen la publica `gcloud run jobs deploy --source ingesta`; Terraform no la pisa
    ignore_changes = [template[0].template[0].containers[0].image, client, client_version]
  }
}

resource "google_cloud_scheduler_job" "ingesta" {
  for_each         = local.jobs
  name             = each.value.scheduler
  region           = var.region
  schedule         = each.value.programa
  time_zone        = "Etc/UTC"
  attempt_deadline = "180s"
  paused           = !var.ingesta_activa

  retry_config {
    max_backoff_duration = "3600s"
    max_doublings        = 5
    max_retry_duration   = "0s"
    min_backoff_duration = "5s"
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.proyecto}/locations/${var.region}/jobs/${each.key}:run"
    oauth_token {
      service_account_email = google_service_account.cuentas["sa-ingesta"].email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_cloud_run_v2_job_iam_member.scheduler_ejecuta]
}
