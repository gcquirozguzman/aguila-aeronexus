# API privada en Cloud Run: solo lectura sobre la capa oro con sa-api; sin acceso público.

resource "google_cloud_run_v2_service" "api" {
  name                = "aguila-api"
  location            = var.region
  ingress             = "INGRESS_TRAFFIC_ALL"
  deletion_protection = true

  template {
    service_account                  = google_service_account.cuentas["sa-api"].email
    max_instance_request_concurrency = 80
    timeout                          = "300s"

    scaling {
      max_instance_count = 2 # tope de costo
    }

    containers {
      image = var.imagen_api
      resources {
        limits            = { cpu = "1000m", memory = "512Mi" }
        startup_cpu_boost = true
        cpu_idle          = true
      }
    }
  }

  lifecycle {
    # la imagen la publica `gcloud run deploy --source api`; Terraform no la pisa
    ignore_changes = [template[0].containers[0].image, client, client_version, build_config, scaling]
  }
}
