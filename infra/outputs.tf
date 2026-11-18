output "bucket_bronce" {
  value = google_storage_bucket.bronce.url
}

output "datasets" {
  value = [google_bigquery_dataset.bronce.dataset_id, google_bigquery_dataset.plata.dataset_id, google_bigquery_dataset.oro.dataset_id]
}

output "topicos" {
  value = [for t in google_pubsub_topic.topicos : t.name]
}

output "cuentas_de_servicio" {
  value = { for k, c in google_service_account.cuentas : k => c.email }
}

output "api_url" {
  value = google_cloud_run_v2_service.api.uri
}

output "ingesta_activa" {
  value = var.ingesta_activa
}
