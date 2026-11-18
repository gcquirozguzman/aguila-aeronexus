# Plantilla de gobierno en Knowledge Catalog (Dataplex). Los valores por tabla los aplica gobierno/etiquetar.py.

resource "google_dataplex_aspect_type" "gobierno" {
  aspect_type_id    = "gobierno-aguila"
  location          = "global"
  display_name      = "Gobierno Águila"
  description       = "Dueño, custodio, clasificación, calidad y retención de cada tabla de Águila"
  metadata_template = file("${path.module}/../gobierno/plantilla.json")

  lifecycle {
    ignore_changes = [metadata_template] # Dataplex devuelve la plantilla normalizada
  }
}
