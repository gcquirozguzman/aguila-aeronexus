terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # El estado vive en la capa bronce, en una carpeta propia (versionada por el bucket).
  backend "gcs" {
    bucket = "aguila-aeronexus-bronce"
    prefix = "terraform/estado"
  }
}

provider "google" {
  project = var.proyecto
  region  = var.region
}
