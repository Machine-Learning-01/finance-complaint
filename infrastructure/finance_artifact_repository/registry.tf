provider "google" {
  project = var.project_name
}

resource "google_artifact_registry_repository" "finance_repo" {
  location      = var.artifact_repository_location
  repository_id = var.artifact_repository_repository_id
  format        = var.artifact_repository_format
}