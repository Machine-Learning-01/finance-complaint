data "google_iam_policy" "finance_artifact_repository_iam_policy" {
  binding {
    role = var.artifact_repository_iam_role_binding
    members = [
      var.artifact_repository_iam_members
    ]
  }
}


resource "google_artifact_registry_repository_iam_policy" "finance_policy" {
  project     = google_artifact_registry_repository.finance_repo.project
  location    = google_artifact_registry_repository.finance_repo.location
  repository  = google_artifact_registry_repository.finance_repo.name
  policy_data = data.google_iam_policy.finance_artifact_repository_iam_policy.policy_data

}