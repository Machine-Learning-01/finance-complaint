data "google_iam_policy" "admin" {
  binding {
    role = "roles/compute.admin"
    members = [
      "user:cloud@ineuron.ai",
    ]
  }
}

resource "google_compute_instance_iam_policy" "finance_compute_instance_iam_policy" {
  project       = google_compute_instance.finance_compute_instance.project
  zone          = google_compute_instance.finance_compute_instance.zone
  instance_name = google_compute_instance.finance_compute_instance.name
  policy_data   = data.google_iam_policy.admin.policy_data
}