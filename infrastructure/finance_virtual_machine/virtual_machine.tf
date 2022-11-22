resource "google_compute_instance" "finance_compute_instance" {
  name         = var.finance_compute_instance_name
  machine_type = var.finance_compute_instance_compute_type
  zone         = var.finance_compute_instance_zone
  project      = var.finance_project_name
  boot_disk {
    initialize_params {
      image = var.finance_compute_instance_base_image
    }
  }

  network_interface {
    network = var.finance_network_interface

    access_config {
    }
  }

  service_account {
    email  = google_service_account.finance_service_account.email
    scopes = [var.finance_compute_service_account_scopes]
  }
  tags = var.finance_compute_firewall_tags
  depends_on = [
    google_compute_firewall.finance_compute_firewall
  ]
}

