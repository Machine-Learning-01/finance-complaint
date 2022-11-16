resource "google_compute_instance" "finance_compute_instance" {
  name         = "test"
  machine_type = "e2-medium"
  zone         = "us-central1-a"
  project      = "industry-ready-finance"
  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2004-lts"
    }
  }

  network_interface {
    network = "default"

    access_config {
    }
  }

  service_account {
    email  = google_service_account.finance_service_account.email
    scopes = ["cloud-platform"]
  }
  tags = ["http-server", "https-server"]
  depends_on = [
    google_compute_firewall.finance_compute_firewall
  ]

}
