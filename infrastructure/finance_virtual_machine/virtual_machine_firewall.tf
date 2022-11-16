resource "google_compute_network" "finance_compute_network" {
  name                    = "finance-network"
  project                 = "industry-ready-finance"
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "finance_compute_firewall" {
  name          = "finance-firewall"
  project       = "industry-ready-finance"
  network       = google_compute_network.finance_compute_network.name
  source_ranges = ["0.0.0.0/0"]

  allow {
    protocol = "icmp"
  }

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }
}

