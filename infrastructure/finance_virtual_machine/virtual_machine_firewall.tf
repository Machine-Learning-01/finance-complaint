resource "google_compute_network" "finance_compute_network" {
  name                    = var.finance_compute_network_name
  project                 = var.finance_project_name
  auto_create_subnetworks = var.finance_auto_create_subnetworks
}

resource "google_compute_firewall" "finance_compute_firewall" {
  name          = var.finance_firewall_name
  project       = var.finance_project_name
  network       = google_compute_network.finance_compute_network.name
  source_ranges = var.finance_firewall_source_ranges

  allow {
    protocol = var.finance_firewall_protocol_1
  }

  allow {
    protocol = var.finance_protocol
    ports    = var.finance_firewall_ports
  }
  target_tags = [var.finance_firewall_name]
}
