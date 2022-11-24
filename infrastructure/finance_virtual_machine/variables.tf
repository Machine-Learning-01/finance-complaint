variable "finance_network_name" {
  default = "finance-network"
  type    = string
}

variable "finance_compute_network_name" {
  type    = string
  default = "finance-network"
}


variable "finance_project_name" {
  type    = string
  default = "industry-ready-finance"
}

variable "finance_auto_create_subnetworks" {
  type    = bool
  default = true
}

variable "finance_firewall_protocol_1" {
  type    = string
  default = "icmp"
}

variable "finance_protocol" {
  type    = string
  default = "tcp"
}

variable "finance_firewall_ports" {
  type    = list(string)
  default = ["22", "80", "443", "8080", "3000", "9100", "9090"]
}

variable "finance_firewall_name" {
  type    = string
  default = "finance-firewall"
}

variable "finance_firewall_source_ranges" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

variable "finance_iam_user_role" {
  type    = string
  default = "roles/compute.admin"
}


variable "finance_iam_user_email" {
  type    = string
  default = "user:cloud@ineuron.ai"
}

variable "finance_service_account_id" {
  type    = string
  default = "finance-service-account"
}

variable "finance_service_account_display_id" {
  type    = string
  default = "Finance Service Account"
}

variable "finance_compute_instance_name" {
  type    = string
  default = "test"
}

variable "finance_compute_instance_compute_type" {
  type    = string
  default = "c2-standard-4"
}

variable "finance_compute_instance_zone" {
  type    = string
  default = "us-central1-a"
}

variable "finance_compute_instance_base_image" {
  type    = string
  default = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "finance_network_interface" {
  type    = string
  default = "default"
}

variable "finance_compute_service_account_scopes" {
  type    = string
  default = "cloud-platform"
}
