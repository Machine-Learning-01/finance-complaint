resource "google_service_account" "finance_service_account" {
  account_id   = "finance-service-account"
  display_name = "Finance Service Account"
  project      = "industry-ready-finance"
}

