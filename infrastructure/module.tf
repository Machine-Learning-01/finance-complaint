terraform {
  backend "s3" {
    bucket = "finance-tf-state"
    key    = "tf_state"
    region = "ap-south-1"
  }
}

module "finance_artifact_repository" {
  source = "./finance_artifact_repository"
}

module "finance_model_bucket" {
  source = "./finance_model_bucket"
}

module "finance_virtual_machine" {
  source = "./finance_virtual_machine"
}