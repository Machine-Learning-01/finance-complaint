terraform {
  backend "s3" {
    bucket = "finance-tf-state"
    key    = "tf_state"
    region = "ap-south-1"
  }
}

module "finance_artifact_repository" {
  source = "./artifact_repository"
}
