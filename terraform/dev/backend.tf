terraform {
  backend "s3" {
    bucket         = "insightflow-imba-group-state"
    key            = "state/terraform.tfstate"
    region         = "ap-southeast-2"
    dynamodb_table = "insightflow_imba_group"
    encrypt        = true
  }
}