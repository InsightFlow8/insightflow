# bucket, dynamodb_table根据实际情况替换

terraform {
  backend "s3" {
    bucket         = "insightflow-imba-group-state-22"
    key            = "state/terraform.tfstate"
    region         = "ap-southeast-2"
    dynamodb_table = "insightflow_imba_group"
    encrypt        = true
  }
}
