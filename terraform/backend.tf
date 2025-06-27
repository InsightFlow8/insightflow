terraform {
  backend "s3" {
    bucket         = "imba-terraform-state-aaron"
    key            = "state/terraform.tfstate"
    region         = "ap-southeast-2"
    dynamodb_table = "imba-terraform-lock"
    encrypt        = true
  }
}