terraform {
  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "~>5.0"
    }
  }
}

# TODO add backend for tf state bucket

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      name = "Dimensional Transformers Project"
      description = "Totesy Data Management Platform"
      environment = "Dev"
      date = "11/11/24"
    }
  }
}