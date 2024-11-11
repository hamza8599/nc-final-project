variable "region" {
  default = "eu-west-2"
}

variable "ingestion_bucket" {
    default = "dimensional-transformers-ingestion-bucket"
}

variable "processed_bucket" {
  default = "dimensional-transformers-process-bucket"
}

variable "lambda_bucket" {
    default = "dimensional-transformers-lambda-bucket"
}