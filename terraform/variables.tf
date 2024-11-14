variable "region" {
  default = "eu-west-2"
}

variable "ingestion_bucket" {
    default = "dimensional-transformers-ingestion-bucket-1"
}

variable "processed_bucket" {
  default = "dimensional-transformers-process-bucket-1"
}

variable "lambda_bucket" {
    default = "dimensional-transformers-lambda-bucket-1"
}

variable "lambda_ingestion" {
  default = "lambda-ingestion-handler-1"
}