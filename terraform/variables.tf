variable "region" {
  default = "eu-west-2"
}

variable "ingestion_bucket" {
    default = "team-12-dimensional-transformers-ingestion-bucket"
}

variable "processed_bucket" {
  default = "team-12-dimensional-transformers-process-bucket"
}

variable "lambda_bucket" {
    default = "team-12-dimensional-transformers-lambda-bucket"
}

variable "lambda_ingestion" {
  default = "lambda-ingestion-handler-1"
}

variable "python_runtime" {
  type    = string
  default = "python3.9"
}