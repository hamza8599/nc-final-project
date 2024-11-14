variable "region" {
  default = "eu-west-2"
}

variable "ingestion_bucket" {
    default = "team10-dimensional-transformers-ingestion-bucket"
}

variable "processed_bucket" {
  default = "team10-dimensional-transformers-process-bucket"
}

variable "lambda_bucket" {
    default = "team10-dimensional-transformers-lambda-bucket"
}

variable "lambda_ingestion" {
  default = "lambda-ingestion-handler"
}

variable "python_runtime" {
  type    = string
  default = "python3.9"
}