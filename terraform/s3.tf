resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = var.ingestion_bucket
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = var.processed_bucket
}

resource "aws_s3_bucket" "lambda_code" {
    bucket = var.lambda_bucket
}