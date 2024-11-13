resource "aws_s3_bucket" "ingestion_bucket" {
    bucket = var.ingestion_bucket
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket = var.processed_bucket
}

resource "aws_s3_bucket" "lambda_code" {
    bucket = var.lambda_bucket
}

resource "aws_s3_object" "lambda_code" {
  bucket = aws_s3_bucket.lambda_code.id
  key = "ingestion-lambda.zip"
  source = "${path.module}/../ingestion-lambda.zip"
}

resource "aws_s3_object" "layer_code" {
  bucket = aws_s3_bucket.lambda_code.id
  key = "lambda_layer.zip"
  source = "${path.module}/../lambda_layer.zip"
}