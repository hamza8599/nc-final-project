

resource "aws_lambda_function" "lambda_ingestion_func" {
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_executive_role.arn
  handler       = "data_extraction.lambda_handler"  #TODO - name of final lambda func
  runtime       = var.python_runtime
  timeout = 600
  s3_bucket = aws_s3_bucket.lambda_code.id
  s3_key = aws_s3_object.lambda_code.key
  #layers = [aws_lambda_layer_version.requests_layer.arn]
  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:26"
  ]
}



data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../data_extraction.py" #TODO - name of final lambda func
  output_path      = "${path.module}/../ingestion-lambda.zip"
}

# data "archive_file" "layer" {
#   type = "zip"
#   source_dir = "${path.module}/../python"
#   output_path = "${path.module}/../lambda_layer.zip"
# }

# resource "aws_lambda_layer_version" "requests_layer" {
#   layer_name          = "requests_layer"
#   compatible_runtimes = [var.python_runtime]
#   #s3_bucket           = aws_s3_bucket.code_bucket.id
#   #s3_key              = "${path.module}/../lambda_layer.zip"
#   filename = "${path.module}/../lambda_layer.zip"
# }
