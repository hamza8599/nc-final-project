

resource "aws_lambda_function" "lambda_ingestion_func" {
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_executive_role.arn
  handler       = "data_extraction.lambda_handler"  
  runtime       = var.python_runtime
  timeout = 600
  s3_bucket = aws_s3_bucket.lambda_code.id
  s3_key = aws_s3_object.lambda_code.key
  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python39:26"
  ]
}



data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../data_extraction.py" 
  output_path      = "${path.module}/../ingestion-lambda.zip"
}

