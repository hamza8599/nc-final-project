

resource "aws_lambda_function" "lambda_ingestion_func" {
  #filename      = "lambda_function_payload.zip"
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_executive_role.arn
  handler       = "test.lambda_handler"  #TODO
  runtime       = "python3.12"
  timeout = 10
  s3_bucket = aws_s3_bucket.lambda_code.id
  s3_key = aws_s3_object.lambda_code.key
}

resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "lambda_5_minutes"
  schedule_expression = "rate(2 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
    target_id = "lambda_target"
    rule = aws_cloudwatch_event_rule.scheduler.name
    arn = aws_lambda_function.lambda_ingestion_func.arn # TODO
}
 
resource "aws_lambda_permission" "allow_cloudwatch_events" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.lambda_ingestion_func.function_name #TODO
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.scheduler.arn

}

data "archive_file" "lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../test.py"
  output_path      = "${path.module}/../function.zip"
}
