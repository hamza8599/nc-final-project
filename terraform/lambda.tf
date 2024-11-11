



resource "aws_lambda_function" "lambda_ingestion_func" {
  filename      = "lambda_function_payload.zip"
  function_name = var.lambda_ingestion
  role          = aws_iam_role.lambda_executive_role.arn
  handler       = "index.test"  #TODO
  runtime       = "python3.12"
  timeout = 10
  s3_bucket = aws_s3_bucket.lambda_code.id
  s3_key = aws_s3_bucket.lambda_code.key

}

resource "aws_cloudwatch_event_rule" "scheduler" {
  name = "lambda_5_minutes"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
    target_id = "lambda_target"
    rule = aws_cloudwatch_event_rule.scheduler.name
    arn = aws_lambda_function.lambda_ingestion_func.arn # TODO
}



resource "aws_cloudwatch_event_rule" "scheduler" {

  name = "lambda_5_minutes"

  schedule_expression = "rate(5 minutes)"

}
 
resource "aws_cloudwatch_event_target" "lambda_target" {

  target_id = "lambda_target"

  rule      = aws_cloudwatch_event_rule.scheduler.name

  arn       = aws_lambda_function.quote_handler.arn

}
 
resource "aws_lambda_permission" "allow_cloudwatch_events" {

    statement_id = "AllowExecutionFromCloudWatch"

    action = "lambda:InvokeFunction"

    function_name = aws_lambda_function.quote_handler.function_name #TODO

    principal = "events.amazonaws.com"

    source_arn = aws_cloudwatch_event_rule.scheduler.arn

}
 