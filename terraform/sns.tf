resource "aws_sns_topic" "ingestion-topic" {
  name = "ingestion-topic"
  }

resource "aws_sns_topic_subscription" "email-target" {
  topic_arn = aws_sns_topic.ingestion-topic.arn
  protocol  = "email"
  endpoint  = "dimensionaltransformers1@gmail.com" 
}

resource "aws_cloudwatch_log_metric_filter" "ingestion-error-filter" {
  name           = "ingestion-error-filter"
  pattern        = "Alert"
  log_group_name = "/aws/lambda/${var.lambda_ingestion}"

  metric_transformation {
    name      = "ErrorCount"
    namespace = "Lambda/test"
    value     = "1"
  }
}


resource "aws_cloudwatch_metric_alarm" "ingestion_sns_alarm" {
  alarm_name          = "ingestion_sns_alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorCount"
  namespace           = "Lambda/test"
  period              = 300 #TODO depends on eventbridge timer
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alert! ingestion phase needs your attention" 
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.ingestion-topic.arn]
  ok_actions          = [aws_sns_topic.ingestion-topic.arn]
  insufficient_data_actions = []
  treat_missing_data = "notBreaching"
  
}

# SNS for process lambda function 
# resource "aws_sns_topic" "process-topic" {
#   name = "process-topic"
#   }

# resource "aws_sns_topic_subscription" "email-target-for-process" {
#   topic_arn = aws_sns_topic.process-topic.arn
#   protocol  = "email"
#   endpoint  = "dimensionaltransformers1@gmail.com" 
# }

# resource "aws_cloudwatch_log_metric_filter" "process-error-filter" {
#   name           = "process-error-filter"
#   pattern        = "Alert"
#   log_group_name = "/aws/lambda/${var.lambda_process}"

#   metric_transformation {
#     name      = "ProcessErrorCount"
#     namespace = "Lambda/test-process"
#     value     = "1"
#   }
# }


# resource "aws_cloudwatch_metric_alarm" "process_sns_alarm" {
#   alarm_name          = "process_sns_alarm"
#   comparison_operator = "GreaterThanOrEqualToThreshold"
#   evaluation_periods  = 1
#   metric_name         = "ErrorCount"
#   namespace           = "Lambda/test"
#   period              = 300 #TODO depends on eventbridge timer
#   statistic           = "Sum"
#   threshold           = 1
#   alarm_description   = "Alert! process phase needs your attention" 
#   actions_enabled     = "true"
#   alarm_actions       = [aws_sns_topic.process-topic.arn]
#   ok_actions          = [aws_sns_topic.process-topic.arn]
#   insufficient_data_actions = []
#   treat_missing_data = "notBreaching"
  
# }