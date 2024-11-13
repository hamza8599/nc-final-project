resource "aws_sns_topic" "ingestion-topic" {
  name = "ingestion-topic"
  }

resource "aws_sns_topic_subscription" "email-target" {
  topic_arn = aws_sns_topic.ingestion-topic.arn
  protocol  = "email"
  endpoint  = "rz.dhothar@gmail.com" #TODO set up generic email address
}

resource "aws_cloudwatch_log_metric_filter" "error_filter" {
  name           = "ingestion-error-filter"
  pattern        = "Event recieved" #TODO update with final lambda logging message
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
  period              = 120 #TODO depends on eventbridge timer
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "We Failed" #TODO update to relevant alarm message
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.ingestion-topic.arn]
  ok_actions          = [aws_sns_topic.ingestion-topic.arn]
  insufficient_data_actions = []
  treat_missing_data = "notBreaching"
  
}