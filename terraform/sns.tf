resource "aws_sns_topic" "ingestion-topic" {
  name = "ingestion-topic"
  }

resource "aws_sns_topic_subscription" "email-target" {
  topic_arn = aws_sns_topic.ingestion-topic.arn
  protocol  = "email"
  endpoint  = "rz.dhothar@gmail.com"
}

resource "aws_cloudwatch_log_metric_filter" "error_filter" {
  name           = "ingestion-error-filter"
  pattern        = "Event recieved" #TODO
  log_group_name = aws_cloudwatch_log_group.lambda_ingestion_group.name

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
  period              = 60
  statistic           = "Average"
  threshold           = 1
  alarm_description   = "We Failed"
  actions_enabled     = "true"
  alarm_actions       = [aws_sns_topic.ingestion-topic.arn]
  ok_actions          = [aws_sns_topic.ingestion-topic.arn]
  
}