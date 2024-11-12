resource "aws_iam_role" "lambda_executive_role" {
  name = "lambda_exercutive_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      },
    ]
  })
  }


resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
    role = aws_iam_role.lambda_executive_role.id
    policy_arn = aws_iam_policy.s3_lambda_policy.arn
}



resource "aws_iam_policy" "s3_lambda_policy" {
  name        = "s3_lambda_policy"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject"
        ]
        Effect   = "Allow"
        Resource = ["${aws_s3_bucket.ingestion_bucket.arn}", 
        "${aws_s3_bucket.processed_bucket.arn}"
        ]
      },
    ]
  })
}

resource "aws_cloudwatch_log_group" "lambda_ingestion_group" {
  name = var.lambda_ingestion
}

resource "aws_iam_policy"  "lambda_cloudwatch_policy" {
  name = "${var.lambda_ingestion}-lambda-cloudwatch-policy"
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Effect   = "Allow"
        Resource = "arn:aws:logs:eu-west-2:137068251264:log-group:lambda-ingestion-handler:*"
         
      },
    ]
  })
}
    
resource "aws_iam_role_policy_attachment" "lambda_cloud_watch_policy" {
  role = aws_iam_role.lambda_executive_role.id
  policy_arn = aws_iam_policy.lambda_cloudwatch_policy.arn
}

resource "aws_iam_policy"  "lambda_sns_policy" {
  name = "${var.lambda_ingestion}-lambda-sns-policy"
    policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
    "Action" : [
        "sns:Publish",
        "sns:Subscribe"
    ],
    "Effect" : "Allow",
    "Resource" : [
         aws_sns_topic.ingestion-topic.arn
    ]
}]
    })
}

resource "aws_iam_role_policy_attachment" "lambda_sns_policy_attachemnet" {
  role = aws_iam_role.lambda_executive_role.id
  policy_arn = aws_iam_policy.lambda_sns_policy.arn
}