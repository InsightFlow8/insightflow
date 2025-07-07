# =============================
# Terraform: Trigger for Glue Crawlers via EventBridge & Lambda
# Lambda zip: terraform/assets/2.1_glue_crawler_raw_scheduler.zip
# =============================

resource "aws_iam_role" "glue_trigger_raw_lambda_role" {
  name = "glue-crawler-raw-trigger-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_trigger_raw_lambda_policy" {
  name = "glue-trigger-lambda-policy"
  role = aws_iam_role.glue_trigger_raw_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow",
        Action = ["glue:StartCrawler"],
        Resource = [
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/crawler_raw_orders",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/crawler_raw_products",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/crawler_raw_departments",
          "arn:aws:glue:${var.aws_region}:${data.aws_caller_identity.current.account_id}:crawler/crawler_raw_aisles"
        ]
      }
    ]
  })
}

resource "aws_lambda_function" "glue_trigger_raw_lambda" {
  function_name    = "glue_crawler_trigger_raw_lambda"
  handler          = "lambda_trigger_raw_crawlers.lambda_handler"
  runtime          = "python3.13"
  role             = aws_iam_role.glue_trigger_raw_lambda_role.arn
  timeout          = 60
  memory_size      = 128
  filename         = "./assets/2.1_glue_crawler_raw_scheduler.zip"
  source_code_hash = filebase64sha256("./assets/2.1_glue_crawler_raw_scheduler.zip")

  environment {
    variables = {
      CRAWLER_NAMES = var.raw_crawler_names
    }
  }
}

resource "aws_cloudwatch_event_rule" "trigger_glue_raw_monthly" {
  name                = "run_all_crawlers_monthly"
  description         = "Trigger Lambda to start Glue Crawlers on raw data monthly on 30th at 01:00 Sydney time"
  schedule_expression = "cron(0 15 30 * ? *)" # UTC 15:00 = Sydney 01:00
}

resource "aws_cloudwatch_event_target" "lambda_event_target" {
  rule      = aws_cloudwatch_event_rule.trigger_glue_raw_monthly.name
  target_id = "lambda-glue-trigger-raw"
  arn       = aws_lambda_function.glue_trigger_raw_lambda.arn
}

resource "aws_lambda_permission" "allow_eventbridge_crawler_raw" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.glue_trigger_raw_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trigger_glue_raw_monthly.arn
}
