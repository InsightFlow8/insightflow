# Data sources for current region and account
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "sfn_role" {
  name = "StepFunctionsExecutionRole-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Principal = {
          Service = "states.amazonaws.com"
        },
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_policy" "sfn_policy" {
  name        = "StepFunctionsPolicy-${var.env}"
  description = "Policy for Step Functions to execute tasks"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "glue:StartCrawler",
          "glue:GetCrawler",
          "glue:GetCrawlerRuns",
          "glue:GetCrawlerMetrics"
        ],
        Resource = [
          for crawler_name in var.crawler_names : "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:crawler/${crawler_name}"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:StartJobRun",
          "glue:GetJobRun",
          "glue:GetJobRuns"
        ],
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.etl_glue_job_name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:job/${var.clean_glue_job_name}"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "sagemaker:CreateTrainingJob",
          "sagemaker:DescribeTrainingJob",
          "sagemaker:CreateModel",
          "sagemaker:CreateEndpointConfig",
          "sagemaker:CreateEndpoint",
          "sagemaker:DescribeEndpoint",
          "sagemaker:AddTags",
          "sagemaker:ListTags",
          "sagemaker:StopTrainingJob"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "events:PutTargets",
          "events:PutRule",
          "events:DescribeRule",
          "events:DeleteRule",
          "events:RemoveTargets"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "iam:PassRole"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "lambda:InvokeFunction",
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:ListVersionsByFunction",
          "lambda:ListAliases"
        ],
        Resource = [
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.batch_ingestion_lambda_name}",
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:${var.batch_ingestion_lambda_name}:*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "lambda:InvokeFunction"
        ],
        Resource = [
          "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "sfn_policy_attach" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_policy.arn
}

resource "aws_sfn_state_machine" "data_pipeline" {
  name     = "data-pipeline-${var.env}"
  role_arn = aws_iam_role.sfn_role.arn

  definition = templatefile("${path.module}/state-machine.json", {
    batch_ingestion_lambda_name = var.batch_ingestion_lambda_name
    clean_glue_job_name         = var.clean_glue_job_name
    etl_glue_job_name           = var.etl_glue_job_name
    crawler_name_0              = var.crawler_names[0]
    crawler_name_1              = var.crawler_names[1]
    crawler_name_2              = var.crawler_names[2]
    crawler_name_3              = var.crawler_names[3]
    crawler_name_4              = var.crawler_names[4]
    env                         = var.env
  })

  tags = {
    Name        = "data-pipeline-${var.env}"
    Environment = var.env
  }
}


