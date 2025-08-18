resource "aws_iam_role" "batch_ingestion_lambda_role" {
  name = "batch_ingestion_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "batch_ingestion_lambda_policy" {
  name = "batch_ingestion_lambda_policy"
  role = aws_iam_role.batch_ingestion_lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_bucket}",
          "arn:aws:s3:::${var.raw_bucket}/*",
          "arn:aws:s3:::${var.clean_bucket}",
          "arn:aws:s3:::${var.clean_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:*:secret:snowflake-insightflow*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "glue:CreatePartition",
          "glue:GetPartition",
          "glue:GetTable",
          "glue:GetDatabase",
          "glue:BatchGetPartition"
        ]
        Resource = [
          "arn:aws:glue:${var.aws_region}:*:catalog",
          var.raw_database_arn,
          "arn:aws:glue:${var.aws_region}:*:table/${var.raw_database_name}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeVpcs",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups"
        ]
        Resource = "*"
      }
    ]
  })
}


# IAM role for the batch ingestion Lambda function
