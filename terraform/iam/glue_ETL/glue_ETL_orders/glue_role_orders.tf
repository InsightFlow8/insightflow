# terraform/iam/glue_ETL/glue_role_orders.tf

resource "aws_iam_role" "glue_role_orders" {
  name = var.role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" },
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "glue_policy_orders" {
  name = "glue-policy-orders"
  role = aws_iam_role.glue_role_orders.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::imba-test-aaron-landing",
          "arn:aws:s3:::imba-test-aaron-landing/*",
          "arn:aws:s3:::imba-test-glue-etl-aaron",
          "arn:aws:s3:::imba-test-glue-etl-aaron/*"
        ]
      },
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
        Action = [
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetPartitions"
        ],
        Resource = "*"
      }
    ]
  })
}