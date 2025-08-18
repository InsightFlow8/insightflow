resource "aws_iam_role" "glue_data_clean" {
  name               = var.iam_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy.json
  tags               = var.tags
}

data "aws_iam_policy_document" "glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_data_clean.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.glue_data_clean.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}


# CloudWatch Logs 权限 - 使用自定义策略确保完整权限
resource "aws_iam_role_policy" "cloudwatch_logs_custom" {
  name = "${var.iam_role_name}-cloudwatch-logs"
  role = aws_iam_role.glue_data_clean.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogGroups",
          "logs:DescribeLogStreams"
        ]
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*",
          "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*:*"
        ]
      }
    ]
  })
}
