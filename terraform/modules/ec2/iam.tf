# IAM Role & Policy for Bastion EC2

#  fetch details about the current AWS account.
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "ec2_role" {
  name = "${var.env}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = ["ec2.amazonaws.com", "airflow.amazonaws.com"]
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_s3_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy" "secrets_access" {
  name = "secrets-access"
  role = aws_iam_role.ec2_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Effect   = "Allow",
        Resource = "arn:aws:secretsmanager:${var.region}:${data.aws_caller_identity.current.account_id}:secret:postgresql_dms-*"
      }
    ]
  })
}

resource "aws_iam_role_policy" "mwaa_access" {
  name = "mwaa-access"
  role = aws_iam_role.ec2_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "VisualEditor0",
        Effect = "Allow",
        Action = [
          "airflow:CreateCliToken",
          "airflow:GetEnvironment",
          "airflow:PublishMetrics",
          "airflow:CreateWebLoginToken"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs",
          "ec2:DescribeSecurityGroups"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "execute-api:Invoke"
        ],
        Resource = "arn:aws:execute-api:ap-southeast-2:794038230051:*/*/*/aws_mwaa/cli"
      }
    ]
  })
}

# 为EC2实例创建一个IAM实例配置文件（Instance Profile），用于将上面定义的IAM Role（ec2_role）实际关联到EC2实例上。

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "${var.env}-ec2-instance-profile"
  role = aws_iam_role.ec2_role.name
}