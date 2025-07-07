# IAM roles and policies for Data Ingestion

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "uploader" {
  name               = "ingest_uploader_role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_policy" "uploader_policy" {
  name   = "uploader_s3_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect   = "Allow",
      Action   = ["s3:ListBucket","s3:PutObject"],
      Resource = [
        "arn:aws:s3:::${var.bucket}",
        "arn:aws:s3:::${var.bucket}/${var.input_prefix}/*"
      ]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "upl_attach" {
  role       = aws_iam_role.uploader.name
  policy_arn = aws_iam_policy.uploader_policy.arn
}

resource "aws_iam_role" "glue_exec" {
  name               = "ingest_glue_exec_role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_policy" "glue_policy" {
  name   = "glue_ingest_policy"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject","s3:ListBucket","s3:PutObject"],
        Resource = [
          "arn:aws:s3:::${var.bucket}/${var.input_prefix}/*",
          "arn:aws:s3:::${var.bucket}/${var.output_prefix}/*"
        ]
      },
      {
        Effect   = "Allow",
        Action   = ["glue:GetTable","glue:GetDatabase"],
        Resource = "*"
      },
      {
        Effect   = "Allow",
        Action   = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_attach" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = aws_iam_policy.glue_policy.arn
}
