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


# S3 Vectors permissions for AI chat agent
resource "aws_iam_role_policy" "s3_vectors_access" {
  name = "s3-vectors-access"
  role = aws_iam_role.ec2_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "S3VectorsBucketOperations",
        Effect = "Allow",
        Action = [
          "s3vectors:ListVectorBuckets",
          "s3vectors:CreateVectorBucket",
          "s3vectors:GetVectorBucket",
          "s3vectors:DeleteVectorBucket",
          "s3vectors:ListIndexes",
          "s3vectors:PutVectorBucketPolicy",
          "s3vectors:GetVectorBucketPolicy",
          "s3vectors:DeleteVectorBucketPolicy"
        ],
        Resource = [
          "arn:aws:s3vectors:${var.region}:${data.aws_caller_identity.current.account_id}:bucket/*"
        ]
      },
      {
        Sid    = "S3VectorsIndexOperations",
        Effect = "Allow",
        Action = [
          "s3vectors:CreateIndex",
          "s3vectors:GetIndex",
          "s3vectors:DeleteIndex",
          "s3vectors:QueryVectors",
          "s3vectors:PutVectors",
          "s3vectors:GetVectors",
          "s3vectors:ListVectors",
          "s3vectors:DeleteVectors"
        ],
        Resource = [
          "arn:aws:s3vectors:${var.region}:${data.aws_caller_identity.current.account_id}:bucket/*/index/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name = "s3-access"
  role = aws_iam_role.ec2_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid    = "S3BucketOperations",
        Effect = "Allow",
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation",
          "s3:GetBucketVersioning",
          "s3:GetBucketAcl",
          "s3:GetBucketPolicy",
          "s3:GetBucketCORS",
          "s3:GetBucketNotification",
          "s3:GetBucketLogging",
          "s3:GetBucketTagging",
          "s3:GetBucketWebsite",
          "s3:GetBucketRequestPayment",
          "s3:GetBucketAccelerateConfiguration",
          "s3:GetBucketReplication",
          "s3:GetBucketLifecycle",
          "s3:GetBucketLifecycleConfiguration",
          "s3:GetBucketEncryption",
          "s3:GetBucketIntelligentTieringConfiguration",
          "s3:GetBucketInventoryConfiguration",
          "s3:GetBucketMetricsConfiguration",
          "s3:GetBucketOwnershipControls",
          "s3:GetBucketPublicAccessBlock",
          "s3:GetBucketPolicyStatus",
          "s3:GetBucketObjectLockConfiguration",
          "s3:GetBucketVersioning",
          "s3:GetBucketWebsite",
          "s3:GetObject",
          "s3:GetObjectAcl",
          "s3:GetObjectAttributes",
          "s3:GetObjectLegalHold",
          "s3:GetObjectRetention",
          "s3:GetObjectTagging",
          "s3:GetObjectTorrent",
          "s3:GetObjectVersion",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionAttributes",
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionTagging",
          "s3:GetObjectVersionTorrent",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListBucketVersions",
          "s3:ListMultipartUploadParts",
          "s3:ListObjects",
          "s3:ListObjectsV2",
          "s3:ListObjectVersions"
        ],
        Resource = [
          "arn:aws:s3:::*",
          "arn:aws:s3:::*/*"
        ]
      },
      {
        Sid    = "S3WriteOperations",
        Effect = "Allow",
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:PutObjectLegalHold",
          "s3:PutObjectRetention",
          "s3:PutObjectTagging",
          "s3:PutObjectVersionTagging",
          "s3:PutBucketAcl",
          "s3:PutBucketCORS",
          "s3:PutBucketLogging",
          "s3:PutBucketNotification",
          "s3:PutBucketPolicy",
          "s3:PutBucketRequestPayment",
          "s3:PutBucketTagging",
          "s3:PutBucketVersioning",
          "s3:PutBucketWebsite",
          "s3:PutBucketAccelerateConfiguration",
          "s3:PutBucketReplication",
          "s3:PutBucketLifecycle",
          "s3:PutBucketLifecycleConfiguration",
          "s3:PutBucketEncryption",
          "s3:PutBucketIntelligentTieringConfiguration",
          "s3:PutBucketInventoryConfiguration",
          "s3:PutBucketMetricsConfiguration",
          "s3:PutBucketOwnershipControls",
          "s3:PutBucketPublicAccessBlock",
          "s3:PutBucketObjectLockConfiguration",
          "s3:PutBucketVersioning",
          "s3:PutBucketWebsite"
        ],
        Resource = [
          "arn:aws:s3:::*",
          "arn:aws:s3:::*/*"
        ]
      }
    ]
  })
}

# Add Athena permissions to EC2 role
resource "aws_iam_role_policy" "athena_access" {
  name = "athena-access"
  role = aws_iam_role.ec2_role.name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:ListQueryExecutions"
        ],
        Resource = "*"
      },
      {
        Effect = "Allow",
        Action = [
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
          "s3:AbortMultipartUpload",
          "s3:PutObject"
        ],
        Resource = [
          "arn:aws:s3:::*",
          "arn:aws:s3:::*/*"
        ]
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions"
        ],
        Resource = "*"
      }
    ]
  })
}

# 为EC2实例创建一个IAM实例配置文件（Instance Profile），用于将上面定义的IAM Role（ec2_role）实际关联到EC2实例上。

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "${var.env}-ec2-instance-profile"
  role = aws_iam_role.ec2_role.name
}