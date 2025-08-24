# IAM Roles & Policies for VPC-related services
# ---------------------------------------------
# 本文件集中管理与VPC相关的IAM资源，便于权限审计和后续维护。
# 包含EC2 Bastion、RDS PostgreSQL、DMS、Glue等服务的最小权限Role/Policy。

# EC2 Bastion Host IAM Role & Policy
resource "aws_iam_role" "bastion_ec2_role" {
  name               = "${var.env}-bastion-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.bastion_ec2_assume_role.json
}

data "aws_iam_policy_document" "bastion_ec2_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy_attachment" "bastion_ec2_ssm" {
  role       = aws_iam_role.bastion_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# RDS PostgreSQL IAM Role (for enhanced monitoring, if needed)
# resource "aws_iam_role" "rds_monitoring_role" {
#   name = "${var.env}-rds-monitoring-role"
#   assume_role_policy = data.aws_iam_policy_document.rds_monitoring_assume_role.json
# }

# data "aws_iam_policy_document" "rds_monitoring_assume_role" {
#   statement {
#     actions = ["sts:AssumeRole"]
#     principals {
#       type        = "Service"
#       identifiers = ["monitoring.rds.amazonaws.com"]
#     }
#   }
# }

# resource "aws_iam_role_policy_attachment" "rds_monitoring_policy" {
#   role       = aws_iam_role.rds_monitoring_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonRDSEnhancedMonitoringRole"
# }

# DMS Replication Instance IAM Role
# resource "aws_iam_role" "dms_vpc_role" {
#   name = "${var.env}-dms-vpc-role"
#   assume_role_policy = data.aws_iam_policy_document.dms_vpc_assume_role.json
# }

# data "aws_iam_policy_document" "dms_vpc_assume_role" {
#   statement {
#     actions = ["sts:AssumeRole"]
#     principals {
#       type        = "Service"
#       identifiers = ["dms.amazonaws.com"]
#     }
#   }
# }

# resource "aws_iam_role_policy_attachment" "dms_vpc_policy" {
#   role       = aws_iam_role.dms_vpc_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"
# }

# Glue ETL IAM Role (预留，后续可扩展)
# resource "aws_iam_role" "glue_etl_role" {
#  name = "${var.env}-glue-etl-role"
#  assume_role_policy = data.aws_iam_policy_document.glue_etl_assume_role.json
# }

# data "aws_iam_policy_document" "glue_etl_assume_role" {
#  statement {
#    actions = ["sts:AssumeRole"]
#    principals {
#      type        = "Service"
#      identifiers = ["glue.amazonaws.com"]
#    }
#  }
# }

# resource "aws_iam_role_policy_attachment" "glue_etl_policy" {
#  role       = aws_iam_role.glue_etl_role.name
#  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
# }

# 如需自定义最小权限策略，可在此处添加自定义policy并attach到对应role。
# 例如：S3只读、特定bucket访问等。
