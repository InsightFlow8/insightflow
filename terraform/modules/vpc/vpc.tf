resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr_block
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "${var.env}-vpc"
  }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags = {
    Name = "${var.env}-igw"
  }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = var.public_subnet_cidrs[count.index]
  map_public_ip_on_launch = true
  availability_zone       = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "${var.env}-public-${count.index + 1}"
  }
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.main.id
  cidr_block        = var.private_subnet_cidrs[count.index]
  availability_zone = data.aws_availability_zones.available.names[count.index]

  tags = {
    Name = "${var.env}-private-${count.index + 1}"
  }
}

# Reserves an Elastic IP for the NAT Gateway
resource "aws_eip" "nat" {
  tags = {
    Name = "${var.env}-nat-eip"
  }
}

resource "aws_nat_gateway" "nat" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id

  tags = {
    Name = "${var.env}-nat"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }

  tags = {
    Name = "${var.env}-public-rt"
  }
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.nat.id
  }

  tags = {
    Name = "${var.env}-private-rt"
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table_association" "private" {
  count          = 2
  subnet_id      = aws_subnet.private[count.index].id
  route_table_id = aws_route_table.private.id
}

data "aws_availability_zones" "available" {
  state = "available"
}



#----------------------
# Security Group
#----------------------

# Bastion EC2 instance

resource "aws_security_group" "bastion" {
  name        = "${var.env}-bastion-sg"
  description = "Allow SSH"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.env}-bastion-sg"
  }
}

resource "aws_security_group_rule" "bastion_ingress_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.bastion.id
  description       = "Allow SSH from anywhere"
}

# RDS postgresql
resource "aws_security_group" "postgres" {
  name        = "${var.env}-postgres-sg"
  description = "Allow PostgreSQL"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.env}-postgres-sg"
  }
}

resource "aws_security_group_rule" "postgres_ingress_ssh_from_bastion" {
  type                     = "ingress"
  from_port                = 22
  to_port                  = 22
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.bastion.id
  security_group_id        = aws_security_group.postgres.id
  description              = "Allow SSH from Bastion"
}

resource "aws_security_group_rule" "postgres_ingress_pgsql_from_bastion" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.bastion.id
  security_group_id        = aws_security_group.postgres.id
  description              = "Allow PostgreSQL from Bastion"
}


# Lambda function Security Group
resource "aws_security_group" "lambda_sg" {
  name        = "${var.env}-lambda-sg"
  description = "Allow Lambda outbound"
  vpc_id      = aws_vpc.main.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.env}-lambda-sg"
  }
}

resource "aws_security_group_rule" "postgres_ingress_pgsql_from_lambda" {
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.lambda_sg.id
  security_group_id        = aws_security_group.postgres.id
  description              = "Allow PostgreSQL from Lambda"
}


# resource "aws_security_group_rule" "postgres_ingress_pgsql_from_dms" {
#   type                     = "ingress"
#   from_port                = 5432
#   to_port                  = 5432
#   protocol                 = "tcp"
#   source_security_group_id = aws_security_group.dms_sg.id
#   security_group_id        = aws_security_group.postgres.id
#   description              = "Allow PostgreSQL from DMS"
# }




# DMS Security Group
# resource "aws_security_group" "dms_sg" {
#   name        = "dms-security-group"
#   description = "Security group for DMS replication instance"
#   vpc_id      = aws_vpc.main.id

#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
# }




## MWAA相关资源（已注释，后续如需MWAA可恢复）
# resource "aws_security_group" "mwaa" {
#   name        = "${var.env}-mwaa-sg"
#   description = "Security group for MWAA"
#   vpc_id      = aws_vpc.main.id
#   # Allow outbound internet access
#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
#   ingress {
#     from_port = 0
#     to_port   = 0
#     protocol  = "-1"
#     self      = true
#   }
#   tags = {
#     Name = "${var.env}-mwaa-sg"
#   }
# }
#
# resource "aws_security_group_rule" "mwaa_ingress_from_bastion" {
#   type                     = "ingress"
#   from_port                = 443
#   to_port                  = 443
#   protocol                 = "tcp"
#   security_group_id        = aws_security_group.mwaa.id
#   source_security_group_id = aws_security_group.bastion.id
#   description              = "Allow bastion EC2"
# }




# ---------------------
# VPC endpoints
# ---------------------
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.main.id
  service_name = "com.amazonaws.${var.region}.s3"
  route_table_ids = [
    aws_route_table.private.id
  ]
  vpc_endpoint_type = "Gateway"

  tags = {
    Name = "${var.env}-s3-endpoint"
  }
}

## Glue VPC Endpoint（已注释，后续如需Glue ETL可恢复）
# resource "aws_vpc_endpoint" "glue" {
#   vpc_id             = aws_vpc.main.id
#   service_name       = "com.amazonaws.${var.region}.glue"
#   vpc_endpoint_type  = "Interface"
#   subnet_ids         = aws_subnet.private[*].id
#   security_group_ids = [aws_security_group.glue.id] # ✅ REQUIRED
#   private_dns_enabled = true
#   tags = {
#     Name = "${var.env}-glue-endpoint"
#   }
# }

## 如需Glue安全组，可参考如下模板（已注释，后续可恢复）
# resource "aws_security_group" "glue" {
#   name        = "${var.env}-glue-sg"
#   description = "Glue ETL security group"
#   vpc_id      = aws_vpc.main.id
#   egress {
#     from_port   = 0
#     to_port     = 0
#     protocol    = "-1"
#     cidr_blocks = ["0.0.0.0/0"]
#   }
#   tags = {
#     Name = "${var.env}-glue-sg"
#   }
# }

# resource "aws_vpc_endpoint" "dms" {
#   service_name       = "com.amazonaws.${var.region}.dms"
#   vpc_id             = aws_vpc.main.id
#   vpc_endpoint_type  = "Interface"
#   subnet_ids         = aws_subnet.private[*].id
#   security_group_ids = [aws_security_group.dms_sg.id]

#   private_dns_enabled = true

#   tags = {
#     Name = "${var.env}-dms-endpoint"
#   }
# }




## MWAA相关VPC Endpoint（已注释，后续如需MWAA可恢复）
# resource "aws_vpc_endpoint" "airflow_env" {
#   vpc_id             = aws_vpc.main.id
#   service_name       = "com.amazonaws.${var.region}.airflow-env"
#   vpc_endpoint_type  = "Interface"
#   subnet_ids         = aws_subnet.private[*].id
#   security_group_ids = [aws_security_group.mwaa.id]
# }
# resource "aws_vpc_endpoint" "sqs" {
#   vpc_id             = aws_vpc.main.id
#   service_name       = "com.amazonaws.${var.region}.sqs"
#   vpc_endpoint_type  = "Interface"
#   subnet_ids         = aws_subnet.private[*].id
#   security_group_ids = [aws_security_group.mwaa.id]
# }
# resource "aws_vpc_endpoint" "logs" {
#   vpc_id             = aws_vpc.main.id
#   service_name       = "com.amazonaws.${var.region}.logs"
#   vpc_endpoint_type  = "Interface"
#   subnet_ids         = aws_subnet.private[*].id
#   security_group_ids = [aws_security_group.mwaa.id]
# }
# resource "aws_vpc_endpoint" "s3" {
#   vpc_id            = aws_vpc.main.id
#   service_name      = "com.amazonaws.${var.region}.s3"
#   vpc_endpoint_type = "Gateway"
#   route_table_ids   = [aws_route_table.private.id]
# }
