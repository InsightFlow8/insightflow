# Only bastion machine EC2 instance in the public subnet. The RDS PostgreSQL will be launched directly in the private subnet.

resource "aws_instance" "bastion" {
  ami                         = var.ami_id
  instance_type               = var.instance_type
  subnet_id                   = var.public_subnet_id
  vpc_security_group_ids      = var.bastion_security_group_ids
  key_name                    = var.key_name
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ec2_instance_profile.name

  # Configure root block device with 25 GB storage
  root_block_device {
    volume_size = 25
    volume_type = "gp3"
    encrypted   = true
    tags = {
      Name = "${var.env}-bastion-root-volume"
    }
  }

  # user_data = templatefile("${path.module}/bastion-init.sh.tpl", {
  # rds_host     = var.rds_host
  # db_username  = var.db_username
  # db_name      = var.db_name
  # db_password  = var.db_password
  # rds_port     = var.rds_port
  # sql_s3_path  = var.sql_s3_path
  # })

  user_data = file("${path.module}/bastion-init.sh")

  tags = { Name = "${var.env}-bastion-ec2" }
}
