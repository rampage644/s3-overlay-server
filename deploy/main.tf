# ami-0c27470ce494ca241
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.1.0"  # specify the version you want to use
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.1.0"  # specify the version you want to use
    }
  }

  required_version = ">= 1.2.0"
}

provider "aws" {
  region = "us-east-2"
}

resource "aws_security_group" "ubuntu" {
  name        = "ubuntu-security-group"
  description = "Allow HTTP, HTTPS and SSH traffic"

  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "terraform"
  }

}

resource "aws_key_pair" "ramp" {
  key_name   = "ramp"
  public_key = file("~/.ssh/id_rsa.pub")
}

resource "random_string" "bucket_suffix" {
  length  = 8
  special = false
  upper   = false
}

resource "aws_s3_bucket" "warehouse" {
  bucket = "warehouse-${random_string.bucket_suffix.id}"
}

resource "aws_iam_role" "ec2_role" {
  name = "ec2-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "s3_full_access" {
  role       = aws_iam_role.ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "ec2-s3-instance-profile"
  role = aws_iam_role.ec2_role.name
}

resource "aws_instance" "app_server" {
  ami           = "ami-036f5574583e16426"
  instance_type = "t2.micro"

  key_name      = "ramp"

  count         = 2

  tags = {
    Name = "instance-${count.index + 1}"
  }

  iam_instance_profile = aws_iam_instance_profile.ec2_instance_profile.name
  vpc_security_group_ids = [
    aws_security_group.ubuntu.id
  ]
  
  user_data = <<-EOF
    #!/bin/bash
    set -ex
    sudo yum update -y
    sudo yum install docker aws-cli -y
    sudo usermod -a -G docker ec2-user
    sudo service docker start
EOF
}

locals {
  inventory = templatefile("${path.module}/inventory.tpl", { ip_addresses = aws_instance.app_server[*].public_dns })
}

resource "local_file" "inventory" {
  content  = local.inventory
  filename = "${path.module}/inventory.ini"
}