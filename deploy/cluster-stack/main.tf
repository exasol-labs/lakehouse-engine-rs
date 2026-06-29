locals {
  prefix      = "spot-strata-${var.env_name}"
  total_nodes = var.node_count + var.reserve_nodes
  vpc_id      = data.terraform_remote_state.data.outputs.vpc_id
  subnet_id   = data.terraform_remote_state.data.outputs.subnet_id
  ssm_root    = "/spot-strata/cluster/${var.env_name}"

  # Ingress allowlist: explicit var, else this machine's public IP /32 (resolved at apply).
  my_ip_cidr      = "${chomp(data.http.my_ip.response_body)}/32"
  effective_cidrs = length(var.allowed_cidrs) > 0 ? var.allowed_cidrs : [local.my_ip_cidr]

  # Exasol SG ports (verified against etc/exasol-sec-group.png).
  mgmt_ports   = [22, 20002, 20003] # SSH, container ssh, confd
  client_ports = [8563, 8443, 2581] # DB, Admin UI, BucketFS
}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd*/ubuntu-noble-24.04-amd64-server-*"]
  }
}

# --- Security group ---------------------------------------------------------
resource "aws_security_group" "exasol" {
  name        = "${local.prefix}-sg"
  description = "Exasol cluster ${var.env_name}"
  vpc_id      = local.vpc_id
  tags        = { Name = "${local.prefix}-sg" }
}

resource "aws_security_group_rule" "ingress" {
  for_each          = toset([for p in concat(local.mgmt_ports, local.client_ports) : tostring(p)])
  type              = "ingress"
  from_port         = tonumber(each.value)
  to_port           = tonumber(each.value)
  protocol          = "tcp"
  cidr_blocks       = local.effective_cidrs
  security_group_id = aws_security_group.exasol.id
  description       = "port ${each.value} from allowlist"
}

# Inter-node: all traffic within the SG.
resource "aws_security_group_rule" "internode" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.exasol.id
  description       = "inter-node all traffic"
}

resource "aws_security_group_rule" "egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.exasol.id
  description       = "all egress"
}

# --- Nodes (node_count active + reserve_nodes) ------------------------------
resource "aws_instance" "node" {
  count                       = local.total_nodes
  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.instance_type
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.exasol.id]
  key_name                    = var.key_pair_name
  associate_public_ip_address = true

  root_block_device {
    volume_size           = var.os_disk_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  # Second blank volume -> /dev/nvme1n1 on Nitro; Exasol claims it.
  ebs_block_device {
    device_name           = "/dev/sdb"
    volume_size           = var.data_disk_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = { Name = "${local.prefix}-n1${count.index + 1}" }
}

# --- Passwords -> SSM SecureString ------------------------------------------
resource "random_password" "db" {
  length  = 20
  special = false
}
resource "random_password" "admin" {
  length  = 20
  special = false
}
resource "random_password" "bucketfs" {
  length  = 20
  special = false
}

resource "aws_ssm_parameter" "db_password" {
  name  = "${local.ssm_root}/db_password"
  type  = "SecureString"
  value = random_password.db.result
}
resource "aws_ssm_parameter" "admin_password" {
  name  = "${local.ssm_root}/admin_password"
  type  = "SecureString"
  value = random_password.admin.result
}
resource "aws_ssm_parameter" "bucketfs_password" {
  name  = "${local.ssm_root}/bucketfs_password"
  type  = "SecureString"
  value = random_password.bucketfs.result
}
