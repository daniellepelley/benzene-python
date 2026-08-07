# Durable state for the Mesh Host: an EFS file system mounted into the collector task at /data, so the
# collector's snapshot (see benzene.mesh.JsonFileCollectorStore) outlives a replaced Fargate task and
# the fleet view rehydrates on the next boot. Gated on var.persist_state — off leaves the catalog in
# memory (it refills from the fleet within one poll interval), and no EFS is created.
#
# Access is by EFS access point over NFS from the task security group only; transit encryption is on and
# IAM authorization is disabled, so the mount needs no task-role permissions — the SG + access point are
# the guardrails. Mirrors the .NET Mesh Host's mounted artifact volume.

locals {
  persist       = var.persist_state
  efs_root_path = "/mesh"
  state_mount   = "/data"
  state_file    = "mesh-state.json"
}

resource "aws_efs_file_system" "state" {
  count          = local.persist ? 1 : 0
  creation_token = "${local.name}-state"
  encrypted      = true
  tags           = merge(local.tags, { Name = "${local.name}-state" })
}

resource "aws_security_group" "efs" {
  count       = local.persist ? 1 : 0
  name        = "${local.name}-efs"
  description = "NFS from the collector task to the state EFS"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description     = "NFS from the collector task"
    from_port       = 2049
    to_port         = 2049
    protocol        = "tcp"
    security_groups = [aws_security_group.service.id]
  }
  tags = local.tags
}

# One mount target per default subnet (one per AZ), so the task's ENI can always reach a local target.
resource "aws_efs_mount_target" "state" {
  count           = local.persist ? length(data.aws_subnets.default.ids) : 0
  file_system_id  = aws_efs_file_system.state[0].id
  subnet_id       = data.aws_subnets.default.ids[count.index]
  security_groups = [aws_security_group.efs[0].id]
}

# An access point pins the collector into its own directory with a fixed POSIX identity, and creates
# that directory (owned by the same identity) on first use — so the container writes as a known uid.
resource "aws_efs_access_point" "state" {
  count          = local.persist ? 1 : 0
  file_system_id = aws_efs_file_system.state[0].id

  posix_user {
    uid = 1000
    gid = 1000
  }
  root_directory {
    path = local.efs_root_path
    creation_info {
      owner_uid   = 1000
      owner_gid   = 1000
      permissions = "0755"
    }
  }
  tags = merge(local.tags, { Name = "${local.name}-state" })
}
