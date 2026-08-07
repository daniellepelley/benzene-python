# The shared-secret store.
#
# When var.mesh_key is set, it is written as an SSM SecureString (the canonical, rotatable home of the
# secret) AND injected as the BENZENE_MESH_KEY env var on both the services (via lambda.tf's
# local.mesh_key_env) and the host (apprunner.tf) — so the two sides always share the same key. Empty
# key → no parameter, and both sides run open (today's behaviour).
#
# The env-var injection is the simple path used here. To avoid the plaintext value ever touching an env
# block, App Runner + Lambda can instead reference this SSM parameter by ARN (App Runner
# `runtime_environment_secrets`, Lambda an SSM extension / boto3 read) — a documented follow-up, noted in
# the runbook, that this parameter already makes possible.

resource "aws_ssm_parameter" "mesh_key" {
  count = var.mesh_key == "" ? 0 : 1

  name        = "/${var.name_prefix}/mesh-key"
  description = "Benzene mesh shared-secret guarding the collector ingest feeds."
  type        = "SecureString"
  value       = var.mesh_key
}
