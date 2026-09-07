# Remaining items

Small open items extracted from otherwise-actioned plans (see `archive/`). Remove each line
when it ships.

- **Move the mesh deploy workflows to OIDC role assumption.**
  `.github/workflows/deploy-mesh.yml` and `destroy-mesh.yml` authenticate to AWS with a
  static access key pair from the `test` environment (`vars.AWS_ACCESS_KEY_ID` +
  `secrets.AWS_SECRET_ACCESS_KEY`, populated by the main repo's "Sync Test Environment"
  workflow). The intent in the archived [mesh AWS plan](archive/mesh-aws-plan.md) was
  OIDC-authenticated deploys — i.e. `aws-actions/configure-aws-credentials` with
  `role-to-assume` and `id-token: write` (as `release.yml` already does for PyPI trusted
  publishing), with an IAM role + GitHub OIDC provider set up in the target account. Needs
  the AWS account's role/naming conventions, which this repository does not hold.
