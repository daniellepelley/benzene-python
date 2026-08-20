# Archived working docs

Actioned plans and design notes, kept as history (matching the main Benzene repo's
`work/archive/` convention). Nothing here describes current work; each file carries an
`> ARCHIVED <date>` stamp pointing at where the truth now lives. One line per file:

- `mesh-aws-plan.md` — the phased plan for the Python mesh on AWS (Lambda fleet + Fargate
  collector + mesh-ui). Archived 2026-08-20: substance shipped as `deploy/mesh/` (runbook in
  `deploy/mesh/README.md`) and `.github/workflows/deploy-mesh.yml` / `destroy-mesh.yml`; the
  one remainder (OIDC instead of static keys in those workflows) moved to
  `work/remaining-items.md`.
