# Deployment & Environments

## Promotion model

| | dev | qa | prod |
|---|---|---|---|
| Trigger | push to `main` | tag `v*-rc*` or manual dispatch | tag `vX.Y.Z` |
| Approval | none | optional | **required** (GitHub environment / Jenkins `input`) |
| Databricks bundle target | `dev` (personal, paused schedules) | `qa` | `prod` (service principal, live schedules) |
| Terraform | `envs/dev.tfvars` | `envs/qa.tfvars` (MSK on) | `envs/prod.tfvars` (MSK on, larger brokers) |
| DQ fail threshold | 5% | 2% | 1% |

## GitHub Actions

- `ci.yml`: ruff, black, mypy, pytest (matrix 3.10/3.11 with local Spark), wheel build, serving Docker build, `terraform validate`.
- `deploy.yml` (reusable): OIDC federation to AWS (no static keys), `terraform apply`, `databricks bundle deploy -t <env>`, ECR image push, Snowflake DDL apply. Wrapped by `cd-dev` / `cd-qa` / `cd-prod`.
- Environment-scoped secrets: `AWS_DEPLOY_ROLE_ARN`, `DATABRICKS_HOST/TOKEN`, `ECR_REPOSITORY`, `SNOWFLAKE_*` — each defined per GitHub environment so QA credentials can never touch prod.

## Jenkins

`ci/jenkins/Jenkinsfile` mirrors the same flow for Jenkins shops: parallel lint/typecheck, JUnit-reported tests, wheel archiving, parameterized `DEPLOY_ENV` with a mandatory human `input` gate before prod, and per-environment credentials bindings.

## Runtime secrets

Application code never reads CI secrets directly; it goes through `lakehouse.common.secrets.resolve_secret`, which tries env vars, then Databricks secret scopes, then AWS Secrets Manager — so the same code works on laptops, Airflow workers, and Databricks clusters.
