# CI/CD Security Guide — blond-beauty-engine

This document is the single source of truth for the CI/CD pipeline, security controls, environment setup, and operational procedures for the `blond-beauty-engine` project.

---

## Table of Contents

1. [Pipeline Overview](#pipeline-overview)
2. [What Runs on Pull Requests](#what-runs-on-pull-requests)
3. [What Runs on Push to Main](#what-runs-on-push-to-main)
4. [What Blocks a Merge](#what-blocks-a-merge)
5. [Staging Deployment](#staging-deployment)
6. [Production Deployment](#production-deployment)
7. [Required GitHub Secrets](#required-github-secrets)
8. [Required Environment Variables](#required-environment-variables)
9. [Branch Protection Setup](#branch-protection-setup)
10. [GitHub Environment Protection Setup](#github-environment-protection-setup)
11. [Security Controls Summary](#security-controls-summary)
12. [Troubleshooting Failed Checks](#troubleshooting-failed-checks)
13. [Rotating Secrets Safely](#rotating-secrets-safely)
14. [Extending the Pipeline](#extending-the-pipeline)
15. [Dependency Updates](#dependency-updates)
16. [Known Limitations and Assumptions](#known-limitations-and-assumptions)

---

## Pipeline Overview

```
Pull Request → main
  └── ci.yml           (lint, vet, test+race, build, govulncheck, hadolint)
  └── security.yml     (CodeQL, Gitleaks, Semgrep)
         │
         ▼  [PR approved + all checks pass]
         │
Push → main (after merge)
  └── ci.yml           (same + Docker build validation + Trivy scan)
  └── security.yml     (same scans)
  └── deploy-staging.yml
        ├── CI Gate    (re-validates lint, test, build, govulncheck)
        ├── Build & push image to GHCR (sha-<short> + :staging tags)
        ├── Trivy scan on pushed image
        └── Deploy to staging (placeholder — fill in your target)
                 │
                 ▼  [Manual workflow_dispatch from main only]
                 │
         deploy-production.yml
           ├── Pre-deploy safety checks
           ├── [Awaits manual approval via GitHub Environment: production]
           └── Deploy to production (placeholder)
```

### Workflow Files

| File | Trigger | Purpose |
|---|---|---|
| `.github/workflows/ci.yml` | PR to main, push to main | Lint, test, build, govulncheck, hadolint, Docker build validate |
| `.github/workflows/security.yml` | PR to main, push to main, weekly schedule | CodeQL, Gitleaks, Semgrep |
| `.github/workflows/deploy-staging.yml` | Push to main | Build+push image, Trivy scan, staging deploy |
| `.github/workflows/deploy-production.yml` | Manual (`workflow_dispatch`) | Production deploy with approval gate |

---

## What Runs on Pull Requests

Every PR targeting `main` triggers `ci.yml` and `security.yml` in parallel.

### `ci.yml` jobs on PR

| Job | Tool | What it checks |
|---|---|---|
| `lint` | `gofmt`, `go vet`, `golangci-lint` | Formatting, static analysis, code quality |
| `test` | `go test -race` | Unit tests with race condition detector |
| `build` | `go build` | Binary compilation succeeds |
| `govulncheck` | `govulncheck` | Known vulnerabilities in Go dependencies |
| `docker-lint` | `hadolint` | Dockerfile best practices |
| `all-checks-pass` | Gate job | Summary status required by branch protection |

### `security.yml` jobs on PR

| Job | Tool | What it checks |
|---|---|---|
| `codeql` | GitHub CodeQL | SAST — security vulnerabilities in source code |
| `gitleaks` | Gitleaks | Secrets committed to git history |
| `semgrep` | Semgrep | OWASP Top 10, Go-specific patterns, secrets |
| `all-security-checks-pass` | Gate job | Summary status required by branch protection |

**No deployment happens from pull requests. Ever.**

---

## What Runs on Push to Main

All PR jobs plus:

- `docker-build-validate` in `ci.yml`: builds the Docker image (no push) and runs a Trivy vulnerability scan. Fails on CRITICAL/HIGH unfixed vulnerabilities.
- `deploy-staging.yml` is triggered as a separate workflow (push to main event) and runs in parallel with `ci.yml`.

---

## What Blocks a Merge

Branch protection requires the following status checks to pass before any PR can be merged into `main`:

| Required Status Check | Workflow |
|---|---|
| `ci / all-checks-pass` | ci.yml |
| `security / all-security-checks-pass` | security.yml |

Individual jobs (`ci / lint`, `ci / test`, etc.) roll up into the gate jobs above. You may also add individual jobs as required checks for earlier visibility in the GitHub PR UI.

Additionally, branch protection enforces:
- At least 1 approved review (see [Branch Protection Setup](#branch-protection-setup))
- Stale reviews dismissed on new commits
- Branch must be up to date with `main` before merging

---

## Staging Deployment

Staging deploys happen automatically after every merge to `main`, provided all CI and security checks pass.

### Flow

1. `deploy-staging.yml` triggers on `push` to `main`.
2. **CI Gate job** re-runs lint, tests, build, and govulncheck for auditability.
3. **Build & Push** builds the Docker image and pushes to GHCR with two tags:
   - `sha-<short-sha>` — immutable, used for rollback and audit.
   - `staging` — mutable, points to the latest staging build.
4. **Trivy scan** scans the pushed image. Fails on CRITICAL/HIGH unfixed vulnerabilities. Results are uploaded to GitHub Security → Code Scanning.
5. **Deploy** runs the deployment placeholder (see `deploy-staging.yml` for instructions on replacing it with your real deployment command).

### Image Registry

Images are pushed to GitHub Container Registry (GHCR):

```
ghcr.io/<org>/<repo>:sha-<short-sha>
ghcr.io/<org>/<repo>:staging
```

### Staging Environment Secrets

Configure in **Settings → Environments → staging**:

| Secret | Description |
|---|---|
| `STAGING_POSTGRES_URL` | PostgreSQL DSN for staging database |
| `STAGING_RABBIT_URL` | RabbitMQ AMQP URL for staging |
| `STAGING_REDIS_URL` | Redis URL for staging (optional if Redis disabled) |

### Wiring the Real Deployment

Replace the placeholder step in `.github/workflows/deploy-staging.yml` under `deploy-staging` → `Deploy to staging` with your actual deployment command. Common options are documented in the workflow file.

---

## Production Deployment

Production deploys are **manual only**. They are never triggered automatically.

### How to Deploy to Production

1. Go to **Actions → Deploy – Production → Run workflow**.
2. Enter the `image_tag` to deploy (use a `sha-<short>` tag from a successful staging run for auditability — avoid deploying the mutable `:staging` tag to production).
3. Enter a `reason` for the audit log.
4. Click **Run workflow** (must be triggered from the `main` branch).
5. The workflow will pause and request approval from the configured reviewers in the `production` GitHub Environment.
6. An authorized reviewer approves the deployment in the GitHub Actions UI.
7. The deployment runs.

### Production Environment Secrets

Configure in **Settings → Environments → production**:

| Secret | Description |
|---|---|
| `PROD_POSTGRES_URL` | PostgreSQL DSN for production database |
| `PROD_RABBIT_URL` | RabbitMQ AMQP URL for production |
| `PROD_REDIS_URL` | Redis URL for production (optional if Redis disabled) |

### Database Migrations in Production

Migrations are **not run automatically** by the CI/CD pipeline.

The engine uses raw SQL files in `migrations/engine/` applied via `make migrate-up` (which calls `psql` in a loop). To run migrations before a production deploy:

1. Ensure `ENGINE_POSTGRES_URL` (or `PROD_POSTGRES_URL`) is set securely.
2. Run `make migrate-up` from a machine with access to the production database, **before** restarting the service.
3. Verify the migration applied cleanly before approving the deployment.
4. If a migration fails, do not proceed with the service deployment.

> **Never run production migrations automatically without review. Never run destructive migrations without a backup.**

---

## Required GitHub Secrets

### Repository-level (available in all workflows)

| Secret | Required | Description |
|---|---|---|
| `GITHUB_TOKEN` | Auto-provided | Push to GHCR, CodeQL SARIF upload, Gitleaks |
| `SEMGREP_APP_TOKEN` | Optional | Enables Semgrep cloud dashboard and PR comments. Without it, Semgrep runs with OSS rules and logs findings only. |
| `GITLEAKS_LICENSE` | Optional | Required only for Gitleaks Pro features. Free OSS version works without it. |

### Environment `staging`

| Secret | Required |
|---|---|
| `STAGING_POSTGRES_URL` | Yes |
| `STAGING_RABBIT_URL` | Yes |
| `STAGING_REDIS_URL` | No (leave empty to disable Redis) |

### Environment `production`

| Secret | Required |
|---|---|
| `PROD_POSTGRES_URL` | Yes |
| `PROD_RABBIT_URL` | Yes |
| `PROD_REDIS_URL` | No (leave empty to disable Redis) |

---

## Required Environment Variables

See `.env.example` for a full list of environment variables the engine reads at startup.

Key variables:

| Variable | Default | Description |
|---|---|---|
| `ENGINE_ENV` | `dev` | Runtime environment. Set to `staging` or `prod` in deployments. Fake providers are refused when `prod`. |
| `ENGINE_POSTGRES_URL` | — | Required. PostgreSQL DSN. |
| `ENGINE_RABBIT_URL` | — | Required. RabbitMQ AMQP URL. |
| `ENGINE_REDIS_URL` | — | Optional. Redis URL. Leave empty to disable. |
| `ENGINE_HTTP_ADDR` | `:8081` | Bind address for `/healthz`, `/readyz`, `/metrics`. |
| `ENGINE_SHUTDOWN_TIMEOUT` | `30s` | Graceful shutdown timeout. |

All `ENGINE_*` variables must be injected at deploy time via your platform's secrets mechanism. They must **never** be committed to the repository or baked into the Docker image.

---

## Branch Protection Setup

Configure these rules manually in **Settings → Branches → Add branch ruleset** (or classic branch protection rules if preferred).

### Rules for `main`

| Rule | Value |
|---|---|
| Require pull request before merging | Enabled |
| Required approvals | 1 (increase to 2 when the team grows) |
| Dismiss stale reviews on new commits | Enabled |
| Require review from code owners | Enabled (once CODEOWNERS has real owners) |
| Require status checks to pass | `ci / all-checks-pass`, `security / all-security-checks-pass` |
| Require branches to be up to date | Enabled |
| Require conversation resolution | Enabled |
| Prevent force pushes | Enabled |
| Prevent branch deletion | Enabled |
| Block direct pushes | Enabled |
| Require signed commits | Optional — recommended for regulated environments |
| Require linear history | Optional — enforces clean rebase/squash merges |

### CODEOWNERS

`.github/CODEOWNERS` is pre-configured with `@blondbeauty/backend-team` as the default owner. Replace the placeholder with real GitHub usernames or team names before enabling CODEOWNERS-based review requirements.

---

## GitHub Environment Protection Setup

### Environment: `staging`

1. Go to **Settings → Environments → New environment**, name it `staging`.
2. Under **Deployment branches**: select `Selected branches` → add `main`.
3. Add environment secrets: `STAGING_POSTGRES_URL`, `STAGING_RABBIT_URL`, `STAGING_REDIS_URL`.
4. No required reviewers — staging deploys are automatic.

### Environment: `production`

1. Go to **Settings → Environments → New environment**, name it `production`.
2. Under **Deployment branches**: select `Selected branches` → add `main`.
3. Under **Required reviewers**: add at least 1 person (the project owner / tech lead).
4. Optionally set a **Wait timer** of 5 minutes as a cancellation window.
5. Add environment secrets: `PROD_POSTGRES_URL`, `PROD_RABBIT_URL`, `PROD_REDIS_URL`.

> Production secrets are **only** available inside the `production` environment. They are not accessible from the `staging` environment, PRs, or any other workflow context.

---

## Security Controls Summary

| Control | Tool / Mechanism | Where |
|---|---|---|
| Format enforcement | `gofmt` | ci.yml |
| Static analysis | `go vet`, `golangci-lint` | ci.yml |
| Unit tests + race detector | `go test -race` | ci.yml |
| Go vulnerability scan | `govulncheck` | ci.yml, deploy-staging.yml |
| Dockerfile lint | `hadolint` | ci.yml |
| Container image scan | `trivy` | ci.yml (build validation), deploy-staging.yml |
| SAST | `CodeQL` | security.yml |
| Secret scanning | `Gitleaks` | security.yml |
| SAST (OWASP/Go patterns) | `Semgrep` | security.yml |
| Dependency updates | `Dependabot` | .github/dependabot.yml |
| Supply chain provenance | Docker SBOM + provenance | deploy-staging.yml |
| Least-privilege CI | Per-job `permissions:` | All workflows |
| Protected branches | Branch protection rules | GitHub Settings |
| Environment protection | GitHub Environments | Settings → Environments |
| Production approval gate | Required reviewers | GitHub Environment: production |
| No auto production deploy | `workflow_dispatch` only | deploy-production.yml |
| Branch validation on deploy | Explicit `ref` check | deploy-production.yml |
| No secrets in logs | No `echo $SECRET` patterns | All workflows |
| GHCR auth via GITHUB_TOKEN | No long-lived credentials | deploy-staging.yml |

---

## Troubleshooting Failed Checks

### `ci / lint` — gofmt failure

```
The following files are not gofmt-formatted: internal/foo/bar.go
```

Fix locally:

```sh
gofmt -w .
git add -p
git commit -m "style: gofmt"
```

### `ci / lint` — golangci-lint failure

Review the failing rule in the job log. To run locally:

```sh
golangci-lint run --timeout=5m
```

To add a justified exception, use `//nolint:rulename // reason` inline — not a blanket file-level suppression.

### `ci / test` — test failure

```sh
go test -count=1 -race ./...
```

The race detector may surface data races that do not appear in non-race runs. Fix the race condition; do not suppress the detector.

### `ci / govulncheck` — vulnerability found

```
Vulnerability #1: GO-XXXX-XXXX
  ...
```

Update the affected dependency:

```sh
go get <module>@latest
go mod tidy
```

If no fix is available, add a documented exception in the project's risk register and create a tracking issue.

### `security / codeql` — CodeQL finding

Review the finding in **Security → Code Scanning**. False positives can be dismissed with a justification. True positives must be fixed before merging.

### `security / gitleaks` — secret detected

A secret pattern was found in the git history. Steps:

1. Identify the secret with `gitleaks detect --source . -v`.
2. **Rotate the credential immediately** — assume it is compromised.
3. Remove the secret from history using `git filter-repo` or BFG Repo Cleaner.
4. Force-push (requires temporarily relaxing branch protection — coordinate with the team).
5. Add the pattern to `.gitleaks.toml` only if it is a confirmed false positive.

### `deploy-staging / scan-image` — Trivy CRITICAL/HIGH

A critical/high vulnerability was found in the container image.

1. Check if the vulnerability is in a base image layer or in your code's dependencies.
2. For base image: update the `golang:1.25.x-alpine` tag or `gcr.io/distroless/static-debian12` in the Dockerfile and rebuild.
3. For Go dependencies: run `go get <module>@latest && go mod tidy`.
4. If no fix exists, set `ignore-unfixed: true` is already configured — it only fails on vulnerabilities that **have** a fix available. If a vulnerability genuinely has no fix, it will be skipped automatically.

### `deploy-production / pre-deploy-checks` — branch validation failure

```
ERROR: Production deploys are only allowed from the main branch.
```

The workflow was triggered from a non-main branch. Switch to `main` before running the production deployment workflow.

---

## Rotating Secrets Safely

### General procedure

1. **Generate the new credential** in the external system (database, RabbitMQ, Redis provider) before invalidating the old one.
2. **Update the GitHub Secret** in Settings → Secrets (repository-level) or Settings → Environments → [env name] → Secrets.
3. **Deploy** using the updated secret.
4. **Verify** the service starts and passes `/healthz` and `/readyz`.
5. **Invalidate the old credential** in the external system.
6. **Document the rotation** in your incident/change log.

### GITHUB_TOKEN

`GITHUB_TOKEN` is automatically rotated by GitHub for each workflow run. No manual rotation needed.

### GHCR access

GHCR authentication uses `GITHUB_TOKEN` with `packages: write` scope. There are no long-lived registry credentials to rotate.

### Database URL rotation (`STAGING_POSTGRES_URL`, `PROD_POSTGRES_URL`)

PostgreSQL DSN rotation may cause brief connection failures. To rotate with zero downtime:
1. Create the new user/password in Postgres.
2. Grant the same permissions as the current user.
3. Update the GitHub Environment secret.
4. Trigger a staging deploy and verify.
5. Trigger a production deploy.
6. Revoke the old user after verifying the new credentials work.

---

## Extending the Pipeline

### Adding a new Go tool to CI

Add a new step to the appropriate job in `.github/workflows/ci.yml`. Follow the existing pattern: install the tool via `go install`, then run it. Cache the Go binary cache using `actions/setup-go` with `cache: true`.

### Adding a new deployment target

Replace the placeholder step in `deploy-staging.yml` and `deploy-production.yml` with your deployment command. If you need additional secrets, add them to the corresponding GitHub Environment.

### Adding integration tests

If integration tests require Postgres and RabbitMQ, use GitHub Actions service containers:

```yaml
services:
  postgres:
    image: postgres:17-alpine
    env:
      POSTGRES_DB: blondbeauty
      POSTGRES_USER: engine
      POSTGRES_PASSWORD: engine
    ports:
      - 5432:5432
    options: >-
      --health-cmd pg_isready
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
  rabbitmq:
    image: rabbitmq:4-alpine
    ports:
      - 5672:5672
    options: >-
      --health-cmd "rabbitmq-diagnostics -q ping"
      --health-interval 10s
      --health-timeout 5s
      --health-retries 5
```

### Adding a real payment provider

When the Getnet adapter is implemented, add the following to the staging environment secrets:
- `GETNET_CLIENT_ID`
- `GETNET_CLIENT_SECRET`
- `GETNET_SELLER_ID`
- `GETNET_API_URL` (sandbox vs production)

Never add real payment credentials to the repository or to any CI log.

### Adding a coverage threshold

Once a stable baseline is established, add a coverage gate to `ci.yml`:

```yaml
- name: Check coverage threshold
  run: |
    COVERAGE=$(go tool cover -func=coverage.out | tail -n 1 | awk '{print $3}' | tr -d '%')
    THRESHOLD=70
    if (( $(echo "$COVERAGE < $THRESHOLD" | bc -l) )); then
      echo "Coverage $COVERAGE% is below threshold $THRESHOLD%"
      exit 1
    fi
```

Do not set the threshold before running the pipeline on the real codebase and measuring the baseline.

### Adding OIDC cloud authentication

When a cloud deployment target is configured (AWS, GCP, Azure), replace static credentials with OIDC federation:

```yaml
- name: Configure AWS credentials (OIDC)
  uses: aws-actions/configure-aws-credentials@v4
  with:
    role-to-assume: arn:aws:iam::ACCOUNT_ID:role/github-actions-engine
    aws-region: sa-east-1
```

This eliminates long-lived `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` credentials entirely.

---

## Dependency Updates

Dependabot is configured in `.github/dependabot.yml` to open pull requests for:

| Ecosystem | Schedule | Grouping |
|---|---|---|
| Go modules (`go.mod`) | Weekly (Monday 06:00 BRT) | Single grouped PR |
| GitHub Actions | Weekly (Monday 06:00 BRT) | Single grouped PR |
| Docker (`Dockerfile` FROM) | Weekly (Monday 06:00 BRT) | Individual PRs |

All Dependabot PRs must pass the full CI and security pipeline before merging. Security updates are always created as individual PRs by Dependabot, regardless of grouping configuration.

To disable noisy updates temporarily, set `open-pull-requests-limit: 0` for that ecosystem in `.github/dependabot.yml`.

---

## Known Limitations and Assumptions

1. **Deployment target is unknown.** The staging and production deploy steps are placeholders. You must replace them with real deployment commands before the pipeline is fully functional. See the inline comments in `deploy-staging.yml` and `deploy-production.yml`.

2. **No integration tests in CI.** The current test suite is unit tests only (fake providers). Integration tests against real Postgres and RabbitMQ are not wired yet. When added, they should use GitHub Actions service containers (see [Extending the Pipeline](#extending-the-pipeline)).

3. **No coverage threshold enforced.** A coverage gate is not set because the baseline has not been measured. Add one after running the pipeline and reviewing the initial coverage numbers.

4. **Semgrep runs with `continue-on-error: true`.** This is intentional until the OSS ruleset is tuned for this codebase and false positives are reviewed. Once the findings are clean, remove `continue-on-error: true` to make Semgrep a blocking check.

5. **CODEOWNERS uses a placeholder team.** Replace `@blondbeauty/backend-team` with real GitHub usernames or team names. Until then, CODEOWNERS-based review requirements should not be enforced.

6. **Go 1.25.4.** The project requires Go 1.25.4. If `golangci-lint` does not yet support this version, pin it to the latest compatible release in `.github/workflows/ci.yml`.

7. **No Kubernetes or Terraform files exist.** No `kubeconform`, `tflint`, or `checkov` checks are included. Add them if infrastructure-as-code is introduced.

8. **Database migrations are manual.** The engine uses raw `psql` for migrations (no migration library). Migrations are not validated in CI and are not run automatically in staging or production. This is intentional — schema changes require manual review and coordinated execution.

9. **No frontend or Node.js tooling.** No npm/pnpm/yarn Dependabot configuration is included. Add it if a frontend or Node tooling is introduced.

10. **GHCR is used as the container registry.** If a different registry (ECR, GCR, Artifact Registry) is used, update the `REGISTRY` env var and the `docker/login-action` step in `deploy-staging.yml`.
