# Chainguard → AWS CodeArtifact Python Sync

AWS Lambda function that syncs Python packages from [Chainguard Libraries](https://libraries.cgr.dev) to an AWS CodeArtifact PyPI repository. It downloads wheels and sdists from Chainguard's secure registries and publishes them to your private CodeArtifact repository, giving your organization a private mirror of Chainguard's hardened Python packages.

By default, the function syncs **all available versions** of every package listed in your `requirements.txt` — not just the pinned version. This ensures your CodeArtifact mirror has a complete set of Chainguard-hardened packages available for every team and environment. Set `SYNC_ALL_VERSIONS=false` or pass `"sync_all_versions": false` in the event to revert to pinned-only behaviour.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Supported Chainguard Registries](#supported-chainguard-registries)
- [Entry Modes](#entry-modes)
- [Event Payloads](#event-payloads)
- [Options](#options)
- [Environment Variables](#environment-variables)
- [IAM Permissions](#iam-permissions)
- [VPC Configuration](#vpc-configuration)
- [Deployment](#deployment)
- [Usage Examples](#usage-examples)
- [Scheduled Sync (Cron Job)](#scheduled-sync-cron-job)
- [Checkpointing and Resumption](#checkpointing-and-resumption)
- [Distribution File Filtering](#distribution-file-filtering)
- [Package Filtering](#package-filtering)
- [Observability](#observability)
- [Garbage Cleanup](#garbage-cleanup)
- [Troubleshooting](#troubleshooting)

---

## How It Works

The function operates in two phases:

**Phase 1 — Discovery:** For each package, the function queries Chainguard's PEP 503/691 Simple API indexes to find available distribution files (wheels and sdists). When `sync_all_versions` is enabled (the default), it scans the entire index for every version available in Chainguard, not just the pinned version from the requirements file. It compares these against what already exists in CodeArtifact and builds a list of work items (files to download and publish).

**Phase 2 — Processing:** Work items are processed in parallel batches. Each item is downloaded from Chainguard (with SHA-256 verification), origin controls are set on the CodeArtifact package, and the file is published via a multipart POST to the CodeArtifact repository endpoint.

If the Lambda runs out of time during either phase, it checkpoints its progress to S3 and asynchronously re-invokes itself to continue.

---

## Supported Chainguard Registries

| Registry     | URL                                                    | Selection Rule                            |
|-------------|--------------------------------------------------------|-------------------------------------------|
| Remediated  | `https://libraries.cgr.dev/python-remediated/simple/`  | Default (tried first)                     |
| Standard    | `https://libraries.cgr.dev/python/simple/`             | Fallback if not in remediated             |
| CUDA 12.6   | `https://libraries.cgr.dev/cu126/simple/`              | Version string contains `+cu126`          |
| CUDA 12.8   | `https://libraries.cgr.dev/cu128/simple/`              | Version string contains `+cu128`          |
| CUDA 12.9   | `https://libraries.cgr.dev/cu129/simple/`              | Version string contains `+cu129`          |

Registry selection is automatic per package version. CUDA versions route exclusively to their corresponding CUDA registry. All other versions try the remediated registry first, then fall back to standard.

When `sync_all_versions` is enabled, all non-CUDA versions are discovered from the standard registries (remediated → standard). Any CUDA versions pinned in the requirements file are additionally fetched from their dedicated CUDA registries.

---

## Entry Modes

The function supports seven entry modes, determined by the event payload:

| Mode                | Trigger Key              | Description                                                   |
|--------------------|--------------------------|---------------------------------------------------------------|
| Sync lockfile       | `s3_bucket` + `s3_key`   | Sync packages from a `requirements.txt` stored in S3          |
| Inline requirements | `requirements_content`   | Sync packages from requirements content passed directly       |
| Update existing     | `update_existing: true`  | Check all CodeArtifact packages for new Chainguard versions   |
| Resume run          | `resume_run_id`          | Resume a timed-out or failed run from its checkpoint          |
| List checkpoints    | `list_checkpoints: true` | List all available checkpoint files in S3                     |
| Cleanup only        | `cleanup_only: true`     | Remove packages with invalid PyPI names from CodeArtifact     |
| Dry run             | `dry_run: true`          | Preview what would sync without writing anything              |

---

## Event Payloads

### Sync from S3 requirements file (all versions — default)

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/requirements.txt"
}
```

### Sync only pinned versions

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/requirements.txt",
  "sync_all_versions": false
}
```

### Sync from inline requirements

```json
{
  "requirements_content": "numpy==1.26.4\ntorch==2.3.0+cu126\nrequests==2.32.3\n"
}
```

### Update all existing packages

```json
{
  "update_existing": true
}
```

### Resume a previous run

```json
{
  "resume_run_id": "20260402T034658Z-abc12345"
}
```

### List available checkpoints

```json
{
  "list_checkpoints": true
}
```

### Cleanup only

```json
{
  "cleanup_only": true
}
```

### Dry run (preview only, no writes)

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "requirements.txt",
  "dry_run": true
}
```

---

## Options

These keys can be added to any event payload (except `list_checkpoints` and `cleanup_only`):

| Key                    | Type           | Default                           | Description                                          |
|------------------------|----------------|-----------------------------------|------------------------------------------------------|
| `setup_codeartifact`   | `bool`         | `false`                           | Create the CodeArtifact domain and repos if missing  |
| `cleanup_garbage`      | `bool`         | `true`                            | Run garbage cleanup before syncing                   |
| `dry_run`              | `bool`         | env `DRY_RUN` or `false`          | Log what would happen without writing anything       |
| `sync_all_versions`    | `bool`         | env `SYNC_ALL_VERSIONS` or `true` | Sync all Chainguard versions per package, not just the pinned one |
| `allowlist`            | `list[str]`    | `[]`                              | Only sync these package names                        |
| `denylist`             | `list[str]`    | `[]`                              | Skip these package names                             |
| `max_packages`         | `int`          | `50000`                           | Cap on the number of packages to process             |
| `errors_s3_bucket`     | `str`          | env var                           | Override the S3 bucket for error output              |
| `errors_s3_prefix`     | `str`          | env var                           | Override the S3 key prefix for error output          |
| `checkpoint_s3_bucket` | `str`          | env var                           | Override the S3 bucket for checkpoint state          |

---

## Environment Variables

### Required

| Variable        | Description                          |
|----------------|--------------------------------------|
| `CGR_USERNAME` | Chainguard registry username         |
| `CGR_TOKEN`    | Chainguard registry token            |

### AWS / CodeArtifact

| Variable                   | Default            | Description                                    |
|---------------------------|--------------------|------------------------------------------------|
| `AWS_REGION`              | `us-east-2`       | AWS region                                     |
| `CODEARTIFACT_DOMAIN`     | `my-pypi-domain`  | CodeArtifact domain name                       |
| `CODEARTIFACT_REPO`       | `my-pypi-repo`    | CodeArtifact repository name                   |
| `CODEARTIFACT_UPSTREAM`   | `pypi-public`     | Upstream repo name (public PyPI pass-through)  |
| `CODEARTIFACT_DOMAIN_OWNER` | `""`            | Account ID for cross-account access            |

### Feature Flags

| Variable             | Default | Description                                                                      |
|---------------------|---------|----------------------------------------------------------------------------------|
| `SYNC_ALL_VERSIONS` | `true`  | Sync all Chainguard versions of each package, not just the pinned version        |
| `DRY_RUN`           | `false` | Environment-level dry-run flag; can also be set per-invocation via event payload  |

When `SYNC_ALL_VERSIONS` is `true` (the default), a requirements file like `numpy==1.26.4` causes the function to sync **every** numpy version available in Chainguard — 1.26.3, 1.26.4, 1.27.0, etc. The requirements file is treated as a list of package names to mirror. Set to `false` to sync only the exact pinned versions.

`DRY_RUN` can be set to `true` as an environment variable to make the function preview-only by default. Individual invocations can still override this by passing `"dry_run": false` in the event.

### Registry URLs (overridable)

| Variable                    | Default                                                   |
|-----------------------------|-----------------------------------------------------------|
| `CGR_STANDARD_REGISTRY`     | `https://libraries.cgr.dev/python/simple/`                |
| `CGR_REMEDIATED_REGISTRY`   | `https://libraries.cgr.dev/python-remediated/simple/`     |
| `CGR_CUDA_126_REGISTRY`     | `https://libraries.cgr.dev/cu126/simple/`                 |
| `CGR_CUDA_128_REGISTRY`     | `https://libraries.cgr.dev/cu128/simple/`                 |
| `CGR_CUDA_129_REGISTRY`     | `https://libraries.cgr.dev/cu129/simple/`                 |

### File Filtering

| Variable             | Default | Description                                                  |
|---------------------|---------|--------------------------------------------------------------|
| `WHEEL_PYTHON_TAGS` | `""`    | Comma-separated Python tags to sync (e.g. `cp311,cp312,py3`) |
| `WHEEL_PLATFORM_TAGS` | `""`  | Comma-separated platform tags (e.g. `linux_x86_64,none-any`) |
| `SYNC_SDIST`        | `true`  | Sync `.tar.gz` sdists                                        |
| `SYNC_WHEELS`       | `true`  | Sync `.whl` wheels                                           |

### S3 / Checkpointing

| Variable               | Default                           | Description                                  |
|------------------------|-----------------------------------|----------------------------------------------|
| `ERRORS_S3_BUCKET`     | `""`                              | S3 bucket for `errors.txt` output            |
| `ERRORS_S3_PREFIX`     | `chainguard-sync/`                | S3 key prefix for errors                     |
| `CHECKPOINT_S3_BUCKET` | `""`                              | S3 bucket for checkpoint state               |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/`    | S3 key prefix for checkpoints                |
| `CHECKPOINT_BUFFER_MS` | `120000`                          | Milliseconds before timeout to save checkpoint |
| `MAX_INVOCATIONS`      | `50`                              | Safety cap on auto re-invocations            |
| `MAX_PACKAGES`         | `50000`                           | Max packages per run                         |

### Retry / Backoff

| Variable             | Default | Description                      |
|---------------------|---------|----------------------------------|
| `RETRY_MAX_ATTEMPTS` | `3`    | Max HTTP retry attempts          |
| `RETRY_BASE_DELAY_S` | `1.0`  | Base backoff delay in seconds    |

### Parallel Downloads

| Variable      | Default | Description        |
|--------------|---------|--------------------|
| `MAX_WORKERS` | `10`   | Thread pool size   |

### Package Filtering

| Variable            | Default | Description                                              |
|--------------------|---------|----------------------------------------------------------|
| `PACKAGE_ALLOWLIST` | `""`   | Comma-separated names OR `s3://bucket/key`               |
| `PACKAGE_DENYLIST`  | `""`   | Comma-separated names OR `s3://bucket/key`               |

### Notifications / Metrics

| Variable               | Default           | Description                              |
|------------------------|-------------------|------------------------------------------|
| `SNS_TOPIC_ARN`        | `""`              | SNS topic for completion/failure alerts  |
| `CLOUDWATCH_NAMESPACE` | `ChainguardSync`  | CloudWatch metrics namespace             |
| `LOG_LEVEL`            | `DEBUG`           | Logging level (`DEBUG`, `INFO`, `WARN`)  |

---

## IAM Permissions

The Lambda **execution role** needs the following permissions:

### CodeArtifact

```
codeartifact:GetAuthorizationToken
codeartifact:GetRepositoryEndpoint
codeartifact:ListPackages
codeartifact:ListPackageVersions
codeartifact:PublishPackageVersion
codeartifact:PutPackageOriginConfiguration
codeartifact:DeletePackageVersions          # for garbage cleanup
```

If using `setup_codeartifact: true`:

```
codeartifact:CreateDomain
codeartifact:CreateRepository
codeartifact:UpdateRepository
codeartifact:AssociateExternalConnection
```

### S3 (for checkpoints and error logs)

```
s3:GetObject
s3:PutObject
s3:DeleteObject
s3:ListBucket            # for listing checkpoints
```

### Lambda (for self-invocation on checkpoint)

```
lambda:InvokeFunction    # on the function's own ARN
```

### STS

```
sts:GetCallerIdentity    # logged at cold-start for diagnostics
sts:GetServiceBearerToken
```

### Optional

```
sns:Publish              # if SNS_TOPIC_ARN is set
cloudwatch:PutMetricData # for CloudWatch metrics
```

> **Note:** CodeArtifact returns HTTP 404 (not 403) when `codeartifact:PublishPackageVersion` is missing. The function logs the execution role ARN at cold-start to help identify which role to update.

---

## VPC Configuration

If the Lambda runs inside a VPC, you need **two** VPC Interface Endpoints for CodeArtifact:

| Endpoint                                           | Used For                              |
|---------------------------------------------------|---------------------------------------|
| `com.amazonaws.REGION.codeartifact.api`            | Boto3 SDK calls (create repo, list packages, etc.) |
| `com.amazonaws.REGION.codeartifact.repositories`   | HTTP registry traffic (upload/download packages)   |

If only the `.api` endpoint is present, SDK calls succeed but package uploads fail with HTTP 404. The function runs a pre-flight connectivity check before processing and logs diagnostic information including DNS resolution and HTTP status.

The Lambda also needs outbound HTTPS (443) access to `libraries.cgr.dev` for downloading packages from Chainguard.

---

## Deployment

### Lambda Configuration

| Setting          | Recommendation                                |
|-----------------|-----------------------------------------------|
| Runtime         | Python 3.11+                                  |
| Memory          | 512 MB–1024 MB (higher for large syncs)       |
| Timeout         | 900 seconds (15 minutes, the maximum)         |
| Ephemeral storage | 512 MB (default is sufficient)              |

The function uses `boto3` (included in the Lambda runtime) and Python standard library only — no additional dependencies are required.

### Deploying

1. Package `lambda.py` as a zip (or use a container image).
2. Set the required environment variables (`CGR_USERNAME`, `CGR_TOKEN`, `CODEARTIFACT_DOMAIN`, `CODEARTIFACT_REPO`).
3. Attach an execution role with the permissions listed above.
4. If running in a VPC, configure both CodeArtifact VPC endpoints.
5. Optionally configure S3 buckets for checkpoints and error logs.
6. Optionally configure an SNS topic for notifications.

---

## Usage Examples

### First-time setup + sync (all versions)

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "setup_codeartifact": true
}
```

This creates the CodeArtifact domain, upstream (public PyPI) repo, and main repo, then syncs **all Chainguard versions** of every package in the requirements file.

### Sync only the pinned versions

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "sync_all_versions": false
}
```

### Sync specific packages only

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "allowlist": ["numpy", "pandas", "torch"]
}
```

### Sync CUDA packages

CUDA routing is automatic based on version strings. Include versions with `+cuXXX` suffixes in your requirements file:

```
torch==2.3.0+cu126
torchvision==0.18.0+cu126
```

When `sync_all_versions` is enabled, all standard (non-CUDA) versions are pulled from the remediated/standard registries, and the pinned CUDA versions are additionally pulled from their dedicated CUDA registries.

### Preview a sync without writing

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "dry_run": true
}
```

Or set `DRY_RUN=true` as an environment variable to make dry-run the default for all invocations.

### Keep CodeArtifact up to date

Periodically invoke with `update_existing` to pull new Chainguard versions for all packages already in your repository:

```json
{
  "update_existing": true
}
```

---

## Scheduled Sync (Cron Job)

Use Amazon EventBridge Scheduler (formerly CloudWatch Events) to automatically keep your CodeArtifact mirror up to date. Two complementary schedules are recommended: a daily lockfile sync to pick up new packages, and a periodic update-existing sweep to catch new upstream versions.

### Option 1: EventBridge Scheduler (recommended)

#### Daily lockfile sync

Create a schedule that invokes the Lambda with your requirements file every day at 2:00 AM UTC:

**AWS CLI:**

```bash
aws scheduler create-schedule \
  --name chainguard-pypi-daily-sync \
  --schedule-expression "cron(0 2 * * ? *)" \
  --flexible-time-window '{"Mode": "OFF"}' \
  --target '{
    "Arn": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync",
    "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/EventBridgeSchedulerLambdaRole",
    "Input": "{\"s3_bucket\": \"my-config-bucket\", \"s3_key\": \"requirements.txt\"}"
  }'
```

#### Weekly update-existing sweep

Catch new Chainguard versions for packages already in CodeArtifact every Sunday at 4:00 AM UTC:

```bash
aws scheduler create-schedule \
  --name chainguard-pypi-weekly-update \
  --schedule-expression "cron(0 4 ? * SUN *)" \
  --flexible-time-window '{"Mode": "OFF"}' \
  --target '{
    "Arn": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync",
    "RoleArn": "arn:aws:iam::ACCOUNT_ID:role/EventBridgeSchedulerLambdaRole",
    "Input": "{\"update_existing\": true}"
  }'
```

### Option 2: EventBridge Rule (CloudWatch Events — classic)

If you prefer classic EventBridge rules:

```bash
# Create the rule
aws events put-rule \
  --name chainguard-pypi-daily-sync \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED

# Add the Lambda as a target
aws events put-targets \
  --rule chainguard-pypi-daily-sync \
  --targets '[{
    "Id": "chainguard-sync-lambda",
    "Arn": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync",
    "Input": "{\"s3_bucket\": \"my-config-bucket\", \"s3_key\": \"requirements.txt\"}"
  }]'

# Grant EventBridge permission to invoke the Lambda
aws lambda add-permission \
  --function-name chainguard-pypi-codeartifact-sync \
  --statement-id eventbridge-daily-sync \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-2:ACCOUNT_ID:rule/chainguard-pypi-daily-sync
```

### Option 3: Terraform / OpenTofu

```hcl
# Daily lockfile sync
resource "aws_cloudwatch_event_rule" "chainguard_daily_sync" {
  name                = "chainguard-pypi-daily-sync"
  description         = "Daily sync of Chainguard Python packages to CodeArtifact"
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "chainguard_daily_sync" {
  rule  = aws_cloudwatch_event_rule.chainguard_daily_sync.name
  arn   = aws_lambda_function.chainguard_sync.arn
  input = jsonencode({
    s3_bucket = "my-config-bucket"
    s3_key    = "requirements.txt"
  })
}

resource "aws_lambda_permission" "allow_eventbridge_daily" {
  statement_id  = "AllowEventBridgeDailySync"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.chainguard_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.chainguard_daily_sync.arn
}

# Weekly update-existing sweep
resource "aws_cloudwatch_event_rule" "chainguard_weekly_update" {
  name                = "chainguard-pypi-weekly-update"
  description         = "Weekly check for new Chainguard versions of existing packages"
  schedule_expression = "cron(0 4 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "chainguard_weekly_update" {
  rule  = aws_cloudwatch_event_rule.chainguard_weekly_update.name
  arn   = aws_lambda_function.chainguard_sync.arn
  input = jsonencode({
    update_existing = true
  })
}

resource "aws_lambda_permission" "allow_eventbridge_weekly" {
  statement_id  = "AllowEventBridgeWeeklyUpdate"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.chainguard_sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.chainguard_weekly_update.arn
}
```

### IAM Role for EventBridge Scheduler

If using EventBridge Scheduler (Option 1), the scheduler needs a role with permission to invoke the Lambda:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync"
    }
  ]
}
```

Trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "scheduler.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

### Recommended Schedule Strategy

| Schedule        | Frequency             | Event Payload                                          | Purpose                                                  |
|-----------------|-----------------------|--------------------------------------------------------|----------------------------------------------------------|
| Daily sync      | `cron(0 2 * * ? *)`  | `{"s3_bucket": "...", "s3_key": "requirements.txt"}`   | Pick up new packages added to requirements.txt           |
| Weekly update   | `cron(0 4 ? * SUN *)` | `{"update_existing": true}`                           | Catch new Chainguard versions of existing packages       |
| Monthly dry-run | `cron(0 6 1 * ? *)`   | `{"s3_bucket": "...", "s3_key": "...", "dry_run": true}` | Audit what would change (sends SNS notification)       |

> **Tip:** With `SYNC_ALL_VERSIONS=true` (the default), the daily sync already pulls all versions, so the weekly `update_existing` sweep serves as a safety net for any packages that may have been added to CodeArtifact outside of the requirements file workflow.

---

## Checkpointing and Resumption

For large syncs that exceed the Lambda timeout (15 minutes), the function automatically checkpoints its progress to S3 and re-invokes itself. This is transparent — the sync continues where it left off.

Checkpointing is available in both the discovery and processing phases. The `CHECKPOINT_BUFFER_MS` variable controls how far before the timeout the function saves state (default: 2 minutes).

A safety cap (`MAX_INVOCATIONS`, default 50) prevents runaway invocation chains.

The `sync_all_versions` setting is persisted in checkpoints, so resumed runs maintain the same behaviour as the original invocation.

### Listing checkpoints

```json
{"list_checkpoints": true}
```

Returns a list of available checkpoints with run IDs, progress, and counters.

### Manual resumption

If a run fails (as opposed to timing out), it saves a failure checkpoint that can be manually resumed:

```json
{"resume_run_id": "20260402T034658Z-abc12345"}
```

---

## Distribution File Filtering

By default, all distribution files (all wheel variants + sdists) are synced for each package version. Use environment variables to restrict what gets downloaded:

**Sync only specific Python versions:**

```
WHEEL_PYTHON_TAGS=cp311,cp312,py3
```

**Sync only specific platforms:**

```
WHEEL_PLATFORM_TAGS=linux_x86_64,none-any
```

Platform tag matching is substring-based, so `linux_x86_64` also matches `manylinux2014_x86_64`.

**Disable sdists or wheels entirely:**

```
SYNC_SDIST=false
SYNC_WHEELS=false
```

> **Note:** These filters are especially useful when `SYNC_ALL_VERSIONS=true`, as pulling all versions of a large package (e.g. numpy, torch) can produce thousands of wheel files across platforms.

---

## Package Filtering

Packages can be filtered via allowlist/denylist at two levels:

**Environment variables** (apply to all invocations):

```
PACKAGE_ALLOWLIST=numpy,pandas,torch
PACKAGE_DENYLIST=legacy-pkg,deprecated-pkg
```

**Event payload** (per-invocation, overrides env vars):

```json
{
  "allowlist": ["numpy", "pandas"],
  "denylist": ["legacy-pkg"]
}
```

Both support `s3://bucket/key` references to a text file with one package name per line (lines starting with `#` are comments). Names are compared using PEP 503 normalization.

---

## Observability

### Logging

All operations are logged via Python's `logging` module. Set `LOG_LEVEL` to control verbosity (`DEBUG`, `INFO`, `WARN`). At cold-start, the function logs the Lambda execution role ARN for IAM troubleshooting.

### Error Tracking

Per-package errors are collected in memory and written to S3 as a timestamped `errors.txt` file at the end of each run (or on failure). Each error includes the package name, phase (discovery/download/publish), HTTP status, error type, and detail.

### SNS Notifications

If `SNS_TOPIC_ARN` is set, the function publishes a JSON summary to SNS on completion or failure. The subject line includes sync/failure counts and the run ID. This is particularly useful with scheduled invocations — configure an SNS subscription to email or Slack to get notified of sync results.

### CloudWatch Metrics

The function emits the following custom metrics to the `ChainguardSync` namespace (configurable via `CLOUDWATCH_NAMESPACE`):

- `PackagesSynced` — files successfully published
- `PackagesFailed` — files that failed to publish
- `PackagesSkipped` — versions already in CodeArtifact
- `DownloadFailed` — files that couldn't be downloaded
- `HttpErrors` — total HTTP errors encountered
- `TotalWorkItems` — total files attempted

All metrics include a `FunctionName` dimension.

### CloudWatch Alarms (recommended for scheduled syncs)

Set up alarms on the custom metrics to catch issues early:

```bash
# Alert if any packages fail to publish
aws cloudwatch put-metric-alarm \
  --alarm-name chainguard-sync-publish-failures \
  --namespace ChainguardSync \
  --metric-name PackagesFailed \
  --dimensions Name=FunctionName,Value=chainguard-pypi-codeartifact-sync \
  --statistic Sum \
  --period 86400 \
  --evaluation-periods 1 \
  --threshold 0 \
  --comparison-operator GreaterThanThreshold \
  --alarm-actions arn:aws:sns:us-east-2:ACCOUNT_ID:ops-alerts
```

### Response Summary

Every invocation returns a JSON summary:

```json
{
  "statusCode": 200,
  "body": {
    "message": "Sync complete",
    "summary": {
      "run_id": "20260402T120000Z-a1b2c3d4",
      "total_invocations": 1,
      "total_work_items": 142,
      "synced_to_codeartifact": 138,
      "skipped_existing": 24,
      "tarball_download_failed": 2,
      "failed_to_publish": 2,
      "http_errors": 4,
      "errors_file": "s3://my-bucket/chainguard-sync/errors-20260402T120045Z.txt",
      "errored_packages": ["pkg-a==1.0.0 (pkg_a-1.0.0.tar.gz)"],
      "not_found_packages": ["obscure-pkg==0.1.0"],
      "status": "complete"
    }
  }
}
```

---

## Garbage Cleanup

The function can remove packages from CodeArtifact whose names are clearly invalid for PyPI — for example, names containing path separators, `@`, `node_modules`, or `..`. This targets artifacts that may have been accidentally pushed.

Cleanup runs automatically before each sync (disable with `cleanup_garbage: false`) or on-demand:

```json
{"cleanup_only": true}
```

---

## Troubleshooting

### All publishes fail with HTTP 404

This is the most common issue. Check in order:

1. **IAM permissions** — Ensure the Lambda execution role has `codeartifact:PublishPackageVersion`. CodeArtifact returns 404 (not 403) when this permission is missing. The function logs the execution role ARN at cold-start.

2. **VPC endpoints** — If the Lambda runs in a VPC, ensure the `com.amazonaws.REGION.codeartifact.repositories` VPC Interface Endpoint exists. The `.api` endpoint alone is not sufficient for package uploads.

3. **Repository name** — Verify that `CODEARTIFACT_REPO` matches the actual repository name. The function logs the resolved endpoint URL.

### Auth errors (401/403) from Chainguard

- Verify `CGR_USERNAME` and `CGR_TOKEN` are set and valid.
- Tokens may expire — rotate them if the function was working previously.

### SHA-256 mismatches

The function verifies SHA-256 hashes when provided by the Chainguard Simple API index. Mismatches indicate corrupted downloads or registry inconsistencies. The affected files are logged and skipped.

### Timeouts during large syncs

- Increase `MAX_WORKERS` for more parallelism.
- Use `WHEEL_PYTHON_TAGS` and `WHEEL_PLATFORM_TAGS` to reduce the number of files per package — this is especially important when `SYNC_ALL_VERSIONS=true`.
- Ensure `CHECKPOINT_S3_BUCKET` is set — the function will automatically checkpoint and continue.

### "All packages were filtered out"

- Check that names in your allowlist match PEP 503 normalized forms (lowercase, hyphens instead of underscores).
- If using `s3://` filter lists, verify the S3 object is accessible and properly formatted.

### Pre-flight check fails

The function runs a connectivity check before processing. If it reports a connection error, the VPC `.repositories` endpoint is missing or misconfigured. If it reports a private IP for the CodeArtifact hostname, the `.api` endpoint's DNS may be intercepting registry traffic.

### Scheduled sync not running

- Verify the EventBridge rule/schedule is ENABLED.
- Check that the Lambda resource-based policy allows `events.amazonaws.com` (for rules) or that the scheduler role has `lambda:InvokeFunction` (for Scheduler).
- Check the Lambda's CloudWatch Logs for invocation evidence.
- Verify the input JSON in the EventBridge target is valid.

---

## Architecture Diagram

```
                        ┌──────────────────────────┐
                        │  EventBridge Scheduler    │
                        │  (cron: daily / weekly)   │
                        └─────────┬────────────────┘
                                  │ invoke
                                  ▼
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│  S3             │     │  Lambda          │     │  Chainguard Libraries    │
│  requirements   │────▶│  (this function) │────▶│  libraries.cgr.dev       │
│  .txt           │     │                  │     │  PEP 503/691 Simple API  │
└─────────────────┘     │                  │     └──────────────────────────┘
                        │                  │                │
┌─────────────────┐     │                  │     ┌──────────▼───────────────┐
│  S3             │◀────│  Checkpoints +   │     │  Download wheels/sdists  │
│  checkpoints/   │     │  Error tracking  │     │  (SHA-256 verified)      │
│  errors         │     │                  │     └──────────────────────────┘
└─────────────────┘     │                  │                │
                        │                  │     ┌──────────▼───────────────┐
┌─────────────────┐     │                  │────▶│  AWS CodeArtifact        │
│  SNS / CW       │◀────│  Notifications   │     │  PyPI repository         │
│                 │     │  + Metrics       │     │  (multipart POST upload) │
└─────────────────┘     └──────────────────┘     └──────────────────────────┘
```
