# Chainguard → AWS CodeArtifact Python Sync

AWS Lambda function that syncs Python packages from [Chainguard Libraries](https://libraries.cgr.dev) to an AWS CodeArtifact PyPI repository. It downloads wheels and sdists from Chainguard's secure, hardened registries and publishes them to your private CodeArtifact repository, giving your organization a vetted, private mirror of the Python packages you depend on.

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
- [Terraform Reference](#terraform-reference)
- [Checkpointing and Resumption](#checkpointing-and-resumption)
- [Distribution File Filtering](#distribution-file-filtering)
- [Package Filtering](#package-filtering)
- [Observability](#observability)
- [Garbage Cleanup](#garbage-cleanup)
- [Troubleshooting](#troubleshooting)

---

## How It Works

The function operates in two phases:

**Phase 1 — Discovery:** For each `==`-pinned package in the requirements file, the function queries Chainguard's PEP 503/691 Simple API index to find available distribution files (wheels and sdists) for that exact version. It compares these against what already exists in CodeArtifact and builds a list of work items — individual files to download and publish.

**Phase 2 — Processing:** Work items are processed in parallel batches using a configurable thread pool. Each file is downloaded from Chainguard (with SHA-256 verification), origin controls are set on the CodeArtifact package (`publish:ALLOW`, `upstream:BLOCK`), and the file is published via a multipart POST to the CodeArtifact repository endpoint.

If the Lambda approaches its timeout during either phase, it checkpoints progress to S3 and asynchronously re-invokes itself to continue where it left off.

---

## Supported Chainguard Registries

| Registry    | URL                                                   | Selection Rule                   |
|-------------|-------------------------------------------------------|----------------------------------|
| Remediated  | `https://libraries.cgr.dev/python-remediated/simple/` | Default (tried first)            |
| Standard    | `https://libraries.cgr.dev/python/simple/`            | Fallback if not in remediated    |
| CUDA 12.6   | `https://libraries.cgr.dev/cu126/simple/`            | Version contains `+cu126`        |
| CUDA 12.8   | `https://libraries.cgr.dev/cu128/simple/`            | Version contains `+cu128`        |
| CUDA 12.9   | `https://libraries.cgr.dev/cu129/simple/`            | Version contains `+cu129`        |

Registry selection is automatic per package version. CUDA versions (identified by `+cuXXX` in the version string) route exclusively to their corresponding CUDA registry. All other versions try the remediated registry first, then fall back to standard.

---

## Entry Modes

| Mode                | Trigger Key              | Description                                                      |
|---------------------|--------------------------|------------------------------------------------------------------|
| Sync lockfile       | `s3_bucket` + `s3_key`   | Sync exact pinned versions from a `requirements.txt` in S3       |
| Inline requirements | `requirements_content`   | Sync from requirements content passed directly in the event      |
| Update existing     | `update_existing: true`  | Check all CodeArtifact packages for new Chainguard versions      |
| Resume run          | `resume_run_id`          | Resume a timed-out or failed run from its checkpoint             |
| List checkpoints    | `list_checkpoints: true` | List all available checkpoint files in S3                        |
| Cleanup only        | `cleanup_only: true`     | Remove packages with invalid PyPI names from CodeArtifact        |
| Dry run             | `dry_run: true`          | Preview what would sync without writing anything                 |

---

## Event Payloads

### Sync from S3 requirements file

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/requirements.txt"
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
{ "update_existing": true }
```

### Resume a previous run

```json
{ "resume_run_id": "20260402T034658Z-abc12345" }
```

### List available checkpoints

```json
{ "list_checkpoints": true }
```

### Cleanup only

```json
{ "cleanup_only": true }
```

### Dry run

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "requirements.txt",
  "dry_run": true
}
```

---

## Options

These keys can be added to any sync event payload:

| Key                    | Type        | Default  | Description                                         |
|------------------------|-------------|----------|-----------------------------------------------------|
| `setup_codeartifact`   | `bool`      | `false`  | Create the CodeArtifact domain and repos if missing |
| `cleanup_garbage`      | `bool`      | `true`   | Run garbage cleanup before syncing                  |
| `dry_run`              | `bool`      | `false`  | Log what would happen without writing anything      |
| `allowlist`            | `list[str]` | `[]`     | Only sync these package names                       |
| `denylist`             | `list[str]` | `[]`     | Skip these package names                            |
| `max_packages`         | `int`       | `50000`  | Cap on the number of packages to process            |
| `errors_s3_bucket`     | `str`       | env var  | Override S3 bucket for error output                 |
| `errors_s3_prefix`     | `str`       | env var  | Override S3 key prefix for error output             |
| `checkpoint_s3_bucket` | `str`       | env var  | Override S3 bucket for checkpoint state             |

---

## Environment Variables

### Required

| Variable       | Description                  |
|----------------|------------------------------|
| `CGR_USERNAME` | Chainguard registry username |
| `CGR_TOKEN`    | Chainguard registry token    |

### AWS / CodeArtifact

| Variable                     | Default           | Description                                   |
|------------------------------|--------------------|-----------------------------------------------|
| `AWS_REGION`                 | `us-east-2`       | AWS region                                    |
| `CODEARTIFACT_DOMAIN`        | `my-pypi-domain`  | CodeArtifact domain name                      |
| `CODEARTIFACT_REPO`          | `my-pypi-repo`    | CodeArtifact repository name                  |
| `CODEARTIFACT_UPSTREAM`      | `pypi-public`     | Upstream repo (public PyPI pass-through)      |
| `CODEARTIFACT_DOMAIN_OWNER`  | `""`              | Account ID for cross-account access           |

### Registry URLs (overridable)

| Variable                    | Default                                               |
|-----------------------------|-------------------------------------------------------|
| `CGR_STANDARD_REGISTRY`     | `https://libraries.cgr.dev/python/simple/`            |
| `CGR_REMEDIATED_REGISTRY`   | `https://libraries.cgr.dev/python-remediated/simple/` |
| `CGR_CUDA_126_REGISTRY`     | `https://libraries.cgr.dev/cu126/simple/`             |
| `CGR_CUDA_128_REGISTRY`     | `https://libraries.cgr.dev/cu128/simple/`             |
| `CGR_CUDA_129_REGISTRY`     | `https://libraries.cgr.dev/cu129/simple/`             |

### File Filtering

| Variable             | Default | Description                                                 |
|----------------------|---------|-------------------------------------------------------------|
| `WHEEL_PYTHON_TAGS`  | `""`    | Comma-separated Python tags (e.g. `cp311,cp312,py3`)        |
| `WHEEL_PLATFORM_TAGS`| `""`    | Comma-separated platform tags (e.g. `linux_x86_64,none-any`)|
| `SYNC_SDIST`         | `true`  | Sync `.tar.gz` sdists                                       |
| `SYNC_WHEELS`        | `true`  | Sync `.whl` wheels                                          |

### S3 / Checkpointing

| Variable               | Default                        | Description                                     |
|------------------------|--------------------------------|-------------------------------------------------|
| `ERRORS_S3_BUCKET`     | `""`                           | S3 bucket for `errors.txt` output               |
| `ERRORS_S3_PREFIX`     | `chainguard-sync/`             | S3 key prefix for errors                        |
| `CHECKPOINT_S3_BUCKET` | `""`                           | S3 bucket for checkpoint state                  |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/` | S3 key prefix for checkpoints                   |
| `CHECKPOINT_BUFFER_MS` | `120000`                       | Milliseconds before timeout to checkpoint       |
| `MAX_INVOCATIONS`      | `50`                           | Safety cap on auto re-invocations               |
| `MAX_PACKAGES`         | `50000`                        | Max packages per run                            |

### Retry / Parallel

| Variable             | Default | Description                 |
|----------------------|---------|-----------------------------|
| `RETRY_MAX_ATTEMPTS` | `3`     | Max HTTP retry attempts     |
| `RETRY_BASE_DELAY_S` | `1.0`   | Base backoff delay (seconds)|
| `MAX_WORKERS`        | `10`    | Thread pool size            |

### Package Filtering

| Variable            | Default | Description                                |
|---------------------|---------|--------------------------------------------|
| `PACKAGE_ALLOWLIST` | `""`    | Comma-separated names OR `s3://bucket/key` |
| `PACKAGE_DENYLIST`  | `""`    | Comma-separated names OR `s3://bucket/key` |

### Notifications / Metrics

| Variable               | Default          | Description                             |
|------------------------|------------------|-----------------------------------------|
| `SNS_TOPIC_ARN`        | `""`             | SNS topic for completion/failure alerts |
| `CLOUDWATCH_NAMESPACE` | `ChainguardSync` | CloudWatch metrics namespace            |
| `LOG_LEVEL`            | `DEBUG`          | Logging level (`DEBUG`/`INFO`/`WARN`)   |

---

## IAM Permissions

The Lambda **execution role** requires the following. The function logs its execution role ARN at cold-start to help identify which role to update.

### CodeArtifact

```
codeartifact:GetAuthorizationToken
codeartifact:GetRepositoryEndpoint
codeartifact:ListPackages
codeartifact:ListPackageVersions
codeartifact:PublishPackageVersion
codeartifact:PutPackageOriginConfiguration
codeartifact:DeletePackageVersions          # garbage cleanup
```

If using `setup_codeartifact: true`:

```
codeartifact:CreateDomain
codeartifact:CreateRepository
codeartifact:UpdateRepository
codeartifact:AssociateExternalConnection
```

### S3

```
s3:GetObject       # requirements.txt, checkpoints, filter lists
s3:PutObject       # checkpoints and error logs
s3:DeleteObject    # completed checkpoints
s3:ListBucket      # list available checkpoints
```

### Lambda / STS / Optional

```
lambda:InvokeFunction       # self-invocation on checkpoint (own ARN only)
sts:GetCallerIdentity       # cold-start identity logging
sts:GetServiceBearerToken   # CodeArtifact auth token
sns:Publish                 # if SNS_TOPIC_ARN is set
cloudwatch:PutMetricData    # for CloudWatch metrics
```

> **Important:** CodeArtifact returns HTTP 404 (not 403) when `codeartifact:PublishPackageVersion` is missing. This is the #1 cause of publish failures.

---

## VPC Configuration

If the Lambda runs inside a VPC, you need **two** VPC Interface Endpoints:

| Endpoint                                        | Used For                                          |
|-------------------------------------------------|---------------------------------------------------|
| `com.amazonaws.REGION.codeartifact.api`         | Boto3 SDK calls (list packages, auth, etc.)       |
| `com.amazonaws.REGION.codeartifact.repositories`| HTTP registry traffic (upload/download packages)  |

If only the `.api` endpoint is present, SDK calls succeed but package uploads fail with HTTP 404. The function runs a pre-flight DNS + HTTP check and logs diagnostics.

The Lambda also needs outbound HTTPS (443) to `libraries.cgr.dev`.

---

## Deployment

| Setting            | Recommendation                           |
|--------------------|------------------------------------------|
| Runtime            | Python 3.11+                             |
| Handler            | `lambda_function.lambda_handler`         |
| Memory             | 512 MB – 1024 MB                         |
| Timeout            | 900 seconds (15 min — the maximum)       |
| Ephemeral storage  | 512 MB (default is sufficient)           |

The function uses only `boto3` (included in the runtime) and the standard library — no extra layers required.

### Quick Start

1. Package `lambda_function.py` as a zip or container image.
2. Set `CGR_USERNAME`, `CGR_TOKEN`, `CODEARTIFACT_DOMAIN`, `CODEARTIFACT_REPO`.
3. Attach an execution role with the permissions above.
4. If in a VPC, configure both CodeArtifact VPC endpoints.
5. Upload `requirements.txt` to S3.
6. Invoke: `{"s3_bucket": "...", "s3_key": "requirements.txt", "setup_codeartifact": true}`

---

## Usage Examples

### First-time setup + sync

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "setup_codeartifact": true
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

CUDA routing is automatic — include `+cuXXX` versions in your requirements:

```
torch==2.3.0+cu126
torchvision==0.18.0+cu126
```

### Preview a sync

```json
{
  "s3_bucket": "my-config-bucket",
  "s3_key": "requirements.txt",
  "dry_run": true
}
```

### Catch new upstream versions

```json
{ "update_existing": true }
```

---

## Scheduled Sync (Cron Job)

Use Amazon EventBridge to keep your CodeArtifact mirror up to date automatically.

### EventBridge Scheduler (recommended)

**Daily lockfile sync — 2:00 AM UTC:**

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

**Weekly update-existing sweep — Sundays 4:00 AM UTC:**

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

### EventBridge Rule (classic)

```bash
aws events put-rule \
  --name chainguard-pypi-daily-sync \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED

aws events put-targets \
  --rule chainguard-pypi-daily-sync \
  --targets '[{
    "Id": "sync-lambda",
    "Arn": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync",
    "Input": "{\"s3_bucket\": \"my-config-bucket\", \"s3_key\": \"requirements.txt\"}"
  }]'

aws lambda add-permission \
  --function-name chainguard-pypi-codeartifact-sync \
  --statement-id eventbridge-daily-sync \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-2:ACCOUNT_ID:rule/chainguard-pypi-daily-sync
```

### Scheduler IAM Role

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "lambda:InvokeFunction",
    "Resource": "arn:aws:lambda:us-east-2:ACCOUNT_ID:function:chainguard-pypi-codeartifact-sync"
  }]
}
```

Trust policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "scheduler.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
```

### Recommended Schedule Strategy

| Schedule              | Cron                    | Payload                                                      | Purpose                                            |
|-----------------------|-------------------------|--------------------------------------------------------------|----------------------------------------------------|
| Daily sync            | `cron(0 2 * * ? *)`    | `{"s3_bucket":"...","s3_key":"requirements.txt"}`            | Process requirements file                          |
| Weekly update         | `cron(0 4 ? * SUN *)`  | `{"update_existing":true}`                                   | Catch new Chainguard versions                      |
| Monthly audit dry-run | `cron(0 6 1 * ? *)`    | `{"s3_bucket":"...","s3_key":"...","dry_run":true}`          | Preview changes (sends SNS summary)                |

---

## Terraform Reference

Complete Terraform configuration for deploying the Lambda, IAM role, S3 bucket, EventBridge schedules, SNS topic, and CloudWatch alarms.

### Variables

```hcl
variable "aws_region" {
  type    = string
  default = "us-east-2"
}

variable "cgr_username" {
  type      = string
  sensitive = true
}

variable "cgr_token" {
  type      = string
  sensitive = true
}

variable "codeartifact_domain" {
  type    = string
  default = "my-pypi-domain"
}

variable "codeartifact_repo" {
  type    = string
  default = "my-pypi-repo"
}

variable "notification_email" {
  type        = string
  description = "Email address for SNS sync notifications"
}
```

### Provider and Data Sources

```hcl
provider "aws" {
  region = var.aws_region
}

data "aws_caller_identity" "current" {}
```

### S3 Bucket

```hcl
resource "aws_s3_bucket" "sync" {
  bucket = "chainguard-pypi-sync-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_object" "requirements" {
  bucket = aws_s3_bucket.sync.id
  key    = "requirements.txt"
  source = "${path.module}/requirements.txt"
  etag   = filemd5("${path.module}/requirements.txt")
}
```

### IAM Role

```hcl
resource "aws_iam_role" "lambda" {
  name = "chainguard-pypi-sync-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "chainguard-pypi-sync"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CodeArtifact"
        Effect = "Allow"
        Action = [
          "codeartifact:GetAuthorizationToken",
          "codeartifact:GetRepositoryEndpoint",
          "codeartifact:ListPackages",
          "codeartifact:ListPackageVersions",
          "codeartifact:PublishPackageVersion",
          "codeartifact:PutPackageOriginConfiguration",
          "codeartifact:DeletePackageVersions",
          "codeartifact:CreateDomain",
          "codeartifact:CreateRepository",
          "codeartifact:UpdateRepository",
          "codeartifact:AssociateExternalConnection",
        ]
        Resource = "*"
      },
      {
        Sid      = "STS"
        Effect   = "Allow"
        Action   = ["sts:GetCallerIdentity", "sts:GetServiceBearerToken"]
        Resource = "*"
      },
      {
        Sid    = "S3"
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.sync.arn,
          "${aws_s3_bucket.sync.arn}/*",
        ]
      },
      {
        Sid      = "SelfInvoke"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.sync.arn
      },
      {
        Sid    = "Observability"
        Effect = "Allow"
        Action = [
          "sns:Publish",
          "cloudwatch:PutMetricData",
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "*"
      },
    ]
  })
}
```

### Lambda Function

```hcl
data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda_function.py"
  output_path = "${path.module}/.build/lambda_function.zip"
}

resource "aws_lambda_function" "sync" {
  function_name    = "chainguard-pypi-codeartifact-sync"
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda.arn
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 900
  memory_size      = 1024

  environment {
    variables = {
      CGR_USERNAME         = var.cgr_username
      CGR_TOKEN            = var.cgr_token
      CODEARTIFACT_DOMAIN  = var.codeartifact_domain
      CODEARTIFACT_REPO    = var.codeartifact_repo
      ERRORS_S3_BUCKET     = aws_s3_bucket.sync.id
      CHECKPOINT_S3_BUCKET = aws_s3_bucket.sync.id
      SNS_TOPIC_ARN        = aws_sns_topic.sync.arn
      LOG_LEVEL            = "INFO"
      MAX_WORKERS          = "10"
    }
  }
}
```

### SNS Notifications

```hcl
resource "aws_sns_topic" "sync" {
  name = "chainguard-pypi-sync-notifications"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.sync.arn
  protocol  = "email"
  endpoint  = var.notification_email
}
```

### EventBridge Schedules

```hcl
# ── Daily lockfile sync ─────────────────────────────────────
resource "aws_cloudwatch_event_rule" "daily_sync" {
  name                = "chainguard-pypi-daily-sync"
  description         = "Daily Chainguard Python sync"
  schedule_expression = "cron(0 2 * * ? *)"
}

resource "aws_cloudwatch_event_target" "daily_sync" {
  rule  = aws_cloudwatch_event_rule.daily_sync.name
  arn   = aws_lambda_function.sync.arn
  input = jsonencode({
    s3_bucket = aws_s3_bucket.sync.id
    s3_key    = aws_s3_object.requirements.key
  })
}

resource "aws_lambda_permission" "daily_sync" {
  statement_id  = "AllowEventBridgeDailySync"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_sync.arn
}

# ── Weekly update-existing sweep ────────────────────────────
resource "aws_cloudwatch_event_rule" "weekly_update" {
  name                = "chainguard-pypi-weekly-update"
  description         = "Weekly new-version check"
  schedule_expression = "cron(0 4 ? * SUN *)"
}

resource "aws_cloudwatch_event_target" "weekly_update" {
  rule  = aws_cloudwatch_event_rule.weekly_update.name
  arn   = aws_lambda_function.sync.arn
  input = jsonencode({ update_existing = true })
}

resource "aws_lambda_permission" "weekly_update" {
  statement_id  = "AllowEventBridgeWeeklyUpdate"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.weekly_update.arn
}
```

### CloudWatch Alarms

```hcl
resource "aws_cloudwatch_metric_alarm" "publish_failures" {
  alarm_name          = "chainguard-sync-publish-failures"
  alarm_description   = "One or more packages failed to publish"
  namespace           = "ChainguardSync"
  metric_name         = "PackagesFailed"
  dimensions          = { FunctionName = aws_lambda_function.sync.function_name }
  statistic           = "Sum"
  period              = 86400
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.sync.arn]
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "chainguard-sync-lambda-errors"
  alarm_description   = "Lambda invocation errors"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.sync.function_name }
  statistic           = "Sum"
  period              = 3600
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  alarm_actions       = [aws_sns_topic.sync.arn]
}
```

---

## Checkpointing and Resumption

For large syncs that exceed the 15-minute Lambda timeout, the function automatically checkpoints progress to S3 and re-invokes itself. This is fully transparent — the sync continues where it left off.

Checkpointing works in both discovery and processing phases. `CHECKPOINT_BUFFER_MS` controls the buffer before timeout (default: 2 minutes). `MAX_INVOCATIONS` (default 50) caps the invocation chain.

### Listing checkpoints

```json
{ "list_checkpoints": true }
```

### Manual resumption

```json
{ "resume_run_id": "20260402T034658Z-abc12345" }
```

Completed checkpoints are automatically deleted.

---

## Distribution File Filtering

By default all distribution files are synced. Use env vars to restrict:

**Python versions:** `WHEEL_PYTHON_TAGS=cp311,cp312,py3`

**Platforms:** `WHEEL_PLATFORM_TAGS=linux_x86_64,none-any` (substring matching — also matches `manylinux2014_x86_64`)

**Disable file types:** `SYNC_SDIST=false` or `SYNC_WHEELS=false`

---

## Package Filtering

**Environment variables** (all invocations): `PACKAGE_ALLOWLIST=numpy,pandas` / `PACKAGE_DENYLIST=legacy-pkg`

**Event payload** (per-invocation, overrides env vars):

```json
{ "allowlist": ["numpy", "pandas"], "denylist": ["legacy-pkg"] }
```

Both accept `s3://bucket/key` pointing to a text file (one name per line, `#` = comment). Names use PEP 503 normalization.

---

## Observability

### CloudWatch Metrics

| Metric             | Description                       |
|--------------------|-----------------------------------|
| `PackagesSynced`   | Files successfully published      |
| `PackagesFailed`   | Files that failed to publish      |
| `PackagesSkipped`  | Versions already in CodeArtifact  |
| `DownloadFailed`   | Files that couldn't be downloaded |
| `HttpErrors`       | Total HTTP errors encountered     |
| `TotalWorkItems`   | Total files attempted             |

### SNS Notifications

JSON summary on completion or failure when `SNS_TOPIC_ARN` is set. Subject includes sync counts and run ID.

### Error Tracking

Per-package errors written to S3 as timestamped `errors-YYYYMMDDTHHMMSSZ.txt`.

### Response Summary

Every invocation returns:

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
      "not_found_packages": ["obscure-pkg==0.1.0"],
      "status": "complete"
    }
  }
}
```

---

## Garbage Cleanup

Removes CodeArtifact packages with invalid PyPI names (path separators, `@`, `node_modules`, `..`). Runs automatically before each sync (disable with `"cleanup_garbage": false`) or on-demand with `{"cleanup_only": true}`.

---

## Troubleshooting

### All publishes fail with HTTP 404

1. **IAM** — Add `codeartifact:PublishPackageVersion` to the execution role. CodeArtifact returns 404, not 403, when this is missing.
2. **VPC** — Add the `com.amazonaws.REGION.codeartifact.repositories` VPC endpoint. The `.api` endpoint alone is not enough.
3. **Repo name** — Verify `CODEARTIFACT_REPO` matches the actual repository.

### Auth errors (401/403) from Chainguard

Verify `CGR_USERNAME` and `CGR_TOKEN`. Tokens may expire — rotate if it was working before.

### Timeouts during large syncs

Increase `MAX_WORKERS`. Use `WHEEL_PYTHON_TAGS`/`WHEEL_PLATFORM_TAGS` to reduce file count. Set `CHECKPOINT_S3_BUCKET` to enable auto-checkpointing.

### Scheduled sync not running

Verify the EventBridge rule is ENABLED. Confirm the Lambda resource-based policy grants `events.amazonaws.com` invoke access. Check CloudWatch Logs for invocation evidence.

---

## Architecture

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
┌─────────────────┐     │   Checkpoints    │     ┌──────────▼───────────────┐
│  S3             │◀────│   + Error logs   │     │  Download wheels/sdists  │
│  state/errors   │     │                  │     │  (SHA-256 verified)      │
└─────────────────┘     │                  │     └──────────────────────────┘
                        │                  │                │
┌─────────────────┐     │  Notifications   │     ┌──────────▼───────────────┐
│  SNS → Email    │◀────│  + CloudWatch    │────▶│  AWS CodeArtifact        │
│  SNS → Slack    │     │    Metrics       │     │  PyPI repository         │
└─────────────────┘     └──────────────────┘     └──────────────────────────┘
```
