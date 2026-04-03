# Chainguard → CodeArtifact NPM Sync Lambda

Sync **all** Chainguard NPM package versions to an AWS CodeArtifact repository. For each unique package name found in a `package-lock.json` lockfile, the Lambda fetches every version available from Chainguard Libraries, diffs against what exists in CodeArtifact, and publishes only new versions.

The function is designed for long-running syncs: it auto-checkpoints to S3 when the Lambda timeout approaches, re-invokes itself, and resumes from where it left off — fully hands-free.

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [Environment Variables](#environment-variables)
- [Invocation Payloads](#invocation-payloads)
- [Feature Details](#feature-details)
  - [SNS/Email Notifications](#snsemail-notifications)
  - [Dry-Run Mode](#dry-run-mode)
  - [Parallel Tarball Downloads](#parallel-tarball-downloads)
  - [Retry Logic with Exponential Backoff](#retry-logic-with-exponential-backoff)
  - [Package Allowlist / Denylist](#package-allowlist--denylist)
  - [CloudWatch Custom Metrics](#cloudwatch-custom-metrics)
- [Cron Job Setup (EventBridge Scheduler)](#cron-job-setup-eventbridge-scheduler)
  - [Console Setup](#console-setup)
  - [AWS CLI Setup](#aws-cli-setup)
  - [Common Schedules](#common-schedules)
- [Terraform Deployment](#terraform-deployment)
  - [Project Structure](#project-structure)
  - [Full Terraform Configuration](#full-terraform-configuration)
  - [Deploying](#deploying)
  - [Variables Reference](#variables-reference)
  - [Outputs Reference](#outputs-reference)
- [Monitoring & Alerting](#monitoring--alerting)
- [Troubleshooting](#troubleshooting)

---

## Features

| Feature | Description |
|---------|-------------|
| **SNS/Email Notifications** | Get notified on completion or failure via SNS (email, Slack, PagerDuty, etc.) |
| **Dry-Run Mode** | Preview what would sync without publishing anything |
| **Parallel Downloads** | Configurable thread pool for concurrent tarball downloads |
| **Retry with Backoff** | Exponential backoff on HTTP 429/5xx errors with configurable attempts |
| **Allowlist / Denylist** | Glob-pattern filtering (`@types/*`, `lodash`, `*-loader`) |
| **CloudWatch Metrics** | Custom metrics: PackagesSynced, PackagesFailed, PackagesSkipped, etc. |
| **Checkpointing** | Auto-saves to S3 before timeout, self-invokes to continue |
| **Garbage Cleanup** | Removes malformed packages from previous parser bugs |

---

## Architecture Overview

```
┌──────────────┐       ┌────────────────────┐       ┌──────────────────┐
│  EventBridge │──────▶│  Lambda Function    │──────▶│  CodeArtifact    │
│  (cron/rate) │       │                    │       │  (npm registry)  │
└──────────────┘       │  1. Parse lockfile  │       └──────────────────┘
                       │  2. Filter packages │
┌──────────────┐       │  3. Discover diffs  │       ┌──────────────────┐
│  S3 Bucket   │◀─────▶│  4. Download (‖)    │◀─────▶│  Chainguard      │
│  (lockfile,  │       │  5. Publish         │       │  Libraries       │
│  checkpoints)│       │  6. Notify + Metrics│       └──────────────────┘
└──────────────┘       └────────────────────┘
                              │        │
                              ▼        ▼
                        ┌─────────┐ ┌──────────┐
                        │   SNS   │ │CloudWatch│
                        │ (email) │ │ (metrics)│
                        └─────────┘ └──────────┘
```

---

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `CGR_USERNAME` | Chainguard registry username |
| `CGR_TOKEN` | Chainguard registry token |

### CodeArtifact Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_REGION` | `us-east-2` | AWS region for all services |
| `CODEARTIFACT_DOMAIN` | `my-npm-domain` | CodeArtifact domain name |
| `CODEARTIFACT_REPO` | `my-npm-repo` | CodeArtifact repository name |
| `CODEARTIFACT_UPSTREAM` | `npm-public` | Upstream repo with npmjs connection |

### S3 & Checkpointing

| Variable | Default | Description |
|----------|---------|-------------|
| `ERRORS_S3_BUCKET` | _(none)_ | S3 bucket for `errors.txt` output |
| `ERRORS_S3_PREFIX` | `chainguard-sync-all/` | Key prefix for error files |
| `CHECKPOINT_S3_BUCKET` | _(falls back to ERRORS_S3_BUCKET)_ | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync-all/checkpoints/` | Key prefix for checkpoints |
| `CHECKPOINT_BUFFER_MS` | `120000` | Milliseconds before timeout to save checkpoint |
| `MAX_INVOCATIONS` | `50` | Safety cap on self-re-invocations |
| `MAX_PACKAGES` | `50000` | Max packages to process per run |

### New Feature Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SNS_TOPIC_ARN` | _(none)_ | SNS topic ARN for completion/failure notifications |
| `DRY_RUN` | `false` | Default dry-run mode (`true`/`false`) |
| `PACKAGE_ALLOWLIST` | _(none)_ | Comma-separated glob patterns (e.g., `@types/*,lodash`) |
| `PACKAGE_DENYLIST` | _(none)_ | Comma-separated glob patterns (e.g., `debug,colors`) |
| `PARALLEL_DOWNLOADS` | `5` | Max concurrent tarball download threads |
| `RETRY_MAX_ATTEMPTS` | `3` | Max retry attempts for HTTP errors |
| `RETRY_BASE_DELAY` | `1.0` | Base delay in seconds (doubles each retry) |
| `LOG_LEVEL` | `DEBUG` | Logging level: `DEBUG`, `INFO`, `WARN` |

---

## Invocation Payloads

### Sync from lockfile
```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "lockfiles/package-lock.json"
}
```

### Sync with filtering and dry-run
```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "lockfiles/package-lock.json",
  "dry_run": true,
  "allowlist": ["@types/*", "react", "react-dom"],
  "denylist": ["debug", "colors"]
}
```

### Update all existing CodeArtifact packages
```json
{
  "update_existing": true
}
```

### Resume a failed/timed-out run
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

---

## Feature Details

### SNS/Email Notifications

The Lambda publishes to an SNS topic on every sync completion and on failures. Each message includes the full summary JSON and a `status` message attribute (`SUCCESS`, `FAILURE`) so subscribers can filter.

**Setup:**

1. Create an SNS topic in your AWS account.
2. Subscribe your email address (or Slack webhook, PagerDuty, etc.) to the topic.
3. Confirm the subscription via the email you receive.
4. Set the `SNS_TOPIC_ARN` environment variable on the Lambda.

**What you receive:**

- On success: Subject like `Chainguard Sync COMPLETE — run 20260402T...` with a JSON body containing counts of synced, skipped, and failed packages.
- On failure: Subject like `Chainguard Sync FAILED — run 20260402T...` with the error message.
- Dry-run completions are prefixed with `[DRY-RUN]`.

**Filtering by status (SQS/Lambda subscriber):**

```json
{
  "status": ["FAILURE"]
}
```

This subscription filter policy delivers only failure notifications.

---

### Dry-Run Mode

Dry-run executes the full discovery phase (fetching metadata from Chainguard, diffing against CodeArtifact) but skips all publishing. The response includes a sample of what *would* be synced.

**Enable via event payload:**
```json
{ "dry_run": true, "s3_bucket": "...", "s3_key": "..." }
```

**Enable as default via environment variable:**
```
DRY_RUN=true
```

The event-level `dry_run` flag always takes precedence over the environment variable.

**Dry-run response:**
```json
{
  "statusCode": 200,
  "body": {
    "message": "Dry-run complete — no packages were published",
    "summary": {
      "run_id": "20260402T120000Z-abc12345",
      "dry_run": true,
      "total_work_items": 142,
      "would_sync": 142,
      "skipped_existing": 1803,
      "sample_work_items": [
        "@types/node@22.15.2",
        "lodash@4.17.22",
        "..."
      ],
      "remaining_items": 117,
      "status": "dry_run_complete"
    }
  }
}
```

---

### Parallel Tarball Downloads

Tarballs are downloaded in batches using a `ThreadPoolExecutor`. Each batch fetches `PARALLEL_DOWNLOADS` tarballs concurrently, then publishes them sequentially to CodeArtifact.

**Configuration:**
```
PARALLEL_DOWNLOADS=5    # default: 5 threads
```

You can also set it higher (e.g., `10` or `15`) depending on your Lambda memory and network capacity. Each thread holds one tarball in memory, so keep memory in mind for large packages.

**How it works internally:**

1. Processing loop advances in batches of `PARALLEL_DOWNLOADS` items.
2. A `ThreadPoolExecutor` downloads all tarballs in the batch concurrently.
3. Results are collected into a dict mapping index → bytes.
4. Each downloaded tarball is published to CodeArtifact sequentially.
5. The loop advances to the next batch.

---

### Retry Logic with Exponential Backoff

All HTTP operations (metadata fetch, tarball download, CodeArtifact publish) now use automatic retry with exponential backoff.

**Configuration:**
```
RETRY_MAX_ATTEMPTS=3    # total attempts (1 = no retry)
RETRY_BASE_DELAY=1.0    # initial delay; doubles each retry
```

**Retry schedule (defaults):**

| Attempt | Delay Before |
|---------|-------------|
| 1st | immediate |
| 2nd | 1.0s |
| 3rd | 2.0s |

**Retryable HTTP status codes:** `429` (rate limited), `500`, `502`, `503`, `504`.

Non-retryable errors (e.g., `404`, `401`, `403`) fail immediately. Network-level errors (`URLError`, `TimeoutError`, `OSError`) are also retried.

---

### Package Allowlist / Denylist

Filter packages by name using glob patterns. This is useful for syncing only specific scopes or excluding known-problematic packages.

**Via event payload:**
```json
{
  "allowlist": ["@types/*", "react", "react-dom", "@babel/*"],
  "denylist": ["debug", "colors", "*-deprecated"]
}
```

**Via environment variables:**
```
PACKAGE_ALLOWLIST=@types/*,react,react-dom
PACKAGE_DENYLIST=debug,colors
```

**Filtering rules:**

1. If an allowlist is provided, **only** packages matching at least one allowlist pattern are kept.
2. If a denylist is provided, packages matching **any** denylist pattern are removed.
3. The denylist is applied after the allowlist.
4. If both are empty, all packages pass through.

**Supported glob patterns:**

| Pattern | Matches |
|---------|---------|
| `lodash` | Exact match only |
| `@types/*` | Any package in the `@types` scope |
| `*-loader` | Any package ending in `-loader` |
| `@babel/*` | Any package in the `@babel` scope |

The summary response includes `filtered_out` count and the patterns used.

---

### CloudWatch Custom Metrics

After every run (including dry-runs), the Lambda publishes custom metrics to CloudWatch under the `Chainguard/NPMSync` namespace.

**Metrics published:**

| Metric Name | Description |
|-------------|-------------|
| `PackagesSynced` | Successfully published to CodeArtifact |
| `PackagesFailed` | Failed to publish |
| `PackagesSkipped` | Already existed in CodeArtifact |
| `TarballDownloadFailed` | Tarball download failures |
| `HTTPErrors` | Total HTTP errors encountered |

**Dimensions:**

| Dimension | Value |
|-----------|-------|
| `Domain` | CodeArtifact domain name |
| `Repository` | CodeArtifact repository name |
| `Mode` | `DryRun` (only present in dry-run mode) |

**Example CloudWatch alarm (Terraform):**
```hcl
resource "aws_cloudwatch_metric_alarm" "sync_failures" {
  alarm_name          = "chainguard-sync-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "PackagesFailed"
  namespace           = "Chainguard/NPMSync"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  alarm_actions       = [aws_sns_topic.alerts.arn]

  dimensions = {
    Domain     = "my-npm-domain"
    Repository = "my-npm-repo"
  }
}
```

---

## Cron Job Setup (EventBridge Scheduler)

### Console Setup

1. Navigate to **Amazon EventBridge → Rules** in the AWS Console.
2. Click **Create rule**.
3. Enter a name like `chainguard-npm-sync-daily`.
4. Under **Rule type**, select **Schedule**.
5. Choose a schedule expression (see [Common Schedules](#common-schedules)).
6. Under **Target**, select **Lambda function** and choose your function.
7. For **Configure input**, select **Constant (JSON text)** and enter:
   ```json
   {
     "s3_bucket": "my-lockfile-bucket",
     "s3_key": "lockfiles/package-lock.json"
   }
   ```
8. Click **Create**.

### AWS CLI Setup

**Create the EventBridge rule:**
```bash
aws events put-rule \
  --name chainguard-npm-sync-daily \
  --schedule-expression "cron(0 6 * * ? *)" \
  --state ENABLED \
  --description "Daily Chainguard NPM sync at 6 AM UTC"
```

**Add the Lambda as a target:**
```bash
aws events put-targets \
  --rule chainguard-npm-sync-daily \
  --targets '[{
    "Id": "chainguard-sync-lambda",
    "Arn": "arn:aws:lambda:us-east-2:123456789012:function:chainguard-codeartifact-sync-all",
    "Input": "{\"s3_bucket\":\"my-lockfile-bucket\",\"s3_key\":\"lockfiles/package-lock.json\"}"
  }]'
```

**Grant EventBridge permission to invoke the Lambda:**
```bash
aws lambda add-permission \
  --function-name chainguard-codeartifact-sync-all \
  --statement-id eventbridge-daily-sync \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-2:123456789012:rule/chainguard-npm-sync-daily
```

### Common Schedules

| Schedule | Expression | Description |
|----------|-----------|-------------|
| Daily at 6 AM UTC | `cron(0 6 * * ? *)` | Good for most teams |
| Every 6 hours | `rate(6 hours)` | More frequent updates |
| Weekdays at midnight | `cron(0 0 ? * MON-FRI *)` | Business-hours alignment |
| Weekly on Sunday | `cron(0 2 ? * SUN *)` | Low-frequency mirror |
| Every 12 hours | `cron(0 */12 * * ? *)` | Twice-daily sync |

**Tip:** Use `rate(1 hour)` with `"update_existing": true` as the input payload to continuously keep your mirror current.

---

## Terraform Deployment

### Project Structure

```
chainguard-sync/
├── main.tf              # Lambda, IAM, S3, CodeArtifact
├── variables.tf         # Input variables
├── outputs.tf           # Output values
├── eventbridge.tf       # Cron schedule
├── sns.tf               # Notifications
├── cloudwatch.tf        # Alarms
└── lambda/
    └── lambda_function-sync-all.py
```

### Full Terraform Configuration

#### `variables.tf`

```hcl
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "function_name" {
  description = "Lambda function name"
  type        = string
  default     = "chainguard-codeartifact-sync-all"
}

variable "s3_bucket_name" {
  description = "S3 bucket for lockfiles, checkpoints, and error logs"
  type        = string
}

variable "lockfile_s3_key" {
  description = "S3 key for the package-lock.json file"
  type        = string
  default     = "lockfiles/package-lock.json"
}

variable "codeartifact_domain" {
  description = "CodeArtifact domain name"
  type        = string
  default     = "my-npm-domain"
}

variable "codeartifact_repo" {
  description = "CodeArtifact repository name"
  type        = string
  default     = "my-npm-repo"
}

variable "codeartifact_upstream" {
  description = "CodeArtifact upstream repo name"
  type        = string
  default     = "npm-public"
}

variable "cgr_username" {
  description = "Chainguard registry username"
  type        = string
  sensitive   = true
}

variable "cgr_token" {
  description = "Chainguard registry token"
  type        = string
  sensitive   = true
}

variable "notification_email" {
  description = "Email address for SNS notifications (leave empty to skip)"
  type        = string
  default     = ""
}

variable "schedule_expression" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "cron(0 6 * * ? *)"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds (max 900)"
  type        = number
  default     = 900
}

variable "lambda_memory" {
  description = "Lambda memory in MB"
  type        = number
  default     = 1024
}

variable "parallel_downloads" {
  description = "Max concurrent tarball download threads"
  type        = number
  default     = 5
}

variable "retry_max_attempts" {
  description = "Max retry attempts for HTTP errors"
  type        = number
  default     = 3
}

variable "package_allowlist" {
  description = "Comma-separated glob patterns for package allowlist"
  type        = string
  default     = ""
}

variable "package_denylist" {
  description = "Comma-separated glob patterns for package denylist"
  type        = string
  default     = ""
}

variable "dry_run" {
  description = "Enable dry-run mode by default"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "chainguard-npm-sync"
    ManagedBy = "terraform"
  }
}
```

#### `main.tf`

```hcl
terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ─── S3 Bucket ───────────────────────────────────────────────────────────────

resource "aws_s3_bucket" "sync" {
  bucket = var.s3_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "sync" {
  bucket = aws_s3_bucket.sync.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "sync" {
  bucket = aws_s3_bucket.sync.id

  rule {
    id     = "expire-old-checkpoints"
    status = "Enabled"

    filter {
      prefix = "chainguard-sync-all/checkpoints/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-old-errors"
    status = "Enabled"

    filter {
      prefix = "chainguard-sync-all/errors-"
    }

    expiration {
      days = 30
    }
  }
}

# ─── IAM Role ────────────────────────────────────────────────────────────────

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "lambda" {
  name = "${var.function_name}-role"
  tags = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda" {
  name = "${var.function_name}-policy"
  role = aws_iam_role.lambda.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:HeadObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.sync.arn,
          "${aws_s3_bucket.sync.arn}/*"
        ]
      },
      {
        Sid    = "CodeArtifact"
        Effect = "Allow"
        Action = [
          "codeartifact:GetAuthorizationToken",
          "codeartifact:GetRepositoryEndpoint",
          "codeartifact:ReadFromRepository",
          "codeartifact:PublishPackageVersion",
          "codeartifact:PutPackageOriginConfiguration",
          "codeartifact:ListPackages",
          "codeartifact:ListPackageVersions",
          "codeartifact:DeletePackageVersions",
          "codeartifact:CreateDomain",
          "codeartifact:CreateRepository",
          "codeartifact:UpdateRepository",
          "codeartifact:AssociateExternalConnection"
        ]
        Resource = "*"
      },
      {
        Sid      = "StsForCodeArtifact"
        Effect   = "Allow"
        Action   = "sts:GetServiceBearerToken"
        Resource = "*"
      },
      {
        Sid    = "LambdaSelfInvoke"
        Effect = "Allow"
        Action = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.function_name}"
      },
      {
        Sid    = "CloudWatchMetrics"
        Effect = "Allow"
        Action = "cloudwatch:PutMetricData"
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "Chainguard/NPMSync"
          }
        }
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = "sns:Publish"
        Resource = var.notification_email != "" ? aws_sns_topic.sync[0].arn : "*"
      }
    ]
  })
}

# ─── Lambda Function ─────────────────────────────────────────────────────────

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda/lambda_function-sync-all.py"
  output_path = "${path.module}/.build/lambda.zip"
}

resource "aws_lambda_function" "sync" {
  function_name    = var.function_name
  role             = aws_iam_role.lambda.arn
  handler          = "lambda_function-sync-all.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  tags             = var.tags

  environment {
    variables = {
      CGR_USERNAME       = var.cgr_username
      CGR_TOKEN          = var.cgr_token
      AWS_REGION_OVERRIDE = var.aws_region
      CODEARTIFACT_DOMAIN = var.codeartifact_domain
      CODEARTIFACT_REPO   = var.codeartifact_repo
      CODEARTIFACT_UPSTREAM = var.codeartifact_upstream
      ERRORS_S3_BUCKET    = aws_s3_bucket.sync.id
      CHECKPOINT_S3_BUCKET = aws_s3_bucket.sync.id
      SNS_TOPIC_ARN       = var.notification_email != "" ? aws_sns_topic.sync[0].arn : ""
      DRY_RUN             = var.dry_run ? "true" : "false"
      PACKAGE_ALLOWLIST   = var.package_allowlist
      PACKAGE_DENYLIST    = var.package_denylist
      PARALLEL_DOWNLOADS  = tostring(var.parallel_downloads)
      RETRY_MAX_ATTEMPTS  = tostring(var.retry_max_attempts)
      LOG_LEVEL           = "INFO"
    }
  }
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 14
  tags              = var.tags
}
```

#### `sns.tf`

```hcl
resource "aws_sns_topic" "sync" {
  count = var.notification_email != "" ? 1 : 0
  name  = "${var.function_name}-notifications"
  tags  = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.sync[0].arn
  protocol  = "email"
  endpoint  = var.notification_email
}
```

#### `eventbridge.tf`

```hcl
resource "aws_cloudwatch_event_rule" "sync_schedule" {
  name                = "${var.function_name}-schedule"
  description         = "Scheduled trigger for Chainguard NPM sync"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "sync_lambda" {
  rule      = aws_cloudwatch_event_rule.sync_schedule.name
  target_id = "chainguard-sync"
  arn       = aws_lambda_function.sync.arn

  input = jsonencode({
    s3_bucket = aws_s3_bucket.sync.id
    s3_key    = var.lockfile_s3_key
  })
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_schedule.arn
}
```

#### `cloudwatch.tf`

```hcl
resource "aws_cloudwatch_metric_alarm" "sync_failures" {
  alarm_name          = "${var.function_name}-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "PackagesFailed"
  namespace           = "Chainguard/NPMSync"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert when Chainguard sync has package publish failures"
  alarm_actions       = var.notification_email != "" ? [aws_sns_topic.sync[0].arn] : []
  tags                = var.tags

  dimensions = {
    Domain     = var.codeartifact_domain
    Repository = var.codeartifact_repo
  }
}

resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  alarm_name          = "${var.function_name}-lambda-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 3600
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Alert when the Lambda function itself errors"
  alarm_actions       = var.notification_email != "" ? [aws_sns_topic.sync[0].arn] : []
  tags                = var.tags

  dimensions = {
    FunctionName = aws_lambda_function.sync.function_name
  }
}

resource "aws_cloudwatch_dashboard" "sync" {
  dashboard_name = "${var.function_name}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Packages Synced vs Failed"
          metrics = [
            ["Chainguard/NPMSync", "PackagesSynced", "Domain", var.codeartifact_domain, "Repository", var.codeartifact_repo],
            [".", "PackagesFailed", ".", ".", ".", "."],
            [".", "TarballDownloadFailed", ".", ".", ".", "."]
          ]
          period = 86400
          stat   = "Sum"
          region = var.aws_region
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6
        properties = {
          title   = "Lambda Duration & Invocations"
          metrics = [
            ["AWS/Lambda", "Duration", "FunctionName", var.function_name],
            [".", "Invocations", ".", "."],
            [".", "Errors", ".", "."]
          ]
          period = 86400
          stat   = "Sum"
          region = var.aws_region
          view   = "timeSeries"
        }
      }
    ]
  })
}
```

#### `outputs.tf`

```hcl
output "lambda_function_arn" {
  description = "ARN of the sync Lambda function"
  value       = aws_lambda_function.sync.arn
}

output "lambda_function_name" {
  description = "Name of the sync Lambda function"
  value       = aws_lambda_function.sync.function_name
}

output "s3_bucket_name" {
  description = "S3 bucket for lockfiles, checkpoints, and errors"
  value       = aws_s3_bucket.sync.id
}

output "sns_topic_arn" {
  description = "SNS topic ARN for notifications"
  value       = var.notification_email != "" ? aws_sns_topic.sync[0].arn : "N/A (no email configured)"
}

output "cloudwatch_dashboard_url" {
  description = "URL for the CloudWatch dashboard"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.sync.dashboard_name}"
}

output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge schedule rule"
  value       = aws_cloudwatch_event_rule.sync_schedule.arn
}
```

### Deploying

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan \
  -var="s3_bucket_name=my-chainguard-sync" \
  -var="cgr_username=your-cgr-user" \
  -var="cgr_token=your-cgr-token" \
  -var="notification_email=team@example.com"

# Apply
terraform apply \
  -var="s3_bucket_name=my-chainguard-sync" \
  -var="cgr_username=your-cgr-user" \
  -var="cgr_token=your-cgr-token" \
  -var="notification_email=team@example.com"

# Upload your lockfile
aws s3 cp package-lock.json s3://my-chainguard-sync/lockfiles/package-lock.json

# Test with a dry-run
aws lambda invoke \
  --function-name chainguard-codeartifact-sync-all \
  --payload '{"s3_bucket":"my-chainguard-sync","s3_key":"lockfiles/package-lock.json","dry_run":true}' \
  --cli-binary-format raw-in-base64-out \
  response.json && cat response.json | jq .
```

### Variables Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `s3_bucket_name` | Yes | — | S3 bucket name |
| `cgr_username` | Yes | — | Chainguard username |
| `cgr_token` | Yes | — | Chainguard token |
| `notification_email` | No | `""` | Email for alerts |
| `schedule_expression` | No | `cron(0 6 * * ? *)` | EventBridge schedule |
| `lambda_timeout` | No | `900` | Timeout in seconds |
| `lambda_memory` | No | `1024` | Memory in MB |
| `parallel_downloads` | No | `5` | Download threads |
| `package_allowlist` | No | `""` | Comma-separated globs |
| `package_denylist` | No | `""` | Comma-separated globs |
| `dry_run` | No | `false` | Default dry-run mode |

### Outputs Reference

| Output | Description |
|--------|-------------|
| `lambda_function_arn` | Lambda ARN for cross-account invocations |
| `lambda_function_name` | Function name for CLI invocations |
| `s3_bucket_name` | Bucket name for uploading lockfiles |
| `sns_topic_arn` | SNS topic for adding more subscribers |
| `cloudwatch_dashboard_url` | Direct link to the monitoring dashboard |
| `eventbridge_rule_arn` | Schedule rule ARN |

---

## Monitoring & Alerting

Once deployed, you get three layers of observability:

**CloudWatch Logs** — Every Lambda invocation logs to `/aws/lambda/chainguard-codeartifact-sync-all` with structured messages showing discovery progress, per-package actions, and timing.

**CloudWatch Custom Metrics** — Under the `Chainguard/NPMSync` namespace, you can build dashboards and alarms on `PackagesSynced`, `PackagesFailed`, `PackagesSkipped`, `TarballDownloadFailed`, and `HTTPErrors`.

**SNS Notifications** — Every completion/failure sends a JSON summary to your SNS topic. Use subscription filter policies to route failures to PagerDuty and successes to a Slack channel.

**Recommended alarms:**

- `PackagesFailed > 0` — Something failed to publish; investigate errors.txt in S3.
- `HTTPErrors > 10` — Chainguard or CodeArtifact may be having issues.
- `Lambda Errors > 0` — The function itself crashed (OOM, timeout without checkpoint, etc.).

---

## Troubleshooting

**"Exceeded max invocations" error** — The sync needed more than 50 Lambda invocations. Increase `MAX_INVOCATIONS` or use `resume_run_id` to continue.

**"No S3 bucket for checkpoints" error** — Set `CHECKPOINT_S3_BUCKET` or `ERRORS_S3_BUCKET` environment variable.

**Slow syncs** — Increase `PARALLEL_DOWNLOADS` (try `10` or `15`) and ensure Lambda memory is at least 1024 MB.

**Rate limiting (HTTP 429)** — The retry logic handles this automatically. If persistent, reduce `PARALLEL_DOWNLOADS` or increase `RETRY_BASE_DELAY`.

**Packages not appearing** — Check that they're not being filtered by your allowlist/denylist. Run a dry-run to see what would sync.

**SNS notifications not arriving** — Confirm the subscription in your email, and verify `SNS_TOPIC_ARN` is set correctly. Check the Lambda logs for "Failed to send SNS notification" errors.
