# Chainguard NPM → AWS CodeArtifact Sync Lambda

An AWS Lambda function that synchronizes NPM packages from [Chainguard Libraries for JavaScript](https://libraries.cgr.dev/javascript) into an AWS CodeArtifact repository. It reads a `package-lock.json` file from S3, determines which package versions are available in Chainguard but missing from CodeArtifact, and publishes them — handling authentication, retries with exponential backoff, parallel downloads, checkpointing, and self-continuation across Lambda timeout boundaries.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Architecture](#architecture)
- [Entry Modes](#entry-modes)
- [Event Payloads](#event-payloads)
- [Environment Variables](#environment-variables)
- [Execution Phases](#execution-phases)
- [Checkpoint & Continuation System](#checkpoint--continuation-system)
- [Error Handling](#error-handling)
- [Retry Logic with Exponential Backoff](#retry-logic-with-exponential-backoff)
- [Parallel Tarball Downloads](#parallel-tarball-downloads)
- [Dry-Run Mode](#dry-run-mode)
- [Package Allowlist / Denylist](#package-allowlist--denylist)
- [SNS Notifications](#sns-notifications)
- [CloudWatch Custom Metrics](#cloudwatch-custom-metrics)
- [Garbage Cleanup](#garbage-cleanup)
- [Key Implementation Details](#key-implementation-details)
- [IAM Permissions Required](#iam-permissions-required)
- [Deployment Notes](#deployment-notes)
- [Scheduled Execution (Cron)](#scheduled-execution-cron)
- [Terraform Deployment](#terraform-deployment)
- [Examples](#examples)

---

## How It Works

1. The function reads a `package-lock.json` from S3 (or scans existing CodeArtifact packages in update mode).
2. Package names are filtered through optional allowlist/denylist patterns.
3. For each unique package, it queries the Chainguard JavaScript registry to see which versions are available, with automatic retries on transient failures.
4. It diffs those against the versions already in CodeArtifact, producing a list of "work items" — `(name, version, tarball_url)` tuples that need syncing.
5. In **dry-run mode**, the function returns the plan here without making any changes.
6. For each work item it downloads tarballs from Chainguard **in parallel** (configurable concurrency) and publishes them to CodeArtifact using the npm registry PUT protocol.
7. If the Lambda is about to hit its execution timeout, it saves a checkpoint to S3 and asynchronously re-invokes itself to continue from where it left off.
8. On completion or failure, the function emits **CloudWatch custom metrics** and sends an **SNS notification**.

---

## Architecture

```
┌──────────────┐          ┌──────────────────────┐
│  S3 Bucket   │          │  Chainguard Registry  │
│  (lockfile)  │          │  libraries.cgr.dev    │
└──────┬───────┘          └──────────┬────────────┘
       │                             │
       ▼                             ▼
┌──────────────────────────────────────────────────┐
│              Lambda Function                     │
│                                                  │
│  1. Parse lockfile                               │
│  2. Filter packages (allowlist / denylist)        │
│  3. Discovery: fetch metadata from CGR (w/retry),│
│     diff against CodeArtifact versions           │
│  4. [Dry-run? → return plan, stop here]          │
│  5. Processing: parallel tarball download,       │
│     sequential publish (w/retry)                 │
│  6. Checkpoint & self-invoke if timing out       │
│  7. Emit CloudWatch metrics + SNS notification   │
└────┬─────────────┬──────────────┬────────────────┘
     │             │              │
     ▼             ▼              ▼
┌──────────┐ ┌──────────┐ ┌──────────────────────┐
│CodeArtif.│ │  S3      │ │  CloudWatch + SNS    │
│(target)  │ │(ckpt/err)│ │  (metrics + alerts)  │
└──────────┘ └──────────┘ └──────────────────────┘
```

---

## Entry Modes

The Lambda handler supports five distinct entry modes:

| Mode | Trigger | Description |
|------|---------|-------------|
| **New Session (lockfile)** | `s3_bucket` + `s3_key` | Parse a `package-lock.json` from S3 and sync the exact versions it specifies. |
| **Update Existing** | `update_existing: true` | Scan all packages already published in CodeArtifact and check Chainguard for any newer versions. |
| **Resume** | `resume_run_id: "<id>"` | Manually resume a previously checkpointed or failed run. |
| **List Checkpoints** | `list_checkpoints: true` | Return metadata about all available checkpoints in S3. |
| **Cleanup Only** | `cleanup_only: true` | Remove garbage packages (see [Garbage Cleanup](#garbage-cleanup)) without running any sync. |

---

## Event Payloads

### New session — sync from lockfile

```json
{
  "s3_bucket": "my-lockfiles-bucket",
  "s3_key": "path/to/package-lock.json",
  "setup_codeartifact": false,
  "cleanup_garbage": true,
  "dry_run": false,
  "allowlist": ["@mycompany/*", "lodash", "express"],
  "denylist": ["deprecated-*"],
  "parallel_downloads": 5
}
```

- `setup_codeartifact` (optional, default `false`): Idempotently create the CodeArtifact domain, upstream repo, external npm connection, and main repository before syncing.
- `cleanup_garbage` (optional, default `true`): Run garbage package cleanup before the sync begins.
- `dry_run` (optional, default `false`): Run discovery only and return the list of packages that *would* be synced, without downloading or publishing anything.
- `allowlist` (optional): Array of glob patterns. Only packages matching at least one pattern are synced.
- `denylist` (optional): Array of glob patterns. Packages matching any pattern are excluded.
- `parallel_downloads` (optional): Override the `PARALLEL_DOWNLOADS` env var for this invocation.

### Update existing packages

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

### List checkpoints

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

### Dry-run preview

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/package-lock.json",
  "dry_run": true
}
```

### Inline lockfile (alternative to S3)

```json
{
  "lockfile_content": "{ ... raw package-lock.json string ... }"
}
```

---

## Environment Variables

### Core Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CGR_USERNAME` | **Yes** | — | Chainguard registry username |
| `CGR_TOKEN` | **Yes** | — | Chainguard registry token |
| `AWS_REGION` | No | `us-east-2` | AWS region for all service clients |
| `CODEARTIFACT_DOMAIN` | No | `my-npm-domain` | CodeArtifact domain name |
| `CODEARTIFACT_REPO` | No | `my-npm-repo` | Target CodeArtifact repository |
| `CODEARTIFACT_UPSTREAM` | No | `npm-public` | Upstream repository (public npm proxy) |
| `LOG_LEVEL` | No | `DEBUG` | Logging verbosity (`DEBUG`, `INFO`, `WARN`) |

### S3 & Checkpointing

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ERRORS_S3_BUCKET` | No | — | S3 bucket for error report uploads |
| `ERRORS_S3_PREFIX` | No | `chainguard-sync/` | Key prefix for error reports |
| `CHECKPOINT_S3_BUCKET` | No | *(falls back to `ERRORS_S3_BUCKET`)* | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | No | `chainguard-sync/checkpoints/` | Key prefix for checkpoint files |
| `CHECKPOINT_BUFFER_MS` | No | `120000` (120s) | Milliseconds before Lambda timeout to trigger a checkpoint save |
| `MAX_INVOCATIONS` | No | `50` | Safety cap — max number of self-reinvocations per run |
| `MAX_PACKAGES` | No | `50000` | Max packages to process in a single run |

### Retry & Parallelism

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MAX_RETRIES` | No | `3` | Maximum retry attempts for transient HTTP errors (429, 5xx) |
| `RETRY_BASE_DELAY` | No | `1.0` | Base delay in seconds for exponential backoff |
| `PARALLEL_DOWNLOADS` | No | `5` | Max concurrent tarball downloads per batch |

### Notifications & Metrics

| Variable | Required | Default | Description |
|----------|---------|---------|-------------|
| `SNS_TOPIC_ARN` | No | — | SNS topic ARN for completion/failure email notifications |
| `CW_METRICS_NAMESPACE` | No | `ChainguardSync` | CloudWatch custom metrics namespace |
| `CW_METRICS_ENABLED` | No | `true` | Enable/disable CloudWatch metric emission |

### Package Filtering

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ALLOWLIST` | No | — | JSON array of glob patterns to include (e.g., `["@myco/*", "lodash"]`) |
| `DENYLIST` | No | — | JSON array of glob patterns to exclude (e.g., `["deprecated-*"]`) |

---

## Execution Phases

Every sync run proceeds through two phases, both of which are independently checkpointable:

### Phase 1 — Discovery

Iterates over the list of package names (from the lockfile or from CodeArtifact in update mode). For each package:

1. Fetches full metadata from the Chainguard registry (`GET https://libraries.cgr.dev/javascript/<package>`) with automatic retry on transient errors.
2. Determines which versions are candidates (lockfile-pinned versions in lockfile mode; all Chainguard versions in update mode).
3. Lists versions already present in CodeArtifact.
4. Produces work items for versions that exist in Chainguard but are missing from CodeArtifact.

If the Lambda is running low on time, it checkpoints the current discovery position and self-invokes to continue.

If **dry-run mode** is active, the function returns the discovered work items here and stops — nothing is downloaded or published.

### Phase 2 — Processing

Iterates over the discovered work items in batches. For each batch:

1. Downloads tarballs from Chainguard **in parallel** using a `ThreadPoolExecutor` (configurable concurrency via `parallel_downloads` or `PARALLEL_DOWNLOADS`), with auth-stripping redirect handling and retry on transient errors.
2. Publishes each tarball to CodeArtifact **sequentially** using the npm registry PUT protocol, including SHA-1/SHA-512 integrity hashes and retry with exponential backoff.
3. Handles `409 Conflict` / "already exists" responses gracefully (treated as success).

Checkpointing and self-invocation work the same way as in the discovery phase.

---

## Checkpoint & Continuation System

The function is designed to process arbitrarily large lockfiles despite Lambda's 15-minute execution limit. It achieves this through:

- **Time awareness**: Before each iteration, it checks `context.get_remaining_time_in_millis()` against `CHECKPOINT_BUFFER_MS`. When time is running low, it saves state and stops.
- **Checkpoint persistence**: State (current phase, progress index, work items, counters, accumulated errors) is serialized to JSON and written to S3.
- **Self-invocation**: After saving, the function asynchronously invokes itself (`InvocationType: "Event"`) with the checkpoint key embedded in the event, picking up exactly where it left off.
- **Safety cap**: `MAX_INVOCATIONS` (default 50) prevents runaway invocation chains. If hit, the run stops with a resume-able checkpoint and an SNS notification is sent.
- **Manual resume**: Any checkpointed run — whether from a timeout, an unexpected error, or hitting the invocation cap — can be resumed later with `{"resume_run_id": "<run_id>"}`.
- **Checkpoint cleanup**: On successful completion, the checkpoint file is deleted from S3.

### Run IDs

Each run gets a unique ID in the format `YYYYMMDDTHHMMSSz-<8-hex-chars>` (e.g., `20260402T034658Z-abc12345`). This ID is used as the checkpoint filename and is returned in all responses.

---

## Error Handling

### ErrorTracker

All per-package errors are collected by an `ErrorTracker` instance that records the package name, phase, error type, HTTP status code, and a truncated detail message. At every exit point (success, failure, or checkpoint), accumulated errors are uploaded to S3 as a timestamped `errors-<timestamp>.txt` file.

### HTTP Error Classification

Errors are classified as:

- **HTTPError** — non-recoverable HTTP failures (with status code and response body captured).
- **Timeout** — connection or read timeouts.
- **URLError** — DNS, connection refused, and other transport-level failures.
- **Other** — any unexpected exception type.

### Graceful Handling

- 404 / 401 / 403 responses from Chainguard are treated as "package not found in Chainguard" rather than hard errors.
- 409 responses from CodeArtifact (or responses containing "already exists" / "previously published" / "EPUBLISHCONFLICT") are treated as success.
- Unexpected exceptions during discovery or processing trigger a failure checkpoint (if an S3 bucket is configured), an SNS notification, and allow manual resume.

---

## Retry Logic with Exponential Backoff

All HTTP operations (metadata fetches, tarball downloads, and CodeArtifact publishes) are wrapped in a retry loop with exponential backoff. This handles transient failures from rate limiting, server errors, and network blips without manual intervention.

### Behavior

- **Retryable status codes**: `429` (Too Many Requests), `500`, `502`, `503`, `504`
- **Non-retryable codes**: `404`, `401`, `403`, `409` — these are handled by existing application logic (not-found, already-exists, etc.)
- **Network errors**: `URLError`, `TimeoutError`, and `OSError` are also retried
- **Backoff formula**: `base_delay × 2^attempt` — e.g., with defaults: 1s, 2s, 4s
- **Max attempts**: Configurable via `MAX_RETRIES` (default 3), so up to 4 total attempts (1 initial + 3 retries)

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_RETRIES` | `3` | Number of retry attempts after initial failure |
| `RETRY_BASE_DELAY` | `1.0` | Base delay in seconds (doubles each attempt) |

### Example Timeline

```
Attempt 1: request fails with HTTP 503
  → wait 1.0s
Attempt 2: request fails with HTTP 503
  → wait 2.0s
Attempt 3: request fails with HTTP 503
  → wait 4.0s
Attempt 4: request succeeds (or error is recorded)
```

---

## Parallel Tarball Downloads

The processing phase downloads tarballs using a `ThreadPoolExecutor`, significantly speeding up syncs with many packages. Downloads happen in parallel while publishes remain sequential (CodeArtifact does not handle concurrent PUTs to the same repo reliably).

### Behavior

- Work items are processed in batches of size `parallel_downloads`.
- Each batch downloads up to N tarballs concurrently, then publishes them one by one.
- Time checks happen between batches to ensure checkpointing still works.
- If `parallel_downloads` is `1`, the function falls back to fully sequential processing.

### Configuration

| Source | Key | Default |
|--------|-----|---------|
| Environment variable | `PARALLEL_DOWNLOADS` | `5` |
| Event payload | `parallel_downloads` | *(uses env var)* |

### Tuning

- **Lambda memory**: Higher parallelism holds more tarballs in memory simultaneously. For `parallel_downloads=5` with average tarball sizes of 1–5 MB, 512 MB Lambda memory is usually sufficient.
- **Network**: Chainguard's registry handles concurrent requests well, but very high parallelism (>10) may trigger rate limiting — the retry logic handles this gracefully.

---

## Dry-Run Mode

Dry-run mode runs the full discovery phase (parsing lockfile, querying Chainguard, diffing against CodeArtifact) but **skips all downloads and publishes**. This is useful for previewing what a sync would do before committing.

### Usage

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/package-lock.json",
  "dry_run": true
}
```

### Response

```json
{
  "statusCode": 200,
  "body": {
    "message": "Dry run complete — no changes made",
    "summary": {
      "run_id": "20260403T120000Z-deadbeef",
      "dry_run": true,
      "total_work_items": 47,
      "skipped_existing": 312,
      "not_found_packages": ["internal-pkg"],
      "packages_to_sync": [
        "lodash@4.17.21",
        "@types/node@18.19.3",
        "express@4.18.2"
      ],
      "status": "dry_run_complete"
    }
  }
}
```

Dry-run mode can be combined with allowlist/denylist filtering to validate filter patterns before running a real sync.

---

## Package Allowlist / Denylist

Package filtering lets you control exactly which packages are synced using glob patterns. Filters are applied after lockfile parsing and before discovery begins.

### Pattern Syntax

Standard `fnmatch` glob patterns are supported:

| Pattern | Matches |
|---------|---------|
| `lodash` | Exact match only |
| `@mycompany/*` | All packages in the `@mycompany` scope |
| `*lodash*` | Any package containing "lodash" |
| `express` | Exact match for `express` |

### Precedence

1. If an **allowlist** is provided, only packages matching at least one allowlist pattern are kept.
2. Then, any packages matching a **denylist** pattern are removed from the remaining set.

This means the denylist can narrow down an allowlist. If neither is provided, all packages pass through.

### Configuration

Filters can be set via environment variables (applied to all invocations) or per-invocation event payload (overrides env vars):

**Environment variables** (JSON arrays):
```bash
ALLOWLIST='["@mycompany/*", "lodash", "express"]'
DENYLIST='["deprecated-*", "@test/*"]'
```

**Event payload** (overrides env vars when present):
```json
{
  "s3_bucket": "...",
  "s3_key": "...",
  "allowlist": ["@mycompany/*"],
  "denylist": ["@mycompany/internal-*"]
}
```

---

## SNS Notifications

The function publishes notifications to an SNS topic on sync completion, failure, or dry-run completion. This enables email alerts, Slack integration, or any other SNS subscriber.

### Setup

1. Create an SNS topic and subscribe your email/Slack/PagerDuty endpoint.
2. Set `SNS_TOPIC_ARN` to the topic ARN.

### Notification Types

| Event | Subject Tag | When |
|-------|-------------|------|
| Sync complete | `[SUCCESS]` | All work items processed successfully (no errors) |
| Sync complete with errors | `[FAILURE]` | Sync finished but some packages had errors |
| Unexpected error | `[FAILURE]` | Unhandled exception during discovery or processing |
| Max invocations reached | `[FAILURE]` | Safety cap hit |
| Dry-run complete | `[INFO]` | Dry-run discovery finished |
| Nothing to sync | `[SUCCESS]` | All packages already up to date |

### Subject Format

```
[ChainguardSync] [SUCCESS] Sync complete — 831 synced
[ChainguardSync] [FAILURE] Sync FAILED — ConnectionError
[ChainguardSync] [INFO] Dry run complete — 47 packages would sync
```

The message body contains the full JSON summary, making it easy to parse programmatically for downstream integrations.

---

## CloudWatch Custom Metrics

The function emits custom CloudWatch metrics at the end of each run (including partial runs that checkpoint). These enable dashboards, alarms, and trend analysis.

### Metrics Emitted

All metrics are published under the namespace configured by `CW_METRICS_NAMESPACE` (default: `ChainguardSync`) with dimensions `Domain` and `Repository`.

| Metric Name | Unit | Description |
|-------------|------|-------------|
| `PackagesSynced` | Count | Packages successfully downloaded and published |
| `PackagesFailed` | Count | Packages that failed to publish |
| `PackagesSkipped` | Count | Package versions already in CodeArtifact |
| `TarballDownloadFailed` | Count | Tarballs that could not be downloaded |
| `HttpErrors` | Count | Total HTTP errors encountered |
| `TotalWorkItems` | Count | Total work items discovered for this run |

### Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `CW_METRICS_ENABLED` | `true` | Set to `false` to disable metric emission |
| `CW_METRICS_NAMESPACE` | `ChainguardSync` | CloudWatch namespace for all metrics |

### Example CloudWatch Alarm

```bash
aws cloudwatch put-metric-alarm \
  --alarm-name "ChainguardSync-HighFailureRate" \
  --namespace ChainguardSync \
  --metric-name PackagesFailed \
  --dimensions Name=Domain,Value=my-npm-domain Name=Repository,Value=my-npm-repo \
  --statistic Sum \
  --period 3600 \
  --threshold 10 \
  --comparison-operator GreaterThanThreshold \
  --evaluation-periods 1 \
  --alarm-actions arn:aws:sns:us-east-2:123456789:my-alerts
```

---

## Garbage Cleanup

The function includes a `cleanup_garbage_packages()` utility that removes packages with `node_modules` in their name from CodeArtifact. These are artifacts from an earlier lockfile parser bug that incorrectly treated nested dependency paths (e.g., `node_modules/@radix-ui/react-alert-dialog/node_modules/@radix-ui/primitive`) as the package name.

Cleanup runs automatically before new sync sessions (controlled by `cleanup_garbage: true/false` in the event) and can also be triggered independently with `{"cleanup_only": true}`.

Versions are deleted in batches of 50 via the CodeArtifact `delete_package_versions` API.

---

## Key Implementation Details

### Authentication

- **Chainguard**: HTTP Basic auth (`CGR_USERNAME:CGR_TOKEN`) sent in the `Authorization` header. A custom `urllib` redirect handler strips this header on 302 redirects so that Chainguard's presigned S3 tarball URLs don't fail with a `Missing x-amz-content-sha256` error.
- **CodeArtifact**: Bearer token obtained via `get_authorization_token`. The token is fetched fresh at the start of each processing phase.

### Publishing Protocol

CodeArtifact's `PublishPackageVersion` API does not support npm. Instead, the function uses the **npm registry PUT protocol**: it constructs a JSON payload with the package metadata, version info, integrity hashes, and a base64-encoded tarball attachment, then PUTs it to the CodeArtifact repository endpoint.

### Origin Controls

Before publishing each package, the function sets origin controls to `publish: ALLOW, upstream: BLOCK`. This ensures the package is treated as a directly-published package rather than one cached from the upstream npm connection.

### Lockfile Parsing

The parser handles npm v2/v3 lockfile format (`packages` field with `node_modules/` path keys). It extracts the package name from the last `node_modules/` segment in the path to correctly handle nested dependencies, and deduplicates `name@version` pairs.

---

## IAM Permissions Required

The Lambda execution role needs the following permissions:

### CodeArtifact

```json
{
  "Effect": "Allow",
  "Action": [
    "codeartifact:CreateDomain",
    "codeartifact:CreateRepository",
    "codeartifact:UpdateRepository",
    "codeartifact:AssociateExternalConnection",
    "codeartifact:GetRepositoryEndpoint",
    "codeartifact:GetAuthorizationToken",
    "codeartifact:ListPackages",
    "codeartifact:ListPackageVersions",
    "codeartifact:DeletePackageVersions",
    "codeartifact:PutPackageOriginConfiguration",
    "sts:GetServiceBearerToken"
  ],
  "Resource": "*"
}
```

### S3 (lockfile, checkpoints, error logs)

```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetObject",
    "s3:PutObject",
    "s3:DeleteObject",
    "s3:ListBucket",
    "s3:HeadObject"
  ],
  "Resource": [
    "arn:aws:s3:::<your-bucket>",
    "arn:aws:s3:::<your-bucket>/*"
  ]
}
```

### Lambda (self-invocation)

```json
{
  "Effect": "Allow",
  "Action": "lambda:InvokeFunction",
  "Resource": "arn:aws:lambda:<region>:<account>:function:<function-name>"
}
```

### SNS (notifications)

```json
{
  "Effect": "Allow",
  "Action": "sns:Publish",
  "Resource": "arn:aws:sns:<region>:<account>:<topic-name>"
}
```

### CloudWatch (custom metrics)

```json
{
  "Effect": "Allow",
  "Action": "cloudwatch:PutMetricData",
  "Resource": "*"
}
```

---

## Deployment Notes

- **Runtime**: Python 3.11+ (uses `list[dict]` type hints, `f-strings`, walrus operator patterns).
- **Dependencies**: `boto3` (included in Lambda runtime). No additional layers required — all HTTP calls use `urllib` from the standard library, and parallelism uses `concurrent.futures` from the standard library.
- **Timeout**: Set the Lambda timeout to **15 minutes** (maximum). The checkpoint system is designed around this limit.
- **Memory**: 256–512 MB is typically sufficient. With `PARALLEL_DOWNLOADS=5`, multiple tarballs may be held in memory simultaneously (typically 1–5 MB each). Increase to 1024 MB if using higher parallelism or syncing packages with unusually large tarballs.
- **Concurrency**: The function is **not** designed for parallel execution of the same lockfile. Use reserved concurrency of 1 or rely on run IDs to avoid conflicts.

---

## Scheduled Execution (Cron)

Use Amazon EventBridge (CloudWatch Events) to run the sync on a schedule. This is the recommended approach for keeping CodeArtifact continuously up to date with Chainguard.

### EventBridge Rule — AWS CLI

**Nightly lockfile sync** (every day at 2:00 AM UTC):

```bash
# Create the rule
aws events put-rule \
  --name "chainguard-sync-nightly" \
  --schedule-expression "cron(0 2 * * ? *)" \
  --description "Nightly Chainguard NPM sync from lockfile" \
  --state ENABLED

# Set the Lambda as the target with the desired event payload
aws events put-targets \
  --rule "chainguard-sync-nightly" \
  --targets '[{
    "Id": "chainguard-sync-lambda",
    "Arn": "arn:aws:lambda:us-east-2:123456789012:function:chainguard-codeartifact-sync",
    "Input": "{\"s3_bucket\": \"my-lockfiles-bucket\", \"s3_key\": \"lockfiles/package-lock.json\"}"
  }]'

# Grant EventBridge permission to invoke the Lambda
aws lambda add-permission \
  --function-name chainguard-codeartifact-sync \
  --statement-id eventbridge-nightly-sync \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn arn:aws:events:us-east-2:123456789012:rule/chainguard-sync-nightly
```

**Update existing packages every 6 hours**:

```bash
aws events put-rule \
  --name "chainguard-sync-update-existing" \
  --schedule-expression "rate(6 hours)" \
  --description "Check for new Chainguard versions of existing CodeArtifact packages" \
  --state ENABLED

aws events put-targets \
  --rule "chainguard-sync-update-existing" \
  --targets '[{
    "Id": "chainguard-sync-update",
    "Arn": "arn:aws:lambda:us-east-2:123456789012:function:chainguard-codeartifact-sync",
    "Input": "{\"update_existing\": true}"
  }]'
```

### Schedule Expression Reference

| Expression | Meaning |
|------------|---------|
| `cron(0 2 * * ? *)` | Every day at 2:00 AM UTC |
| `cron(0 */6 * * ? *)` | Every 6 hours |
| `cron(0 9 ? * MON-FRI *)` | Weekdays at 9:00 AM UTC |
| `rate(1 hour)` | Every hour |
| `rate(12 hours)` | Every 12 hours |
| `rate(1 day)` | Every 24 hours |

### Multiple Schedules

You can create separate rules with different payloads to combine strategies. For example, a nightly full lockfile sync plus a frequent update check:

| Rule | Schedule | Payload |
|------|----------|---------|
| `chainguard-sync-nightly` | `cron(0 2 * * ? *)` | `{"s3_bucket": "...", "s3_key": "..."}` |
| `chainguard-sync-update-6h` | `rate(6 hours)` | `{"update_existing": true}` |
| `chainguard-sync-dry-run-weekly` | `cron(0 10 ? * MON *)` | `{"s3_bucket": "...", "s3_key": "...", "dry_run": true}` |

---

## Terraform Deployment

A complete Terraform configuration for deploying the Lambda function with all supporting resources.

### Directory Structure

```
terraform/
├── main.tf
├── variables.tf
├── outputs.tf
└── lambda/
    └── lambda_function.py
```

### variables.tf

```hcl
variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-2"
}

variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
  default     = "chainguard-codeartifact-sync"
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
  description = "CodeArtifact upstream repository name"
  type        = string
  default     = "npm-public"
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

variable "lambda_memory_mb" {
  description = "Lambda memory allocation in MB"
  type        = number
  default     = 512
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds (max 900)"
  type        = number
  default     = 900
}

variable "parallel_downloads" {
  description = "Max concurrent tarball downloads"
  type        = number
  default     = 5
}

variable "max_retries" {
  description = "Max retry attempts for transient HTTP errors"
  type        = number
  default     = 3
}

variable "schedule_expression" {
  description = "EventBridge schedule expression for the cron job (set to empty string to disable)"
  type        = string
  default     = "cron(0 2 * * ? *)"
}

variable "enable_update_schedule" {
  description = "Enable a separate schedule for update_existing mode"
  type        = bool
  default     = false
}

variable "update_schedule_expression" {
  description = "EventBridge schedule for update_existing checks"
  type        = string
  default     = "rate(6 hours)"
}

variable "sns_notification_email" {
  description = "Email address for SNS notifications (set to empty string to disable)"
  type        = string
  default     = ""
}

variable "cw_metrics_enabled" {
  description = "Enable CloudWatch custom metrics"
  type        = bool
  default     = true
}

variable "cw_metrics_namespace" {
  description = "CloudWatch metrics namespace"
  type        = string
  default     = "ChainguardSync"
}

variable "log_level" {
  description = "Lambda log level (DEBUG, INFO, WARN)"
  type        = string
  default     = "INFO"
}

variable "allowlist" {
  description = "JSON array of package glob patterns to include"
  type        = string
  default     = ""
}

variable "denylist" {
  description = "JSON array of package glob patterns to exclude"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "chainguard-codeartifact-sync"
    ManagedBy = "terraform"
  }
}
```

### main.tf

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

# -------------------------------------------------------------------
# S3 bucket for lockfiles, checkpoints, and error logs
# -------------------------------------------------------------------
resource "aws_s3_bucket" "sync_bucket" {
  bucket = var.s3_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_versioning" "sync_bucket" {
  bucket = aws_s3_bucket.sync_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "sync_bucket" {
  bucket = aws_s3_bucket.sync_bucket.id

  rule {
    id     = "expire-checkpoints"
    status = "Enabled"
    filter {
      prefix = "chainguard-sync/checkpoints/"
    }
    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-error-logs"
    status = "Enabled"
    filter {
      prefix = "chainguard-sync/errors-"
    }
    expiration {
      days = 30
    }
  }
}

# -------------------------------------------------------------------
# SNS topic for notifications (optional)
# -------------------------------------------------------------------
resource "aws_sns_topic" "sync_notifications" {
  count = var.sns_notification_email != "" ? 1 : 0
  name  = "${var.function_name}-notifications"
  tags  = var.tags
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.sns_notification_email != "" ? 1 : 0
  topic_arn = aws_sns_topic.sync_notifications[0].arn
  protocol  = "email"
  endpoint  = var.sns_notification_email
}

# -------------------------------------------------------------------
# IAM role and policy for the Lambda function
# -------------------------------------------------------------------
data "aws_caller_identity" "current" {}

resource "aws_iam_role" "lambda_role" {
  name = "${var.function_name}-role"
  tags = var.tags

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.function_name}-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
      {
        Sid    = "CodeArtifact"
        Effect = "Allow"
        Action = [
          "codeartifact:CreateDomain",
          "codeartifact:CreateRepository",
          "codeartifact:UpdateRepository",
          "codeartifact:AssociateExternalConnection",
          "codeartifact:GetRepositoryEndpoint",
          "codeartifact:GetAuthorizationToken",
          "codeartifact:ListPackages",
          "codeartifact:ListPackageVersions",
          "codeartifact:DeletePackageVersions",
          "codeartifact:PutPackageOriginConfiguration",
        ]
        Resource = "*"
      },
      {
        Sid      = "STSBearerToken"
        Effect   = "Allow"
        Action   = "sts:GetServiceBearerToken"
        Resource = "*"
      },
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:HeadObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.sync_bucket.arn,
          "${aws_s3_bucket.sync_bucket.arn}/*",
        ]
      },
      {
        Sid      = "LambdaSelfInvoke"
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = "arn:aws:lambda:${var.aws_region}:${data.aws_caller_identity.current.account_id}:function:${var.function_name}"
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = var.cw_metrics_namespace
          }
        }
      },
      {
        Sid    = "SNSPublish"
        Effect = "Allow"
        Action = "sns:Publish"
        Resource = var.sns_notification_email != "" ? aws_sns_topic.sync_notifications[0].arn : "arn:aws:sns:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"
      },
    ]
  })
}

# -------------------------------------------------------------------
# Lambda function
# -------------------------------------------------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/lambda/lambda_function.py"
  output_path = "${path.module}/.build/lambda_function.zip"
}

resource "aws_lambda_function" "sync" {
  function_name    = var.function_name
  role             = aws_iam_role.lambda_role.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_mb
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  reserved_concurrent_executions = 1

  environment {
    variables = {
      CGR_USERNAME         = var.cgr_username
      CGR_TOKEN            = var.cgr_token
      AWS_REGION           = var.aws_region
      CODEARTIFACT_DOMAIN  = var.codeartifact_domain
      CODEARTIFACT_REPO    = var.codeartifact_repo
      CODEARTIFACT_UPSTREAM = var.codeartifact_upstream
      ERRORS_S3_BUCKET     = aws_s3_bucket.sync_bucket.id
      ERRORS_S3_PREFIX     = "chainguard-sync/"
      CHECKPOINT_S3_BUCKET = aws_s3_bucket.sync_bucket.id
      CHECKPOINT_S3_PREFIX = "chainguard-sync/checkpoints/"
      PARALLEL_DOWNLOADS   = tostring(var.parallel_downloads)
      MAX_RETRIES          = tostring(var.max_retries)
      LOG_LEVEL            = var.log_level
      SNS_TOPIC_ARN        = var.sns_notification_email != "" ? aws_sns_topic.sync_notifications[0].arn : ""
      CW_METRICS_ENABLED   = tostring(var.cw_metrics_enabled)
      CW_METRICS_NAMESPACE = var.cw_metrics_namespace
      ALLOWLIST            = var.allowlist
      DENYLIST             = var.denylist
    }
  }

  tags = var.tags
}

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 14
  tags              = var.tags
}

# -------------------------------------------------------------------
# EventBridge schedule — lockfile sync (cron)
# -------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "sync_schedule" {
  count               = var.schedule_expression != "" ? 1 : 0
  name                = "${var.function_name}-schedule"
  description         = "Scheduled Chainguard NPM sync from lockfile"
  schedule_expression = var.schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "sync_target" {
  count = var.schedule_expression != "" ? 1 : 0
  rule  = aws_cloudwatch_event_rule.sync_schedule[0].name
  arn   = aws_lambda_function.sync.arn

  input = jsonencode({
    s3_bucket = aws_s3_bucket.sync_bucket.id
    s3_key    = var.lockfile_s3_key
  })
}

resource "aws_lambda_permission" "eventbridge_sync" {
  count         = var.schedule_expression != "" ? 1 : 0
  statement_id  = "AllowEventBridgeSync"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sync_schedule[0].arn
}

# -------------------------------------------------------------------
# EventBridge schedule — update existing packages (optional)
# -------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "update_schedule" {
  count               = var.enable_update_schedule ? 1 : 0
  name                = "${var.function_name}-update-schedule"
  description         = "Check for new Chainguard versions of existing CodeArtifact packages"
  schedule_expression = var.update_schedule_expression
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "update_target" {
  count = var.enable_update_schedule ? 1 : 0
  rule  = aws_cloudwatch_event_rule.update_schedule[0].name
  arn   = aws_lambda_function.sync.arn

  input = jsonencode({
    update_existing = true
  })
}

resource "aws_lambda_permission" "eventbridge_update" {
  count         = var.enable_update_schedule ? 1 : 0
  statement_id  = "AllowEventBridgeUpdate"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sync.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.update_schedule[0].arn
}

# -------------------------------------------------------------------
# CloudWatch alarm — high failure rate (optional)
# -------------------------------------------------------------------
resource "aws_cloudwatch_metric_alarm" "high_failure_rate" {
  count               = var.cw_metrics_enabled ? 1 : 0
  alarm_name          = "${var.function_name}-high-failure-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "PackagesFailed"
  namespace           = var.cw_metrics_namespace
  period              = 3600
  statistic           = "Sum"
  threshold           = 10
  alarm_description   = "Fires when more than 10 packages fail to sync in an hour"
  treat_missing_data  = "notBreaching"

  dimensions = {
    Domain     = var.codeartifact_domain
    Repository = var.codeartifact_repo
  }

  alarm_actions = var.sns_notification_email != "" ? [aws_sns_topic.sync_notifications[0].arn] : []
  tags          = var.tags
}
```

### outputs.tf

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
  value       = aws_s3_bucket.sync_bucket.id
}

output "sns_topic_arn" {
  description = "SNS topic ARN for notifications (if enabled)"
  value       = var.sns_notification_email != "" ? aws_sns_topic.sync_notifications[0].arn : null
}

output "sync_schedule_rule" {
  description = "EventBridge rule name for scheduled sync (if enabled)"
  value       = var.schedule_expression != "" ? aws_cloudwatch_event_rule.sync_schedule[0].name : null
}

output "update_schedule_rule" {
  description = "EventBridge rule name for update schedule (if enabled)"
  value       = var.enable_update_schedule ? aws_cloudwatch_event_rule.update_schedule[0].name : null
}
```

### Usage

**Minimal deployment** (lockfile sync, nightly cron, no notifications):

```bash
terraform init
terraform apply \
  -var="cgr_username=my-user" \
  -var="cgr_token=my-token" \
  -var="s3_bucket_name=my-chainguard-sync-bucket"
```

**Full deployment** (notifications, update schedule, filtering):

```bash
terraform apply \
  -var="cgr_username=my-user" \
  -var="cgr_token=my-token" \
  -var="s3_bucket_name=my-chainguard-sync-bucket" \
  -var="lockfile_s3_key=lockfiles/package-lock.json" \
  -var='sns_notification_email=team@example.com' \
  -var="enable_update_schedule=true" \
  -var='allowlist=["@mycompany/*", "react", "express"]' \
  -var='denylist=["deprecated-*"]' \
  -var="parallel_downloads=8" \
  -var="log_level=INFO"
```

**Using a `terraform.tfvars` file**:

```hcl
# terraform.tfvars
aws_region        = "us-east-2"
s3_bucket_name    = "acme-chainguard-sync"
cgr_username      = "acme-service-account"
cgr_token         = "cgr-token-value"

codeartifact_domain   = "acme-npm"
codeartifact_repo     = "acme-packages"
codeartifact_upstream = "npm-public"

lockfile_s3_key       = "lockfiles/package-lock.json"
schedule_expression   = "cron(0 2 * * ? *)"

enable_update_schedule    = true
update_schedule_expression = "rate(6 hours)"

sns_notification_email = "platform-eng@acme.com"
parallel_downloads     = 5
max_retries            = 3
log_level              = "INFO"

allowlist = "[\"@acme/*\", \"react\", \"express\"]"
denylist  = "[\"@acme/deprecated-*\"]"

tags = {
  Project     = "chainguard-sync"
  Team        = "platform-engineering"
  Environment = "production"
  ManagedBy   = "terraform"
}
```

### Sensitive Variables

Store `cgr_username` and `cgr_token` securely. Recommended approaches:

- **Terraform Cloud / HCP**: Mark as sensitive workspace variables.
- **AWS Secrets Manager**: Store the credentials there and reference them via a `data` source, or use the Lambda's environment variable encryption with a KMS key.
- **CI/CD**: Inject via `TF_VAR_cgr_username` and `TF_VAR_cgr_token` environment variables from your pipeline's secret store.

Never commit credentials to version control or `.tfvars` files checked into git.

---

## Examples

### Typical response — sync complete

```json
{
  "statusCode": 200,
  "body": {
    "message": "Sync complete",
    "summary": {
      "run_id": "20260403T120000Z-deadbeef",
      "total_invocations": 3,
      "total_work_items": 847,
      "synced_to_codeartifact": 831,
      "skipped_existing": 412,
      "tarball_download_failed": 12,
      "failed_to_publish": 4,
      "http_errors": 16,
      "errors_file": "s3://my-bucket/chainguard-sync/errors-20260403T121500Z.txt",
      "errored_packages": ["pkg-a@1.0.0", "pkg-b@2.3.1"],
      "not_found_packages": ["internal-pkg", "deprecated-pkg"],
      "status": "complete"
    }
  }
}
```

### Response — checkpointed mid-run

```json
{
  "statusCode": 200,
  "body": {
    "message": "Checkpointed at 340/847 — continuation #2 launched",
    "summary": {
      "run_id": "20260403T120000Z-deadbeef",
      "invocation_number": 1,
      "phase": "processing",
      "processed_so_far": 340,
      "total_work_items": 847,
      "status": "continuing"
    }
  }
}
```

### Response — dry-run preview

```json
{
  "statusCode": 200,
  "body": {
    "message": "Dry run complete — no changes made",
    "summary": {
      "run_id": "20260403T120000Z-deadbeef",
      "dry_run": true,
      "total_work_items": 47,
      "skipped_existing": 312,
      "not_found_packages": ["internal-pkg"],
      "packages_to_sync": [
        "lodash@4.17.21",
        "@types/node@18.19.3",
        "express@4.18.2"
      ],
      "status": "dry_run_complete"
    }
  }
}
```

### Scoped sync with filtering

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "lockfile/package-lock.json",
  "allowlist": ["@mycompany/*", "react", "react-dom"],
  "denylist": ["@mycompany/deprecated-*"],
  "parallel_downloads": 8
}
```

---

## Companion Functions

This Lambda syncs **only the exact versions pinned in a lockfile**. For syncing **all available versions** of each package from Chainguard, use the companion Lambda referenced in the docstring: `sync_all_versions.py`.
