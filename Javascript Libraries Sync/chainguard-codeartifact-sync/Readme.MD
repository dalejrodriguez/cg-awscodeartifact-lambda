# chainguard-codeartifact-sync

## Sync Lockfile Versions from Chainguard Libraries to AWS CodeArtifact

**Handler:** `lambda_function.lambda_handler`
**Runtime:** Python 3.12 | arm64 | 1024 MB | 15-minute timeout
**File:** `lambda_function.py` (1,702 lines)

---

## Table of Contents

- [What It Does](#what-it-does)
  - [Version Selection Logic](#version-selection-logic)
- [Architecture](#architecture)
- [Entry Modes](#entry-modes)
  - [1. New Session — Sync Lockfile](#1-new-session--sync-lockfile)
  - [2. Update Existing — Check for New Versions](#2-update-existing--check-for-new-versions)
  - [3. Resume Previous Session](#3-resume-previous-session)
  - [4. List Checkpoints](#4-list-checkpoints)
  - [5. Cleanup Only](#5-cleanup-only)
  - [6. Dry Run](#6-dry-run)
- [Event Payload Reference](#event-payload-reference)
  - [Allowlist / Denylist Examples](#allowlist--denylist-examples)
- [Environment Variables](#environment-variables)
- [Response Formats](#response-formats)
  - [Successful Completion](#successful-completion)
  - [Discovery Checkpoint (Mid-Run)](#discovery-checkpoint-mid-run)
  - [Processing Checkpoint (Mid-Run)](#processing-checkpoint-mid-run)
  - [Dry Run](#dry-run)
- [Features](#features)
  - [Two-Phase Checkpointing](#two-phase-checkpointing)
  - [Parallel Tarball Downloads](#parallel-tarball-downloads)
  - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
  - [SNS Notifications](#sns-notifications)
  - [CloudWatch Custom Metrics](#cloudwatch-custom-metrics)
  - [Garbage Cleanup](#garbage-cleanup)
  - [Auth Redirect Handling](#auth-redirect-handling)
- [IAM Permissions Required](#iam-permissions-required)
- [Error Handling](#error-handling)
- [Scaling Guide](#scaling-guide)

## What It Does

This Lambda function reads a `package-lock.json` file, extracts the exact `name@version` pairs your project depends on, checks which of those versions exist in Chainguard Libraries, and publishes the matching tarballs to your AWS CodeArtifact NPM repository. Versions already in CodeArtifact are skipped.

This is the **conservative** sync function — it only mirrors the specific versions your project uses, not every version Chainguard has. For a full mirror of all available versions, use the companion function `chainguard-codeartifact-sync-all`.

### Version Selection Logic

```
Lockfile versions:     {lodash@4.17.21, express@4.18.2, axios@1.6.0}
Chainguard versions:   {lodash@4.17.21, lodash@4.17.20, express@4.18.2, express@4.17.1}
CodeArtifact existing: {lodash@4.17.21}

Candidates = lockfile ∩ chainguard = {lodash@4.17.21, express@4.18.2}
New = candidates - existing          = {express@4.18.2}

Result: Only express@4.18.2 is downloaded and published.
```

---

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              Lambda Invocation               │
                    └──────────────────┬──────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────────┐
                    │         PHASE 1: DISCOVERY                  │
                    │                                              │
                    │  For each package name in lockfile:          │
                    │  1. Fetch metadata from Chainguard           │
                    │  2. Intersect lockfile versions ∩ Chainguard │
                    │  3. Subtract existing CodeArtifact versions  │
                    │  4. Build work item list                     │
                    │                                              │
                    │  ⏱ Checkpoints to S3 on timeout             │
                    │  ↻ Self-invokes to continue                  │
                    └──────────────────┬──────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────────┐
                    │         PHASE 2: PROCESSING                 │
                    │                                              │
                    │  For each work item:                         │
                    │  1. Download tarball from Chainguard (retry) │
                    │  2. Publish to CodeArtifact (npm PUT)        │
                    │                                              │
                    │  ⏱ Checkpoints to S3 on timeout             │
                    │  ↻ Self-invokes to continue                  │
                    └──────────────────┬──────────────────────────┘
                                       │
                    ┌──────────────────▼──────────────────────────┐
                    │             FINALIZE                         │
                    │  • Publish CloudWatch metrics                │
                    │  • Send SNS notification                     │
                    │  • Upload errors.txt to S3                   │
                    │  • Delete checkpoint                         │
                    └─────────────────────────────────────────────┘
```

Both phases independently check remaining execution time before each item and save a checkpoint to S3 if running low. The function then asynchronously invokes itself to continue from the checkpoint. This enables processing of 50,000+ packages across unlimited 15-minute invocations.

---

## Entry Modes

### 1. New Session — Sync Lockfile

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "my-bucket",
        "s3_key": "lockfiles/package-lock.json"
    }' response.json
```

### 2. Update Existing — Check for New Versions

Scans all packages already in CodeArtifact, checks Chainguard for newer versions of those specific packages, and publishes the diff. No lockfile required.

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"update_existing": true}' response.json
```

### 3. Resume Previous Session

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"resume_run_id": "20260402T034658Z-abc12345"}' response.json
```

### 4. List Checkpoints

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"list_checkpoints": true}' response.json
```

### 5. Cleanup Only

Removes garbage packages with `node_modules` in their name (artifacts from a previous parser bug).

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"cleanup_only": true}' response.json
```

### 6. Dry Run

Preview what would be synced without downloading or publishing.

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{
        "s3_bucket": "my-bucket",
        "s3_key": "lockfiles/package-lock.json",
        "dry_run": true
    }' response.json
```

---

## Event Payload Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `s3_bucket` | string | — | S3 bucket containing package-lock.json |
| `s3_key` | string | — | S3 key for package-lock.json |
| `lockfile_content` | string | — | Raw JSON lockfile (alternative to S3, for files < 6 MB) |
| `setup_codeartifact` | bool | `false` | Create CodeArtifact domain/repo if they don't exist |
| `update_existing` | bool | `false` | Scan existing CodeArtifact packages for new Chainguard versions |
| `resume_run_id` | string | — | Resume a previous run by its run ID |
| `list_checkpoints` | bool | `false` | List all resumable checkpoint sessions |
| `cleanup_only` | bool | `false` | Only run garbage package cleanup |
| `cleanup_garbage` | bool | `true` | Run garbage cleanup before new sessions |
| `dry_run` | bool | `false` | Preview sync without downloading or publishing |
| `max_packages` | int | `50000` | Max packages to process (overrides env var) |
| `allowlist` | list | — | Only sync packages matching these names/prefixes |
| `denylist` | list | — | Exclude packages matching these names/prefixes |
| `parallel_downloads` | int | `5` | Parallel tarball download threads |

### Allowlist / Denylist Examples

Prefix matching uses a trailing `/`. Exact matching uses the full package name.

```json
{"allowlist": ["@types/", "lodash"]}
```

Syncs only packages under `@types/*` and the exact package `lodash`.

```json
{"denylist": ["@test/", "eslint", "@dev/"]}
```

Syncs everything except packages under `@test/*`, `@dev/*`, and the exact package `eslint`.

If both are provided, allowlist is applied first, then denylist.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CGR_USERNAME` | *(required)* | Chainguard Libraries username |
| `CGR_TOKEN` | *(required)* | Chainguard Libraries token |
| `AWS_REGION` | `us-east-2` | AWS region |
| `CODEARTIFACT_DOMAIN` | `my-npm-domain` | CodeArtifact domain name |
| `CODEARTIFACT_REPO` | `my-npm-repo` | CodeArtifact repository name |
| `CODEARTIFACT_UPSTREAM` | `npm-public` | Upstream repository (public npm fallback) |
| `ERRORS_S3_BUCKET` | — | S3 bucket for error logs |
| `ERRORS_S3_PREFIX` | `chainguard-sync/` | S3 prefix for error logs |
| `CHECKPOINT_S3_BUCKET` | *(falls back to ERRORS_S3_BUCKET)* | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/` | S3 prefix for checkpoints |
| `CHECKPOINT_BUFFER_MS` | `120000` | Milliseconds before timeout to save checkpoint |
| `MAX_INVOCATIONS` | `50` | Max chained self-invocations per run |
| `MAX_PACKAGES` | `50000` | Max packages to process per run |
| `SNS_TOPIC_ARN` | — | SNS topic ARN for notifications (empty to disable) |
| `PARALLEL_DOWNLOADS` | `5` | Default parallel download threads |
| `MAX_RETRIES` | `3` | Max retries on HTTP 500/502/503/429 |
| `RETRY_BASE_DELAY` | `1.0` | Base delay in seconds for exponential backoff |
| `CW_NAMESPACE` | `ChainguardSync` | CloudWatch custom metrics namespace |
| `CW_METRICS_ENABLED` | `true` | Enable/disable CloudWatch metrics |
| `LOG_LEVEL` | `DEBUG` | Logging level |

**Bucket fallback chain:** `checkpoint_s3_bucket` in event → `CHECKPOINT_S3_BUCKET` env var → `errors_s3_bucket` in event → `ERRORS_S3_BUCKET` env var. At least one must be set.

---

## Response Formats

### Successful Completion

```json
{
    "statusCode": 200,
    "body": {
        "message": "Sync complete",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "total_invocations": 4,
            "total_work_items": 892,
            "synced_to_codeartifact": 845,
            "skipped_existing": 1203,
            "tarball_download_failed": 12,
            "failed_to_publish": 3,
            "http_errors": 15,
            "errors_file": "s3://my-bucket/chainguard-sync/errors-20260402T035723Z.txt",
            "errored_packages": ["wrappy@1.0.2"],
            "not_found_packages": ["some-private-pkg"],
            "status": "complete"
        }
    }
}
```

### Discovery Checkpoint (Mid-Run)

```json
{
    "statusCode": 200,
    "body": {
        "message": "Discovery checkpointed at package 800/2519 — continuation #2 launched",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "phase": "discovery",
            "discovery_progress": "800/2519",
            "work_items_so_far": 234,
            "status": "continuing"
        }
    }
}
```

### Processing Checkpoint (Mid-Run)

```json
{
    "statusCode": 200,
    "body": {
        "message": "Checkpointed at 400/892 — continuation #5 launched",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "phase": "processing",
            "processed_so_far": 400,
            "total_work_items": 892,
            "status": "continuing"
        }
    }
}
```

### Dry Run

```json
{
    "statusCode": 200,
    "body": {
        "message": "Dry run complete",
        "summary": {
            "mode": "dry_run",
            "total_work_items": 142,
            "skipped_existing": 1203,
            "packages": ["@types/node@22.15.10", "express@4.18.2"],
            "status": "dry_run_complete"
        }
    }
}
```

---

## Features

### Two-Phase Checkpointing

Both discovery and processing check `context.get_remaining_time_in_millis()` before each item. When time is low, the full state (work items, counters, errors, progress index) is saved to S3, and the function invokes itself asynchronously. The next invocation loads the checkpoint and resumes exactly where the previous one stopped.

Checkpoint format:

```json
{
    "phase": "discovery",
    "package_names": ["lodash", "@types/node", "..."],
    "discovery_index": 800,
    "work_items": [{"name": "lodash", "version": "4.17.21", "tarball_url": "..."}],
    "counters": {"found": 0, "not_found": 0, "failed": 0, "skipped_existing": 1580}
}
```

### Parallel Tarball Downloads

When `parallel_downloads > 1`, tarballs are downloaded in batches using `ThreadPoolExecutor`. Each batch downloads `parallel_downloads × 2` items concurrently, then publishes them sequentially (CodeArtifact publishing is not parallelized to avoid rate limits).

### Retry with Exponential Backoff

HTTP errors on status codes 500, 502, 503, and 429 (rate limit) are automatically retried. The delay doubles each retry: 1s → 2s → 4s → fail. Client errors (400, 404) fail immediately with no retries.

### SNS Notifications

When `SNS_TOPIC_ARN` is set, notifications are sent:

- **Completion:** `[INFO] Chainguard Sync — Sync complete successfully — 845 synced`
- **Completion with errors:** `[ERROR] Chainguard Sync — Sync complete with errors — 830 synced`
- **Discovery crash:** `[ERROR] Chainguard Sync — FAILED — <run_id>`
- **Processing crash:** `[ERROR] Chainguard Sync — FAILED — <run_id>`
- **Dry run:** `[INFO] Chainguard Sync — Dry run complete — 142 items`

### CloudWatch Custom Metrics

Published to the `ChainguardSync` namespace after each completed run:

| Metric | Description |
|---|---|
| `PackagesSynced` | Successfully published to CodeArtifact |
| `PackagesFailed` | Failed to publish |
| `PackagesSkipped` | Already existed in CodeArtifact |
| `TarballDownloadFailed` | Tarball download failures |
| `TotalWorkItems` | Total items discovered |

Dimensions: `FunctionName`, `Repository`.

### Garbage Cleanup

Automatically removes packages with `node_modules` in their name before each new session. These were created by a previous parser bug that treated nested lockfile paths as package names. Disable per-run with `"cleanup_garbage": false`.

### Auth Redirect Handling

Chainguard tarball URLs redirect (302) to presigned Cloudflare R2/S3 URLs. Python's `urllib` forwards the `Authorization` header on redirect, causing S3 to reject with "Missing x-amz-content-sha256". The `_StripAuthOnRedirectHandler` class removes the auth header before following redirects.

---

## IAM Permissions Required

| Service | Actions |
|---|---|
| CodeArtifact | `CreateDomain`, `CreateRepository`, `UpdateRepository`, `AssociateExternalConnection`, `GetAuthorizationToken`, `GetRepositoryEndpoint`, `PutPackageOriginConfiguration`, `PublishPackageVersion`, `ListPackageVersions`, `ListPackages`, `DeletePackageVersions` |
| STS | `GetServiceBearerToken` |
| S3 | `GetObject`, `PutObject`, `DeleteObject`, `ListBucket` |
| Lambda | `InvokeFunction` (self) |
| SNS | `Publish` |
| CloudWatch | `PutMetricData` |

---

## Error Handling

Errors are handled at three levels:

1. **Per-package (non-fatal):** HTTP errors during metadata fetch, tarball download, or publishing are caught, recorded to the `ErrorTracker`, and processing continues.
2. **Error log files:** `errors.txt` is written to S3 at every exit point — timeouts, max invocations, crashes, and clean completions.
3. **Crash recovery:** Unexpected exceptions save a checkpoint and send an SNS notification before returning the error with resume instructions.

---

## Scaling Guide

| Setting | Default | 50K packages | 100K packages |
|---|---|---|---|
| `MAX_PACKAGES` | 50,000 | 50,000 | 100,000 |
| `MAX_INVOCATIONS` | 50 | 50 | 100 |
| `PARALLEL_DOWNLOADS` | 5 | 10 | 10 |
| `CHECKPOINT_BUFFER_MS` | 120,000 | 120,000 | 120,000 |
| Estimated discovery time | — | ~2.7 hours | ~5.4 hours |
| Estimated total time | — | ~4 hours | ~8 hours |

At ~200ms per metadata fetch + version check, discovery processes ~4,500 packages per 15-minute invocation. Processing throughput depends on tarball sizes but typically handles 200-400 packages per invocation with parallel downloads.
