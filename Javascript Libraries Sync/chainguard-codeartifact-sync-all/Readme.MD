# chainguard-codeartifact-sync-all

## Sync ALL Chainguard NPM Package Versions to AWS CodeArtifact

**Handler:** `sync_all_versions.lambda_handler`
**Runtime:** Python 3.12 | arm64 | 1024 MB | 15-minute timeout
**File:** `sync_all_versions.py` (1,699 lines)

---

## Table of Contents

- [What It Does](#what-it-does)
  - [Version Selection Logic](#version-selection-logic)
- [Architecture](#architecture)
- [When to Use This vs. the Lockfile Sync](#when-to-use-this-vs-the-lockfile-sync)
- [Entry Modes](#entry-modes)
  - [1. New Session — Sync All Versions](#1-new-session--sync-all-versions)
  - [2. Update Existing — Check for New Versions](#2-update-existing--check-for-new-versions)
  - [3. Resume Previous Session](#3-resume-previous-session)
  - [4. List Checkpoints](#4-list-checkpoints)
  - [5. Cleanup Only](#5-cleanup-only)
  - [6. Dry Run](#6-dry-run)
- [Event Payload Reference](#event-payload-reference)
  - [Allowlist / Denylist](#allowlist--denylist)
- [Environment Variables](#environment-variables)
- [Response Formats](#response-formats)
  - [Successful Completion](#successful-completion)
  - [Dry Run](#dry-run)
  - [Checkpoint Continuation](#checkpoint-continuation)
- [Features](#features)
  - [Two-Phase Checkpointing](#two-phase-checkpointing)
  - [Parallel Tarball Downloads](#parallel-tarball-downloads)
  - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
  - [SNS Notifications](#sns-notifications)
  - [CloudWatch Custom Metrics](#cloudwatch-custom-metrics)
  - [Package Allowlist / Denylist](#package-allowlist--denylist)
  - [Garbage Cleanup](#garbage-cleanup)
  - [Auth Redirect Handling](#auth-redirect-handling)
- [Differences from Lockfile Sync](#differences-from-lockfile-sync)
- [Scheduling with EventBridge](#scheduling-with-eventbridge)
- [IAM Permissions Required](#iam-permissions-required)
- [Error Handling](#error-handling)
- [Scaling Guide](#scaling-guide)
  - [Recommended Strategy for Initial Population](#recommended-strategy-for-initial-population)

## What It Does

This Lambda function reads a `package-lock.json` file to identify which packages your project uses, then fetches **every version** of each package that Chainguard Libraries offers and publishes them all to your AWS CodeArtifact NPM repository. Versions already in CodeArtifact are skipped.

This is the **aggressive** sync function — it fully populates your CodeArtifact mirror for every package in your dependency tree. For syncing only the exact versions your project uses, use the companion function `chainguard-codeartifact-sync`.

### Version Selection Logic

```
Lockfile packages:     {lodash, express}
Chainguard versions:   {lodash@4.17.21, lodash@4.17.20, lodash@4.17.19,
                        express@4.18.2, express@4.17.1, express@4.16.4}
CodeArtifact existing: {lodash@4.17.21}

Candidates = ALL chainguard versions for each package name
New = candidates - existing = {lodash@4.17.20, lodash@4.17.19,
                               express@4.18.2, express@4.17.1, express@4.16.4}

Result: 5 versions downloaded and published.
```

Compare this to the lockfile-only sync which would produce only 1 work item (`express@4.18.2`). This function is designed for building a complete, version-rich mirror.

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
                    │  2. Collect ALL Chainguard versions          │
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

The key difference from the lockfile-only lambda is in Phase 1: instead of intersecting with lockfile versions, this function takes **all** versions from Chainguard for each discovered package name. This typically produces 10-50× more work items.

---

## When to Use This vs. the Lockfile Sync

| Scenario | Use This Function | Use Lockfile Sync |
|---|---|---|
| Initial mirror population | Yes — get all available versions | — |
| CI/CD pipeline (on deploy) | — | Yes — mirror exactly what you use |
| Scheduled daily sync | Yes — catches all new versions | — |
| Emergency patch (need one version fast) | — | Yes — targeted, faster |
| Multi-project monorepo shared mirror | Yes — covers all version needs | — |
| Minimal CodeArtifact storage | — | Yes — stores only what you need |
| Compliance (full audit trail) | Yes — complete version history | — |

---

## Entry Modes

### 1. New Session — Sync All Versions

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "my-bucket",
        "s3_key": "lockfiles/package-lock.json"
    }' response.json
```

### 2. Update Existing — Check for New Versions

Scans all packages already in CodeArtifact and checks Chainguard for any versions not yet mirrored. No lockfile required — ideal for daily cron.

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"update_existing": true}' response.json
```

### 3. Resume Previous Session

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"resume_run_id": "20260402T034658Z-abc12345"}' response.json
```

### 4. List Checkpoints

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"list_checkpoints": true}' response.json
```

### 5. Cleanup Only

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"cleanup_only": true}' response.json
```

### 6. Dry Run

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
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

### Allowlist / Denylist

Filter which packages to discover. The lockfile might reference 2,000 packages, but you may only want to mirror a subset.

```json
{
    "s3_bucket": "my-bucket",
    "s3_key": "lockfiles/package-lock.json",
    "allowlist": ["@types/", "lodash", "express"]
}
```

This syncs all Chainguard versions of `@types/*`, `lodash`, and `express` — but none of the other 1,997 packages in the lockfile.

```json
{
    "s3_bucket": "my-bucket",
    "s3_key": "lockfiles/package-lock.json",
    "denylist": ["@test/", "@dev/", "eslint"]
}
```

This syncs everything except test/dev tooling.

Prefix matching uses trailing `/`. Exact matching uses the full name. Allowlist is applied first, then denylist.

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
| `ERRORS_S3_PREFIX` | `chainguard-sync-all/` | S3 prefix for error logs |
| `CHECKPOINT_S3_BUCKET` | *(falls back to ERRORS_S3_BUCKET)* | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync-all/checkpoints/` | S3 prefix for checkpoints |
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

Note the S3 prefixes use `chainguard-sync-all/` (not `chainguard-sync/`) to keep state separate from the lockfile-only lambda.

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
            "total_invocations": 12,
            "total_work_items": 4521,
            "synced_to_codeartifact": 4480,
            "skipped_existing": 8930,
            "tarball_download_failed": 28,
            "failed_to_publish": 13,
            "http_errors": 41,
            "errors_file": "s3://my-bucket/chainguard-sync-all/errors-20260402T051200Z.txt",
            "errored_packages": ["wrappy@1.0.2"],
            "not_found_packages": ["some-private-pkg"],
            "status": "complete"
        }
    }
}
```

Note the higher numbers compared to lockfile sync — this function syncs every version, so `total_work_items` and `total_invocations` are typically much larger.

### Dry Run

```json
{
    "statusCode": 200,
    "body": {
        "message": "Dry run complete",
        "summary": {
            "mode": "dry_run",
            "total_work_items": 4521,
            "skipped_existing": 8930,
            "packages": ["lodash@4.17.21", "lodash@4.17.20", "lodash@4.17.19", "..."],
            "status": "dry_run_complete"
        }
    }
}
```

### Checkpoint Continuation

```json
{
    "statusCode": 200,
    "body": {
        "message": "Checkpointed at 1200/4521 — continuation #4 launched",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "phase": "processing",
            "processed_so_far": 1200,
            "total_work_items": 4521,
            "status": "continuing"
        }
    }
}
```

---

## Features

All features are identical to the lockfile-only lambda. The only difference is the version selection logic in Phase 1 (discovery).

### Two-Phase Checkpointing

Discovery and processing each independently checkpoint to S3 and self-invoke on timeout. Supports processing tens of thousands of versions across unlimited 15-minute invocations.

### Parallel Tarball Downloads

`ThreadPoolExecutor` with configurable `parallel_downloads` (default 5). Downloads batches concurrently, publishes sequentially.

### Retry with Exponential Backoff

Retries HTTP 500/502/503/429 with exponential backoff: 1s → 2s → 4s → fail. Configurable via `MAX_RETRIES` and `RETRY_BASE_DELAY`.

### SNS Notifications

Sent on completion, completion with errors, discovery crash, processing crash, and dry run. Set `SNS_TOPIC_ARN` to enable.

### CloudWatch Custom Metrics

Published to `ChainguardSync` namespace: `PackagesSynced`, `PackagesFailed`, `PackagesSkipped`, `TarballDownloadFailed`, `TotalWorkItems`. Dimensions: `FunctionName`, `Repository`.

### Package Allowlist / Denylist

Filter which packages to discover. Supports exact names and prefix matching.

### Garbage Cleanup

Removes packages with `node_modules` in their name. Runs automatically before new sessions.

### Auth Redirect Handling

Strips `Authorization` header on 302 redirects from Chainguard to presigned S3/R2 URLs.

---

## Differences from Lockfile Sync

| Aspect | This Function (all-versions) | Lockfile Sync |
|---|---|---|
| **Version selection** | All Chainguard versions per package | Only lockfile-pinned versions |
| **Work item volume** | 10-50× more items (e.g., 4,500 vs 150) | Minimal — only what you use |
| **Discovery time** | Longer — more versions to diff | Shorter — fewer candidates |
| **Processing time** | Much longer — more tarballs | Shorter |
| **Storage usage** | Higher — all versions stored | Minimal |
| **S3 prefix** | `chainguard-sync-all/` | `chainguard-sync/` |
| **Function name** | `chainguard-codeartifact-sync-all` | `chainguard-codeartifact-sync` |
| **Best for** | Full mirrors, daily cron, compliance | CI/CD, targeted deploys |

---

## Scheduling with EventBridge

This function is ideal for daily scheduled runs to keep your mirror up to date:

```yaml
UpdateSchedule:
    Type: AWS::Events::Rule
    Properties:
        ScheduleExpression: 'cron(0 2 * * ? *)'
        Targets:
            - Arn: !GetAtt SyncAllVersionsFunction.Arn
              Id: DailyFullSync
              Input: '{"update_existing": true}'

UpdateSchedulePermission:
    Type: AWS::Lambda::Permission
    Properties:
        FunctionName: !Ref SyncAllVersionsFunction
        Action: lambda:InvokeFunction
        Principal: events.amazonaws.com
        SourceArn: !GetAtt UpdateSchedule.Arn
```

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

Identical to the lockfile sync:

1. **Per-package (non-fatal):** HTTP errors during metadata fetch, tarball download, or publishing are caught, recorded, and processing continues.
2. **Error log files:** `errors.txt` uploaded to S3 at every exit point.
3. **Crash recovery:** Unexpected exceptions save a checkpoint, send an SNS notification, and return an error with resume instructions.

---

## Scaling Guide

This function produces significantly more work than the lockfile sync, so scaling matters more.

| Setting | Default | 500 packages × 50 versions each | 2,000 packages × 50 versions each |
|---|---|---|---|
| `MAX_PACKAGES` | 50,000 | 50,000 | 50,000 |
| `MAX_INVOCATIONS` | 50 | 50 | 100 |
| `PARALLEL_DOWNLOADS` | 5 | 10 | 10 |
| Estimated discovery | — | ~1.7 hours (8 invocations) | ~6.7 hours (30 invocations) |
| Estimated processing | — | ~6 hours (25 invocations) | ~25 hours (100 invocations) |
| Estimated total | — | ~8 hours | ~32 hours |

For large mirrors, increase `MAX_INVOCATIONS` to 100+ and `PARALLEL_DOWNLOADS` to 10. Consider using the allowlist to focus on critical packages first, then gradually expand.

### Recommended Strategy for Initial Population

1. **Dry run first** — see how many work items your lockfile produces:
   ```json
   {"s3_bucket": "...", "s3_key": "...", "dry_run": true}
   ```

2. **Start with high-priority packages** — use an allowlist for the most critical:
   ```json
   {"s3_bucket": "...", "s3_key": "...", "allowlist": ["@types/", "express", "react"]}
   ```

3. **Full run** — remove the allowlist and let it process everything:
   ```json
   {"s3_bucket": "...", "s3_key": "...", "max_packages": 100000}
   ```

4. **Schedule daily updates** — use `update_existing` via EventBridge to keep the mirror current.
