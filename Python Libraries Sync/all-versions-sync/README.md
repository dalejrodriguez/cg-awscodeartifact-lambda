# Chainguard Python → CodeArtifact All-Versions Sync

`lambda.py` — AWS Lambda function that syncs Python packages from Chainguard's secure registries into an AWS CodeArtifact PyPI repository. By default it syncs **every version** Chainguard publishes for each package in your `requirements.txt`, building a fully populated private mirror. Set `SYNC_ALL_VERSIONS=false` to revert to pinned-only behavior.

---

## Table of Contents

- [How It Works](#how-it-works)
- [Chainguard Registries](#chainguard-registries)
- [Invocation Payloads](#invocation-payloads)
- [Inline Options](#inline-options)
- [Environment Variables](#environment-variables)
- [Features](#features)
  - [All-Versions vs Pinned-Only Mode](#all-versions-vs-pinned-only-mode)
  - [Parallel Downloads with Per-Package Publish Locks](#parallel-downloads-with-per-package-publish-locks)
  - [Retry with Exponential Backoff](#retry-with-exponential-backoff)
  - [Distribution File Filtering](#distribution-file-filtering)
  - [Package Allowlist / Denylist](#package-allowlist--denylist)
  - [Dry-Run Mode](#dry-run-mode)
  - [SNS Notifications](#sns-notifications)
  - [CloudWatch Custom Metrics](#cloudwatch-custom-metrics)
  - [Checkpointing and Auto-Continuation](#checkpointing-and-auto-continuation)
  - [Garbage Cleanup](#garbage-cleanup)
  - [Cross-Account CodeArtifact](#cross-account-codeartifact)
- [Architecture](#architecture)
  - [Two-Phase Execution](#two-phase-execution)
  - [Auth Flow](#auth-flow)
  - [Publish Protocol](#publish-protocol)
- [Deployment (SAM)](#deployment-sam)
- [Cron Job Setup (EventBridge)](#cron-job-setup-eventbridge)
- [Monitoring & Alerting](#monitoring--alerting)
- [Troubleshooting](#troubleshooting)

---

## How It Works

1. Reads a `requirements.txt` from S3 (or inline), parsing all `==`-pinned packages.
2. For each package, queries Chainguard's PEP 503/691 Simple API to discover available distribution files (wheels + sdists) across all versions.
3. Diffs against what already exists in CodeArtifact, skipping versions already present.
4. Downloads new distributions in parallel using a thread pool, with retry and SHA-256 verification.
5. Publishes each file to CodeArtifact via the PyPI legacy upload protocol (`multipart/form-data` POST), serializing publishes per-package to avoid concurrent modification errors.
6. Auto-checkpoints to S3 if the Lambda timeout approaches, re-invokes itself, and resumes seamlessly.

---

## Chainguard Registries

Packages are routed to the correct Chainguard registry automatically based on their version string:

| Registry | URL | Selection Rule |
|----------|-----|----------------|
| Remediated | `https://libraries.cgr.dev/python-remediated/simple/` | Default — tried first |
| Standard | `https://libraries.cgr.dev/python/simple/` | Fallback if not in remediated |
| CUDA 12.6 | `https://libraries.cgr.dev/cu126/simple/` | Version contains `+cu126` |
| CUDA 12.8 | `https://libraries.cgr.dev/cu128/simple/` | Version contains `+cu128` |
| CUDA 12.9 | `https://libraries.cgr.dev/cu129/simple/` | Version contains `+cu129` |

All registry base URLs can be overridden via environment variables. CUDA registries are selected exclusively when the version string contains the corresponding `+cuXXX` suffix; non-CUDA packages always try remediated first, then standard.

---

## Invocation Payloads

### Sync from requirements file (new session)
```json
{"s3_bucket": "my-bucket", "s3_key": "path/to/requirements.txt"}
```

### Sync with inline requirements content
```json
{"requirements_content": "numpy==1.26.4\ntorch==2.3.0+cu126\nrequests==2.31.0\n"}
```

### Update all existing CodeArtifact packages with latest Chainguard versions
```json
{"update_existing": true}
```

### Resume a timed-out or failed run
```json
{"resume_run_id": "20260402T034658Z-abc12345"}
```

### List available checkpoints
```json
{"list_checkpoints": true}
```

### Cleanup only — remove packages with invalid PyPI names
```json
{"cleanup_only": true}
```

### Dry-run — preview what would sync
```json
{"s3_bucket": "my-bucket", "s3_key": "requirements.txt", "dry_run": true}
```

---

## Inline Options

These optional keys can be added to any event payload:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `setup_codeartifact` | bool | `false` | Create CodeArtifact domain/repo if they don't exist |
| `cleanup_garbage` | bool | `true` | Run a pre-sync pass to delete packages with invalid PyPI names |
| `dry_run` | bool | `false` | Log intended actions without publishing anything |
| `sync_all_versions` | bool | `true` | Sync every Chainguard version per package; set `false` for pinned-only |
| `allowlist` | list | `[]` | Only sync these package names |
| `denylist` | list | `[]` | Skip these package names |

---

## Environment Variables

### Required

| Variable | Description |
|----------|-------------|
| `CGR_USERNAME` | Chainguard registry username |
| `CGR_TOKEN` | Chainguard registry token |

### AWS / CodeArtifact

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_REGION` | `us-east-2` | AWS region for all service clients |
| `CODEARTIFACT_DOMAIN` | `my-pypi-domain` | CodeArtifact domain name |
| `CODEARTIFACT_REPO` | `my-pypi-repo` | CodeArtifact repository name |
| `CODEARTIFACT_UPSTREAM` | `pypi-public` | Upstream repository name |
| `CODEARTIFACT_DOMAIN_OWNER` | _(empty)_ | Account ID for cross-account access |

### Registry URL Overrides

| Variable | Default |
|----------|---------|
| `CGR_STANDARD_REGISTRY` | `https://libraries.cgr.dev/python/simple/` |
| `CGR_REMEDIATED_REGISTRY` | `https://libraries.cgr.dev/python-remediated/simple/` |
| `CGR_CUDA_126_REGISTRY` | `https://libraries.cgr.dev/cu126/simple/` |
| `CGR_CUDA_128_REGISTRY` | `https://libraries.cgr.dev/cu128/simple/` |
| `CGR_CUDA_129_REGISTRY` | `https://libraries.cgr.dev/cu129/simple/` |

### Feature Flags

| Variable | Default | Description |
|----------|---------|-------------|
| `SYNC_ALL_VERSIONS` | `true` | Sync every Chainguard version per package. `false` for pinned-only |
| `DRY_RUN` | `false` | Environment-level dry-run override |

### Distribution File Filtering

| Variable | Default | Description |
|----------|---------|-------------|
| `WHEEL_PYTHON_TAGS` | _(all)_ | Comma-separated python tags, e.g. `cp311,cp312,py3` |
| `WHEEL_PLATFORM_TAGS` | _(all)_ | Comma-separated platform tags, e.g. `linux_x86_64,none-any` |
| `SYNC_SDIST` | `true` | Sync `.tar.gz` source distributions |
| `SYNC_WHEELS` | `true` | Sync `.whl` wheel files |

### Checkpointing & S3

| Variable | Default | Description |
|----------|---------|-------------|
| `ERRORS_S3_BUCKET` | _(empty)_ | S3 bucket for `errors.txt` output |
| `ERRORS_S3_PREFIX` | `chainguard-sync/` | Key prefix for error files |
| `CHECKPOINT_S3_BUCKET` | _(falls back to ERRORS_S3_BUCKET)_ | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/` | Key prefix for checkpoints |
| `CHECKPOINT_BUFFER_MS` | `120000` | Milliseconds before Lambda timeout to save checkpoint |
| `MAX_INVOCATIONS` | `50` | Safety cap on self-re-invocations per run |
| `MAX_PACKAGES` | `50000` | Maximum packages processed per run |

### Retry, Parallelism & Filtering

| Variable | Default | Description |
|----------|---------|-------------|
| `RETRY_MAX_ATTEMPTS` | `3` | Max HTTP retry attempts per request |
| `RETRY_BASE_DELAY_S` | `1.0` | Base backoff delay in seconds (doubles each retry, with jitter) |
| `MAX_WORKERS` | `10` | Thread pool size for parallel downloads |
| `PACKAGE_ALLOWLIST` | _(empty)_ | Comma-separated names OR `s3://bucket/key` |
| `PACKAGE_DENYLIST` | _(empty)_ | Comma-separated names OR `s3://bucket/key` |

### Notifications & Metrics

| Variable | Default | Description |
|----------|---------|-------------|
| `SNS_TOPIC_ARN` | _(empty)_ | SNS topic for completion/failure notifications |
| `CLOUDWATCH_NAMESPACE` | `ChainguardSync` | CloudWatch custom metrics namespace |
| `LOG_LEVEL` | `DEBUG` | Logging level: `DEBUG`, `INFO`, `WARN` |

---

## Features

### All-Versions vs Pinned-Only Mode

When `SYNC_ALL_VERSIONS=true` (the default), the function reads your `requirements.txt` to get the list of package *names*, then syncs **every version** of each package that Chainguard publishes — not just the pinned version. This builds a fully populated mirror where teams can upgrade or downgrade within CodeArtifact without re-running a sync.

Set `SYNC_ALL_VERSIONS=false` (or pass `"sync_all_versions": false` in the event) to only sync the exact `==`-pinned versions from the requirements file.

The `update_existing` mode ignores requirements files entirely and scans all existing CodeArtifact packages for new versions in Chainguard.

---

### Parallel Downloads with Per-Package Publish Locks

Distribution files are downloaded in parallel using a `ThreadPoolExecutor` with `MAX_WORKERS` threads (default: 10). Each batch of files downloads concurrently, then publishes to CodeArtifact.

**Per-package publish serialization** prevents the CodeArtifact 429 "concurrent modification" error that occurs when multiple threads publish different wheels for the same package simultaneously (e.g., `sqlalchemy-2.0.49-cp310-...whl` and `sqlalchemy-2.0.49-cp311-...whl`). A per-package `threading.Lock` ensures that only one thread publishes to a given package at a time, while downloads and publishes for *different* packages still run concurrently.

**How it works internally:**

1. Processing advances in batches of `MAX_WORKERS` work items.
2. Each thread in the pool downloads its file, verifies SHA-256, and then acquires the per-package lock.
3. While holding the lock, the thread publishes to CodeArtifact. Other threads for the same package wait; threads for different packages proceed independently.
4. After all threads in the batch complete, the loop advances.

---

### Retry with Exponential Backoff

All HTTP operations use automatic retry with exponential backoff and jitter:

**Registry metadata and file downloads** use the `_with_retry` wrapper around urllib operations. **CodeArtifact publishes** use a dedicated retry loop inside `publish_to_codeartifact` around the `_http_post` call (raw `http.client`), since the publish protocol requires POST-through-redirect handling that urllib can't provide.

**Retryable HTTP status codes:** `429` (rate limited / concurrent modification), `500`, `502`, `503`, `504`

**Non-retryable codes:** `400`, `401`, `403`, `404` — these fail immediately (except 409/400 "already exists" which is treated as success).

**Retry schedule (defaults, `RETRY_MAX_ATTEMPTS=3`):**

| Attempt | Delay Before |
|---------|-------------|
| 1st | immediate |
| 2nd | ~1.0–1.5s |
| 3rd | ~2.0–2.5s |

The jitter (`random.uniform(0, 0.5)`) desynchronizes retries when multiple threads hit the same 429 simultaneously.

---

### Distribution File Filtering

Each package version in Chainguard may have dozens of distribution files — wheels per Python version and platform, plus source distributions. Use filtering to sync only what you need:

**Sync only specific Python versions:**
```
WHEEL_PYTHON_TAGS=cp311,cp312
```

**Sync only specific platforms:**
```
WHEEL_PLATFORM_TAGS=linux_x86_64,manylinux_2_28_x86_64
```

**Skip source distributions (sdists):**
```
SYNC_SDIST=false
```

**Skip wheels (sync sdists only):**
```
SYNC_WHEELS=false
```

Platform tag matching is substring-based, so `linux_x86_64` will match `manylinux2014_x86_64`, `manylinux_2_28_x86_64`, etc.

---

### Package Allowlist / Denylist

Filter packages by name using either comma-separated values or an S3-hosted file (one name per line).

**Via event payload:**
```json
{
  "allowlist": ["numpy", "torch", "tensorflow"],
  "denylist": ["legacy-pkg", "deprecated-lib"]
}
```

**Via environment variables:**
```
PACKAGE_ALLOWLIST=numpy,torch,tensorflow
PACKAGE_DENYLIST=legacy-pkg
```

**Via S3 file:**
```
PACKAGE_ALLOWLIST=s3://my-bucket/config/allowlist.txt
```

Where `allowlist.txt` contains one PEP 503-normalized package name per line, with `#` comments supported.

All package names are compared in PEP 503 normalized form (lowercase, runs of `[-_.]` collapsed to a single hyphen), so `scikit-learn`, `scikit_learn`, and `Scikit.Learn` all match.

---

### Dry-Run Mode

Executes the full discovery phase (fetching metadata from Chainguard, diffing against CodeArtifact, building the work item list) but skips all file downloads and publishes. The completion summary reports what *would* have been synced.

**Enable via event:** `"dry_run": true`
**Enable via environment:** `DRY_RUN=true`

The event-level flag always takes precedence over the environment variable. Dry-runs still publish CloudWatch metrics (with the same metric names) and send SNS notifications (with a `[DRY-RUN]` subject prefix).

---

### SNS Notifications

Set `SNS_TOPIC_ARN` to receive notifications on every sync completion and failure. Messages include the full JSON summary (run ID, counts, error details). Failure notifications are also sent on checkpoint failures and discovery errors.

**Subject format:**
- Success: `[Chainguard PyPI Sync] Complete — 142 synced, 0 failed | 20260408T...`
- Dry-run: `[DRY-RUN Chainguard PyPI Sync] Preview — 142 synced, 0 failed | 20260408T...`
- Failure: `[Chainguard Sync] FAILED — 20260408T...`

---

### CloudWatch Custom Metrics

After every run, the function publishes custom metrics to the `ChainguardSync` namespace (configurable via `CLOUDWATCH_NAMESPACE`):

| Metric | Description |
|--------|-------------|
| `PackagesSynced` | Files successfully published to CodeArtifact |
| `PackagesFailed` | Failed to publish |
| `PackagesSkipped` | Already existed in CodeArtifact |
| `DownloadFailed` | File download failures |
| `HttpErrors` | Total HTTP errors encountered |
| `TotalWorkItems` | Total distribution files processed |

**Dimensions:** `FunctionName` (the Lambda function name)

---

### Checkpointing and Auto-Continuation

When the Lambda timeout approaches (configurable via `CHECKPOINT_BUFFER_MS`, default 120 seconds before timeout), the function saves its full state — work items, counters, error tracker, position index — to S3 as a JSON checkpoint, then asynchronously invokes itself with the checkpoint reference.

The continuation picks up exactly where it left off. This process repeats as many times as needed, up to `MAX_INVOCATIONS` (default: 50). Both the discovery phase and the processing phase support checkpointing independently.

**Manual resume after failure:**
```json
{"resume_run_id": "20260408T195913Z-abc12345"}
```

**List available checkpoints:**
```json
{"list_checkpoints": true}
```

Completed runs automatically delete their checkpoint files. S3 lifecycle rules clean up orphaned checkpoints after 1 day.

---

### Garbage Cleanup

Before each new sync (unless `"cleanup_garbage": false`), the function scans CodeArtifact for packages with invalid PyPI names (containing path separators, `@`, `node_modules`, `..`, or other illegal characters) and deletes them. This handles artifacts from previous parser bugs or accidental publishes.

---

### Cross-Account CodeArtifact

Set `CODEARTIFACT_DOMAIN_OWNER` to the AWS account ID that owns the CodeArtifact domain. This passes the `domainOwner` parameter to `GetAuthorizationToken`, `GetRepositoryEndpoint`, and `PutPackageOriginConfiguration` for cross-account access.

---

## Architecture

### Two-Phase Execution

**Phase 1 — Discovery:** Iterates package names, queries Chainguard Simple API indexes, diffs versions against CodeArtifact, and builds a flat list of work items (one per distribution file). Checkpointable at the package level.

**Phase 2 — Processing:** Downloads and publishes work items in parallel batches. Checkpointable at the batch level.

### Auth Flow

Chainguard's download URLs redirect internally (`libraries.cgr.dev` → `libraries.cgr.dev/artifacts-downloads/`), and potentially externally to CDN/S3. A custom `_StripAuthOnRedirectHandler` keeps the `Authorization` header when redirects stay on the `libraries.cgr.dev` domain and strips it when redirects leave — preventing S3 presigned URL signature conflicts.

### Publish Protocol

CodeArtifact does not support the standard PyPI JSON upload API for Python packages. Publishes use the legacy `multipart/form-data` POST protocol (same wire format as `twine upload`) via raw `http.client` to preserve POST through redirect chains. Auth is HTTP Basic with username `aws` and password set to the CodeArtifact auth token.

The upload URL is the repository endpoint root — **not** the `/legacy/` sub-path that `twine` uses for pypi.org.

---

## Deployment (SAM)

The SAM template creates the Lambda function, S3 bucket, and IAM policies:

```bash
sam build
sam deploy --guided \
    --parameter-overrides \
        CgrUsername=<your-chainguard-username> \
        CgrToken=<your-chainguard-token> \
        CodeArtifactDomain=my-pypi-domain \
        CodeArtifactRepo=my-pypi-repo
```

### First-Time CodeArtifact Setup

If your domain and repository don't exist yet, include `"setup_codeartifact": true` in your first invocation:

```bash
aws lambda invoke \
    --function-name chainguard-pypi-codeartifact-sync \
    --payload '{"s3_bucket":"my-bucket","s3_key":"requirements.txt","setup_codeartifact":true}' \
    --cli-binary-format raw-in-base64-out \
    response.json
```

### Required IAM Permissions

The Lambda execution role needs: `codeartifact:*` (scoped to your domain/repo), `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`, `lambda:InvokeFunction` (self), `sns:Publish`, `cloudwatch:PutMetricData`, `sts:GetServiceBearerToken`, and `logs:*`.

---

## Cron Job Setup (EventBridge)

### Daily sync at 6 AM UTC

```bash
aws events put-rule \
    --name chainguard-pypi-sync-daily \
    --schedule-expression "cron(0 6 * * ? *)" \
    --state ENABLED

aws events put-targets \
    --rule chainguard-pypi-sync-daily \
    --targets '[{
        "Id": "sync-lambda",
        "Arn": "arn:aws:lambda:us-east-2:123456789012:function:chainguard-pypi-codeartifact-sync",
        "Input": "{\"s3_bucket\":\"my-bucket\",\"s3_key\":\"requirements.txt\"}"
    }]'

aws lambda add-permission \
    --function-name chainguard-pypi-codeartifact-sync \
    --statement-id eventbridge-daily-sync \
    --action lambda:InvokeFunction \
    --principal events.amazonaws.com \
    --source-arn arn:aws:events:us-east-2:123456789012:rule/chainguard-pypi-sync-daily
```

### Hourly update of existing packages

```bash
aws events put-rule \
    --name chainguard-pypi-update-hourly \
    --schedule-expression "rate(1 hour)" \
    --state ENABLED

aws events put-targets \
    --rule chainguard-pypi-update-hourly \
    --targets '[{
        "Id": "update-lambda",
        "Arn": "arn:aws:lambda:us-east-2:123456789012:function:chainguard-pypi-codeartifact-sync",
        "Input": "{\"update_existing\":true}"
    }]'
```

### Common Schedules

| Schedule | Expression | Use Case |
|----------|-----------|----------|
| Daily at 6 AM UTC | `cron(0 6 * * ? *)` | Standard daily mirror |
| Every 6 hours | `rate(6 hours)` | Frequent updates |
| Weekdays at midnight | `cron(0 0 ? * MON-FRI *)` | Business-hours alignment |
| Hourly (update mode) | `rate(1 hour)` | Continuous mirror with `update_existing` |

---

## Monitoring & Alerting

**CloudWatch Logs:** Every invocation logs to `/aws/lambda/<function-name>` with per-package progress, timing, and error details.

**CloudWatch Metrics:** The `ChainguardSync` namespace provides `PackagesSynced`, `PackagesFailed`, `PackagesSkipped`, `DownloadFailed`, `HttpErrors`, and `TotalWorkItems`. Build dashboards and alarms on these.

**SNS Notifications:** The full JSON summary is published to your SNS topic on every completion or failure.

**S3 Error Logs:** When errors occur, an `errors-<timestamp>.txt` file is uploaded to S3 with per-package error details (phase, HTTP status, error type, and truncated response body).

**Recommended alarms:**
- `PackagesFailed > 0` — Publish failures; check the errors.txt file in S3.
- `HttpErrors > 10` — Chainguard or CodeArtifact may be experiencing issues.
- `Lambda Errors > 0` — The function itself crashed (OOM, timeout without checkpoint, etc.).

---

## Troubleshooting

**HTTP 429 "concurrent modification" during publish** — This happens when multiple wheels for the same package version are published simultaneously. The per-package publish lock and publish retry logic handle this automatically. If you still see 429s, reduce `MAX_WORKERS` or increase `RETRY_MAX_ATTEMPTS`.

**HTTP 404 on publish** — CodeArtifact has two VPC endpoints: `.api` (for boto3 SDK calls) and `.repositories` (for HTTP registry uploads). If SDK calls work but publishes return 404, add the `com.amazonaws.<region>.codeartifact.repositories` VPC Interface Endpoint. The function runs a pre-flight connectivity check on every non-dry-run invocation and logs the result.

**"Exceeded max invocations" error** — The sync needed more than 50 Lambda invocations. Increase `MAX_INVOCATIONS` or resume with `{"resume_run_id": "..."}`.

**Slow syncs** — Increase `MAX_WORKERS` (try 15–20). Each thread holds one distribution file in memory, so ensure Lambda memory is at least 1024 MB. Use `WHEEL_PYTHON_TAGS` and `WHEEL_PLATFORM_TAGS` to filter out unnecessary platform/Python version combinations.

**SHA-256 mismatch** — The downloaded file doesn't match the hash from the Simple API index. This usually indicates a transient CDN issue. The file is skipped and recorded in the error tracker. Re-running the sync will retry it.

**Packages not appearing** — Verify they're not filtered by your allowlist/denylist. Run a dry-run to see what would sync. Check that the package name in `requirements.txt` matches what Chainguard publishes (PEP 503 normalization is applied automatically).

**SNS notifications not arriving** — Confirm the subscription, verify `SNS_TOPIC_ARN` is correct, and check Lambda logs for "Failed to send SNS notification" warnings.
