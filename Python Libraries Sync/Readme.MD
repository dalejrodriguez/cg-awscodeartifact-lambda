# Chainguard Python → AWS CodeArtifact Sync

A pair of AWS Lambda functions that sync Python packages from Chainguard's secure registries into AWS CodeArtifact, giving your organization a private, vulnerability-remediated PyPI mirror.

## Overview

Both functions read a `requirements.txt` from S3, resolve each package against Chainguard's PEP 503 Simple API registries, download the distribution files (wheels and/or sdists), and publish them to a CodeArtifact repository. They share a common architecture — checkpointing, parallel downloads, retry with backoff, error tracking, and observability via SNS and CloudWatch — but differ in **scope**:

| Function | File | What it syncs |
|---|---|---|
| **Pinned-Only Sync** | `lambda_function_py_sync.py` | Exact `==` pinned versions from the requirements file only. |
| **All-Versions Sync** | `py-sync-all.py` | Every version Chainguard publishes for each listed package (default), with a flag to fall back to pinned-only. |

Use **Pinned-Only** when you want a lean mirror that contains exactly what your lockfile declares. Use **All-Versions** when teams need the freedom to upgrade or downgrade within CodeArtifact without re-running a sync.

## Supported Chainguard Registries

Packages are routed to the correct registry automatically based on their version string:

| Registry | URL | Selection rule |
|---|---|---|
| Remediated | `https://libraries.cgr.dev/python-remediated/simple/` | Default (tried first) |
| Standard | `https://libraries.cgr.dev/python/simple/` | Fallback if not in remediated |
| CUDA 12.6 | `https://libraries.cgr.dev/cu126/simple/` | Version contains `+cu126` |
| CUDA 12.8 | `https://libraries.cgr.dev/cu128/simple/` | Version contains `+cu128` |
| CUDA 12.9 | `https://libraries.cgr.dev/cu129/simple/` | Version contains `+cu129` |

All registry base URLs can be overridden via environment variables.

## Entry Modes

Both functions support the same set of invocation patterns, passed as the Lambda event JSON:

**1. Sync from a requirements file (new session)**
```json
{"s3_bucket": "my-bucket", "s3_key": "path/to/requirements.txt"}
```

**2. Update every existing CodeArtifact package with all Chainguard versions**
```json
{"update_existing": true}
```

**3. Resume a timed-out or failed run**
```json
{"resume_run_id": "20260402T034658Z-abc12345"}
```

**4. List available checkpoints**
```json
{"list_checkpoints": true}
```

**5. Cleanup only — remove packages with invalid PyPI names**
```json
{"cleanup_only": true}
```

**6. Dry-run — preview what would sync without writing anything**
```json
{"s3_bucket": "my-bucket", "s3_key": "requirements.txt", "dry_run": true}
```

## Inline Options

These optional keys can be added to any event payload:

| Key | Type | Default | Description |
|---|---|---|---|
| `setup_codeartifact` | bool | `false` | Create the CodeArtifact domain and repo if they don't exist. |
| `cleanup_garbage` | bool | `true` | Run a pre-sync pass to delete packages with invalid PyPI names. |
| `dry_run` | bool | `false` | Log intended actions without publishing anything. |
| `sync_all_versions` | bool | `true` | *All-Versions function only.* Set `false` for pinned-only behavior. |
| `allowlist` | list | `[]` | Only sync these package names. |
| `denylist` | list | `[]` | Skip these package names. |

## Environment Variables

### Required

| Variable | Description |
|---|---|
| `CGR_USERNAME` | Chainguard registry username. |
| `CGR_TOKEN` | Chainguard registry token. |

### AWS / CodeArtifact

| Variable | Default | Description |
|---|---|---|
| `AWS_REGION` | `us-east-2` | AWS region for all service clients. |
| `CODEARTIFACT_DOMAIN` | `my-pypi-domain` | CodeArtifact domain name. |
| `CODEARTIFACT_REPO` | `my-pypi-repo` | CodeArtifact repository name. |
| `CODEARTIFACT_UPSTREAM` | `pypi-public` | Upstream repository name. |
| `CODEARTIFACT_DOMAIN_OWNER` | *(empty)* | Account ID for cross-account access. |

### Registry URL Overrides

| Variable | Default |
|---|---|
| `CGR_STANDARD_REGISTRY` | `https://libraries.cgr.dev/python/simple/` |
| `CGR_REMEDIATED_REGISTRY` | `https://libraries.cgr.dev/python-remediated/simple/` |
| `CGR_CUDA_126_REGISTRY` | `https://libraries.cgr.dev/cu126/simple/` |
| `CGR_CUDA_128_REGISTRY` | `https://libraries.cgr.dev/cu128/simple/` |
| `CGR_CUDA_129_REGISTRY` | `https://libraries.cgr.dev/cu129/simple/` |

### Distribution File Filtering

| Variable | Default | Description |
|---|---|---|
| `WHEEL_PYTHON_TAGS` | *(all)* | Comma-separated python tags, e.g. `cp311,cp312,py3`. |
| `WHEEL_PLATFORM_TAGS` | *(all)* | Comma-separated platform tags, e.g. `linux_x86_64,none-any`. |
| `SYNC_SDIST` | `true` | Sync `.tar.gz` source distributions. |
| `SYNC_WHEELS` | `true` | Sync `.whl` wheel files. |

### Feature Flags (All-Versions function only)

| Variable | Default | Description |
|---|---|---|
| `SYNC_ALL_VERSIONS` | `true` | Sync every Chainguard version per package. Set `false` for pinned-only. |
| `DRY_RUN` | `false` | Environment-level dry-run override. |

### Checkpointing & S3

| Variable | Default | Description |
|---|---|---|
| `ERRORS_S3_BUCKET` | *(empty)* | S3 bucket for `errors.txt` output. |
| `ERRORS_S3_PREFIX` | `chainguard-sync/` | Key prefix for error files. |
| `CHECKPOINT_S3_BUCKET` | *(falls back to ERRORS_S3_BUCKET)* | S3 bucket for checkpoint state. |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/` | Key prefix for checkpoints. |
| `CHECKPOINT_BUFFER_MS` | `120000` | Milliseconds before Lambda timeout to save a checkpoint. |
| `MAX_INVOCATIONS` | `50` | Safety cap on self-re-invocations per run. |
| `MAX_PACKAGES` | `50000` | Maximum packages processed per run. |

### Retry, Parallelism & Filtering

| Variable | Default | Description |
|---|---|---|
| `RETRY_MAX_ATTEMPTS` | `3` | Max HTTP retry attempts per request. |
| `RETRY_BASE_DELAY_S` | `1.0` | Base backoff delay in seconds. |
| `MAX_WORKERS` | `10` | Thread pool size for parallel downloads. |
| `PACKAGE_ALLOWLIST` | *(empty)* | Comma-separated names or `s3://bucket/key`. |
| `PACKAGE_DENYLIST` | *(empty)* | Comma-separated names or `s3://bucket/key`. |

### Observability

| Variable | Default | Description |
|---|---|---|
| `SNS_TOPIC_ARN` | *(empty)* | SNS topic for completion/failure alerts. |
| `CLOUDWATCH_NAMESPACE` | `ChainguardSync` | CloudWatch metrics namespace. |
| `LOG_LEVEL` | `DEBUG` | Logging level (`DEBUG`, `INFO`, `WARN`). |

## Architecture Highlights

**Checkpointing & Auto-Resume** — When a Lambda invocation approaches its timeout, the function serializes its progress to S3 and asynchronously re-invokes itself to continue. Failed runs can also be manually resumed by passing the checkpoint's `run_id`.

**Parallel Downloads** — Distribution files are fetched and published in batches using a thread pool (`MAX_WORKERS`), significantly reducing wall-clock time for large syncs.

**Retry with Exponential Backoff** — Transient HTTP errors (429, 5xx) and network exceptions are retried with jittered exponential backoff. Permanent 4xx errors fail immediately.

**Auth-Stripping Redirect Handler** — Chainguard download URLs redirect through internal and external CDN hops. A custom `urllib` handler retains credentials for same-host redirects and strips them when leaving `libraries.cgr.dev`, preventing poisoned presigned S3 URLs.

**Error Tracking** — Per-package errors are collected in-memory, written to S3 as `errors.txt` at the end of the run, and surfaced in the Lambda response and SNS notification.

**CloudWatch Metrics** — Each run emits metric data points for packages synced, failed, skipped, and download errors under the configured namespace.

## IAM Permissions

The Lambda execution role needs at minimum:

- `codeartifact:GetAuthorizationToken`
- `codeartifact:GetRepositoryEndpoint`
- `codeartifact:PublishPackageVersion`
- `codeartifact:ListPackages` / `codeartifact:ListPackageVersions` (for `update_existing` and cleanup)
- `codeartifact:DeletePackage` (for garbage cleanup)
- `s3:GetObject` (requirements file and filter lists)
- `s3:PutObject` (error logs and checkpoints)
- `lambda:InvokeFunction` (self-invocation for checkpoint resume)
- `sns:Publish` (if SNS notifications are configured)
- `cloudwatch:PutMetricData` (if CloudWatch metrics are enabled)
- `sts:GetCallerIdentity` (identity logging at cold-start)

If `setup_codeartifact` is used, add `codeartifact:CreateDomain` and `codeartifact:CreateRepository`.
