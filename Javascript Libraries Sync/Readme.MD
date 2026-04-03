# Chainguard → AWS CodeArtifact NPM Sync

Automated Lambda functions that mirror NPM packages from [Chainguard Libraries](https://libraries.cgr.dev) to AWS CodeArtifact. Provides supply-chain security by pulling vetted packages from Chainguard instead of public npm.

## Overview

This project deploys two Lambda functions and supporting infrastructure via AWS SAM:

| Function | Handler | Purpose |
|---|---|---|
| `chainguard-codeartifact-sync` | `lambda_function.lambda_handler` | Sync **exact lockfile versions** — mirrors only the specific versions your project uses |
| `chainguard-codeartifact-sync-all` | `sync_all_versions.lambda_handler` | Sync **all available versions** — fully populates your CodeArtifact mirror for every package in the lockfile |

Both functions share the same architecture: two-phase checkpointing, automatic continuation across Lambda invocations, error tracking, and garbage cleanup.

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐     ┌──────────────┐
│  S3 Bucket   │────▶│  Lambda Function  │────▶│  Chainguard     │────▶│ CodeArtifact │
│ (lockfile)   │     │  (sync engine)    │     │  Libraries      │     │ (npm mirror) │
└─────────────┘     └──────────────────┘     └─────────────────┘     └──────────────┘
                           │     ▲
                           │     │ self-invoke on timeout
                           ▼     │
                     ┌──────────────────┐
                     │  S3 Bucket       │
                     │  (checkpoints    │
                     │   + errors)      │
                     └──────────────────┘
```

### Two-Phase Checkpointing

Large lockfiles (50,000+ packages) cannot be processed in a single 15-minute Lambda invocation. The function splits work into two checkpointable phases:

**Phase 1 — Discovery:** Iterates package names, fetches metadata from Chainguard, diffs against CodeArtifact, and builds a work items list. Checks remaining execution time before each package and saves a checkpoint if running low.

**Phase 2 — Processing:** Iterates work items, downloads tarballs from Chainguard, and publishes to CodeArtifact via the npm PUT protocol. Same time-check and checkpoint behavior.

```
Invocation 1: Discovery [packages 1-800]     → checkpoint → self-invoke
Invocation 2: Discovery [packages 801-1500]   → checkpoint → self-invoke
Invocation 3: Discovery [1501-2000] complete  → Processing [items 1-400] → checkpoint
Invocation 4: Processing [items 401-900]      → complete
```

Checkpoints are saved to S3 and include the full state: work items, counters, error list, and progress indices. On successful completion, checkpoint files are automatically deleted.

### Auth Flow (Chainguard → S3 Redirect)

Chainguard's npm registry returns 302 redirects from tarball URLs to presigned Cloudflare R2/S3 URLs. Python's `urllib` forwards the `Authorization` header on redirect, causing S3 to reject it. The functions use a custom `_StripAuthOnRedirectHandler` that removes the auth header before following redirects.

## Prerequisites

- AWS CLI and SAM CLI installed
- Chainguard Libraries credentials (`CGR_USERNAME` and `CGR_TOKEN`)
- An AWS account with permissions to create Lambda, S3, CodeArtifact, and IAM resources

## Deployment

### Deploy with SAM

```bash
sam build
sam deploy --guided \
    --parameter-overrides \
        CgrUsername=<your-chainguard-username> \
        CgrToken=<your-chainguard-token> \
        CodeArtifactDomain=my-npm-domain \
        CodeArtifactRepo=my-npm-repo
```

The SAM template creates:

- **S3 Bucket** — for checkpoints, error logs, and lockfile uploads. Lifecycle rules auto-delete checkpoints after 1 day and error logs after 30 days.
- **Two Lambda Functions** — lockfile sync and all-versions sync, both with arm64/1024MB/15min timeout.
- **IAM Policies** — scoped CodeArtifact, S3, STS, and Lambda self-invoke permissions.

### SAM Parameters

| Parameter | Default | Description |
|---|---|---|
| `CgrUsername` | *(required)* | Chainguard registry username |
| `CgrToken` | *(required)* | Chainguard registry token |
| `CodeArtifactDomain` | `my-npm-domain` | CodeArtifact domain name |
| `CodeArtifactRepo` | `my-npm-repo` | CodeArtifact repository name |
| `CodeArtifactUpstream` | `npm-public` | Upstream repo name (public npm fallback) |
| `MaxInvocations` | `50` | Safety cap on self-re-invocations per run |
| `MaxPackages` | `50000` | Max packages to process per run |

### First-Time CodeArtifact Setup

If your CodeArtifact domain and repository don't exist yet, include `"setup_codeartifact": true` in your first invocation. This idempotently creates the domain, upstream repository, npm external connection, and main repository.

## Usage

### Upload Your Lockfile to S3

```bash
aws s3 cp package-lock.json s3://<sync-bucket>/lockfiles/package-lock.json
```

### Function 1: Sync Lockfile Versions Only

Syncs the exact `name@version` pairs from your lockfile. Best for CI/CD pipelines where you want to mirror exactly what your project depends on.

```bash
# Start a new sync
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "<sync-bucket>",
        "s3_key": "lockfiles/package-lock.json"
    }' response.json
```

### Function 2: Sync All Versions

For each package in your lockfile, syncs **every version** available in Chainguard. Best for building a fully populated mirror.

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync-all \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "<sync-bucket>",
        "s3_key": "lockfiles/package-lock.json"
    }' response.json
```

### Update Existing Packages

Check all packages already in your CodeArtifact repository for new versions in Chainguard. No lockfile required — it scans your CodeArtifact repo directly.

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"update_existing": true}' response.json
```

This is ideal for scheduled runs to keep your mirror current.

### List Checkpoints (Resumable Sessions)

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"list_checkpoints": true}' response.json

cat response.json
```

Returns:

```json
{
    "statusCode": 200,
    "body": {
        "message": "Found 2 checkpoint(s)",
        "summary": {
            "checkpoints": [
                {
                    "run_id": "20260402T034658Z-abc12345",
                    "next_index": 142,
                    "total_work_items": 500,
                    "invocation_number": 3,
                    "counters": {"found": 130, "not_found": 8, "failed": 4},
                    "last_modified": "2026-04-02T03:50:00+00:00",
                    "resume_with": {"resume_run_id": "20260402T034658Z-abc12345"}
                }
            ],
            "status": "listing"
        }
    }
}
```

### Resume a Previous Run

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"resume_run_id": "20260402T034658Z-abc12345"}' response.json
```

Resumes from exactly where the previous run stopped — no work is repeated.

### Cleanup Garbage Packages

Removes packages with `node_modules` in their name (artifacts from a previous parser bug). Runs automatically before every new session by default.

```bash
# Cleanup only (no sync)
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload '{"cleanup_only": true}' response.json

# Disable automatic cleanup for a specific run
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "<sync-bucket>",
        "s3_key": "lockfiles/package-lock.json",
        "cleanup_garbage": false
    }' response.json
```

### Inline Lockfile (Small Projects)

For lockfiles under 6MB, you can pass the content directly without S3:

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --cli-read-timeout 900 \
    --payload "$(jq -n --arg lf "$(cat package-lock.json)" '{
        "lockfile_content": $lf
    }')" response.json
```

### Override Max Packages

By default, the function processes up to 50,000 packages per run. Override per-invocation for larger monorepos:

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{
        "s3_bucket": "<sync-bucket>",
        "s3_key": "lockfiles/package-lock.json",
        "max_packages": 100000
    }' response.json
```

## Event Reference

### Common Fields (Both Functions)

| Field | Type | Default | Description |
|---|---|---|---|
| `s3_bucket` | string | — | S3 bucket containing package-lock.json |
| `s3_key` | string | — | S3 key for package-lock.json |
| `lockfile_content` | string | — | Raw JSON string of lockfile (alternative to S3) |
| `setup_codeartifact` | bool | `false` | Create CodeArtifact domain/repo if they don't exist |
| `update_existing` | bool | `false` | Scan existing CodeArtifact packages for new Chainguard versions |
| `resume_run_id` | string | — | Resume a previous run by its run ID |
| `list_checkpoints` | bool | `false` | List all resumable checkpoint sessions |
| `cleanup_only` | bool | `false` | Only run garbage package cleanup |
| `cleanup_garbage` | bool | `true` | Run cleanup before new sessions |
| `max_packages` | int | `50000` | Max packages to process (overrides env var) |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `CGR_USERNAME` | *(required)* | Chainguard Libraries username |
| `CGR_TOKEN` | *(required)* | Chainguard Libraries token |
| `AWS_REGION` | `us-east-2` | AWS region for all services |
| `CODEARTIFACT_DOMAIN` | `my-npm-domain` | CodeArtifact domain name |
| `CODEARTIFACT_REPO` | `my-npm-repo` | CodeArtifact repository name |
| `CODEARTIFACT_UPSTREAM` | `npm-public` | Upstream repository name |
| `ERRORS_S3_BUCKET` | — | S3 bucket for error log files |
| `ERRORS_S3_PREFIX` | `chainguard-sync/` | S3 prefix for error logs |
| `CHECKPOINT_S3_BUCKET` | *(falls back to ERRORS_S3_BUCKET)* | S3 bucket for checkpoint state |
| `CHECKPOINT_S3_PREFIX` | `chainguard-sync/checkpoints/` | S3 prefix for checkpoints |
| `CHECKPOINT_BUFFER_MS` | `120000` | Milliseconds before timeout to save checkpoint (120s) |
| `MAX_INVOCATIONS` | `50` | Maximum self-invocations per run |
| `MAX_PACKAGES` | `50000` | Maximum packages to process per run |
| `LOG_LEVEL` | `DEBUG` | Logging level (DEBUG, INFO, WARNING) |

> **Note:** The all-versions function uses `chainguard-sync-all/` and `chainguard-sync-all/checkpoints/` as default prefixes to keep state separate.

> **Bucket fallback chain:** The checkpoint bucket resolves in this order: `checkpoint_s3_bucket` in event → `CHECKPOINT_S3_BUCKET` env var → `errors_s3_bucket` in event → `ERRORS_S3_BUCKET` env var. At least one must be set or checkpointing will fail. The SAM template automatically wires both `CHECKPOINT_S3_BUCKET` and `ERRORS_S3_BUCKET` to the created S3 bucket.

## Response Format

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
            "errored_packages": ["wrappy@1.0.2", "..."],
            "not_found_packages": ["some-private-pkg"],
            "status": "complete"
        }
    }
}
```

### Checkpoint Continuation — Discovery Phase

```json
{
    "statusCode": 200,
    "body": {
        "message": "Discovery checkpointed at package 800/2519 — continuation #2 launched",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "invocation_number": 1,
            "phase": "discovery",
            "discovery_progress": "800/2519",
            "work_items_so_far": 234,
            "status": "continuing",
            "found": 0,
            "not_found": 0,
            "failed": 0,
            "skipped_existing": 1580
        }
    }
}
```

### Checkpoint Continuation — Processing Phase

```json
{
    "statusCode": 200,
    "body": {
        "message": "Checkpointed at 400/892 — continuation #5 launched",
        "summary": {
            "run_id": "20260402T034658Z-abc12345",
            "invocation_number": 4,
            "phase": "processing",
            "processed_so_far": 400,
            "total_work_items": 892,
            "status": "continuing",
            "found": 380,
            "not_found": 15,
            "failed": 5,
            "http_errors": 20
        }
    }
}
```

### Error with Resume Info

```json
{
    "statusCode": 400,
    "body": {
        "error": "Exceeded max invocations (50). Resume with: {\"resume_run_id\": \"20260402T034658Z-abc12345\"}"
    }
}
```

## Error Handling

Errors are handled at three levels:

### Per-Package Errors (Non-Fatal)

HTTP/HTTPS errors during metadata fetch, tarball download, or publishing are caught per-package and recorded to an `ErrorTracker`. Processing always continues to the next package.

### Error Log Files

An `errors.txt` file is written to S3 at **every** exit point — not just clean completion. This includes timeouts, MAX_INVOCATIONS limits, and unexpected crashes.

```
# Chainguard -> CodeArtifact sync errors
# Generated: 2026-04-02T03:57:23Z
# Total errors: 3

wrappy@1.0.2  |  phase=fetch_tarball  |  HTTP 500  |  HTTPError  |  Internal Server Error
@types/node@22.15.10  |  phase=publish  |  HTTP 400  |  HTTPError  |  Bad Request — ...
```

### Failure Checkpoints (Crash Recovery)

If an unexpected exception crashes the function mid-run, a checkpoint is automatically saved before the error is returned. The error message includes the resume command:

```
Unexpected error at item 142/500: MemoryError: ...
Resume with: {"resume_run_id": "20260402T034658Z-abc12345"}
```

## Scheduling with EventBridge

To keep your CodeArtifact mirror current automatically:

```yaml
# Add to template.yaml Resources section
UpdateSchedule:
    Type: AWS::Events::Rule
    Properties:
        ScheduleExpression: 'cron(0 2 * * ? *)'  # Daily at 2am UTC
        Targets:
            - Arn: !GetAtt SyncFunction.Arn
              Id: DailyUpdateCheck
              Input: '{"update_existing": true}'

UpdateSchedulePermission:
    Type: AWS::Lambda::Permission
    Properties:
        FunctionName: !Ref SyncFunction
        Action: lambda:InvokeFunction
        Principal: events.amazonaws.com
        SourceArn: !GetAtt UpdateSchedule.Arn
```

## IAM Permissions

Both functions require these permissions (automatically configured by the SAM template):

| Service | Actions | Purpose |
|---|---|---|
| CodeArtifact | `CreateDomain`, `CreateRepository`, `UpdateRepository`, `AssociateExternalConnection` | First-time setup |
| CodeArtifact | `GetAuthorizationToken`, `GetRepositoryEndpoint` | Authentication |
| CodeArtifact | `PutPackageOriginConfiguration` | Block upstream for published packages |
| CodeArtifact | `PublishPackageVersion` | Publish tarballs |
| CodeArtifact | `ListPackageVersions`, `ListPackages` | Discovery and version diffing |
| CodeArtifact | `DeletePackageVersions` | Garbage cleanup |
| STS | `GetServiceBearerToken` | CodeArtifact auth token |
| S3 | `GetObject`, `PutObject`, `DeleteObject` | Lockfiles, checkpoints, errors |
| S3 | `ListBucket` | List checkpoint files |
| Lambda | `InvokeFunction` | Self-invoke for continuation |

## Scaling

| Config | Default | Effect |
|---|---|---|
| `MAX_PACKAGES` | 50,000 | Packages processed per run. Increase for larger monorepos. |
| `MAX_INVOCATIONS` | 50 | Max chained invocations. At ~13 min each = ~10.8 hours max. |
| `CHECKPOINT_BUFFER_MS` | 120,000 | Time reserved for checkpoint save (2 min). Increase if saves are slow. |
| `MemorySize` | 1024 MB | Lambda memory. Increase if processing large tarballs. |

For a lockfile with 50,000 packages where each metadata fetch + version check takes ~200ms, discovery alone needs ~2.7 hours (about 12 invocations). Processing speed depends on tarball sizes but typically handles 200-400 packages per invocation.

## File Structure

```
├── lambda_function.py      # Lockfile-versions-only sync
├── sync_all_versions.py    # All-versions sync
├── template.yaml           # SAM deployment template
└── README.md               # This file
```

## Troubleshooting

### "Invalid bucket name" or "Parameter validation failed" errors

The checkpoint S3 bucket resolved to an empty string. This happens when none of the four bucket sources are configured: `checkpoint_s3_bucket` in event, `CHECKPOINT_S3_BUCKET` env var, `errors_s3_bucket` in event, or `ERRORS_S3_BUCKET` env var.

**Fix:** If using the SAM template, both `CHECKPOINT_S3_BUCKET` and `ERRORS_S3_BUCKET` are automatically set to the created bucket. If deploying manually, set at least `ERRORS_S3_BUCKET`:

```bash
aws lambda update-function-configuration \
    --function-name chainguard-codeartifact-sync \
    --environment "Variables={ERRORS_S3_BUCKET=my-bucket,...}"
```

### Lambda times out during discovery phase

For lockfiles with thousands of packages, the discovery phase (metadata fetch + version diff per package) can exceed 15 minutes. The function now checkpoints during discovery — check CloudWatch for messages like:

```
Discovery checkpointed at package 800/2519 — continuation #2 launched
```

If discovery is too slow, the function automatically chains across multiple invocations. With `MAX_INVOCATIONS=50`, discovery can run for ~10 hours. If it still hits the cap, resume manually:

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --invocation-type Event \
    --payload '{"resume_run_id": "<run_id from error message>"}' response.json
```

### Lambda times out at 15 minutes

This is expected for large lockfiles. Both discovery and processing phases checkpoint automatically and self-invoke. Check CloudWatch for continuation logs showing `phase=discovery` or `phase=processing`. If it hits `MAX_INVOCATIONS`, increase the limit or resume manually with `{"resume_run_id": "..."}`. The checkpoint is preserved so no work is lost.

### "Missing x-amz-content-sha256" errors

The auth-stripping redirect handler should prevent this. If you see it, ensure you're running the latest code — the `_StripAuthOnRedirectHandler` class strips the `Authorization` header before following redirects to presigned S3/R2 URLs.

### "tarball attachment is missing from the request"

The npm PUT payload requires specific naming conventions. The current code uses `{name}-{version}.tgz` for the `_attachments` key (e.g., `@types/node-16.11.43.tgz`). If you see this error, check that the publish function matches this format.

### Packages with `node_modules` in their name

These are artifacts from a previous lockfile parser bug. Run `{"cleanup_only": true}` to remove them, or rely on the automatic cleanup that runs before every new session.

### CLI timeout: "Read timeout on endpoint URL"

The AWS CLI has a 60-second default timeout. Use `--cli-read-timeout 900` for synchronous invocations, or `--invocation-type Event` for async fire-and-forget.

### Botocore debug noise in CloudWatch

Both functions suppress `botocore`, `boto3`, and `urllib3` loggers at WARNING level. If you see raw HTTP signatures and certificate paths, redeploy to pick up the latest code.

### "Invalid base64" or "Invalid JSON" payload errors

AWS CLI v2 defaults to base64-encoding the `--payload` parameter. If you see errors like `Invalid base64`, `Could not parse request body`, or the Lambda receives garbled input, add the `--cli-binary-format raw-in-base64-out` flag to every `aws lambda invoke` command:

```bash
aws lambda invoke \
    --function-name chainguard-codeartifact-sync \
    --cli-binary-format raw-in-base64-out \
    --payload '{"s3_bucket": "my-bucket", "s3_key": "package-lock.json"}' \
    response.json
```

This tells the CLI to send the payload as raw JSON instead of base64-encoding it. All examples in this README include this flag.

To set this globally so you don't need to pass it every time, add it to your AWS CLI config:

```ini
# ~/.aws/config
[default]
cli_binary_format = raw-in-base64-out
```

> **Note:** AWS CLI v1 sends raw payloads by default and does not need this flag.

## License and Support

Internal use. Chainguard Libraries access requires a valid Chainguard subscription. This function is presented as is and is not officially supported by Chainguard. 
