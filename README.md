# Chainguard → AWS CodeArtifact NPM Sync

Syncs NPM packages from [Chainguard Libraries](https://libraries.chainguard.dev) to a private AWS CodeArtifact repository. Packaged as an AWS SAM application with two Lambda functions and a shared S3 bucket for state management.

---

## Overview

| Component | Description |
|---|---|
| `lambda_function.py` | Syncs **exact versions** specified in a `package-lock.json` |
| `sync_all_versions.py` | Syncs **all available versions** of each package found in a lockfile |
| `template.yaml` | AWS SAM template — Lambda, IAM, and S3 resources |

Both functions fetch tarballs directly from Chainguard Libraries over HTTPS and publish them to CodeArtifact using the npm registry protocol via `boto3`. No `npm` CLI is required in the Lambda runtime.

---

## Architecture

```
S3 (package-lock.json)
        │
        ▼
  Lambda Invocation
        │
        ├── [optional] CodeArtifact Setup
        │       domain / repo / upstream / external connection
        │
        ├── Parse lockfile packages
        │
        ├── For each package:
        │       ├── Fetch tarball from Chainguard Libraries
        │       └── Publish to CodeArtifact (skip if exists)
        │
        ├── [if nearing timeout] Save checkpoint → S3
        │       └── Self-invoke with checkpoint reference
        │
        └── Write errors.txt → S3
```

---

## Prerequisites

- AWS SAM CLI
- An AWS account with permissions to create Lambda, IAM, and S3 resources
- Chainguard Libraries credentials (`CGR_USERNAME` / `CGR_TOKEN`)

---

## Deployment

```bash
sam build
sam deploy --guided
```

SAM will prompt for the following parameters on first deploy:

| Parameter | Description | Default |
|---|---|---|
| `CgrUsername` | Chainguard registry username | *(required)* |
| `CgrToken` | Chainguard registry token | *(required)* |
| `CodeArtifactDomain` | CodeArtifact domain name | `my-npm-domain` |
| `CodeArtifactRepo` | CodeArtifact repo name | `my-npm-repo` |
| `CodeArtifactUpstream` | Upstream repo name | `npm-public` |
| `MaxInvocations` | Max self-invocation chain length | `50` |

---

## Lambda Configuration

| Setting | Value |
|---|---|
| Runtime | Python 3.12 |
| Architecture | arm64 |
| Timeout | 900s (15 min) |
| Memory | 512 MB |

---

## Environment Variables

| Variable | Required | Description | Default |
|---|---|---|---|
| `CGR_USERNAME` | ✅ | Chainguard registry username | — |
| `CGR_TOKEN` | ✅ | Chainguard registry token | — |
| `AWS_REGION` | | AWS region | `us-east-2` |
| `CODEARTIFACT_DOMAIN` | | CodeArtifact domain | `my-npm-domain` |
| `CODEARTIFACT_REPO` | | CodeArtifact repository | `my-npm-repo` |
| `CODEARTIFACT_UPSTREAM` | | Upstream repository name | `npm-public` |
| `ERRORS_S3_BUCKET` | | S3 bucket for error logs | — |
| `ERRORS_S3_PREFIX` | | S3 key prefix for error logs | `chainguard-sync/` |
| `CHECKPOINT_S3_BUCKET` | | S3 bucket for checkpoint state | falls back to `ERRORS_S3_BUCKET` |
| `CHECKPOINT_S3_PREFIX` | | S3 key prefix for checkpoints | `chainguard-sync/checkpoints/` |
| `CHECKPOINT_BUFFER_MS` | | Time before timeout to save checkpoint | `60000` (60s) |
| `MAX_INVOCATIONS` | | Safety cap on self-invocation chain | `50` |
| `LOG_LEVEL` | | Logging verbosity | `DEBUG` |

---

## Invocation

### Sync lockfile versions from S3

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/package-lock.json",
  "setup_codeartifact": false
}
```

### Sync with CodeArtifact setup (first-time)

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/package-lock.json",
  "setup_codeartifact": true
}
```

### Inline lockfile content

```json
{
  "lockfile_content": "{ ... raw package-lock.json JSON ... }",
  "setup_codeartifact": false
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

### Garbage cleanup only (no sync)

```json
{
  "cleanup_only": true
}
```

### Disable automatic cleanup for a run

```json
{
  "s3_bucket": "my-bucket",
  "s3_key": "path/to/package-lock.json",
  "cleanup_garbage": false
}
```

### CLI invocation example

```bash
aws lambda invoke \
  --function-name chainguard-codeartifact-sync \
  --cli-read-timeout 900 \
  --payload '{"s3_bucket": "my-bucket", "s3_key": "package-lock.json"}' \
  out.json
```

---

## Automatic Continuation (Checkpointing)

Lambda has a maximum 15-minute execution limit. For large lockfiles, the function implements a self-continuation mechanism:

1. Before processing each package, the remaining execution time is checked.
2. When less than `CHECKPOINT_BUFFER_MS` (default: 60s) remains, the current state (counters, error list, package index) is serialized to JSON and saved to S3.
3. The function asynchronously re-invokes itself with the checkpoint reference.
4. The next invocation picks up exactly where the previous one stopped.
5. This repeats until all packages are processed or `MAX_INVOCATIONS` is reached.
6. On successful completion, checkpoint files are automatically deleted from S3.

---

## Garbage Cleanup

Packages with `node_modules` in their name (a common artifact from lockfile publishing) are treated as garbage and removed automatically.

- Runs automatically at the start of every **new** sync session.
- Skipped on checkpoint continuations and manual resumes (already ran on first invocation).
- Can be triggered standalone via `{"cleanup_only": true}`.
- Packages are deleted in batches of 50 using `codeartifact:DeletePackageVersions`.

---

## Error Handling

- Errors are caught **per package** and never halt the overall sync.
- All errors are accumulated in memory throughout the run.
- On completion, an `errors-{timestamp}.txt` file is written to S3 under `ERRORS_S3_BUCKET/ERRORS_S3_PREFIX/`.
- Errors are also returned in the Lambda response under `summary.errored_packages`.

---

## IAM Permissions

The Lambda execution role is granted the following permissions via the SAM template:

**CodeArtifact**
- `CreateDomain`, `CreateRepository`, `UpdateRepository`
- `AssociateExternalConnection`
- `GetAuthorizationToken`, `GetRepositoryEndpoint`
- `PutPackageOriginConfiguration`, `PublishPackageVersion`
- `DeletePackageVersions`

**STS**
- `GetServiceBearerToken` (for CodeArtifact auth token)

**S3** *(scoped to the sync bucket)*
- `GetObject`, `PutObject`, `DeleteObject`

**Lambda**
- `InvokeFunction` *(scoped to self — for checkpoint continuation)*

---

## S3 Bucket

A dedicated S3 bucket is provisioned by the SAM template with:
- Server-side encryption (AES-256)
- Public access fully blocked

The bucket is used for:
- Uploading `package-lock.json` input files
- Storing checkpoint JSON state files
- Writing `errors-{timestamp}.txt` output logs

---

## Outputs

After `sam deploy`, the following stack outputs are available:

| Output | Description |
|---|---|
| `FunctionArn` | ARN of the sync Lambda function |
| `FunctionName` | Name of the sync Lambda function |
| `SyncBucketName` | S3 bucket name for errors, checkpoints, and lockfile uploads |
| `SyncBucketArn` | ARN of the S3 bucket |
