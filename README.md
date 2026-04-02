# Chainguard → AWS CodeArtifact  Sync

Syncs Python, Java and Javascript packages from [Chainguard Libraries](https://libraries.chainguard.dev) to a private AWS CodeArtifact repository. Packaged as an AWS SAM application with two Lambda functions and a shared S3 bucket for state management.

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
