# Chainguard → AWS CodeArtifact NPM Sync

Automated Lambda functions that mirror NPM packages from [Chainguard Libraries](https://edu.chainguard.dev/chainguard/libraries/javascript/overview/) to AWS CodeArtifact. Provides supply-chain security by pulling vetted packages from Chainguard instead of public npm.

## Overview

This project deploys two Lambda functions and supporting infrastructure via AWS SAM:

| Function | Handler | Purpose |
|---|---|---|
| `chainguard-codeartifact-sync` | `lambda_function.lambda_handler` | Sync **exact lockfile versions** — mirrors only the specific versions your project uses |
| `chainguard-codeartifact-sync-all` | `sync_all_versions.lambda_handler` | Sync **all available versions** — fully populates your CodeArtifact mirror for every package in the lockfile |

Both functions share the same architecture: two-phase checkpointing, automatic continuation across Lambda invocations, error tracking, and garbage cleanup.

# Screenshot - Version specific mirroring example (accepts):

![Alt text](https://github.com/dalejrodriguez/cg-awscodeartifact-lambda/blob/main/Javascript%20Libraries%20Sync/Accepts%20-%20Single%20Version.png)

# Screenshot - Multiple versions example (accepts):

![Alt text](https://github.com/dalejrodriguez/cg-awscodeartifact-lambda/blob/main/Javascript%20Libraries%20Sync/Accepts%20-%20Multiple%20Versions.png)


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

## Prerequisites

- AWS CLI and SAM CLI installed
- Chainguard Libraries credentials (`CGR_USERNAME` and `CGR_TOKEN`)
- An AWS account with permissions to create Lambda, S3, CodeArtifact, and IAM resources

## License and Support

Internal use. Chainguard Libraries access requires a valid Chainguard subscription. This function is presented as is and is not officially supported by Chainguard. 
