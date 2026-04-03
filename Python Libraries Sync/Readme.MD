# Chainguard Python → AWS CodeArtifact Sync

A pair of AWS Lambda functions that sync Python packages from Chainguard's secure registries into AWS CodeArtifact, giving your organization a private, vulnerability-remediated PyPI mirror.

This configuration does not support the Chainguard cooldown repository. 

These lambdas are not officially supported by Chainguard. 

## Overview

Both functions read a `requirements.txt` from S3, resolve each package against Chainguard's PEP 503 Simple API registries, download the distribution files (wheels and/or sdists), and publish them to a CodeArtifact repository. They share a common architecture — checkpointing, parallel downloads, retry with backoff, error tracking, and observability via SNS and CloudWatch — but differ in **scope**:

| Function | File | What it syncs |
|---|---|---|
| **Pinned-Only Sync** | `lambda_function_.py` | Exact `==` pinned versions from the requirements file only. |
| **All-Versions Sync** | `lambda_function_.py` | Every version Chainguard publishes for each listed package (default), with a flag to fall back to pinned-only. |

Use **Pinned-Only** when you want a lean mirror that contains exactly what your lockfile declares. Use **All-Versions** when teams need the freedom to upgrade or downgrade within CodeArtifact without re-running a sync.

## Screenshots (CodeArtificat repos after a sync):

Pinned only sync:

![Screenshot of a CodeArtifact repo with only one pypi package version synced](https://github.com/dalejrodriguez/cg-awscodeartifact-lambda/blob/main/Python%20Libraries%20Sync/Rich%20Single%20Version.png)

All versions sync:

![Screenshot of a CodeArtifact repo with multiple pypi package versions synced](https://github.com/dalejrodriguez/cg-awscodeartifact-lambda/blob/main/Python%20Libraries%20Sync/Rich%20Multiple%20Versions.png)

## Supported Chainguard Registries

Packages are routed to the correct registry automatically based on their version string:

| Registry | URL | Selection rule |
|---|---|---|
| Remediated | `https://libraries.cgr.dev/python-remediated/simple/` | Default (tried first) |
| Standard | `https://libraries.cgr.dev/python/simple/` | Fallback if not in remediated |
| CUDA 12.6 | `https://libraries.cgr.dev/cu126/simple/` | Version contains `+cu126` |
| CUDA 12.8 | `https://libraries.cgr.dev/cu128/simple/` | Version contains `+cu128` |
| CUDA 12.9 | `https://libraries.cgr.dev/cu129/simple/` | Version contains `+cu129` |

## Architecture

![Architecture diagram of lambda function](https://github.com/dalejrodriguez/cg-awscodeartifact-lambda/blob/main/Python%20Libraries%20Sync/architecture.svg)
