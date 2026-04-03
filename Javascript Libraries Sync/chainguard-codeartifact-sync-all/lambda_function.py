"""
AWS Lambda: Sync ALL Chainguard NPM package versions to AWS CodeArtifact

For each unique package name in the lockfile, fetches ALL versions
available in Chainguard Libraries, diffs against CodeArtifact, and
publishes only new versions. Use this to fully populate your
CodeArtifact mirror. For syncing only the exact lockfile versions,
use the companion lambda: chainguard-codeartifact-sync

Trigger:
{
    "s3_bucket": "my-bucket",
    "s3_key": "path/to/package-lock.json",
    "setup_codeartifact": false,        // optional, default false
    "dry_run": false,                   // optional, preview without syncing
    "allowlist": ["lodash", "@types/*"],// optional, glob patterns
    "denylist": ["debug", "colors"]     // optional, glob patterns
}

Resume a previous run:
    {"resume_run_id": "20260402T034658Z-abc12345"}

List available checkpoints:
    {"list_checkpoints": true}

Environment variables:
    CGR_USERNAME            — Chainguard registry username  (required)
    CGR_TOKEN               — Chainguard registry token     (required)
    AWS_REGION              — AWS region           (default: us-east-2)
    CODEARTIFACT_DOMAIN     — domain name          (default: my-npm-domain)
    CODEARTIFACT_REPO       — repo name            (default: my-npm-repo)
    CODEARTIFACT_UPSTREAM   — upstream repo name   (default: npm-public)
    ERRORS_S3_BUCKET        — S3 bucket for errors.txt output
    ERRORS_S3_PREFIX        — S3 key prefix for errors.txt   (default: chainguard-sync-all/)
    CHECKPOINT_S3_BUCKET    — S3 bucket for checkpoint state (falls back to ERRORS_S3_BUCKET)
    CHECKPOINT_S3_PREFIX    — S3 key prefix for checkpoints  (default: chainguard-sync-all/checkpoints/)
    CHECKPOINT_BUFFER_MS    — ms before timeout to save      (default: 120000 = 120s)
    MAX_INVOCATIONS         — safety cap on re-invocations   (default: 50)
    MAX_PACKAGES            — max packages to process per run (default: 50000)
    LOG_LEVEL               — DEBUG / INFO / WARN  (default: DEBUG)
    SNS_TOPIC_ARN           — SNS topic for notifications    (optional)
    DRY_RUN                 — default dry-run mode           (default: false)
    PACKAGE_ALLOWLIST       — comma-separated glob patterns  (optional)
    PACKAGE_DENYLIST        — comma-separated glob patterns  (optional)
    PARALLEL_DOWNLOADS      — max concurrent tarball threads (default: 5)
    RETRY_MAX_ATTEMPTS      — max retry attempts for HTTP    (default: 3)
    RETRY_BASE_DELAY        — base delay in seconds          (default: 1.0)
"""

import json
import os
import base64
import hashlib
import logging
import urllib.request
import urllib.error
import urllib.parse
import time
import fnmatch
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
DOMAIN_NAME = os.environ.get("CODEARTIFACT_DOMAIN", "my-npm-domain")
REPO_NAME = os.environ.get("CODEARTIFACT_REPO", "my-npm-repo")
UPSTREAM_REPO_NAME = os.environ.get("CODEARTIFACT_UPSTREAM", "npm-public")
CGR_REGISTRY = "https://libraries.cgr.dev/javascript"
CGR_USERNAME = os.environ.get("CGR_USERNAME", "")
CGR_TOKEN = os.environ.get("CGR_TOKEN", "")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()

logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))

logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Reuse across invocations
ca_client = boto3.client("codeartifact", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)

ERRORS_S3_BUCKET = os.environ.get("ERRORS_S3_BUCKET", "")
ERRORS_S3_PREFIX = os.environ.get("ERRORS_S3_PREFIX", "chainguard-sync-all/")

CHECKPOINT_S3_BUCKET = os.environ.get("CHECKPOINT_S3_BUCKET", "")
CHECKPOINT_S3_PREFIX = os.environ.get("CHECKPOINT_S3_PREFIX", "chainguard-sync-all/checkpoints/")
CHECKPOINT_BUFFER_MS = int(os.environ.get("CHECKPOINT_BUFFER_MS", "120000"))
MAX_INVOCATIONS = int(os.environ.get("MAX_INVOCATIONS", "50"))
MAX_PACKAGES = int(os.environ.get("MAX_PACKAGES", "50000"))
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "chainguard-codeartifact-sync-all")

# --- New feature configuration ---
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
DRY_RUN_DEFAULT = os.environ.get("DRY_RUN", "false").lower() in ("true", "1", "yes")
PACKAGE_ALLOWLIST = [p.strip() for p in os.environ.get("PACKAGE_ALLOWLIST", "").split(",") if p.strip()]
PACKAGE_DENYLIST = [p.strip() for p in os.environ.get("PACKAGE_DENYLIST", "").split(",") if p.strip()]
PARALLEL_DOWNLOADS = int(os.environ.get("PARALLEL_DOWNLOADS", "5"))
RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY = float(os.environ.get("RETRY_BASE_DELAY", "1.0"))

lambda_client = boto3.client("lambda", region_name=AWS_REGION)
sns_client = boto3.client("sns", region_name=AWS_REGION) if SNS_TOPIC_ARN else None
cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)


# ===================================================================
# SNS Notifications
# ===================================================================
def _send_notification(subject: str, message: str, status: str = "INFO"):
    """Publish a notification to the configured SNS topic."""
    if not SNS_TOPIC_ARN:
        logger.debug("SNS_TOPIC_ARN not set — skipping notification")
        return
    try:
        client = sns_client or boto3.client("sns", region_name=AWS_REGION)
        client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message,
            MessageAttributes={
                "status": {"DataType": "String", "StringValue": status},
            },
        )
        logger.info("SNS notification sent: %s [%s]", subject[:80], status)
    except Exception as exc:
        logger.error("Failed to send SNS notification: %s", exc)


def _notify_completion(summary: dict, dry_run: bool = False):
    prefix = "[DRY-RUN] " if dry_run else ""
    status = summary.get("status", "complete")
    failed = summary.get("failed_to_publish", 0) + summary.get("tarball_download_failed", 0)
    sns_status = "FAILURE" if failed > 0 else "SUCCESS"
    subject = f"{prefix}Chainguard Sync {status.upper()} — run {summary.get('run_id', 'unknown')}"
    _send_notification(subject, json.dumps(summary, indent=2, default=str), sns_status)


def _notify_failure(message: str, run_id: str = "unknown"):
    _send_notification(f"Chainguard Sync FAILED — run {run_id}", message, "FAILURE")


# ===================================================================
# CloudWatch Custom Metrics
# ===================================================================
def _put_metrics(summary: dict, dry_run: bool = False):
    """Publish custom CloudWatch metrics under Chainguard/NPMSync namespace."""
    namespace = "Chainguard/NPMSync"
    dimensions = [
        {"Name": "Domain", "Value": DOMAIN_NAME},
        {"Name": "Repository", "Value": REPO_NAME},
    ]
    if dry_run:
        dimensions.append({"Name": "Mode", "Value": "DryRun"})

    metric_data = [
        {"MetricName": "PackagesSynced", "Value": summary.get("synced_to_codeartifact", 0),
         "Unit": "Count", "Dimensions": dimensions},
        {"MetricName": "PackagesFailed", "Value": summary.get("failed_to_publish", 0),
         "Unit": "Count", "Dimensions": dimensions},
        {"MetricName": "PackagesSkipped", "Value": summary.get("skipped_existing", 0),
         "Unit": "Count", "Dimensions": dimensions},
        {"MetricName": "TarballDownloadFailed", "Value": summary.get("tarball_download_failed", 0),
         "Unit": "Count", "Dimensions": dimensions},
        {"MetricName": "HTTPErrors", "Value": summary.get("http_errors", 0),
         "Unit": "Count", "Dimensions": dimensions},
    ]
    try:
        cw_client.put_metric_data(Namespace=namespace, MetricData=metric_data)
        logger.info("Published %d CloudWatch metrics to %s", len(metric_data), namespace)
    except Exception as exc:
        logger.error("Failed to publish CloudWatch metrics: %s", exc)


# ===================================================================
# Package Allowlist / Denylist Filtering
# ===================================================================
def _matches_glob_list(name: str, patterns: list[str]) -> bool:
    """Check if a package name matches any glob pattern in the list."""
    for pattern in patterns:
        if fnmatch.fnmatch(name, pattern):
            return True
    return False


def _filter_packages(
    names: list[str], allowlist: list[str], denylist: list[str],
) -> tuple[list[str], list[str]]:
    """Apply allowlist then denylist filters. Returns (accepted, filtered_out)."""
    accepted, filtered_out = [], []
    for name in names:
        if allowlist and not _matches_glob_list(name, allowlist):
            filtered_out.append(name)
            continue
        if denylist and _matches_glob_list(name, denylist):
            filtered_out.append(name)
            continue
        accepted.append(name)
    if filtered_out:
        logger.info("Package filter: %d accepted, %d filtered out "
                     "(allowlist=%d patterns, denylist=%d patterns)",
                     len(accepted), len(filtered_out), len(allowlist), len(denylist))
    return accepted, filtered_out


# ===================================================================
# Retry Logic with Exponential Backoff
# ===================================================================
def _retry_with_backoff(fn, max_attempts=RETRY_MAX_ATTEMPTS,
                        base_delay=RETRY_BASE_DELAY,
                        retryable_codes=(429, 500, 502, 503, 504),
                        label=""):
    """Execute fn() with exponential backoff on retryable HTTP errors."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code not in retryable_codes or attempt == max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("Retry %d/%d for %s — HTTP %d — waiting %.1fs",
                           attempt, max_attempts, label, exc.code, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt == max_attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1))
            logger.warning("Retry %d/%d for %s — %s — waiting %.1fs",
                           attempt, max_attempts, label, type(exc).__name__, delay)
            time.sleep(delay)
    raise last_exc


# ===================================================================
# Error tracking
# ===================================================================
class ErrorTracker:
    """Collects per-package errors and writes them to S3 as errors.txt."""

    def __init__(self, errors: Optional[list] = None):
        self.errors: list[dict] = errors or []

    def record(self, package: str, phase: str, error_type: str,
               status_code: Optional[int], detail: str):
        entry = {
            "package": package, "phase": phase,
            "error_type": error_type, "status_code": status_code,
            "detail": detail[:500],
        }
        self.errors.append(entry)
        logger.error("ErrorTracker — %s | %s | HTTP %s | %s | %s",
                      package, phase, status_code or "N/A", error_type, detail[:200])

    @property
    def count(self) -> int:
        return len(self.errors)

    @property
    def package_names(self) -> list[str]:
        return [e["package"] for e in self.errors]

    def to_text(self) -> str:
        from datetime import datetime, timezone
        lines = [
            "# Chainguard -> CodeArtifact sync errors",
            f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
            f"# Total errors: {self.count}", "",
        ]
        for e in self.errors:
            code = e["status_code"] or "N/A"
            lines.append(f'{e["package"]}  |  phase={e["phase"]}  |  '
                         f'HTTP {code}  |  {e["error_type"]}  |  {e["detail"]}')
        return "\n".join(lines) + "\n"

    def upload_to_s3(self, bucket: str, prefix: str) -> Optional[str]:
        if not self.errors:
            return None
        if not bucket:
            logger.warning("No S3 bucket for errors.txt — skipping upload")
            return None
        from datetime import datetime, timezone
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix.rstrip('/')}/errors-{timestamp}.txt"
        try:
            s3_client.put_object(Bucket=bucket, Key=key,
                                 Body=self.to_text().encode(),
                                 ContentType="text/plain")
            logger.info("Uploaded errors.txt -> s3://%s/%s", bucket, key)
            return f"s3://{bucket}/{key}"
        except Exception as exc:
            logger.error("Failed to upload errors.txt: %s", exc)
            return None


def handle_http_error(exc: Exception, package: str, phase: str,
                      tracker: ErrorTracker) -> None:
    if isinstance(exc, urllib.error.HTTPError):
        body = ""
        try:
            body = exc.read().decode(errors="replace")
        except Exception:
            pass
        tracker.record(package=package, phase=phase, error_type="HTTPError",
                       status_code=exc.code, detail=f"{exc.reason} — {body}")
    elif isinstance(exc, urllib.error.URLError):
        reason = str(getattr(exc, "reason", exc))
        is_timeout = "timed out" in reason.lower() or isinstance(
            getattr(exc, "reason", None), TimeoutError)
        tracker.record(package=package, phase=phase,
                       error_type="Timeout" if is_timeout else "URLError",
                       status_code=None, detail=reason)
    elif isinstance(exc, TimeoutError):
        tracker.record(package=package, phase=phase, error_type="Timeout",
                       status_code=None, detail=str(exc))
    else:
        tracker.record(package=package, phase=phase,
                       error_type=type(exc).__name__,
                       status_code=None, detail=str(exc))


# ===================================================================
# 1. CodeArtifact setup (idempotent)
# ===================================================================
def setup_codeartifact() -> str:
    logger.info("Setting up CodeArtifact — domain=%s repo=%s upstream=%s",
                DOMAIN_NAME, REPO_NAME, UPSTREAM_REPO_NAME)

    for create_fn, name_val, label in [
        (lambda: ca_client.create_domain(domain=DOMAIN_NAME),
         DOMAIN_NAME, "domain"),
        (lambda: ca_client.create_repository(domain=DOMAIN_NAME,
                                             repository=UPSTREAM_REPO_NAME),
         UPSTREAM_REPO_NAME, "upstream repo"),
    ]:
        try:
            create_fn()
            logger.info("Created %s '%s'", label, name_val)
        except ClientError as exc:
            if exc.response["Error"]["Code"] == "ConflictException":
                logger.info("%s '%s' already exists", label.capitalize(), name_val)
            else:
                raise

    try:
        ca_client.associate_external_connection(
            domain=DOMAIN_NAME, repository=UPSTREAM_REPO_NAME,
            externalConnection="public:npmjs")
        logger.info("Associated external connection public:npmjs")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("ConflictException",
                                              "ResourceExistsException"):
            logger.info("External connection already associated")
        else:
            raise

    try:
        ca_client.create_repository(
            domain=DOMAIN_NAME, repository=REPO_NAME,
            upstreams=[{"repositoryName": UPSTREAM_REPO_NAME}])
        logger.info("Created main repository '%s'", REPO_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConflictException":
            logger.info("Main repo '%s' exists — updating upstreams", REPO_NAME)
            ca_client.update_repository(
                domain=DOMAIN_NAME, repository=REPO_NAME,
                upstreams=[{"repositoryName": UPSTREAM_REPO_NAME}])
        else:
            raise

    endpoint = ca_client.get_repository_endpoint(
        domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
    )["repositoryEndpoint"]
    logger.info("CodeArtifact endpoint: %s", endpoint)
    return endpoint


# ===================================================================
# 2. Package-lock.json parsing
# ===================================================================
def parse_lockfile(lockfile_json: dict) -> list[dict]:
    packages = lockfile_json.get("packages", {})
    results = []
    seen = set()
    for key, meta in packages.items():
        if not key or "node_modules/" not in key:
            continue
        name = key.rsplit("node_modules/", 1)[-1]
        version = meta.get("version")
        if not name or not version or "node_modules" in name:
            continue
        dedup_key = f"{name}@{version}"
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        results.append({"name": name, "version": version})
    logger.info("Parsed %d unique packages from lockfile", len(results))
    return results


def unique_package_names(packages: list[dict]) -> list[str]:
    seen = set()
    names = []
    for p in packages:
        n = p["name"]
        if n not in seen:
            seen.add(n)
            names.append(n)
    return names


# ===================================================================
# 3. Chainguard registry helpers
# ===================================================================
def _cgr_auth_header() -> str:
    credentials = f"{CGR_USERNAME}:{CGR_TOKEN}"
    return f"Basic {base64.b64encode(credentials.encode()).decode()}"


class _StripAuthOnRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Strip Authorization header on redirect so S3 presigned URLs work."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is not None:
            new_req.remove_header("Authorization")
            logger.debug("Redirect %s -> %s (stripped Authorization)",
                         req.full_url, newurl)
        return new_req


_cgr_opener = urllib.request.build_opener(_StripAuthOnRedirectHandler)


def fetch_chainguard_metadata(name: str, tracker: ErrorTracker) -> Optional[dict]:
    """Fetch full package metadata from Chainguard with retry + backoff."""
    encoded_name = urllib.parse.quote(name, safe="@")
    metadata_url = f"{CGR_REGISTRY}/{encoded_name}"
    auth = _cgr_auth_header()

    logger.debug("Fetching registry metadata: %s", metadata_url)

    def _do_fetch():
        req = urllib.request.Request(metadata_url, headers={
            "Authorization": auth, "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read())

    try:
        return _retry_with_backoff(_do_fetch, label=f"metadata:{name}")
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401, 403):
            logger.debug("Metadata not found for %s (%s)", name, exc.code)
            return None
        handle_http_error(exc, name, "fetch_metadata", tracker)
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        handle_http_error(exc, name, "fetch_metadata", tracker)
        return None
    except Exception as exc:
        handle_http_error(exc, name, "fetch_metadata", tracker)
        return None


def download_tarball(tarball_url: str, name: str, version: str,
                     tracker: ErrorTracker) -> Optional[bytes]:
    """Download a tarball with retry + backoff."""
    pkg_id = f"{name}@{version}"
    auth = _cgr_auth_header()

    logger.debug("Downloading tarball for %s: %s", pkg_id, tarball_url)

    def _do_download():
        tar_req = urllib.request.Request(tarball_url, headers={"Authorization": auth})
        with _cgr_opener.open(tar_req, timeout=120) as resp:
            return resp.read()

    try:
        return _retry_with_backoff(_do_download, label=f"tarball:{pkg_id}")
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401, 403):
            logger.debug("Tarball download failed for %s (%s)", pkg_id, exc.code)
            return None
        handle_http_error(exc, pkg_id, "fetch_tarball", tracker)
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        handle_http_error(exc, pkg_id, "fetch_tarball", tracker)
        return None
    except Exception as exc:
        handle_http_error(exc, pkg_id, "fetch_tarball", tracker)
        return None


# ===================================================================
# 3b. Parallel Tarball Downloads
# ===================================================================
def download_tarballs_parallel(
    work_items: list[dict], start_index: int, end_index: int,
    tracker: ErrorTracker, max_workers: int = PARALLEL_DOWNLOADS,
) -> dict[int, Optional[bytes]]:
    """Download tarballs in parallel using a thread pool.
    Returns dict mapping item index -> tarball bytes (or None on failure).
    """
    results: dict[int, Optional[bytes]] = {}
    batch = work_items[start_index:end_index]
    if not batch:
        return results

    def _download_one(idx: int, item: dict) -> tuple[int, Optional[bytes]]:
        data = download_tarball(item["tarball_url"], item["name"],
                                item["version"], tracker)
        return idx, data

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_download_one, start_index + i, item): start_index + i
            for i, item in enumerate(batch)
        }
        for future in as_completed(futures):
            try:
                idx, data = future.result()
                results[idx] = data
            except Exception as exc:
                idx = futures[future]
                item = work_items[idx]
                handle_http_error(exc, f"{item['name']}@{item['version']}",
                                  "fetch_tarball", tracker)
                results[idx] = None
    return results


# ===================================================================
# 4. CodeArtifact helpers
# ===================================================================
def _get_codeartifact_auth_token() -> str:
    return ca_client.get_authorization_token(
        domain=DOMAIN_NAME)["authorizationToken"]


def _parse_package_identity(name: str) -> dict:
    if name.startswith("@") and "/" in name:
        namespace, pkg = name.lstrip("@").split("/", 1)
        return {"namespace": namespace, "package": pkg}
    return {"package": name}


def list_codeartifact_versions(name: str) -> set[str]:
    kwargs = {
        "domain": DOMAIN_NAME, "repository": REPO_NAME,
        "format": "npm", **_parse_package_identity(name),
    }
    versions: set[str] = set()
    try:
        paginator = ca_client.get_paginator("list_package_versions")
        for page in paginator.paginate(**kwargs):
            for v in page.get("versions", []):
                versions.add(v["version"])
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.debug("Package %s not yet in CodeArtifact", name)
        else:
            logger.warning("Error listing versions for %s: %s", name, exc)
    return versions


def list_codeartifact_packages() -> list[str]:
    packages: list[str] = []
    upstream_skipped = 0
    try:
        paginator = ca_client.get_paginator("list_packages")
        for page in paginator.paginate(
            domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
        ):
            for pkg in page.get("packages", []):
                origin = pkg.get("originConfiguration", {}).get("restrictions", {})
                if origin.get("publish") == "BLOCK":
                    upstream_skipped += 1
                    continue
                pkg_name = pkg.get("package", "")
                namespace = pkg.get("namespace", "")
                full_name = f"@{namespace}/{pkg_name}" if namespace else pkg_name
                if full_name:
                    packages.append(full_name)
    except ClientError as exc:
        logger.error("Failed to list CodeArtifact packages: %s", exc)
    logger.info("Found %d published npm packages (skipped %d upstream)",
                len(packages), upstream_skipped)
    return packages


def cleanup_garbage_packages() -> dict:
    """Remove packages with 'node_modules' in their name from CodeArtifact."""
    garbage: list[dict] = []
    try:
        paginator = ca_client.get_paginator("list_packages")
        for page in paginator.paginate(
            domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
        ):
            for pkg in page.get("packages", []):
                pkg_name = pkg.get("package", "")
                namespace = pkg.get("namespace", "")
                full_name = f"@{namespace}/{pkg_name}" if namespace else pkg_name
                if "node_modules" in full_name:
                    garbage.append({"full_name": full_name,
                                    "namespace": namespace, "package": pkg_name})
    except ClientError as exc:
        logger.error("Failed to list packages for cleanup: %s", exc)
        return {"cleaned": 0, "failed": 0, "packages": []}

    if not garbage:
        logger.info("Cleanup: no garbage packages found")
        return {"cleaned": 0, "failed": 0, "packages": []}

    logger.info("Cleanup: found %d garbage packages", len(garbage))
    cleaned, failed, cleaned_names = 0, 0, []

    for pkg in garbage:
        full_name = pkg["full_name"]
        list_kwargs = {"domain": DOMAIN_NAME, "repository": REPO_NAME,
                       "format": "npm", "package": pkg["package"]}
        if pkg["namespace"]:
            list_kwargs["namespace"] = pkg["namespace"]

        versions_to_delete: list[str] = []
        try:
            ver_paginator = ca_client.get_paginator("list_package_versions")
            for page in ver_paginator.paginate(**list_kwargs):
                for v in page.get("versions", []):
                    versions_to_delete.append(v["version"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                logger.warning("Could not list versions for %s: %s", full_name, exc)
                failed += 1
                continue

        if not versions_to_delete:
            continue

        batch_size = 50
        all_deleted = True
        for bi in range(0, len(versions_to_delete), batch_size):
            batch = versions_to_delete[bi:bi + batch_size]
            delete_kwargs = {"domain": DOMAIN_NAME, "repository": REPO_NAME,
                             "format": "npm", "package": pkg["package"],
                             "versions": batch}
            if pkg["namespace"]:
                delete_kwargs["namespace"] = pkg["namespace"]
            try:
                result = ca_client.delete_package_versions(**delete_kwargs)
                if result.get("failedVersions", {}):
                    all_deleted = False
            except ClientError:
                all_deleted = False

        if all_deleted:
            cleaned += 1
            cleaned_names.append(full_name)
        else:
            failed += 1

    return {"cleaned": cleaned, "failed": failed,
            "total_garbage_found": len(garbage), "packages_removed": cleaned_names}


def _set_origin_controls(name: str):
    kwargs = {
        "domain": DOMAIN_NAME, "repository": REPO_NAME, "format": "npm",
        "restrictions": {"publish": "ALLOW", "upstream": "BLOCK"},
        **_parse_package_identity(name),
    }
    try:
        ca_client.put_package_origin_configuration(**kwargs)
    except ClientError:
        logger.debug("Could not set origin controls for %s", name)


def publish_to_codeartifact(
    name: str, version: str, tarball_bytes: bytes,
    endpoint: str, auth_token: str, tracker: ErrorTracker,
) -> bool:
    """Publish a tarball to CodeArtifact with retry + backoff."""
    pkg_id = f"{name}@{version}"
    _set_origin_controls(name)

    tarball_b64 = base64.b64encode(tarball_bytes).decode()
    shasum = hashlib.sha1(tarball_bytes).hexdigest()
    integrity = "sha512-" + base64.b64encode(
        hashlib.sha512(tarball_bytes).digest()).decode()
    tarball_name = f"{name}-{version}.tgz"

    payload = {
        "_id": name, "name": name,
        "dist-tags": {"latest": version},
        "versions": {version: {
            "name": name, "version": version,
            "_id": f"{name}@{version}",
            "dist": {"shasum": shasum, "integrity": integrity,
                     "tarball": f"{endpoint.rstrip('/')}/{name}/-/{tarball_name}"},
        }},
        "_attachments": {tarball_name: {
            "content_type": "application/octet-stream",
            "data": tarball_b64, "length": len(tarball_bytes),
        }},
    }

    encoded_name = urllib.parse.quote(name, safe="@")
    put_url = f"{endpoint.rstrip('/')}/{encoded_name}"

    def _do_publish():
        req = urllib.request.Request(
            put_url, data=json.dumps(payload).encode(), method="PUT",
            headers={"Authorization": f"Bearer {auth_token}",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status

    try:
        status = _retry_with_backoff(_do_publish, label=f"publish:{pkg_id}")
        logger.info("Published %s@%s (%s)", name, version, status)
        return True
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode(errors="replace")
        except Exception:
            pass
        if exc.code == 409 or "already exists" in body.lower() \
                or "previously published" in body.lower() \
                or "EPUBLISHCONFLICT" in body:
            logger.info("%s@%s already exists in CodeArtifact", name, version)
            return True
        tracker.record(package=pkg_id, phase="publish", error_type="HTTPError",
                       status_code=exc.code, detail=f"{exc.reason} — {body}")
        return False
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        handle_http_error(exc, pkg_id, "publish", tracker)
        return False
    except Exception as exc:
        handle_http_error(exc, pkg_id, "publish", tracker)
        return False


# ===================================================================
# 5. Work item discovery (unchanged logic, used by build_work_items)
# ===================================================================
def build_work_items(packages, tracker):
    work_items, skipped_existing = [], 0
    not_in_chainguard = []
    names = unique_package_names(packages)
    lockfile_versions = {}
    for p in packages:
        lockfile_versions.setdefault(p["name"], set()).add(p["version"])
    for name in names:
        metadata = fetch_chainguard_metadata(name, tracker)
        if metadata is None:
            not_in_chainguard.append(name)
            continue
        cgr_versions = metadata.get("versions", {})
        if not cgr_versions:
            not_in_chainguard.append(name)
            continue
        candidates = lockfile_versions.get(name, set()) & set(cgr_versions.keys())
        existing = list_codeartifact_versions(name)
        new_versions = candidates - existing
        skipped_existing += len(candidates) - len(new_versions)
        for version in sorted(new_versions):
            vm = cgr_versions.get(version)
            if not vm:
                continue
            tarball_url = vm.get("dist", {}).get("tarball")
            if tarball_url:
                work_items.append({"name": name, "version": version,
                                   "tarball_url": tarball_url})
    return work_items, skipped_existing, not_in_chainguard


def build_update_work_items(tracker):
    work_items, skipped_existing = [], 0
    not_in_chainguard = []
    existing_packages = list_codeartifact_packages()
    if not existing_packages:
        return work_items, 0, 0, []
    for name in existing_packages:
        metadata = fetch_chainguard_metadata(name, tracker)
        if metadata is None:
            not_in_chainguard.append(name)
            continue
        cgr_versions = metadata.get("versions", {})
        if not cgr_versions:
            not_in_chainguard.append(name)
            continue
        existing = list_codeartifact_versions(name)
        new_versions = set(cgr_versions.keys()) - existing
        skipped_existing += len(cgr_versions) - len(new_versions)
        for version in sorted(new_versions):
            vm = cgr_versions.get(version)
            if not vm:
                continue
            tarball_url = vm.get("dist", {}).get("tarball")
            if tarball_url:
                work_items.append({"name": name, "version": version,
                                   "tarball_url": tarball_url})
    return work_items, skipped_existing, len(existing_packages), not_in_chainguard


# ===================================================================
# 6. Checkpoint management
# ===================================================================
def _generate_run_id() -> str:
    from datetime import datetime, timezone
    import uuid
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}-{uuid.uuid4().hex[:8]}"


def _checkpoint_key(run_id: str) -> str:
    return f"{CHECKPOINT_S3_PREFIX.rstrip('/')}/{run_id}.json"


def _save_checkpoint(run_id, event, phase, work_items, counters,
                     not_found_list, tracker, invocation_number,
                     package_names=None, discovery_index=0,
                     processing_index=0) -> str:
    bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
              or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
    if not bucket:
        raise ValueError("No S3 bucket for checkpoints.")
    key = _checkpoint_key(run_id)
    checkpoint = {
        "run_id": run_id, "invocation_number": invocation_number,
        "phase": phase, "work_items": work_items, "counters": counters,
        "not_found_list": not_found_list, "errors": tracker.errors,
        "original_event": {
            "s3_bucket": event.get("s3_bucket"), "s3_key": event.get("s3_key"),
            "setup_codeartifact": False,
            "update_existing": event.get("update_existing", False),
            "cleanup_garbage": False,
            "max_packages": event.get("max_packages", MAX_PACKAGES),
            "errors_s3_bucket": event.get("errors_s3_bucket", ERRORS_S3_BUCKET),
            "errors_s3_prefix": event.get("errors_s3_prefix", ERRORS_S3_PREFIX),
            "checkpoint_s3_bucket": bucket,
            "dry_run": event.get("dry_run", False),
            "allowlist": event.get("allowlist", []),
            "denylist": event.get("denylist", []),
        },
    }
    if phase == "discovery":
        checkpoint["package_names"] = package_names or []
        checkpoint["discovery_index"] = discovery_index
    elif phase == "processing":
        checkpoint["next_index"] = processing_index
    s3_client.put_object(Bucket=bucket, Key=key,
                         Body=json.dumps(checkpoint).encode(),
                         ContentType="application/json")
    return key


def _load_checkpoint(bucket, key):
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def _delete_checkpoint(bucket, run_id):
    try:
        s3_client.delete_object(Bucket=bucket, Key=_checkpoint_key(run_id))
    except Exception as exc:
        logger.warning("Could not delete checkpoint: %s", exc)


def _checkpoint_exists(bucket, run_id):
    if not bucket:
        return False
    try:
        s3_client.head_object(Bucket=bucket, Key=_checkpoint_key(run_id))
        return True
    except ClientError:
        return False


def _try_save_failure_checkpoint(
    run_id, bucket, original_event, phase, work_items, counters,
    not_found_list, tracker, invocation_number,
    package_names=None, discovery_index=0, processing_index=0,
):
    if not bucket:
        return None
    if _checkpoint_exists(bucket, run_id):
        return run_id
    try:
        _save_checkpoint(run_id, original_event, phase, work_items,
                         counters, not_found_list, tracker, invocation_number,
                         package_names=package_names,
                         discovery_index=discovery_index,
                         processing_index=processing_index)
        return run_id
    except Exception as exc:
        logger.error("Failed to save failure checkpoint: %s", exc)
        return None


def _self_invoke(event):
    lambda_client.invoke(FunctionName=FUNCTION_NAME, InvocationType="Event",
                         Payload=json.dumps(event).encode())


def _is_time_running_out(context):
    if context is None:
        return False
    return context.get_remaining_time_in_millis() < CHECKPOINT_BUFFER_MS


def _list_checkpoints(bucket):
    if not bucket:
        return []
    prefix = CHECKPOINT_S3_PREFIX.rstrip("/") + "/"
    checkpoints = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                run_id = key.rsplit("/", 1)[-1].replace(".json", "")
                try:
                    cp_obj = s3_client.get_object(Bucket=bucket, Key=key)
                    cp = json.loads(cp_obj["Body"].read())
                    checkpoints.append({
                        "run_id": run_id,
                        "next_index": cp.get("next_index", 0),
                        "total_work_items": len(cp.get("work_items", [])),
                        "invocation_number": cp.get("invocation_number", 0),
                        "counters": cp.get("counters", {}),
                        "last_modified": obj["LastModified"].isoformat(),
                        "resume_with": {"resume_run_id": run_id},
                    })
                except Exception:
                    pass
    except Exception as exc:
        logger.error("Failed to list checkpoints: %s", exc)
    return checkpoints


# ===================================================================
# 7. Lambda handler
# ===================================================================
def lambda_handler(event, context):
    """
    Entry modes:
      1. NEW SESSION:       {"s3_bucket": "...", "s3_key": "package-lock.json"}
      2. UPDATE EXISTING:   {"update_existing": true}
      3. RESUME:            {"resume_run_id": "20260402T034658Z-abc12345"}
      4. LIST CHECKPOINTS:  {"list_checkpoints": true}
      5. CLEANUP ONLY:      {"cleanup_only": true}

    Options:
      "dry_run": true              — preview mode
      "allowlist": ["@types/*"]    — only sync matching packages
      "denylist":  ["debug"]       — skip matching packages
      "cleanup_garbage": true      — run cleanup before sync (default: true)
    """
    dry_run = event.get("dry_run", DRY_RUN_DEFAULT)
    if dry_run:
        logger.info("*** DRY-RUN MODE — no packages will be published ***")

    allowlist = event.get("allowlist", PACKAGE_ALLOWLIST) or []
    denylist = event.get("denylist", PACKAGE_DENYLIST) or []

    # --- list checkpoints ---
    if event.get("list_checkpoints"):
        bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
                  or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
        return _success(f"Found {len(_list_checkpoints(bucket))} checkpoint(s)",
                        {"checkpoints": _list_checkpoints(bucket), "status": "listing"})

    # --- cleanup only ---
    if event.get("cleanup_only"):
        return _success("Cleanup complete",
                        {**cleanup_garbage_packages(), "status": "cleanup_complete"})

    if not CGR_USERNAME or not CGR_TOKEN:
        msg = "CGR_USERNAME and CGR_TOKEN environment variables are required"
        _notify_failure(msg)
        return _error(msg)

    checkpoint_bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
                         or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
    is_update_existing = event.get("update_existing", False)
    do_cleanup = event.get("cleanup_garbage", True)
    max_packages = int(event.get("max_packages", MAX_PACKAGES))

    is_auto_continuation = "_checkpoint_key" in event
    is_manual_resume = "resume_run_id" in event and not is_auto_continuation
    is_new_session = not is_auto_continuation and not is_manual_resume

    if is_auto_continuation:
        run_id = event["_run_id"]
        invocation_number = event["_invocation_number"]
    elif is_manual_resume:
        run_id = event["resume_run_id"]
        invocation_number = event.get("_invocation_number", 1)
    else:
        run_id = _generate_run_id()
        invocation_number = 1

    # --- Load checkpoint ---
    if is_auto_continuation or is_manual_resume:
        ckpt_key = event["_checkpoint_key"] if is_auto_continuation else _checkpoint_key(run_id)
        try:
            cp = _load_checkpoint(checkpoint_bucket, ckpt_key)
        except Exception as exc:
            msg = f"Could not load checkpoint for run '{run_id}': {exc}"
            _notify_failure(msg, run_id)
            return _error(msg)

        phase = cp.get("phase", "processing")
        work_items = cp.get("work_items", [])
        counters = cp["counters"]
        not_found_list = cp["not_found_list"]
        tracker = ErrorTracker(errors=cp.get("errors", []))
        original_event = cp["original_event"]
        dry_run = original_event.get("dry_run", dry_run)
        allowlist = original_event.get("allowlist", allowlist)
        denylist = original_event.get("denylist", denylist)

        if phase == "discovery":
            package_names = cp.get("package_names", [])
            discovery_index = cp.get("discovery_index", 0)
            processing_index = 0
        else:
            package_names = []
            discovery_index = 0
            processing_index = cp.get("next_index", 0)

    # --- New session ---
    elif is_new_session:
        phase = "discovery"
        work_items = []
        counters = {"found": 0, "not_found": 0, "failed": 0, "skipped_existing": 0}
        not_found_list = []
        tracker = ErrorTracker()
        original_event = event
        discovery_index = 0
        processing_index = 0

        if event.get("setup_codeartifact", False):
            setup_codeartifact()
        if do_cleanup:
            cleanup_garbage_packages()

        if is_update_existing:
            package_names = list_codeartifact_packages()
        else:
            if "lockfile_content" in event:
                lockfile = json.loads(event["lockfile_content"])
            elif "s3_bucket" in event and "s3_key" in event:
                obj = s3_client.get_object(Bucket=event["s3_bucket"], Key=event["s3_key"])
                lockfile = json.loads(obj["Body"].read())
            else:
                msg = ("Provide 'lockfile_content', 's3_bucket'+'s3_key', "
                       "'update_existing', 'resume_run_id', or 'list_checkpoints'")
                _notify_failure(msg, run_id)
                return _error(msg)
            packages = parse_lockfile(lockfile)
            if not packages:
                return _success("No packages found in lockfile", _empty_summary())
            package_names = unique_package_names(packages)

        if not package_names:
            return _success("No packages to process", _empty_summary())

        # Apply allowlist/denylist
        if allowlist or denylist:
            package_names, filtered_out = _filter_packages(package_names, allowlist, denylist)
            counters["filtered_out"] = len(filtered_out)
            if not package_names:
                return _success("All packages filtered out by allowlist/denylist",
                                {**_empty_summary(), "filtered_out": len(filtered_out)})

        if len(package_names) > max_packages:
            package_names = package_names[:max_packages]

    # --- Safety cap ---
    if invocation_number > MAX_INVOCATIONS:
        _flush_errors(tracker, original_event)
        msg = f"Exceeded max invocations ({MAX_INVOCATIONS}). Resume with: {{\"resume_run_id\": \"{run_id}\"}}"
        _notify_failure(msg, run_id)
        return _error(msg)

    # ==================================================================
    # PHASE 1: DISCOVERY
    # ==================================================================
    if phase == "discovery":
        lockfile_versions: dict[str, set[str]] = {}
        if not is_update_existing and not (is_auto_continuation or is_manual_resume):
            for p in packages:
                lockfile_versions.setdefault(p["name"], set()).add(p["version"])
        elif is_auto_continuation or is_manual_resume:
            oe = original_event
            if not oe.get("update_existing", False) and oe.get("s3_bucket") and oe.get("s3_key"):
                try:
                    obj = s3_client.get_object(Bucket=oe["s3_bucket"], Key=oe["s3_key"])
                    lf = json.loads(obj["Body"].read())
                    for p in parse_lockfile(lf):
                        lockfile_versions.setdefault(p["name"], set()).add(p["version"])
                except Exception:
                    pass

        try:
            for i in range(discovery_index, len(package_names)):
                if _is_time_running_out(context):
                    _flush_errors(tracker, original_event)
                    ckpt_key = _save_checkpoint(
                        run_id, original_event, "discovery", work_items,
                        counters, not_found_list, tracker, invocation_number,
                        package_names=package_names, discovery_index=i)
                    _self_invoke({**original_event, "_run_id": run_id,
                                  "_checkpoint_key": ckpt_key,
                                  "_invocation_number": invocation_number + 1,
                                  "checkpoint_s3_bucket": checkpoint_bucket})
                    return _success(
                        f"Discovery checkpointed at {i}/{len(package_names)}",
                        {"run_id": run_id, "phase": "discovery", "status": "continuing"})

                name = package_names[i]
                metadata = fetch_chainguard_metadata(name, tracker)
                if metadata is None:
                    not_found_list.append(name)
                    continue
                cgr_versions = metadata.get("versions", {})
                if not cgr_versions:
                    not_found_list.append(name)
                    continue

                candidates = set(cgr_versions.keys())
                existing = list_codeartifact_versions(name)
                new_versions = candidates - existing
                counters["skipped_existing"] += len(candidates) - len(new_versions)

                for version in sorted(new_versions):
                    vm = cgr_versions.get(version)
                    if not vm:
                        continue
                    tarball_url = vm.get("dist", {}).get("tarball")
                    if tarball_url:
                        work_items.append({"name": name, "version": version,
                                           "tarball_url": tarball_url})
                discovery_index = i + 1

        except Exception as exc:
            _flush_errors(tracker, original_event)
            saved = _try_save_failure_checkpoint(
                run_id, checkpoint_bucket, original_event,
                "discovery", work_items, counters, not_found_list,
                tracker, invocation_number,
                package_names=package_names, discovery_index=discovery_index)
            msg = f"Discovery error at {discovery_index}/{len(package_names)}: {type(exc).__name__}: {str(exc)[:200]}"
            if saved:
                msg += f'. Resume with: {{"resume_run_id": "{saved}"}}'
            _notify_failure(msg, run_id)
            return _error(msg)

        if not work_items:
            summary = {**_empty_summary(), "run_id": run_id, "dry_run": dry_run,
                       "skipped_existing": counters["skipped_existing"],
                       "not_found_packages": not_found_list}
            _put_metrics(summary, dry_run)
            _notify_completion(summary, dry_run)
            return _success("No new versions to sync", summary)

        phase = "processing"
        processing_index = 0

    # ==================================================================
    # DRY-RUN EXIT
    # ==================================================================
    if dry_run:
        dry_summary = {
            "run_id": run_id, "dry_run": True,
            "total_work_items": len(work_items), "would_sync": len(work_items),
            "skipped_existing": counters["skipped_existing"],
            "not_found_packages": not_found_list,
            "sample_work_items": [f"{w['name']}@{w['version']}" for w in work_items[:25]],
            "remaining_items": max(0, len(work_items) - 25),
            "http_errors": tracker.count, "status": "dry_run_complete",
        }
        if allowlist or denylist:
            dry_summary.update({"allowlist": allowlist, "denylist": denylist,
                                "filtered_out": counters.get("filtered_out", 0)})
        _put_metrics(dry_summary, dry_run=True)
        _notify_completion(dry_summary, dry_run=True)
        return _success("Dry-run complete — no packages were published", dry_summary)

    # ==================================================================
    # PHASE 2: PROCESSING — parallel download + publish
    # ==================================================================
    endpoint = ca_client.get_repository_endpoint(
        domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
    )["repositoryEndpoint"]
    auth_token = _get_codeartifact_auth_token()

    current_index = processing_index
    timed_out = False

    logger.info("Processing — %d work items from %d (parallel=%d)",
                 len(work_items), processing_index, PARALLEL_DOWNLOADS)

    try:
        i = processing_index
        while i < len(work_items):
            if _is_time_running_out(context):
                timed_out = True
                current_index = i
                break

            batch_end = min(i + PARALLEL_DOWNLOADS, len(work_items))
            tarball_map = download_tarballs_parallel(
                work_items, i, batch_end, tracker, max_workers=PARALLEL_DOWNLOADS)

            for idx in range(i, batch_end):
                if _is_time_running_out(context):
                    timed_out = True
                    current_index = idx
                    break
                item = work_items[idx]
                tarball = tarball_map.get(idx)
                if tarball is None:
                    counters["not_found"] += 1
                    not_found_list.append(f"{item['name']}@{item['version']}")
                else:
                    counters["found"] += 1
                    if not publish_to_codeartifact(item["name"], item["version"],
                                                   tarball, endpoint, auth_token, tracker):
                        counters["failed"] += 1
                current_index = idx + 1

            if timed_out:
                break
            i = batch_end

    except Exception as exc:
        _flush_errors(tracker, original_event)
        saved = _try_save_failure_checkpoint(
            run_id, checkpoint_bucket, original_event,
            "processing", work_items, counters, not_found_list,
            tracker, invocation_number, processing_index=current_index)
        msg = f"Error at item {current_index}/{len(work_items)}: {type(exc).__name__}: {str(exc)[:200]}"
        if saved:
            msg += f'. Resume with: {{"resume_run_id": "{saved}"}}'
        _notify_failure(msg, run_id)
        return _error(msg)

    # --- Checkpoint on timeout ---
    if timed_out and current_index < len(work_items):
        if not checkpoint_bucket:
            _flush_errors(tracker, original_event)
            msg = f"Timed out at {current_index}/{len(work_items)} with no checkpoint bucket."
            _notify_failure(msg, run_id)
            return _error(msg)
        _flush_errors(tracker, original_event)
        ckpt_key = _save_checkpoint(
            run_id, original_event, "processing", work_items,
            counters, not_found_list, tracker, invocation_number,
            processing_index=current_index)
        _self_invoke({**original_event, "_run_id": run_id,
                      "_checkpoint_key": ckpt_key,
                      "_invocation_number": invocation_number + 1,
                      "checkpoint_s3_bucket": checkpoint_bucket})
        return _success(
            f"Checkpointed at {current_index}/{len(work_items)}",
            {"run_id": run_id, "status": "continuing",
             "processed_so_far": current_index, **counters})

    # --- Finalize ---
    if (is_auto_continuation or is_manual_resume) and checkpoint_bucket:
        _delete_checkpoint(checkpoint_bucket, run_id)

    errors_s3_uri = _flush_errors(tracker, original_event)

    summary = {
        "run_id": run_id, "dry_run": dry_run,
        "total_invocations": invocation_number,
        "total_work_items": len(work_items),
        "synced_to_codeartifact": counters["found"],
        "skipped_existing": counters["skipped_existing"],
        "tarball_download_failed": counters["not_found"],
        "failed_to_publish": counters["failed"],
        "http_errors": tracker.count,
        "errors_file": errors_s3_uri,
        "errored_packages": tracker.package_names,
        "not_found_packages": not_found_list,
        "status": "complete",
    }
    if allowlist or denylist:
        summary.update({"allowlist": allowlist, "denylist": denylist,
                        "filtered_out": counters.get("filtered_out", 0)})

    logger.info("Sync complete — %s", json.dumps(summary, indent=2))
    _put_metrics(summary, dry_run)
    _notify_completion(summary, dry_run)
    return _success("Sync complete", summary)


# ===================================================================
# Helpers
# ===================================================================
def _flush_errors(tracker, event):
    if tracker.count == 0:
        return None
    bucket = event.get("errors_s3_bucket", ERRORS_S3_BUCKET)
    prefix = event.get("errors_s3_prefix", ERRORS_S3_PREFIX)
    return tracker.upload_to_s3(bucket, prefix)


def _empty_summary():
    return {
        "total_work_items": 0, "synced_to_codeartifact": 0,
        "skipped_existing": 0, "tarball_download_failed": 0,
        "failed_to_publish": 0, "http_errors": 0,
        "errors_file": None, "errored_packages": [],
        "not_found_packages": [], "status": "complete",
    }


def _success(message, summary):
    return {"statusCode": 200, "body": {"message": message, "summary": summary}}


def _error(message):
    logger.error(message)
    return {"statusCode": 400, "body": {"error": message}}
