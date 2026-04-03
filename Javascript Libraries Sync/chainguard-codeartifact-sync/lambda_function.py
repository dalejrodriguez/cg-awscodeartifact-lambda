"""
AWS Lambda: Sync Chainguard NPM packages to AWS CodeArtifact
(Lockfile versions only)

Syncs the exact package versions specified in a package-lock.json from
Chainguard Libraries to CodeArtifact. Versions already in CodeArtifact
are skipped. For syncing ALL versions of each package, use the
companion lambda: sync_all_versions.py

Trigger:
{
    "s3_bucket": "my-bucket",
    "s3_key": "path/to/package-lock.json",
    "setup_codeartifact": false,        // optional, default false
    "dry_run": false,                   // optional — preview without publishing
    "allowlist": ["@scope/*", "pkg"],   // optional — only sync these patterns
    "denylist": ["unwanted-pkg"]        // optional — skip these patterns
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
    ERRORS_S3_PREFIX        — S3 key prefix for errors.txt   (default: chainguard-sync/)
    CHECKPOINT_S3_BUCKET    — S3 bucket for checkpoint state (falls back to ERRORS_S3_BUCKET)
    CHECKPOINT_S3_PREFIX    — S3 key prefix for checkpoints  (default: chainguard-sync/checkpoints/)
    CHECKPOINT_BUFFER_MS    — ms before timeout to save      (default: 120000 = 120s)
    MAX_INVOCATIONS         — safety cap on re-invocations   (default: 50)
    MAX_PACKAGES            — max packages to process per run (default: 50000)
    LOG_LEVEL               — DEBUG / INFO / WARN  (default: DEBUG)
    SNS_TOPIC_ARN           — SNS topic for completion/failure notifications
    PARALLEL_DOWNLOADS      — max concurrent tarball downloads (default: 5)
    MAX_RETRIES             — max retry attempts for HTTP errors (default: 3)
    RETRY_BASE_DELAY        — base delay in seconds for backoff (default: 1.0)
    CW_METRICS_NAMESPACE    — CloudWatch namespace   (default: ChainguardSync)
    CW_METRICS_ENABLED      — enable CloudWatch metrics (default: true)
    ALLOWLIST               — JSON array of package patterns to include
    DENYLIST                — JSON array of package patterns to exclude
"""

import json
import os
import base64
import hashlib
import logging
import time
import fnmatch
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

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

# Suppress botocore/boto3 raw HTTP debug logs — they flood CloudWatch with
# SigV4 signatures, full request/response bodies, and certificate paths.
# Our module logger stays at DEBUG for useful application-level logs.
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Reuse across invocations
ca_client = boto3.client("codeartifact", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)

ERRORS_S3_BUCKET = os.environ.get("ERRORS_S3_BUCKET", "")
ERRORS_S3_PREFIX = os.environ.get("ERRORS_S3_PREFIX", "chainguard-sync/")

CHECKPOINT_S3_BUCKET = os.environ.get("CHECKPOINT_S3_BUCKET", "")
CHECKPOINT_S3_PREFIX = os.environ.get("CHECKPOINT_S3_PREFIX", "chainguard-sync/checkpoints/")
CHECKPOINT_BUFFER_MS = int(os.environ.get("CHECKPOINT_BUFFER_MS", "120000"))
MAX_INVOCATIONS = int(os.environ.get("MAX_INVOCATIONS", "50"))
MAX_PACKAGES = int(os.environ.get("MAX_PACKAGES", "50000"))
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "chainguard-codeartifact-sync")

lambda_client = boto3.client("lambda", region_name=AWS_REGION)

# SNS notifications
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
sns_client = boto3.client("sns", region_name=AWS_REGION) if SNS_TOPIC_ARN else None

# Parallel downloads
PARALLEL_DOWNLOADS = int(os.environ.get("PARALLEL_DOWNLOADS", "5"))

# Retry configuration
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BASE_DELAY = float(os.environ.get("RETRY_BASE_DELAY", "1.0"))

# CloudWatch custom metrics
CW_METRICS_NAMESPACE = os.environ.get("CW_METRICS_NAMESPACE", "ChainguardSync")
CW_METRICS_ENABLED = os.environ.get("CW_METRICS_ENABLED", "true").lower() == "true"
cw_client = boto3.client("cloudwatch", region_name=AWS_REGION) if CW_METRICS_ENABLED else None

# Package allowlist / denylist (from env — can be overridden per-invocation)
def _parse_json_list(env_var: str) -> list[str]:
    raw = os.environ.get(env_var, "")
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, list) else []
    except (json.JSONDecodeError, TypeError):
        return []

ENV_ALLOWLIST = _parse_json_list("ALLOWLIST")
ENV_DENYLIST = _parse_json_list("DENYLIST")


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
# Retry with exponential backoff
# ===================================================================
def _retry_with_backoff(fn, max_retries: int = MAX_RETRIES,
                        base_delay: float = RETRY_BASE_DELAY,
                        retryable_codes: tuple = (429, 500, 502, 503, 504)):
    """Execute fn() with exponential backoff on retryable HTTP errors.

    Returns the result of fn() on success.
    Raises the last exception if all retries are exhausted.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code not in retryable_codes or attempt == max_retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning("Retry %d/%d for HTTP %d — waiting %.1fs",
                           attempt + 1, max_retries, exc.code, delay)
            time.sleep(delay)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc
            if attempt == max_retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning("Retry %d/%d for %s — waiting %.1fs",
                           attempt + 1, max_retries,
                           type(exc).__name__, delay)
            time.sleep(delay)
    raise last_exc  # type: ignore[misc]


# ===================================================================
# SNS notifications
# ===================================================================
def _send_notification(subject: str, message: str,
                       status: str = "info") -> None:
    """Publish a notification to the configured SNS topic.
    Silently skips if SNS_TOPIC_ARN is not set.
    """
    if not SNS_TOPIC_ARN or not sns_client:
        return
    try:
        # Prefix subject with status for easy email filtering
        tag = {"complete": "SUCCESS", "error": "FAILURE",
               "info": "INFO"}.get(status, "INFO")
        full_subject = f"[ChainguardSync] [{tag}] {subject}"[:100]

        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=full_subject,
            Message=message,
        )
        logger.info("SNS notification sent: %s", full_subject)
    except Exception as exc:
        logger.warning("Failed to send SNS notification: %s", exc)


# ===================================================================
# CloudWatch custom metrics
# ===================================================================
def _put_metrics(counters: dict, run_id: str,
                 extra: Optional[dict] = None) -> None:
    """Emit CloudWatch custom metrics for the sync run."""
    if not CW_METRICS_ENABLED or not cw_client:
        return
    try:
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc)
        dimensions = [
            {"Name": "Domain", "Value": DOMAIN_NAME},
            {"Name": "Repository", "Value": REPO_NAME},
        ]
        metric_data = [
            {"MetricName": "PackagesSynced",
             "Timestamp": ts, "Value": counters.get("found", 0),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "PackagesFailed",
             "Timestamp": ts, "Value": counters.get("failed", 0),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "PackagesSkipped",
             "Timestamp": ts, "Value": counters.get("skipped_existing", 0),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "TarballDownloadFailed",
             "Timestamp": ts, "Value": counters.get("not_found", 0),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "HttpErrors",
             "Timestamp": ts, "Value": counters.get("http_errors", 0),
             "Unit": "Count", "Dimensions": dimensions},
        ]
        if extra:
            for name, value in extra.items():
                metric_data.append({
                    "MetricName": name, "Timestamp": ts,
                    "Value": value, "Unit": "Count",
                    "Dimensions": dimensions})

        # CloudWatch accepts up to 1000 metrics per call; we're well under
        cw_client.put_metric_data(
            Namespace=CW_METRICS_NAMESPACE, MetricData=metric_data)
        logger.info("Published %d CloudWatch metrics", len(metric_data))
    except Exception as exc:
        logger.warning("Failed to publish CloudWatch metrics: %s", exc)


# ===================================================================
# Package allowlist / denylist filtering
# ===================================================================
def _matches_patterns(name: str, patterns: list[str]) -> bool:
    """Check if a package name matches any of the given glob patterns.

    Supports:
      - Exact match:  "lodash"
      - Scope glob:   "@scope/*"
      - Wildcards:    "*lodash*"
    """
    for pattern in patterns:
        if fnmatch.fnmatch(name, pattern):
            return True
    return False


def filter_packages(names: list[str], allowlist: list[str],
                    denylist: list[str]) -> list[str]:
    """Apply allowlist and denylist filtering to a list of package names.

    - If allowlist is non-empty, only packages matching at least one
      allowlist pattern are kept.
    - Then, any packages matching a denylist pattern are removed.
    """
    original_count = len(names)

    if allowlist:
        names = [n for n in names if _matches_patterns(n, allowlist)]
        logger.info("Allowlist filter: %d -> %d packages",
                     original_count, len(names))

    if denylist:
        before = len(names)
        names = [n for n in names if not _matches_patterns(n, denylist)]
        logger.info("Denylist filter: %d -> %d packages", before, len(names))

    if original_count != len(names):
        logger.info("Package filtering: %d -> %d packages after allow/denylist",
                     original_count, len(names))
    return names


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
        # Extract package name from the LAST node_modules/ segment.
        # Lockfiles have nested deps like:
        #   node_modules/@radix-ui/react-alert-dialog/node_modules/@radix-ui/primitive
        # We want just: @radix-ui/primitive
        name = key.rsplit("node_modules/", 1)[-1]
        version = meta.get("version")
        if not name or not version or "node_modules" in name:
            continue
        # Deduplicate — nested deps can duplicate top-level ones
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
    """Strip Authorization header on redirect so S3 presigned URLs work.

    Chainguard's registry returns 302 redirects from tarball URLs to
    presigned S3 URLs. Python's urllib forwards all headers on redirect.
    S3 sees the Basic auth header, treats it as malformed SigV4, and
    returns 400 'Missing x-amz-content-sha256'. Stripping the header
    lets the presigned URL's query-string auth work correctly.
    """
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is not None:
            new_req.remove_header("Authorization")
            logger.debug("Redirect %s -> %s (stripped Authorization)",
                         req.full_url, newurl)
        return new_req


_cgr_opener = urllib.request.build_opener(_StripAuthOnRedirectHandler)


def fetch_chainguard_metadata(name: str, tracker: ErrorTracker) -> Optional[dict]:
    """Fetch full package metadata from Chainguard (all versions).
    Returns parsed JSON dict or None if not found.
    Uses exponential backoff on retryable HTTP errors.
    """
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
        return _retry_with_backoff(_do_fetch)
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
    """Download a tarball using the auth-stripping opener.
    Returns bytes on success, None on failure.
    Uses exponential backoff on retryable HTTP errors.
    """
    pkg_id = f"{name}@{version}"
    auth = _cgr_auth_header()

    logger.debug("Downloading tarball for %s: %s", pkg_id, tarball_url)

    def _do_download():
        tar_req = urllib.request.Request(tarball_url, headers={"Authorization": auth})
        with _cgr_opener.open(tar_req, timeout=120) as resp:
            return resp.read()

    try:
        return _retry_with_backoff(_do_download)
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
# 4. CodeArtifact helpers
# ===================================================================
def _get_codeartifact_auth_token() -> str:
    return ca_client.get_authorization_token(
        domain=DOMAIN_NAME)["authorizationToken"]


def _parse_package_identity(name: str) -> dict:
    """Return kwargs for CodeArtifact API calls (namespace + package)."""
    if name.startswith("@") and "/" in name:
        namespace, pkg = name.lstrip("@").split("/", 1)
        return {"namespace": namespace, "package": pkg}
    return {"package": name}


def list_codeartifact_versions(name: str) -> set[str]:
    """Return set of version strings already in CodeArtifact for this package.
    Returns empty set if package doesn't exist yet or on error.
    """
    kwargs = {
        "domain": DOMAIN_NAME,
        "repository": REPO_NAME,
        "format": "npm",
        **_parse_package_identity(name),
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
    """Return a list of npm package names directly published to CodeArtifact.
    Filters out packages pulled from the upstream npm connection.
    Scoped packages are returned as '@scope/name'.
    """
    packages: list[str] = []
    upstream_skipped = 0
    try:
        paginator = ca_client.get_paginator("list_packages")
        for page in paginator.paginate(
            domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
        ):
            for pkg in page.get("packages", []):
                # Skip packages from upstream — they have publish=BLOCK
                # and were cached from the public npm connection, not
                # published by us from Chainguard.
                origin = pkg.get("originConfiguration", {}).get("restrictions", {})
                if origin.get("publish") == "BLOCK":
                    upstream_skipped += 1
                    continue

                pkg_name = pkg.get("package", "")
                namespace = pkg.get("namespace", "")
                if namespace:
                    full_name = f"@{namespace}/{pkg_name}"
                else:
                    full_name = pkg_name
                if full_name:
                    packages.append(full_name)
    except ClientError as exc:
        logger.error("Failed to list CodeArtifact packages: %s", exc)
    logger.info("Found %d published npm packages in CodeArtifact "
                "(skipped %d upstream-cached packages)",
                len(packages), upstream_skipped)
    return packages


def cleanup_garbage_packages() -> dict:
    """Remove packages with 'node_modules' in their name from CodeArtifact.

    These are artifacts from a previous lockfile parser bug that treated
    nested dependency paths like:
        node_modules/@radix-ui/react-alert-dialog/node_modules/@radix-ui/primitive
    as the package name instead of extracting just '@radix-ui/primitive'.

    Returns a summary dict with counts and package names removed.
    """
    garbage: list[dict] = []  # list of {"name": str, "namespace": str, "package": str}

    try:
        paginator = ca_client.get_paginator("list_packages")
        for page in paginator.paginate(
            domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
        ):
            for pkg in page.get("packages", []):
                pkg_name = pkg.get("package", "")
                namespace = pkg.get("namespace", "")
                if namespace:
                    full_name = f"@{namespace}/{pkg_name}"
                else:
                    full_name = pkg_name

                if "node_modules" in full_name:
                    garbage.append({
                        "full_name": full_name,
                        "namespace": namespace,
                        "package": pkg_name,
                    })
    except ClientError as exc:
        logger.error("Failed to list packages for cleanup: %s", exc)
        return {"cleaned": 0, "failed": 0, "packages": []}

    if not garbage:
        logger.info("Cleanup: no garbage packages found")
        return {"cleaned": 0, "failed": 0, "packages": []}

    logger.info("Cleanup: found %d garbage packages with 'node_modules' in name",
                len(garbage))

    cleaned = 0
    failed = 0
    cleaned_names: list[str] = []

    for pkg in garbage:
        full_name = pkg["full_name"]
        logger.info("  Cleaning up: %s", full_name)

        # First, list all versions of this garbage package
        list_kwargs = {
            "domain": DOMAIN_NAME,
            "repository": REPO_NAME,
            "format": "npm",
            "package": pkg["package"],
        }
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
                logger.warning("  Could not list versions for %s: %s", full_name, exc)
                failed += 1
                continue

        if not versions_to_delete:
            logger.debug("  %s has no versions — skipping", full_name)
            continue

        # Delete versions in batches (API supports up to 100 at a time, but
        # we'll do smaller batches to be safe)
        batch_size = 50
        all_deleted = True
        for i in range(0, len(versions_to_delete), batch_size):
            batch = versions_to_delete[i:i + batch_size]
            delete_kwargs = {
                "domain": DOMAIN_NAME,
                "repository": REPO_NAME,
                "format": "npm",
                "package": pkg["package"],
                "versions": batch,
            }
            if pkg["namespace"]:
                delete_kwargs["namespace"] = pkg["namespace"]

            try:
                result = ca_client.delete_package_versions(**delete_kwargs)
                successful = result.get("successfulVersions", {})
                failed_versions = result.get("failedVersions", {})
                if failed_versions:
                    logger.warning("  Failed to delete %d versions of %s: %s",
                                   len(failed_versions), full_name,
                                   list(failed_versions.keys())[:5])
                    all_deleted = False
                logger.debug("  Deleted %d versions of %s",
                             len(successful), full_name)
            except ClientError as exc:
                logger.warning("  Error deleting versions of %s: %s", full_name, exc)
                all_deleted = False

        if all_deleted:
            cleaned += 1
            cleaned_names.append(full_name)
            logger.info("  Removed %s (%d versions)", full_name, len(versions_to_delete))
        else:
            failed += 1

    summary = {
        "cleaned": cleaned,
        "failed": failed,
        "total_garbage_found": len(garbage),
        "packages_removed": cleaned_names,
    }
    logger.info("Cleanup complete: removed %d packages, %d failed", cleaned, failed)
    return summary


def _set_origin_controls(name: str):
    kwargs = {
        "domain": DOMAIN_NAME,
        "repository": REPO_NAME,
        "format": "npm",
        "restrictions": {"publish": "ALLOW", "upstream": "BLOCK"},
        **_parse_package_identity(name),
    }
    try:
        ca_client.put_package_origin_configuration(**kwargs)
    except ClientError:
        logger.debug("Could not set origin controls for %s (may already be set)", name)


def publish_to_codeartifact(
    name: str, version: str, tarball_bytes: bytes,
    endpoint: str, auth_token: str, tracker: ErrorTracker,
) -> bool:
    """Publish a tarball to CodeArtifact using the npm registry PUT protocol.
    Returns True on success (including 'already exists').
    Uses exponential backoff on retryable HTTP errors.

    Note: CodeArtifact's PublishPackageVersion API does NOT support npm.
    npm packages must be published via the npm registry PUT protocol.
    """
    pkg_id = f"{name}@{version}"
    _set_origin_controls(name)

    tarball_b64 = base64.b64encode(tarball_bytes).decode()
    shasum = hashlib.sha1(tarball_bytes).hexdigest()
    integrity = "sha512-" + base64.b64encode(
        hashlib.sha512(tarball_bytes).digest()).decode()

    # npm's libnpmpublish uses: `${pkg.name}-${pkg.version}.tgz`
    # for BOTH the _attachments key AND the dist.tarball filename.
    #   wrappy       → wrappy-1.0.2.tgz
    #   @types/node  → @types/node-16.11.43.tgz
    tarball_name = f"{name}-{version}.tgz"

    payload = {
        "_id": name,
        "name": name,
        "dist-tags": {"latest": version},
        "versions": {
            version: {
                "name": name,
                "version": version,
                "_id": f"{name}@{version}",
                "dist": {
                    "shasum": shasum,
                    "integrity": integrity,
                    "tarball": f"{endpoint.rstrip('/')}/{name}/-/{tarball_name}",
                },
            }
        },
        "_attachments": {
            tarball_name: {
                "content_type": "application/octet-stream",
                "data": tarball_b64,
                "length": len(tarball_bytes),
            }
        },
    }

    encoded_name = urllib.parse.quote(name, safe="@")
    put_url = f"{endpoint.rstrip('/')}/{encoded_name}"
    payload_bytes = json.dumps(payload).encode()

    logger.debug("Publishing %s — PUT %s — attachment_key=%s",
                  pkg_id, put_url, tarball_name)

    def _do_publish():
        req = urllib.request.Request(
            put_url, data=payload_bytes, method="PUT",
            headers={"Authorization": f"Bearer {auth_token}",
                     "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as resp:
            return resp.status

    try:
        status = _retry_with_backoff(_do_publish)
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
        logger.debug("Publish failed for %s — HTTP %s — %s", pkg_id, exc.code, body[:300])
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
# 5. Work item discovery
# ===================================================================
def build_work_items(
    packages: list[dict],
    tracker: ErrorTracker,
) -> tuple[list[dict], int, list[str]]:
    """Build work items for the exact versions in the lockfile.

    Each work item: {"name": str, "version": str, "tarball_url": str}

    For each unique package name, fetches metadata from Chainguard,
    checks which lockfile versions exist in CodeArtifact, and returns
    only new versions that need to be synced.

    Returns: (work_items, skipped_existing_count, not_in_chainguard_names)
    """
    work_items: list[dict] = []
    skipped_existing = 0
    not_in_chainguard: list[str] = []
    names = unique_package_names(packages)

    # Build lockfile version map
    lockfile_versions: dict[str, set[str]] = {}
    for p in packages:
        lockfile_versions.setdefault(p["name"], set()).add(p["version"])

    logger.info("Discovery — %d unique packages (lockfile versions only)", len(names))

    for name in names:
        metadata = fetch_chainguard_metadata(name, tracker)
        if metadata is None:
            not_in_chainguard.append(name)
            logger.info("  %s — not in Chainguard", name)
            continue

        cgr_versions = metadata.get("versions", {})
        if not cgr_versions:
            not_in_chainguard.append(name)
            logger.info("  %s — no versions in Chainguard metadata", name)
            continue

        # Only consider versions that are both in the lockfile AND in Chainguard
        candidates = lockfile_versions.get(name, set()) & set(cgr_versions.keys())

        # What already exists in CodeArtifact?
        existing = list_codeartifact_versions(name)
        new_versions = candidates - existing
        pkg_skipped = len(candidates) - len(new_versions)
        skipped_existing += pkg_skipped

        if not new_versions:
            logger.info("  %s — all %d version(s) already in CodeArtifact",
                        name, len(candidates))
            continue

        logger.info("  %s — %d in lockfile, %d existing, %d new to sync",
                     name, len(candidates), pkg_skipped, len(new_versions))

        for version in sorted(new_versions):
            version_meta = cgr_versions.get(version)
            if not version_meta:
                continue
            tarball_url = version_meta.get("dist", {}).get("tarball")
            if not tarball_url:
                logger.debug("  %s@%s — no tarball URL, skipping", name, version)
                continue
            work_items.append({
                "name": name, "version": version, "tarball_url": tarball_url,
            })

    logger.info("Discovery complete — %d work items, %d skipped (existing), "
                "%d packages not in Chainguard",
                len(work_items), skipped_existing, len(not_in_chainguard))
    return work_items, skipped_existing, not_in_chainguard


def build_update_work_items(
    tracker: ErrorTracker,
) -> tuple[list[dict], int, int, list[str]]:
    """Check all existing CodeArtifact packages for new versions in Chainguard.

    Scans the CodeArtifact repository for all npm packages, then for each
    package checks Chainguard for any versions not yet in CodeArtifact.

    Returns: (work_items, skipped_existing_count, total_packages, not_in_chainguard_names)
    """
    work_items: list[dict] = []
    skipped_existing = 0
    not_in_chainguard: list[str] = []

    # Get all existing packages from CodeArtifact
    existing_packages = list_codeartifact_packages()
    if not existing_packages:
        logger.info("No existing packages found in CodeArtifact — nothing to update")
        return work_items, 0, 0, []

    logger.info("Update check — scanning %d existing packages for new Chainguard versions",
                len(existing_packages))

    for name in existing_packages:
        # Get all versions from Chainguard
        metadata = fetch_chainguard_metadata(name, tracker)
        if metadata is None:
            not_in_chainguard.append(name)
            logger.debug("  %s — not in Chainguard", name)
            continue

        cgr_versions = metadata.get("versions", {})
        if not cgr_versions:
            not_in_chainguard.append(name)
            logger.debug("  %s — no versions in Chainguard metadata", name)
            continue

        # Get existing versions in CodeArtifact
        existing = list_codeartifact_versions(name)
        new_versions = set(cgr_versions.keys()) - existing
        pkg_skipped = len(cgr_versions) - len(new_versions)
        skipped_existing += pkg_skipped

        if not new_versions:
            logger.debug("  %s — up to date (%d versions)", name, len(existing))
            continue

        logger.info("  %s — %d new version(s) available in Chainguard "
                     "(%d in Chainguard, %d in CodeArtifact)",
                     name, len(new_versions), len(cgr_versions), len(existing))

        for version in sorted(new_versions):
            version_meta = cgr_versions.get(version)
            if not version_meta:
                continue
            tarball_url = version_meta.get("dist", {}).get("tarball")
            if not tarball_url:
                logger.debug("  %s@%s — no tarball URL, skipping", name, version)
                continue
            work_items.append({
                "name": name, "version": version, "tarball_url": tarball_url,
            })

    logger.info("Update discovery complete — %d new versions to sync across %d packages, "
                "%d versions already current, %d packages not in Chainguard",
                len(work_items), len(existing_packages),
                skipped_existing, len(not_in_chainguard))
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
    """Save checkpoint for either discovery or processing phase.

    phase="discovery": saves package_names + discovery_index + partial work_items
    phase="processing": saves work_items + processing_index
    """
    bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
              or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
    if not bucket:
        raise ValueError("No S3 bucket configured for checkpoints. "
                         "Set CHECKPOINT_S3_BUCKET or ERRORS_S3_BUCKET.")
    key = _checkpoint_key(run_id)
    checkpoint = {
        "run_id": run_id,
        "invocation_number": invocation_number,
        "phase": phase,
        "work_items": work_items,
        "counters": counters,
        "not_found_list": not_found_list,
        "errors": tracker.errors,
        "original_event": {
            "s3_bucket": event.get("s3_bucket"),
            "s3_key": event.get("s3_key"),
            "setup_codeartifact": False,
            "update_existing": event.get("update_existing", False),
            "cleanup_garbage": False,  # already ran on first invocation
            "max_packages": event.get("max_packages", MAX_PACKAGES),
            "errors_s3_bucket": event.get("errors_s3_bucket", ERRORS_S3_BUCKET),
            "errors_s3_prefix": event.get("errors_s3_prefix", ERRORS_S3_PREFIX),
            "checkpoint_s3_bucket": bucket,
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
    idx = discovery_index if phase == "discovery" else processing_index
    logger.info("Checkpoint [%s] -> s3://%s/%s (index=%d, invocation=%d)",
                phase, bucket, key, idx, invocation_number)
    return key


def _load_checkpoint(bucket: str, key: str) -> dict:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    cp = json.loads(obj["Body"].read())
    phase = cp.get("phase", "processing")  # backward compat
    if phase == "discovery":
        idx = cp.get("discovery_index", 0)
    else:
        idx = cp.get("next_index", 0)
    logger.info("Loaded checkpoint s3://%s/%s (phase=%s, index=%d, invocation=%d)",
                bucket, key, phase, idx, cp.get("invocation_number", 0))
    return cp


def _delete_checkpoint(bucket: str, run_id: str):
    try:
        s3_client.delete_object(Bucket=bucket, Key=_checkpoint_key(run_id))
        logger.info("Deleted checkpoint for run %s", run_id)
    except Exception as exc:
        logger.warning("Could not delete checkpoint: %s", exc)


def _checkpoint_exists(bucket: str, run_id: str) -> bool:
    """Check if a checkpoint already exists for this run_id."""
    if not bucket:
        return False
    try:
        s3_client.head_object(Bucket=bucket, Key=_checkpoint_key(run_id))
        return True
    except ClientError:
        return False


def _try_save_failure_checkpoint(
    run_id: str, bucket: str, original_event: dict,
    phase: str, work_items: list[dict],
    counters: dict, not_found_list: list[str],
    tracker: ErrorTracker, invocation_number: int,
    package_names: Optional[list] = None,
    discovery_index: int = 0,
    processing_index: int = 0,
) -> Optional[str]:
    """Save a checkpoint on failure so the run can be resumed.
    Skips if no bucket is configured or a checkpoint already exists.
    Returns the run_id if saved, None otherwise.
    """
    if not bucket:
        logger.warning("No checkpoint bucket — cannot save failure checkpoint")
        return None
    if _checkpoint_exists(bucket, run_id):
        logger.info("Checkpoint already exists for run %s — not overwriting", run_id)
        return run_id
    try:
        _save_checkpoint(
            run_id, original_event, phase, work_items,
            counters, not_found_list, tracker, invocation_number,
            package_names=package_names,
            discovery_index=discovery_index,
            processing_index=processing_index)
        logger.info("Saved failure checkpoint for run %s (phase=%s)", run_id, phase)
        return run_id
    except Exception as exc:
        logger.error("Failed to save failure checkpoint: %s", exc)
        return None


def _self_invoke(event: dict):
    logger.info("Self-invoking (run_id=%s, invocation=%d)",
                event.get("_run_id"), event.get("_invocation_number"))
    lambda_client.invoke(FunctionName=FUNCTION_NAME, InvocationType="Event",
                         Payload=json.dumps(event).encode())


def _is_time_running_out(context) -> bool:
    if context is None:
        return False
    return context.get_remaining_time_in_millis() < CHECKPOINT_BUFFER_MS


def _list_checkpoints(bucket: str) -> list[dict]:
    """List available checkpoint files in S3 with summary metadata."""
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
                except Exception as exc:
                    logger.warning("Could not read checkpoint %s: %s", key, exc)
    except Exception as exc:
        logger.error("Failed to list checkpoints: %s", exc)
    return checkpoints


# ===================================================================
# 7. Lambda handler
# ===================================================================
def lambda_handler(event, context):
    """
    Entry modes:

    1. NEW SESSION — sync lockfile versions from Chainguard to CodeArtifact:
       {"s3_bucket": "...", "s3_key": "package-lock.json"}

    2. UPDATE EXISTING — check all CodeArtifact packages for new Chainguard versions:
       {"update_existing": true}

    3. RESUME PREVIOUS SESSION:
       {"resume_run_id": "20260402T034658Z-abc12345"}

    4. LIST AVAILABLE CHECKPOINTS:
       {"list_checkpoints": true}

    5. CLEANUP ONLY — remove garbage packages with 'node_modules' in their name:
       {"cleanup_only": true}

    Options:
       "cleanup_garbage": true/false  — run cleanup before new sessions (default: true)
       "dry_run": true/false          — preview sync plan without publishing (default: false)
       "allowlist": ["@scope/*"]      — only sync packages matching these patterns
       "denylist": ["unwanted-pkg"]   — skip packages matching these patterns
       "parallel_downloads": N        — override PARALLEL_DOWNLOADS for this run
    """
    # ------------------------------------------------------------------
    # Mode: list checkpoints
    # ------------------------------------------------------------------
    if event.get("list_checkpoints"):
        bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
                  or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
        checkpoints = _list_checkpoints(bucket)
        return _success(
            f"Found {len(checkpoints)} checkpoint(s)",
            {"checkpoints": checkpoints, "status": "listing"})

    # ------------------------------------------------------------------
    # Mode: cleanup only
    # ------------------------------------------------------------------
    if event.get("cleanup_only"):
        summary = cleanup_garbage_packages()
        return _success("Cleanup complete", {**summary, "status": "cleanup_complete"})

    if not CGR_USERNAME or not CGR_TOKEN:
        return _error("CGR_USERNAME and CGR_TOKEN environment variables are required")

    checkpoint_bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
                         or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
    is_update_existing = event.get("update_existing", False)
    do_cleanup = event.get("cleanup_garbage", True)
    dry_run = event.get("dry_run", False)
    allowlist = event.get("allowlist", ENV_ALLOWLIST) or []
    denylist = event.get("denylist", ENV_DENYLIST) or []
    parallel = int(event.get("parallel_downloads", PARALLEL_DOWNLOADS))

    if dry_run:
        logger.info("DRY RUN mode — no packages will be published")

    # ------------------------------------------------------------------
    # Determine entry mode
    # ------------------------------------------------------------------
    is_auto_continuation = "_checkpoint_key" in event
    is_manual_resume = "resume_run_id" in event and not is_auto_continuation
    is_new_session = not is_auto_continuation and not is_manual_resume
    max_packages = int(event.get("max_packages", MAX_PACKAGES))

    if is_auto_continuation:
        run_id = event["_run_id"]
        invocation_number = event["_invocation_number"]
    elif is_manual_resume:
        run_id = event["resume_run_id"]
        invocation_number = event.get("_invocation_number", 1)
    else:
        run_id = _generate_run_id()
        invocation_number = 1

    # ------------------------------------------------------------------
    # Load state from checkpoint (auto-continuation or manual resume)
    # ------------------------------------------------------------------
    if is_auto_continuation or is_manual_resume:
        if is_auto_continuation:
            ckpt_key = event["_checkpoint_key"]
        else:
            ckpt_key = _checkpoint_key(run_id)

        try:
            cp = _load_checkpoint(checkpoint_bucket, ckpt_key)
        except Exception as exc:
            return _error(f"Could not load checkpoint for run '{run_id}': {exc}")

        phase = cp.get("phase", "processing")
        work_items = cp.get("work_items", [])
        counters = cp["counters"]
        not_found_list = cp["not_found_list"]
        tracker = ErrorTracker(errors=cp.get("errors", []))
        original_event = cp["original_event"]

        if phase == "discovery":
            package_names = cp.get("package_names", [])
            discovery_index = cp.get("discovery_index", 0)
            processing_index = 0
        else:
            package_names = []
            discovery_index = 0
            processing_index = cp.get("next_index", 0)

        mode_label = "auto-continuation" if is_auto_continuation else "manual resume"
        logger.info("Resuming run %s (%s) — phase=%s, invocation #%d",
                     run_id, mode_label, phase, invocation_number)

    # ------------------------------------------------------------------
    # New session: build package name list for discovery
    # ------------------------------------------------------------------
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

        # Run garbage cleanup (default: enabled)
        if do_cleanup:
            cleanup_result = cleanup_garbage_packages()
            if cleanup_result["cleaned"] > 0:
                logger.info("Pre-run cleanup removed %d garbage packages",
                             cleanup_result["cleaned"])

        if is_update_existing:
            # Update mode: get package names from CodeArtifact
            package_names = list_codeartifact_packages()
            logger.info("New session %s — update mode, %d existing packages to check",
                         run_id, len(package_names))
        else:
            # Lockfile mode: get package names from lockfile
            if "lockfile_content" in event:
                lockfile = json.loads(event["lockfile_content"])
            elif "s3_bucket" in event and "s3_key" in event:
                obj = s3_client.get_object(Bucket=event["s3_bucket"], Key=event["s3_key"])
                lockfile = json.loads(obj["Body"].read())
            else:
                return _error("Provide 'lockfile_content', 's3_bucket'+'s3_key', "
                              "'update_existing', 'resume_run_id', or 'list_checkpoints'")

            packages = parse_lockfile(lockfile)
            if not packages:
                return _success("No packages found in lockfile", _empty_summary())

            package_names = unique_package_names(packages)
            logger.info("New session %s — %d unique packages from lockfile",
                         run_id, len(package_names))

        if not package_names:
            return _success("No packages to process", _empty_summary())

        # Apply allowlist / denylist filtering
        if allowlist or denylist:
            package_names = filter_packages(package_names, allowlist, denylist)
            if not package_names:
                return _success("No packages remaining after allow/denylist filtering",
                                _empty_summary())

        # Enforce max_packages limit
        if len(package_names) > max_packages:
            logger.warning("Package list (%d) exceeds max_packages (%d) — truncating",
                           len(package_names), max_packages)
            package_names = package_names[:max_packages]

    # ------------------------------------------------------------------
    # Safety cap
    # ------------------------------------------------------------------
    if invocation_number > MAX_INVOCATIONS:
        logger.error("MAX_INVOCATIONS=%d reached for run %s", MAX_INVOCATIONS, run_id)
        _flush_errors(tracker, original_event)
        msg = (f"Exceeded max invocations ({MAX_INVOCATIONS}). "
               f"Resume with: {{\"resume_run_id\": \"{run_id}\"}}")
        _send_notification(f"Sync STOPPED — max invocations ({MAX_INVOCATIONS})",
                           msg, status="error")
        return _error(msg)

    # ==================================================================
    # PHASE 1: DISCOVERY — checkpointable
    # Iterate package names, fetch metadata, diff versions, build work_items
    # ==================================================================
    if phase == "discovery":
        # Build lockfile version map for lockfile mode
        lockfile_versions: dict[str, set[str]] = {}
        if not is_update_existing and not (is_auto_continuation or is_manual_resume):
            for p in packages:
                lockfile_versions.setdefault(p["name"], set()).add(p["version"])
        elif (is_auto_continuation or is_manual_resume):
            # In lockfile mode continuations, we don't have `packages` but
            # we don't need lockfile_versions because update_existing mode
            # uses all Chainguard versions. For lockfile mode continuations,
            # we re-parse the lockfile if available.
            oe = original_event
            if not oe.get("update_existing", False):
                if oe.get("s3_bucket") and oe.get("s3_key"):
                    try:
                        obj = s3_client.get_object(Bucket=oe["s3_bucket"], Key=oe["s3_key"])
                        lf = json.loads(obj["Body"].read())
                        for p in parse_lockfile(lf):
                            lockfile_versions.setdefault(p["name"], set()).add(p["version"])
                    except Exception:
                        logger.warning("Could not re-parse lockfile for version filtering")

        is_update = is_update_existing or original_event.get("update_existing", False)

        logger.info("Discovery phase — starting at %d/%d",
                     discovery_index, len(package_names))

        try:
            for i in range(discovery_index, len(package_names)):
                if _is_time_running_out(context):
                    logger.warning("Low on time during discovery at %d/%d — checkpointing",
                                   i, len(package_names))
                    _flush_errors(tracker, original_event)
                    ckpt_key = _save_checkpoint(
                        run_id, original_event, "discovery", work_items,
                        counters, not_found_list, tracker, invocation_number,
                        package_names=package_names, discovery_index=i)
                    _self_invoke({
                        **original_event,
                        "_run_id": run_id,
                        "_checkpoint_key": ckpt_key,
                        "_invocation_number": invocation_number + 1,
                        "checkpoint_s3_bucket": checkpoint_bucket,
                    })
                    return _success(
                        f"Discovery checkpointed at package {i}/{len(package_names)} — "
                        f"continuation #{invocation_number + 1} launched",
                        {"run_id": run_id, "invocation_number": invocation_number,
                         "phase": "discovery",
                         "discovery_progress": f"{i}/{len(package_names)}",
                         "work_items_so_far": len(work_items),
                         "status": "continuing", **counters})

                name = package_names[i]
                metadata = fetch_chainguard_metadata(name, tracker)
                if metadata is None:
                    not_found_list.append(name)
                    continue

                cgr_versions = metadata.get("versions", {})
                if not cgr_versions:
                    not_found_list.append(name)
                    continue

                # Determine candidate versions
                if is_update:
                    candidates = set(cgr_versions.keys())
                else:
                    candidates = lockfile_versions.get(name, set()) & set(cgr_versions.keys())

                existing = list_codeartifact_versions(name)
                new_versions = candidates - existing
                counters["skipped_existing"] += len(candidates) - len(new_versions)

                if not new_versions:
                    continue

                logger.info("[%d/%d] %s — %d new version(s)",
                             i + 1, len(package_names), name, len(new_versions))

                for version in sorted(new_versions):
                    version_meta = cgr_versions.get(version)
                    if not version_meta:
                        continue
                    tarball_url = version_meta.get("dist", {}).get("tarball")
                    if tarball_url:
                        work_items.append({
                            "name": name, "version": version,
                            "tarball_url": tarball_url,
                        })

                discovery_index = i + 1

        except Exception as exc:
            logger.error("Unexpected error during discovery at %d/%d: %s",
                          discovery_index, len(package_names), exc, exc_info=True)
            _flush_errors(tracker, original_event)
            saved = _try_save_failure_checkpoint(
                run_id, checkpoint_bucket, original_event,
                "discovery", work_items, counters, not_found_list,
                tracker, invocation_number,
                package_names=package_names, discovery_index=discovery_index)
            msg = (f"Unexpected error during discovery at {discovery_index}/"
                   f"{len(package_names)}: {type(exc).__name__}: {str(exc)[:200]}")
            if saved:
                msg += f'. Resume with: {{"resume_run_id": "{saved}"}}'
            _send_notification(f"Sync FAILED during discovery — {type(exc).__name__}",
                               msg, status="error")
            return _error(msg)

        logger.info("Discovery complete — %d work items from %d packages",
                     len(work_items), len(package_names))

        if not work_items:
            result = _success(
                "No new versions to sync — all up to date or not in Chainguard",
                {**_empty_summary(), "run_id": run_id,
                 "total_packages_checked": len(package_names),
                 "skipped_existing": counters["skipped_existing"],
                 "not_found_packages": not_found_list})
            _put_metrics(counters, run_id)
            _send_notification("Sync complete — already up to date",
                               json.dumps(result["body"], indent=2),
                               status="complete")
            return result

        # ---- DRY RUN: return the plan without publishing ----
        if dry_run:
            dry_summary = {
                "run_id": run_id,
                "dry_run": True,
                "total_work_items": len(work_items),
                "skipped_existing": counters["skipped_existing"],
                "not_found_packages": not_found_list,
                "packages_to_sync": [
                    f'{w["name"]}@{w["version"]}' for w in work_items
                ],
                "status": "dry_run_complete",
            }
            logger.info("Dry run complete — %d items would be synced",
                         len(work_items))
            _send_notification(
                f"Dry run complete — {len(work_items)} packages would sync",
                json.dumps(dry_summary, indent=2), status="info")
            return _success("Dry run complete — no changes made", dry_summary)

        # Transition to processing phase
        phase = "processing"
        processing_index = 0

    # ==================================================================
    # PHASE 2: PROCESSING — download tarballs and publish
    # Supports parallel tarball downloads via ThreadPoolExecutor
    # ==================================================================
    endpoint = ca_client.get_repository_endpoint(
        domain=DOMAIN_NAME, repository=REPO_NAME, format="npm",
    )["repositoryEndpoint"]
    auth_token = _get_codeartifact_auth_token()

    current_index = processing_index
    timed_out = False

    logger.info("Processing phase — %d work items starting at %d (parallel=%d)",
                 len(work_items), processing_index, parallel)

    try:
        i = processing_index
        while i < len(work_items):
            if _is_time_running_out(context):
                logger.warning("Low on time at item %d/%d — checkpointing",
                               i, len(work_items))
                timed_out = True
                current_index = i
                break

            # Build a batch for parallel download
            batch_end = min(i + parallel, len(work_items))
            batch = work_items[i:batch_end]

            # Check time before committing to a batch
            if len(batch) > 1 and _is_time_running_out(context):
                timed_out = True
                current_index = i
                break

            # Parallel download tarballs
            download_results: list[tuple[dict, Optional[bytes]]] = []

            if parallel > 1 and len(batch) > 1:
                with ThreadPoolExecutor(max_workers=min(parallel, len(batch))) as pool:
                    futures = {
                        pool.submit(
                            download_tarball, item["tarball_url"],
                            item["name"], item["version"], tracker
                        ): item
                        for item in batch
                    }
                    for future in as_completed(futures):
                        item = futures[future]
                        try:
                            tarball = future.result()
                        except Exception as exc:
                            logger.error("Parallel download exception for %s@%s: %s",
                                         item["name"], item["version"], exc)
                            tarball = None
                        download_results.append((item, tarball))
            else:
                # Sequential fallback for single items or parallel=1
                for item in batch:
                    tarball = download_tarball(
                        item["tarball_url"], item["name"],
                        item["version"], tracker)
                    download_results.append((item, tarball))

            # Publish sequentially (CodeArtifact doesn't handle parallel PUTs well)
            for item, tarball in download_results:
                name, version = item["name"], item["version"]
                logger.info("[%d/%d] %s@%s",
                            i + 1, len(work_items), name, version)

                if tarball is None:
                    counters["not_found"] += 1
                    not_found_list.append(f"{name}@{version}")
                    logger.info("  Tarball download failed")
                else:
                    counters["found"] += 1
                    if not publish_to_codeartifact(name, version, tarball,
                                                   endpoint, auth_token, tracker):
                        counters["failed"] += 1
                i += 1

            current_index = i

    except Exception as exc:
        logger.error("Unexpected error at item %d/%d: %s",
                      current_index, len(work_items), exc, exc_info=True)
        _flush_errors(tracker, original_event)
        saved = _try_save_failure_checkpoint(
            run_id, checkpoint_bucket, original_event,
            "processing", work_items, counters, not_found_list,
            tracker, invocation_number, processing_index=current_index)
        msg = (f"Unexpected error at item {current_index}/{len(work_items)}: "
               f"{type(exc).__name__}: {str(exc)[:200]}")
        if saved:
            msg += f'. Resume with: {{"resume_run_id": "{saved}"}}'
        _put_metrics({**counters, "http_errors": tracker.count}, run_id)
        _send_notification(f"Sync FAILED — {type(exc).__name__}", msg,
                           status="error")
        return _error(msg)

    # ------------------------------------------------------------------
    # Checkpoint if timed out — auto-continue
    # ------------------------------------------------------------------
    if timed_out and current_index < len(work_items):
        if not checkpoint_bucket:
            _flush_errors(tracker, original_event)
            return _error(f"Timed out at {current_index}/{len(work_items)} "
                          f"with no checkpoint bucket configured.")
        _flush_errors(tracker, original_event)
        ckpt_key = _save_checkpoint(
            run_id, original_event, "processing", work_items,
            counters, not_found_list, tracker, invocation_number,
            processing_index=current_index)
        _self_invoke({
            **original_event,
            "_run_id": run_id,
            "_checkpoint_key": ckpt_key,
            "_invocation_number": invocation_number + 1,
            "checkpoint_s3_bucket": checkpoint_bucket,
        })
        return _success(
            f"Checkpointed at {current_index}/{len(work_items)} — "
            f"continuation #{invocation_number + 1} launched",
            {"run_id": run_id, "invocation_number": invocation_number,
             "phase": "processing",
             "processed_so_far": current_index,
             "total_work_items": len(work_items),
             "status": "continuing", **counters,
             "http_errors": tracker.count})

    # ------------------------------------------------------------------
    # All done — finalize
    # ------------------------------------------------------------------
    if (is_auto_continuation or is_manual_resume) and checkpoint_bucket:
        _delete_checkpoint(checkpoint_bucket, run_id)

    errors_s3_uri = _flush_errors(tracker, original_event)

    summary = {
        "run_id": run_id,
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
    logger.info("Sync complete — %s", json.dumps(summary, indent=2))

    # Emit CloudWatch metrics
    _put_metrics({**counters, "http_errors": tracker.count}, run_id,
                 extra={"TotalWorkItems": len(work_items)})

    # Send SNS notification
    has_errors = tracker.count > 0 or counters["failed"] > 0
    notif_status = "error" if has_errors else "complete"
    notif_subject = (f"Sync complete — {counters['found']} synced"
                     f"{', ' + str(tracker.count) + ' errors' if has_errors else ''}")
    _send_notification(notif_subject, json.dumps(summary, indent=2),
                       status=notif_status)

    return _success("Sync complete", summary)


# ===================================================================
# Helpers
# ===================================================================
def _flush_errors(tracker: ErrorTracker, event: dict) -> Optional[str]:
    """Upload errors.txt to S3 if there are any errors.
    Called at EVERY exit point — not just clean completion.
    Returns the S3 URI or None.
    """
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


def _success(message: str, summary: dict):
    return {"statusCode": 200, "body": {"message": message, "summary": summary}}


def _error(message: str):
    logger.error(message)
    return {"statusCode": 400, "body": {"error": message}}
