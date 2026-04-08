"""
AWS Lambda: Sync Chainguard Python packages to AWS CodeArtifact
(Requirements-file pinned versions)

Syncs exact package versions from a requirements.txt to CodeArtifact.
Supports three Chainguard registry families:

  Registry             URL
  ──────────────────── ────────────────────────────────────────────
  standard             https://libraries.cgr.dev/python/simple/
  remediated           https://libraries.cgr.dev/python-remediated/simple/
  cu126 (CUDA 12.6)   https://libraries.cgr.dev/cu126/simple/
  cu128 (CUDA 12.8)   https://libraries.cgr.dev/cu128/simple/
  cu129 (CUDA 12.9)   https://libraries.cgr.dev/cu129/simple/

Registry selection per package:
  • Version string contains "+cu126"  → cu126 registry
  • Version string contains "+cu128"  → cu128 registry
  • Version string contains "+cu129"  → cu129 registry
  • All others                        → remediated first, then standard

Each package version may have many distribution files (wheels per platform +
sdist). By default all files are synced. Use WHEEL_PYTHON_TAGS /
WHEEL_PLATFORM_TAGS / SYNC_SDIST to restrict what gets downloaded.

Trigger:
    {"s3_bucket": "my-bucket", "s3_key": "path/to/requirements.txt"}

Entry modes:
    1. Sync lockfile (requirements.txt) — new session
       {"s3_bucket": "...", "s3_key": "requirements.txt"}

    2. Sync all Chainguard versions for every existing CodeArtifact package
       {"update_existing": true}

    3. Resume a timed-out / failed run
       {"resume_run_id": "20260402T034658Z-abc12345"}

    4. List available checkpoints
       {"list_checkpoints": true}

    5. Cleanup only — remove packages with invalid PyPI names
       {"cleanup_only": true}

    6. Dry-run — preview what would sync without writing anything
       {"s3_bucket": "...", "s3_key": "requirements.txt", "dry_run": true}

Options (all optional):
    "setup_codeartifact": true   — create domain/repo if they don't exist
    "cleanup_garbage":    false  — skip pre-run garbage cleanup (default: true)
    "dry_run":            true   — log what would happen, no writes
    "allowlist":          ["numpy", "torch"]   — only these packages
    "denylist":           ["legacy-pkg"]       — skip these packages

Environment variables:
    CGR_USERNAME            — Chainguard registry username  (required)
    CGR_TOKEN               — Chainguard registry token     (required)
    AWS_REGION              — AWS region           (default: us-east-2)
    CODEARTIFACT_DOMAIN     — domain name          (default: my-pypi-domain)
    CODEARTIFACT_REPO       — repo name            (default: my-pypi-repo)
    CODEARTIFACT_UPSTREAM   — upstream repo name   (default: pypi-public)

    # Registry URLs (overridable)
    CGR_STANDARD_REGISTRY   — (default: https://libraries.cgr.dev/python/simple/)
    CGR_REMEDIATED_REGISTRY — (default: https://libraries.cgr.dev/python-remediated/simple/)
    CGR_CUDA_126_REGISTRY   — (default: https://libraries.cgr.dev/cu126/simple/)
    CGR_CUDA_128_REGISTRY   — (default: https://libraries.cgr.dev/cu128/simple/)
    CGR_CUDA_129_REGISTRY   — (default: https://libraries.cgr.dev/cu129/simple/)

    # File filtering
    WHEEL_PYTHON_TAGS       — comma-separated python tags to sync, e.g. cp311,cp312,py3
                              (default: "" = sync all)
    WHEEL_PLATFORM_TAGS     — comma-separated platform tags, e.g. linux_x86_64,none-any
                              (default: "" = sync all)
    SYNC_SDIST              — "true"/"false" — sync .tar.gz sdists (default: true)
    SYNC_WHEELS             — "true"/"false" — sync .whl wheels    (default: true)

    # S3 / checkpointing
    ERRORS_S3_BUCKET        — S3 bucket for errors.txt output
    ERRORS_S3_PREFIX        — S3 key prefix for errors.txt   (default: chainguard-sync/)
    CHECKPOINT_S3_BUCKET    — S3 bucket for checkpoint state (falls back to ERRORS_S3_BUCKET)
    CHECKPOINT_S3_PREFIX    — S3 key prefix for checkpoints  (default: chainguard-sync/checkpoints/)
    CHECKPOINT_BUFFER_MS    — ms before timeout to save checkpoint (default: 120000)
    MAX_INVOCATIONS         — safety cap on re-invocations   (default: 50)
    MAX_PACKAGES            — max packages per run           (default: 50000)

    # Retry / backoff
    RETRY_MAX_ATTEMPTS      — max HTTP retry attempts        (default: 3)
    RETRY_BASE_DELAY_S      — base backoff delay in seconds  (default: 1.0)

    # Parallel downloads
    MAX_WORKERS             — thread pool size               (default: 10)

    # Filtering
    PACKAGE_ALLOWLIST       — comma-separated names OR s3://bucket/key (default: "")
    PACKAGE_DENYLIST        — comma-separated names OR s3://bucket/key (default: "")

    # Notifications
    SNS_TOPIC_ARN           — SNS topic for completion/failure alerts (default: "")

    # Metrics
    CLOUDWATCH_NAMESPACE    — CloudWatch metrics namespace   (default: ChainguardSync)
    LOG_LEVEL               — DEBUG / INFO / WARN            (default: DEBUG)
"""

import hashlib
import http.client
import io
import json
import logging
import os
import random
import re
import ssl
import threading
import time
import urllib.parse
import urllib.request
import urllib.error
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import boto3
from botocore.exceptions import ClientError


# ===========================================================================
# Configuration
# ===========================================================================
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
DOMAIN_NAME = os.environ.get("CODEARTIFACT_DOMAIN", "my-pypi-domain")
REPO_NAME = os.environ.get("CODEARTIFACT_REPO", "my-pypi-repo")
UPSTREAM_REPO_NAME = os.environ.get("CODEARTIFACT_UPSTREAM", "pypi-public")
# Optional: required for cross-account CodeArtifact access.
# When set, passed as domainOwner to get_authorization_token and
# get_repository_endpoint so the token is scoped to the correct account.
DOMAIN_OWNER = os.environ.get("CODEARTIFACT_DOMAIN_OWNER", "")

CGR_USERNAME = os.environ.get("CGR_USERNAME", "")
CGR_TOKEN = os.environ.get("CGR_TOKEN", "")

# Registry base URLs — all must end with /simple/ for PEP 503 compliance
REGISTRY_STANDARD = os.environ.get(
    "CGR_STANDARD_REGISTRY", "https://libraries.cgr.dev/python/simple/")
REGISTRY_REMEDIATED = os.environ.get(
    "CGR_REMEDIATED_REGISTRY", "https://libraries.cgr.dev/python-remediated/simple/")
REGISTRY_CUDA = {
    "cu126": os.environ.get("CGR_CUDA_126_REGISTRY", "https://libraries.cgr.dev/cu126/simple/"),
    "cu128": os.environ.get("CGR_CUDA_128_REGISTRY", "https://libraries.cgr.dev/cu128/simple/"),
    "cu129": os.environ.get("CGR_CUDA_129_REGISTRY", "https://libraries.cgr.dev/cu129/simple/"),
}
# Ordered list of all non-CUDA registries used in discovery
_STANDARD_REGISTRIES = [
    ("remediated", REGISTRY_REMEDIATED),
    ("standard", REGISTRY_STANDARD),
]

# Distribution file filters
_WHEEL_PYTHON_TAGS: set[str] = {
    t.strip().lower()
    for t in os.environ.get("WHEEL_PYTHON_TAGS", "").split(",")
    if t.strip()
}
_WHEEL_PLATFORM_TAGS: set[str] = {
    t.strip().lower()
    for t in os.environ.get("WHEEL_PLATFORM_TAGS", "").split(",")
    if t.strip()
}
_SYNC_SDIST = os.environ.get("SYNC_SDIST", "true").lower() != "false"
_SYNC_WHEELS = os.environ.get("SYNC_WHEELS", "true").lower() != "false"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.DEBUG))
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

ERRORS_S3_BUCKET = os.environ.get("ERRORS_S3_BUCKET", "")
ERRORS_S3_PREFIX = os.environ.get("ERRORS_S3_PREFIX", "chainguard-sync/")
CHECKPOINT_S3_BUCKET = os.environ.get("CHECKPOINT_S3_BUCKET", "")
CHECKPOINT_S3_PREFIX = os.environ.get(
    "CHECKPOINT_S3_PREFIX", "chainguard-sync/checkpoints/")
CHECKPOINT_BUFFER_MS = int(os.environ.get("CHECKPOINT_BUFFER_MS", "120000"))
MAX_INVOCATIONS = int(os.environ.get("MAX_INVOCATIONS", "50"))
MAX_PACKAGES = int(os.environ.get("MAX_PACKAGES", "50000"))
FUNCTION_NAME = os.environ.get(
    "AWS_LAMBDA_FUNCTION_NAME", "chainguard-pypi-codeartifact-sync")

RETRY_MAX_ATTEMPTS = int(os.environ.get("RETRY_MAX_ATTEMPTS", "3"))
RETRY_BASE_DELAY_S = float(os.environ.get("RETRY_BASE_DELAY_S", "1.0"))
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "10"))

PACKAGE_ALLOWLIST = os.environ.get("PACKAGE_ALLOWLIST", "")
PACKAGE_DENYLIST = os.environ.get("PACKAGE_DENYLIST", "")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "")
CLOUDWATCH_NAMESPACE = os.environ.get("CLOUDWATCH_NAMESPACE", "ChainguardSync")

# Boto3 clients (reused across warm invocations)
ca_client = boto3.client("codeartifact", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)
sts_client = boto3.client("sts", region_name=AWS_REGION)
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
sns_client = boto3.client("sns", region_name=AWS_REGION)
cw_client = boto3.client("cloudwatch", region_name=AWS_REGION)

# Log the Lambda execution identity at cold-start so operators can identify
# exactly which IAM role the function runs as.  This is DIFFERENT from the
# IAM user/role used to invoke or manage Lambda — it is the role listed under
# Lambda → Configuration → Permissions → Execution role.
# That role is what needs codeartifact:PublishPackageVersion.
try:
    _identity = sts_client.get_caller_identity()
    logger.info(
        "Lambda execution identity — Account=%s  Arn=%s  UserId=%s\n"
        "  ↳ This role needs: codeartifact:PublishPackageVersion\n"
        "  ↳ Check: AWS console → IAM → Roles → [role above] → Permissions",
        _identity["Account"], _identity["Arn"], _identity["UserId"],
    )
except Exception as _e:
    logger.warning("Could not retrieve caller identity: %s", _e)


# ===========================================================================
# 1. Error tracking
# ===========================================================================
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
            "# Chainguard -> CodeArtifact Python sync errors",
            f"# Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
            f"# Total errors: {self.count}", "",
        ]
        for e in self.errors:
            code = e["status_code"] or "N/A"
            lines.append(
                f'{e["package"]}  |  phase={e["phase"]}  |  '
                f'HTTP {code}  |  {e["error_type"]}  |  {e["detail"]}'
            )
        return "\n".join(lines) + "\n"

    def upload_to_s3(self, bucket: str, prefix: str) -> Optional[str]:
        if not bucket:
            logger.warning("No S3 bucket configured for error upload — printing to log")
            for line in self.to_text().splitlines():
                logger.error("ERRORS: %s", line)
            return None
        from datetime import datetime, timezone
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{prefix.rstrip('/')}/errors-{ts}.txt"
        try:
            s3_client.put_object(
                Bucket=bucket, Key=key,
                Body=self.to_text().encode(),
                ContentType="text/plain",
            )
            uri = f"s3://{bucket}/{key}"
            logger.info("Errors written to %s", uri)
            return uri
        except Exception as exc:
            logger.error("Could not upload errors to S3: %s", exc)
            return None


# ===========================================================================
# 2. HTTP error handling
# ===========================================================================
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


# ===========================================================================
# 3. Retry with exponential backoff
# ===========================================================================
_RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}


def _with_retry(fn, label: str = "", max_attempts: int = None,
                base_delay: float = None):
    """Call fn(), retrying on transient network/server errors.

    Retries on HTTP 429/5xx and network exceptions.
    Does NOT retry 4xx (except 429) — those are permanent failures.
    """
    max_attempts = max_attempts if max_attempts is not None else RETRY_MAX_ATTEMPTS
    base_delay = base_delay if base_delay is not None else RETRY_BASE_DELAY_S
    last_exc: Exception = RuntimeError("No attempts made")

    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except urllib.error.HTTPError as exc:
            if exc.code in _RETRYABLE_HTTP_CODES:
                last_exc = exc
            else:
                raise
        except (urllib.error.URLError, TimeoutError, OSError, ConnectionError) as exc:
            last_exc = exc

        if attempt < max_attempts:
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            logger.warning("Retry %d/%d for %s (%s: %s) — waiting %.1fs",
                           attempt, max_attempts, label,
                           type(last_exc).__name__, str(last_exc)[:120], delay)
            time.sleep(delay)
        else:
            logger.warning("All %d attempts failed for %s", max_attempts, label)

    raise last_exc


# ===========================================================================
# 4. Package allow/deny filtering
# ===========================================================================
def _load_filter_set(raw: str) -> set[str]:
    """Load package names from a comma-separated string or S3 object.

    S3 format:  s3://bucket/key  (one normalized name per line, # = comment)
    """
    if not raw:
        return set()
    raw = raw.strip()
    if raw.startswith("s3://"):
        parts = raw[5:].split("/", 1)
        bucket, key = parts[0], parts[1] if len(parts) > 1 else ""
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            content = obj["Body"].read().decode()
            return {
                _normalize_name(line.strip())
                for line in content.splitlines()
                if line.strip() and not line.strip().startswith("#")
            }
        except Exception as exc:
            logger.error("Failed to load filter list from %s: %s", raw, exc)
            return set()
    return {_normalize_name(n.strip()) for n in raw.split(",") if n.strip()}


def _apply_package_filters(names: list[str],
                            allowlist: set[str],
                            denylist: set[str]) -> list[str]:
    """Return names after applying allowlist then denylist.

    Names are compared in normalized form (PEP 503).
    """
    before = len(names)
    if allowlist:
        names = [n for n in names if _normalize_name(n) in allowlist]
        logger.info("Allowlist: %d → %d packages", before, len(names))
    if denylist:
        before = len(names)
        names = [n for n in names if _normalize_name(n) not in denylist]
        logger.info("Denylist:  %d → %d packages", before, len(names))
    return names


# ===========================================================================
# 5. Requirements.txt parsing
# ===========================================================================
# PEP 503 name normalization
_NORMALIZE_RE = re.compile(r"[-_.]+")

def _normalize_name(name: str) -> str:
    """PEP 503: lowercase, collapse runs of [-_.] to a single hyphen."""
    return _NORMALIZE_RE.sub("-", name).lower()


# Strip extras like package[security] → package
_EXTRAS_RE = re.compile(r"\[.*?\]")

# Strip environment markers: ; python_version >= "3.9"
_MARKER_RE = re.compile(r"\s*;.*$")

# Detect VCS and URL dependencies
_VCS_RE = re.compile(r"^(git|svn|hg|bzr)\+", re.IGNORECASE)
_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

# Supported version pinning operators — we only process == pins
_PIN_RE = re.compile(r"^([A-Za-z0-9][\w.\-]*)\s*==\s*([^\s;,]+)")


def parse_requirements(content: str) -> list[dict]:
    """Parse a requirements.txt and return pinned packages.

    Only ``==`` pinned entries are returned. Unpinned, VCS, URL, and
    path dependencies are logged and skipped.

    Returns list of {"name": str, "normalized_name": str, "version": str}.
    """
    results: list[dict] = []
    seen: set[str] = set()

    for raw_line in content.splitlines():
        line = raw_line.strip()

        # Skip blank lines, comments, and option flags (-r, -i, -c, --...)
        if not line or line.startswith("#") or line.startswith("-"):
            continue

        # Skip VCS / URL dependencies
        if _VCS_RE.match(line) or _URL_RE.match(line):
            logger.debug("Skipping VCS/URL dep: %s", line[:80])
            continue

        # Strip markers and extras before matching
        line = _MARKER_RE.sub("", line)
        line = _EXTRAS_RE.sub("", line)

        m = _PIN_RE.match(line)
        if not m:
            logger.debug("Skipping unpinned/unsupported: %s", raw_line.strip()[:80])
            continue

        name, version = m.group(1), m.group(2)
        normalized = _normalize_name(name)
        key = f"{normalized}=={version}"

        if key in seen:
            continue
        seen.add(key)
        results.append({"name": name, "normalized_name": normalized, "version": version})

    logger.info("Parsed %d pinned packages from requirements.txt", len(results))
    return results


def unique_package_names(packages: list[dict]) -> list[str]:
    """Return unique normalized package names preserving first-seen order."""
    seen: set[str] = set()
    names: list[str] = []
    for p in packages:
        n = p["normalized_name"]
        if n not in seen:
            seen.add(n)
            names.append(n)
    return names


# ===========================================================================
# 6. Chainguard registry helpers — PyPI Simple API
# ===========================================================================
import base64  # used by _cgr_auth_header below


def _cgr_auth_header() -> str:
    credentials = f"{CGR_USERNAME}:{CGR_TOKEN}"
    return f"Basic {base64.b64encode(credentials.encode()).decode()}"


class _StripAuthOnRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Strip the Authorization header only when a redirect leaves the CGR domain.

    Chainguard file downloads redirect internally:
      libraries.cgr.dev/python/files/...
        → libraries.cgr.dev/artifacts-downloads/...   ← same host, auth required
        → s3.amazonaws.com/... (hypothetical CDN)     ← different host, auth wrong

    Stripping auth on every redirect breaks the first pattern; keeping auth on
    every redirect would break the second.  The safe rule is:

      • Redirect stays on libraries.cgr.dev  → keep Authorization
      • Redirect leaves to a different host  → strip Authorization

    CGR credentials are useless (and harmful to S3 presigned URLs) outside of
    the Chainguard domain, so stripping on domain-crossing is always correct.
    """

    _CGR_HOST = "libraries.cgr.dev"

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is None:
            return new_req

        original_host = urllib.parse.urlparse(req.full_url).netloc.lower()
        new_host = urllib.parse.urlparse(newurl).netloc.lower()

        if original_host == self._CGR_HOST and new_host != self._CGR_HOST:
            # Leaving the Chainguard domain — strip auth so presigned CDN/S3
            # URLs are not poisoned by our CGR Basic/Bearer credentials.
            new_req.remove_header("Authorization")
            new_req.remove_header("authorization")
            logger.debug("Redirect %s -> %s (Authorization stripped, leaving CGR domain)",
                         req.full_url, newurl)
        else:
            # Staying on the same host or moving between CGR hosts — keep auth.
            logger.debug("Redirect %s -> %s (Authorization retained)",
                         req.full_url, newurl)

        return new_req


# Shared opener used for all Chainguard registry HTTP calls.
# Using a module-level opener ensures the redirect handler is always active.
_cgr_opener = urllib.request.build_opener(_StripAuthOnRedirectHandler)


# Accept: JSON first (PEP 691), fall back to HTML (PEP 503)
_SIMPLE_JSON_MIME = "application/vnd.pypi.simple.v1+json"
_SIMPLE_HTML_MIME = "text/html"

_DIST_EXTENSIONS = (".whl", ".tar.gz", ".zip", ".egg")


def _determine_registries(version: str) -> list[tuple[str, str]]:
    """Return ordered list of (registry_key, registry_url) to try for a version.

    CUDA versions route exclusively to their CUDA registry.
    All other versions try remediated → standard.
    """
    v_lower = version.lower()
    for cuda_key in ("cu126", "cu128", "cu129"):
        if f"+{cuda_key}" in v_lower:
            return [(cuda_key, REGISTRY_CUDA[cuda_key])]
    return list(_STANDARD_REGISTRIES)


def _parse_simple_index_json(data: dict, base_url: str) -> list[dict]:
    """Parse a PEP 691 JSON simple index response into a list of file dicts."""
    files = []
    for f in data.get("files", []):
        filename = urllib.parse.unquote(f.get("filename", ""))
        url = f.get("url", "")
        # Resolve relative URLs against the index base
        if url and not url.startswith("http"):
            url = urllib.parse.urljoin(base_url, url)
        sha256 = f.get("hashes", {}).get("sha256", "")
        requires_python = f.get("requires-python", "")
        if filename and url:
            files.append({
                "filename": filename,
                "url": url,
                "sha256": sha256,
                "requires_python": requires_python,
            })
    return files


def _parse_simple_index_html(html: str, base_url: str) -> list[dict]:
    """Parse a PEP 503 HTML simple index page into a list of file dicts.

    Uses stdlib html.parser instead of a regex so that attribute values
    containing '>' (e.g. data-requires-python=">=3.9") are handled
    correctly.  Chainguard adds non-standard attributes — data-provenance,
    data-signature, and download — to every anchor; the `download`
    attribute carries the exact filename and is used in preference to
    deriving the name from the href path.
    """
    from html.parser import HTMLParser

    class _Parser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.files: list[dict] = []

        def handle_starttag(self, tag: str, attrs):
            if tag != "a":
                return
            attr = {k.lower(): v for k, v in attrs}

            href = attr.get("href", "")
            if not href:
                return

            # Chainguard sets a `download` attribute with the bare filename.
            # Fall back to extracting the filename from the href path when
            # that attribute is absent (standard PEP 503 registries).
            filename = attr.get("download", "")
            if not filename:
                url_path = href.split("#")[0].split("?")[0]
                filename = urllib.parse.unquote(
                    url_path.rstrip("/").rsplit("/", 1)[-1]
                )

            filename = urllib.parse.unquote(filename.strip())
            if not filename or not any(
                filename.lower().endswith(ext) for ext in _DIST_EXTENSIONS
            ):
                return

            # Split hash fragment from href: URL#sha256=HEX
            if "#" in href:
                url_part, fragment = href.split("#", 1)
            else:
                url_part, fragment = href, ""

            url = urllib.parse.urljoin(base_url, url_part)
            sha256 = (
                fragment[len("sha256="):]
                if fragment.lower().startswith("sha256=")
                else ""
            )

            self.files.append({
                "filename": filename,
                "url": url,
                "sha256": sha256,
                "requires_python": attr.get("data-requires-python", ""),
            })

    parser = _Parser()
    parser.feed(html)
    return parser.files


def fetch_simple_index(name: str, registry_url: str,
                       tracker: ErrorTracker) -> Optional[list[dict]]:
    """Fetch the Simple API index for a package from the given CGR registry.

    Uses the auth-stripping redirect handler so that Chainguard CDN/S3
    presigned redirects succeed without conflicting auth headers.

    Captures the actual post-redirect URL as base_url so that relative
    hrefs in the HTML index resolve correctly regardless of how many hops
    the redirect chain takes.

    Tries JSON first (PEP 691), falls back to HTML (PEP 503).
    Returns list of file dicts, or None if the package is not found.
    """
    normalized = _normalize_name(name)
    index_url = f"{registry_url.rstrip('/')}/{normalized}/"
    auth = _cgr_auth_header()

    logger.debug("Fetching Simple index: %s", index_url)

    json_req = urllib.request.Request(index_url, headers={
        "Authorization": auth,
        "Accept": f"{_SIMPLE_JSON_MIME}, {_SIMPLE_HTML_MIME};q=0.1",
    })
    try:
        def _do_fetch_json():
            with _cgr_opener.open(json_req, timeout=60) as resp:
                content_type = resp.headers.get("Content-Type", "")
                # Capture the actual URL after any redirect chain so that
                # relative hrefs in the HTML resolve against the real base.
                final_url = resp.geturl() or index_url
                body = resp.read()
                return content_type, final_url, body

        content_type, final_url, body = _with_retry(
            _do_fetch_json, label=f"simple_index:{name}")

        if final_url != index_url:
            logger.debug("  %s — redirected to %s", name, final_url)

        content_type = content_type.split(";")[0].strip().lower()

        if content_type == _SIMPLE_JSON_MIME:
            data = json.loads(body)
            files = _parse_simple_index_json(data, final_url)
            logger.debug("  %s — %d files (JSON index)", name, len(files))
            return files
        else:
            # Server returned HTML (PEP 503 fallback or PEP 691 not supported)
            files = _parse_simple_index_html(body.decode(errors="replace"), final_url)
            logger.debug("  %s — %d files (HTML index, base=%s)", name, len(files), final_url)
            return files

    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            logger.debug("  %s — not found in %s", name, registry_url)
            return None
        if exc.code in (401, 403):
            logger.debug("  %s — auth error (%s) in %s", name, exc.code, registry_url)
            return None
        handle_http_error(exc, name, "fetch_simple_index", tracker)
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        handle_http_error(exc, name, "fetch_simple_index", tracker)
        return None
    except Exception as exc:
        handle_http_error(exc, name, "fetch_simple_index", tracker)
        return None


def download_distribution_file(url: str, filename: str,
                                expected_sha256: str,
                                tracker: ErrorTracker) -> Optional[bytes]:
    """Download a wheel or sdist from the Chainguard registry.

    Uses the auth-stripping redirect handler so that Chainguard CDN/S3
    presigned download redirects succeed.  The Authorization header is
    sent to libraries.cgr.dev but stripped before any redirect to a
    presigned URL, preventing the 400 signature-conflict error from S3.

    Verifies SHA256 if provided by the Simple API index.
    Returns bytes or None on failure.
    """
    auth = _cgr_auth_header()
    req = urllib.request.Request(url, headers={"Authorization": auth})

    try:
        def _do_download():
            with _cgr_opener.open(req, timeout=180) as resp:
                return resp.read()

        data = _with_retry(_do_download, label=f"download:{filename}")
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401, 403):
            logger.debug("Download 404/auth for %s (%s)", filename, exc.code)
            return None
        handle_http_error(exc, filename, "download", tracker)
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        handle_http_error(exc, filename, "download", tracker)
        return None
    except Exception as exc:
        handle_http_error(exc, filename, "download", tracker)
        return None

    # SHA256 verification
    if expected_sha256:
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected_sha256:
            msg = (f"SHA256 mismatch for {filename}: "
                   f"expected={expected_sha256} actual={actual}")
            logger.error(msg)
            tracker.record(package=filename, phase="download",
                           error_type="SHA256Mismatch", status_code=None, detail=msg)
            return None

    return data


# ===========================================================================
# 7. Distribution file metadata helpers
# ===========================================================================
# Wheel filename: {distribution}-{version}(-{build})?-{python}-{abi}-{platform}.whl
_WHEEL_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)"
    r"-(?P<version>[^-]+)"
    r"(?:-(?P<build>\d[^-]*))?"
    r"-(?P<python>[^-]+)"
    r"-(?P<abi>[^-]+)"
    r"-(?P<platform>[^-]+)\.whl$"
)
# sdist: {name}-{version}.tar.gz  or .zip
_SDIST_RE = re.compile(
    r"^(?P<name>[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)"
    r"-(?P<version>.+?)(?:\.tar\.gz|\.zip|\.egg)$"
)


def _parse_filename(filename: str) -> Optional[dict]:
    """Extract metadata from a wheel or sdist filename.

    Returns dict with keys: version, filetype, pyversion, python_tag, platform_tag
    Returns None if the filename cannot be parsed.
    """
    m = _WHEEL_RE.match(filename)
    if m:
        python_tag = m.group("python").lower()
        platform_tag = m.group("platform").lower()
        return {
            "version": m.group("version"),
            "filetype": "bdist_wheel",
            "pyversion": python_tag,
            "python_tag": python_tag,
            "platform_tag": platform_tag,
        }
    m = _SDIST_RE.match(filename)
    if m:
        return {
            "version": m.group("version"),
            "filetype": "sdist",
            "pyversion": "source",
            "python_tag": "source",
            "platform_tag": "any",
        }
    return None


def _should_sync_file(filename: str, file_meta: dict) -> bool:
    """Return True if this distribution file passes the configured filters."""
    filetype = file_meta["filetype"]

    if filetype == "sdist":
        return _SYNC_SDIST

    if filetype == "bdist_wheel":
        if not _SYNC_WHEELS:
            return False
        if _WHEEL_PYTHON_TAGS:
            # e.g. cp311, cp312, py3 — match against python_tag field
            if file_meta["python_tag"] not in _WHEEL_PYTHON_TAGS:
                return False
        if _WHEEL_PLATFORM_TAGS:
            # e.g. linux_x86_64, none-any — check if any configured tag is
            # a substring of the platform tag (handles manylinux2014 etc.)
            platform = file_meta["platform_tag"]
            if not any(t in platform for t in _WHEEL_PLATFORM_TAGS):
                return False
        return True

    return True  # unknown type — include by default


# ===========================================================================
# 8. CodeArtifact helpers
# ===========================================================================
def setup_codeartifact() -> str:
    """Idempotently create the CodeArtifact domain, upstream repo, and main repo.

    Returns the PyPI repository endpoint URL.
    """
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
            externalConnection="public:pypi")
        logger.info("Associated external connection public:pypi")
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
        domain=DOMAIN_NAME, repository=REPO_NAME, format="pypi",
    )["repositoryEndpoint"]
    logger.info("CodeArtifact PyPI endpoint: %s", endpoint)
    return endpoint


def _get_codeartifact_auth_token() -> str:
    """Return a short-lived CodeArtifact auth token.

    Passes domainOwner when CODEARTIFACT_DOMAIN_OWNER is set — required for
    cross-account CodeArtifact access and some single-account configurations
    where the token must be explicitly scoped to an account.
    """
    kwargs: dict = {"domain": DOMAIN_NAME}
    if DOMAIN_OWNER:
        kwargs["domainOwner"] = DOMAIN_OWNER
    return ca_client.get_authorization_token(**kwargs)["authorizationToken"]


def list_codeartifact_versions(name: str) -> set[str]:
    """Return set of version strings already in CodeArtifact for this package."""
    kwargs = {
        "domain": DOMAIN_NAME,
        "repository": REPO_NAME,
        "format": "pypi",
        "package": _normalize_name(name),
    }
    versions: set[str] = set()
    try:
        paginator = ca_client.get_paginator("list_package_versions")
        for page in paginator.paginate(**kwargs):
            for v in page.get("versions", []):
                versions.add(v["version"])
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in (
                "ResourceNotFoundException", "PackageNotFoundException"):
            logger.warning("list_package_versions failed for %s: %s", name, exc)
    return versions


def list_codeartifact_packages() -> list[str]:
    """Return all PyPI package names currently in CodeArtifact."""
    packages: list[str] = []
    try:
        paginator = ca_client.get_paginator("list_packages")
        for page in paginator.paginate(
                domain=DOMAIN_NAME, repository=REPO_NAME, format="pypi"):
            for pkg in page.get("packages", []):
                packages.append(pkg["package"])
    except ClientError as exc:
        logger.error("list_packages failed: %s", exc)
    logger.info("Found %d existing packages in CodeArtifact", len(packages))
    return packages


def _encode_multipart(fields: dict, filename: str,
                      file_bytes: bytes) -> tuple[bytes, str]:
    """Build a multipart/form-data body matching the PyPI legacy upload wire format."""
    boundary = uuid.uuid4().hex
    body = io.BytesIO()

    for key, value in fields.items():
        body.write(f"--{boundary}\r\n".encode())
        body.write(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
        body.write(f"{value}\r\n".encode())

    body.write(f"--{boundary}\r\n".encode())
    body.write(
        f'Content-Disposition: form-data; name="content"; '
        f'filename="{filename}"\r\n'.encode()
    )
    body.write(b"Content-Type: application/octet-stream\r\n\r\n")
    body.write(file_bytes)
    body.write(b"\r\n")
    body.write(f"--{boundary}--\r\n".encode())

    return body.getvalue(), f"multipart/form-data; boundary={boundary}"


# Reusable SSL context for CodeArtifact HTTPS connections
_ca_ssl_ctx = ssl.create_default_context()


def _http_post(url: str, body: bytes, headers: dict,
               max_redirects: int = 5) -> tuple[int, dict, str]:
    """POST to url using http.client, preserving POST through any redirect chain.

    Returns (status_code, lowercase_response_headers, response_body_str).

    Motivation: Python's urllib converts POST→GET on 301/302/303 redirects and
    its opener chain may transform headers.  http.client gives raw control —
    every byte in/out is explicit.
    """
    for _ in range(max_redirects + 1):
        parsed = urllib.parse.urlparse(url)
        host = parsed.netloc
        path = parsed.path + (f"?{parsed.query}" if parsed.query else "")

        send_headers = {
            **headers,
            "Host": host,
            "Content-Length": str(len(body)),
        }

        conn = http.client.HTTPSConnection(host, context=_ca_ssl_ctx, timeout=180)
        try:
            conn.request("POST", path, body, send_headers)
            resp = conn.getresponse()
            status = resp.status
            reason = resp.reason
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            resp_body = resp.read().decode(errors="replace")
        finally:
            conn.close()

        logger.debug("HTTP %d %s  x-amzn-requestid=%s",
                     status, reason,
                     resp_headers.get("x-amzn-requestid", "n/a"))

        if status in (301, 302, 303, 307, 308):
            location = resp_headers.get("location", "")
            if not location:
                logger.warning("Redirect %d with no Location — stopping", status)
                return status, resp_headers, resp_body
            new_url = urllib.parse.urljoin(url, location)
            logger.debug("POST redirect %d -> %s", status, new_url)
            url = new_url
            continue

        return status, resp_headers, resp_body

    return status, resp_headers, resp_body  # type: ignore[return-value]


def _set_pypi_origin_controls(name: str) -> None:
    """Set package-level origin controls: publish=ALLOW, upstream=BLOCK.

    Mirrors what the npm variant of this function does via _set_origin_controls.
    Package-level settings override repository-level settings, so if the
    repository has publish:BLOCK configured (common for pull-through caches),
    this call explicitly permits direct publishing for the named package.

    Failures are logged and swallowed — origin controls are best-effort.
    Requires codeartifact:PutPackageOriginConfiguration on the execution role.
    """
    try:
        ca_client.put_package_origin_configuration(
            domain=DOMAIN_NAME,
            repository=REPO_NAME,
            format="pypi",
            package=_normalize_name(name),
            restrictions={"publish": "ALLOW", "upstream": "BLOCK"},
            **({"domainOwner": DOMAIN_OWNER} if DOMAIN_OWNER else {}),
        )
    except ClientError as exc:
        logger.debug("put_package_origin_configuration for %s: %s", name, exc)


def _preflight_endpoint_check(endpoint: str, auth_token: str) -> None:
    """Verify HTTP connectivity to the CodeArtifact registry endpoint.

    Logs the DNS resolution of the registry hostname (to detect VPC endpoint
    DNS override issues) and the HTTP status of a GET to /simple/.
    """
    import socket

    parsed = urllib.parse.urlparse(endpoint)
    host = parsed.netloc

    # ---- DNS lookup -------------------------------------------------------
    # If this resolves to a private IP (10.x, 172.16-31.x, 192.168.x),
    # the VPC's codeartifact.api endpoint may be intercepting HTTP registry
    # traffic.  Fix: add com.amazonaws.REGION.codeartifact.repositories
    # as a separate VPC Interface Endpoint.
    try:
        addrs = [a[4][0] for a in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)]
        logger.warning(
            "DNS %s → %s  "
            "(private IP = VPC endpoint DNS intercepting HTTP registry traffic)",
            host, addrs,
        )
    except Exception as exc:
        logger.error("DNS lookup for %s FAILED: %s", host, exc)

    # ---- HTTP connectivity check ------------------------------------------
    test_url = f"{endpoint.rstrip('/')}/simple/"
    ca_basic_auth = "Basic " + base64.b64encode(
        f"aws:{auth_token}".encode()
    ).decode()
    try:
        conn = http.client.HTTPSConnection(host, context=_ca_ssl_ctx, timeout=10)
        try:
            conn.request("GET", parsed.path.rstrip("/") + "/simple/",
                         headers={"Authorization": ca_basic_auth, "Host": host})
            resp = conn.getresponse()
            resp_headers = {k.lower(): v for k, v in resp.getheaders()}
            resp.read()
            logger.warning(
                "Pre-flight GET %s → HTTP %d  request-id=%s\n"
                "  200/404 = endpoint reachable (check IAM if uploads still 404)\n"
                "  401/403 = endpoint reachable but auth rejected\n"
                "  connection error = missing codeartifact.repositories VPC endpoint",
                test_url, resp.status,
                resp_headers.get("x-amzn-requestid", "n/a"),
            )
        finally:
            conn.close()
    except Exception as exc:
        logger.error(
            "Pre-flight FAILED connecting to %s: %s\n"
            "  → Add com.amazonaws.%s.codeartifact.repositories VPC Interface Endpoint",
            test_url, exc, AWS_REGION,
        )


def publish_to_codeartifact(
    name: str, version: str, filename: str,
    file_bytes: bytes, endpoint: str, auth_token: str,
    filetype: str, pyversion: str, tracker: ErrorTracker,
) -> bool:
    """Upload a single distribution file to CodeArtifact via its PyPI upload endpoint.

    Uses http.client (not urllib.request) for full control:
    - POST method preserved through redirect chain
    - All response headers logged on failure for diagnosis
    - No opener machinery can silently transform the request

    Auth: HTTP Basic — username "aws", password = CodeArtifact auth token.
    This is the format required by pip and twine.

    NOTE — if publishes return HTTP 404:
    CodeArtifact has two VPC endpoints:
      • com.amazonaws.REGION.codeartifact.api         (boto3 SDK calls)
      • com.amazonaws.REGION.codeartifact.repositories (HTTP registry — this)
    If the Lambda VPC only has the .api endpoint, SDK calls work but HTTP
    uploads fail with 404. Add the .repositories endpoint to fix this.
    """
    pkg_id = f"{name}=={version} ({filename})"

    md5_digest = hashlib.md5(file_bytes).hexdigest()
    sha256_digest = hashlib.sha256(file_bytes).hexdigest()

    fields = {
        ":action": "file_upload",
        "metadata_version": "2.1",   # required by PyPI upload spec
        "protocol_version": "1",
        "name": _normalize_name(name),
        "version": version,
        "filetype": filetype,
        "pyversion": pyversion,
        "md5_digest": md5_digest,
        "sha256_digest": sha256_digest,
    }

    body, content_type = _encode_multipart(fields, filename, file_bytes)

    # Build upload URL.
    # CodeArtifact accepts uploads at the repository endpoint root — NOT at
    # a /legacy/ sub-path.  The /legacy/ convention comes from pypi.org and
    # twine but CodeArtifact does not implement that route (returns 404).
    base_upload = endpoint.rstrip("/")
    qs = (f"?domain-owner={urllib.parse.quote(DOMAIN_OWNER)}"
          if DOMAIN_OWNER else "")
    upload_urls = [
        f"{base_upload}/{qs}",          # CodeArtifact repository endpoint root
    ]

    ca_basic_auth = "Basic " + base64.b64encode(
        f"aws:{auth_token}".encode()
    ).decode()

    request_headers = {
        "Authorization": ca_basic_auth,
        "Content-Type": content_type,
        "User-Agent": "twine/5.1.1 CPython/3.11.0 (linux; x86_64)",
        "Accept": "*/*",
    }

    last_status, last_headers, last_body = 0, {}, ""

    # Retryable status codes — 429 is the CodeArtifact "concurrent modification"
    # error that fires when multiple wheels for the same package version are
    # published in rapid succession.  5xx codes are transient server errors.
    _PUBLISH_RETRYABLE = {429, 500, 502, 503, 504}

    for upload_url in upload_urls:
        logger.debug("Publishing %s — POST %s (%d bytes)", pkg_id, upload_url, len(body))

        # --- Retry loop around _http_post for transient errors ---
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                status, resp_headers, resp_body = _http_post(
                    upload_url, body, request_headers)
            except Exception as exc:
                if attempt < RETRY_MAX_ATTEMPTS:
                    delay = RETRY_BASE_DELAY_S * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                    logger.warning("Publish attempt %d/%d for %s — %s — waiting %.1fs",
                                   attempt, RETRY_MAX_ATTEMPTS, pkg_id,
                                   type(exc).__name__, delay)
                    time.sleep(delay)
                    continue
                handle_http_error(exc, pkg_id, "publish", tracker)
                return False

            # Success
            if status in (200, 201, 204):
                logger.info("Published %s (HTTP %s via %s)", pkg_id, status, upload_url)
                return True

            # Already exists — treat as success
            already_exists = status == 409 or (
                status == 400
                and any(phrase in resp_body.lower() for phrase in (
                    "file already exists", "already been registered",
                    "already exists", "filename has already",
                ))
            )
            if already_exists:
                logger.info("%s already exists in CodeArtifact", pkg_id)
                return True

            # Retryable error — wait and retry
            if status in _PUBLISH_RETRYABLE and attempt < RETRY_MAX_ATTEMPTS:
                delay = RETRY_BASE_DELAY_S * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Publish attempt %d/%d for %s — HTTP %d — waiting %.1fs",
                    attempt, RETRY_MAX_ATTEMPTS, pkg_id, status, delay)
                time.sleep(delay)
                continue

            # Non-retryable error or final attempt — fall through
            last_status, last_headers, last_body = status, resp_headers, resp_body
            break
        else:
            # All retry attempts exhausted inside the inner loop
            last_status, last_headers, last_body = status, resp_headers, resp_body

        if last_status not in (0, 404):
            break

        logger.debug("POST %s → HTTP 404", upload_url)

    # Upload failed.
    # If 404: check that the .repositories VPC endpoint exists and that
    # codeartifact:PublishPackageVersion is on the Lambda execution role.
    logger.warning(
        "Publish failed %s — HTTP %s — all-response-headers=%s — body=%r\n"
        "  → If 404: verify VPC .repositories endpoint and IAM permissions.",
        pkg_id, last_status, dict(last_headers), last_body[:400],
    )
    tracker.record(package=pkg_id, phase="publish", error_type="HTTPError",
                   status_code=last_status,
                   detail=f"HTTP {last_status} — {last_body[:300]}")
    return False


# ===========================================================================
# 9. Garbage cleanup (PyPI)
# ===========================================================================
_INVALID_PYPI_NAME_RE = re.compile(r"[/\\:@]|node_modules|\.\.", re.IGNORECASE)
_VALID_PYPI_NAME_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9._-]*[A-Za-z0-9])?$")


def cleanup_garbage_packages() -> dict:
    """Remove packages from CodeArtifact whose names are clearly invalid PyPI names.

    Targets names containing path separators, '@', 'node_modules', or other
    characters that are illegal in PEP 508 package names.
    """
    logger.info("Scanning CodeArtifact for garbage packages...")
    all_packages = list_codeartifact_packages()

    garbage = [
        p for p in all_packages
        if _INVALID_PYPI_NAME_RE.search(p) or not _VALID_PYPI_NAME_RE.match(p)
    ]
    if not garbage:
        logger.info("No garbage packages found")
        return {"cleaned": 0, "failed": 0, "total_garbage_found": 0, "packages_removed": []}

    logger.info("Found %d garbage package(s): %s", len(garbage),
                [g[:60] for g in garbage[:10]])

    cleaned, failed = 0, 0
    cleaned_names: list[str] = []

    for pkg_name in garbage:
        # List all versions first
        versions_to_delete: list[str] = []
        try:
            ver_paginator = ca_client.get_paginator("list_package_versions")
            for page in ver_paginator.paginate(
                    domain=DOMAIN_NAME, repository=REPO_NAME,
                    format="pypi", package=pkg_name):
                for v in page.get("versions", []):
                    versions_to_delete.append(v["version"])
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceNotFoundException":
                logger.warning("Could not list versions for %s: %s", pkg_name, exc)
                failed += 1
                continue

        if not versions_to_delete:
            continue

        all_deleted = True
        for i in range(0, len(versions_to_delete), 50):
            batch = versions_to_delete[i:i + 50]
            try:
                result = ca_client.delete_package_versions(
                    domain=DOMAIN_NAME, repository=REPO_NAME,
                    format="pypi", package=pkg_name, versions=batch)
                if result.get("failedVersions"):
                    all_deleted = False
            except ClientError as exc:
                logger.warning("Error deleting versions of %s: %s", pkg_name, exc)
                all_deleted = False

        if all_deleted:
            cleaned += 1
            cleaned_names.append(pkg_name)
            logger.info("Removed garbage package %s (%d versions)",
                        pkg_name, len(versions_to_delete))
        else:
            failed += 1

    return {
        "cleaned": cleaned, "failed": failed,
        "total_garbage_found": len(garbage), "packages_removed": cleaned_names,
    }


# ===========================================================================
# 10. Work item discovery
# ===========================================================================
def _files_for_package_version(
    name: str, version: str, registry_key: str, registry_url: str,
    tracker: ErrorTracker,
) -> list[dict]:
    """Return all distribution files for name==version from the given registry.

    Each returned dict is a work item ready to be processed:
    {"name", "version", "filename", "url", "sha256", "registry",
     "filetype", "pyversion"}
    """
    all_files = fetch_simple_index(name, registry_url, tracker)
    if all_files is None:
        return []

    # Collect all versions present in this index for diagnostic logging.
    versions_in_index: set[str] = set()
    work = []
    for f in all_files:
        filename = f["filename"]
        meta = _parse_filename(filename)
        if meta is None:
            logger.debug("  Could not parse filename: %s", filename)
            continue
        versions_in_index.add(meta["version"])
        if _normalize_version(meta["version"]) != _normalize_version(version):
            continue
        if not _should_sync_file(filename, meta):
            logger.debug("  Filtered out %s", filename)
            continue
        work.append({
            "name": name,
            "version": version,
            "filename": filename,
            "url": f["url"],
            "sha256": f.get("sha256", ""),
            "registry": registry_key,
            "filetype": meta["filetype"],
            "pyversion": meta["pyversion"],
        })

    if not work and all_files:
        # Version not matched — log what versions ARE available so the operator
        # can distinguish a redirect-content issue from a genuine mirror gap.
        sample = sorted(versions_in_index)[-5:]  # show the 5 most-recent versions
        logger.debug(
            "  %s==%s not in %s index (%d files, %d unique versions). "
            "Most-recent in index: %s",
            name, version, registry_key,
            len(all_files), len(versions_in_index),
            sample if sample else "(none parsed)",
        )

    return work


def _normalize_version(v: str) -> str:
    """Normalize a PEP 440 version string for comparison.

    Lowercases and normalizes separators.  Handles local version identifiers
    like "2.3.0+cu126" correctly.
    """
    return v.lower().replace("-", ".").replace("_", ".")


def build_work_items(
    packages: list[dict],
    tracker: ErrorTracker,
) -> tuple[list[dict], int, list[str]]:
    """Build download work items for the packages specified in requirements.txt.

    For each package, the registries are tried in priority order until one
    supplies the requested version.  Versions already in CodeArtifact are
    skipped (checked once per package, not per file).

    Returns: (work_items, skipped_existing_count, not_in_chainguard_names)
    """
    work_items: list[dict] = []
    skipped_existing = 0
    not_in_chainguard: list[str] = []

    # Build version map: normalized_name → set of pinned versions
    version_map: dict[str, set[str]] = {}
    for p in packages:
        version_map.setdefault(p["normalized_name"], set()).add(p["version"])

    names = unique_package_names(packages)
    logger.info("Discovery — %d unique packages", len(names))

    for name in names:
        pinned_versions = version_map.get(name, set())
        existing = list_codeartifact_versions(name)

        for version in sorted(pinned_versions):
            if version in existing:
                skipped_existing += 1
                logger.info("  %s==%s — already in CodeArtifact", name, version)
                continue

            # Try registries in priority order
            registries = _determine_registries(version)
            found = False
            for reg_key, reg_url in registries:
                items = _files_for_package_version(name, version, reg_key, reg_url, tracker)
                if items:
                    work_items.extend(items)
                    logger.info("  %s==%s — %d file(s) from %s registry",
                                name, version, len(items), reg_key)
                    found = True
                    break

            if not found:
                not_in_chainguard.append(f"{name}=={version}")
                logger.info("  %s==%s — not found in any Chainguard registry",
                            name, version)

    logger.info("Discovery complete — %d work items (files), %d skipped, %d not found",
                len(work_items), skipped_existing, len(not_in_chainguard))
    return work_items, skipped_existing, not_in_chainguard


def build_update_work_items(
    tracker: ErrorTracker,
) -> tuple[list[dict], int, int, list[str]]:
    """Check all existing CodeArtifact packages for new versions in Chainguard.

    Searches both remediated and standard registries (not CUDA — those are
    handled by explicit requirements.txt entries with +cuXXX version suffixes).

    Returns: (work_items, skipped_existing_count, total_packages, not_in_chainguard)
    """
    existing_packages = list_codeartifact_packages()
    if not existing_packages:
        return [], 0, 0, []

    logger.info("Update check — scanning %d existing packages", len(existing_packages))

    work_items: list[dict] = []
    skipped_existing = 0
    not_in_chainguard: list[str] = []

    for name in existing_packages:
        existing_versions = list_codeartifact_versions(name)

        # Search non-CUDA registries for new versions
        all_cgr_files: list[dict] = []
        found_in_registry = False
        for reg_key, reg_url in _STANDARD_REGISTRIES:
            files = fetch_simple_index(name, reg_url, tracker)
            if files is not None:
                for f in files:
                    f["_registry_key"] = reg_key
                all_cgr_files.extend(files or [])
                found_in_registry = True
                break  # Use first registry that has the package

        if not found_in_registry:
            not_in_chainguard.append(name)
            continue

        # Determine which versions are new
        cgr_versions: set[str] = set()
        for f in all_cgr_files:
            meta = _parse_filename(f["filename"])
            if meta:
                cgr_versions.add(meta["version"])

        new_versions = cgr_versions - existing_versions
        skipped_existing += len(cgr_versions) - len(new_versions)

        if not new_versions:
            logger.debug("  %s — up to date (%d versions)", name, len(existing_versions))
            continue

        logger.info("  %s — %d new version(s)", name, len(new_versions))
        for f in all_cgr_files:
            meta = _parse_filename(f["filename"])
            if meta is None:
                continue
            if meta["version"] not in new_versions:
                continue
            if not _should_sync_file(f["filename"], meta):
                continue
            work_items.append({
                "name": name,
                "version": meta["version"],
                "filename": f["filename"],
                "url": f["url"],
                "sha256": f.get("sha256", ""),
                "registry": f.get("_registry_key", "standard"),
                "filetype": meta["filetype"],
                "pyversion": meta["pyversion"],
            })

    logger.info("Update discovery complete — %d work items across %d packages",
                len(work_items), len(existing_packages))
    return work_items, skipped_existing, len(existing_packages), not_in_chainguard


# ===========================================================================
# 11. Checkpoint management
# ===========================================================================
def _generate_run_id() -> str:
    from datetime import datetime, timezone
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
        raise ValueError("No S3 bucket configured for checkpoints.")
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
            "cleanup_garbage": False,
            "dry_run": event.get("dry_run", False),
            "allowlist": event.get("allowlist"),
            "denylist": event.get("denylist"),
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
    phase = cp.get("phase", "processing")
    idx = cp.get("discovery_index", 0) if phase == "discovery" else cp.get("next_index", 0)
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
    if not bucket:
        return False
    try:
        s3_client.head_object(Bucket=bucket, Key=_checkpoint_key(run_id))
        return True
    except ClientError:
        return False


def _try_save_failure_checkpoint(
    run_id, bucket, original_event, phase, work_items,
    counters, not_found_list, tracker, invocation_number,
    package_names=None, discovery_index=0, processing_index=0,
) -> Optional[str]:
    if not bucket:
        logger.warning("No checkpoint bucket — cannot save failure checkpoint")
        return None
    if _checkpoint_exists(bucket, run_id):
        logger.info("Checkpoint already exists for run %s — not overwriting", run_id)
        return run_id
    try:
        _save_checkpoint(run_id, original_event, phase, work_items,
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


# ===========================================================================
# 12. Parallel work item processor
# ===========================================================================

# Per-package publish locks — prevents concurrent CodeArtifact writes to the
# same package, which causes 429 "concurrent modification" errors.  Multiple
# wheels for sqlalchemy==2.0.49 (cp310, cp311, cp312, etc.) can download in
# parallel but must publish one at a time to the same package.
_publish_locks: dict[str, threading.Lock] = {}
_publish_locks_guard = threading.Lock()


def _get_publish_lock(name: str) -> threading.Lock:
    """Return a per-package lock, creating it if needed.  Thread-safe."""
    normalized = _normalize_name(name)
    with _publish_locks_guard:
        if normalized not in _publish_locks:
            _publish_locks[normalized] = threading.Lock()
        return _publish_locks[normalized]


def _process_work_item(
    item: dict,
    endpoint: str,
    auth_token: str,
    tracker: ErrorTracker,
    lock: threading.Lock,
    counters: dict,
    not_found_list: list,
    dry_run: bool,
) -> None:
    """Download and publish one distribution file.  Thread-safe via lock."""
    name = item["name"]
    version = item["version"]
    filename = item["filename"]
    url = item["url"]
    sha256 = item.get("sha256", "")
    filetype = item["filetype"]
    pyversion = item["pyversion"]
    registry = item.get("registry", "standard")
    pkg_id = f"{name}=={version} ({filename})"

    if dry_run:
        logger.info("[DRY-RUN] Would sync %s from %s registry", pkg_id, registry)
        with lock:
            counters["found"] += 1
        return

    file_bytes = download_distribution_file(url, filename, sha256, tracker)

    if file_bytes is None:
        with lock:
            counters["not_found"] += 1
            not_found_list.append(pkg_id)
        logger.debug("  %s — download failed", pkg_id)
        return

    # Set publish:ALLOW BEFORE acquiring the lock so this network call
    # doesn't block other threads.  Log at INFO so failures are visible.
    try:
        ca_client.put_package_origin_configuration(
            domain=DOMAIN_NAME,
            repository=REPO_NAME,
            format="pypi",
            package=_normalize_name(name),
            restrictions={"publish": "ALLOW", "upstream": "BLOCK"},
            **({"domainOwner": DOMAIN_OWNER} if DOMAIN_OWNER else {}),
        )
        logger.info("Origin controls set to publish:ALLOW for %s", name)
    except ClientError as exc:
        logger.warning("put_package_origin_configuration FAILED for %s: %s %s",
                       name,
                       exc.response["Error"]["Code"],
                       exc.response["Error"].get("Message", ""))

    # Serialize publishes to the same package — CodeArtifact returns 429
    # "concurrent modification" when multiple threads write different files
    # for the same package simultaneously (e.g. sqlalchemy cp310 + cp311).
    # Downloads still happen in parallel; only the publish is serialized.
    pkg_lock = _get_publish_lock(name)
    with pkg_lock:
        success = publish_to_codeartifact(
            name, version, filename, file_bytes,
            endpoint, auth_token, filetype, pyversion, tracker,
        )
    with lock:
        counters["found"] += 1
        if not success:
            counters["failed"] += 1


# ===========================================================================
# 13. Lambda handler
# ===========================================================================
def lambda_handler(event, context):
    """
    Entry modes:

    1. NEW SESSION — sync requirements.txt versions to CodeArtifact:
       {"s3_bucket": "...", "s3_key": "requirements.txt"}

    2. INLINE REQUIREMENTS — pass content directly:
       {"requirements_content": "numpy==1.26.4\ntorch==2.3.0+cu126\n"}

    3. UPDATE EXISTING — check all CodeArtifact packages for new Chainguard versions:
       {"update_existing": true}

    4. RESUME PREVIOUS SESSION:
       {"resume_run_id": "20260402T034658Z-abc12345"}

    5. LIST AVAILABLE CHECKPOINTS:
       {"list_checkpoints": true}

    6. CLEANUP ONLY — remove packages with invalid PyPI names:
       {"cleanup_only": true}

    7. DRY-RUN — preview what would be synced:
       {"s3_bucket": "...", "s3_key": "requirements.txt", "dry_run": true}

    Options:
       "setup_codeartifact": true/false  — create infra if missing (default: false)
       "cleanup_garbage":    true/false  — pre-run cleanup       (default: true)
       "dry_run":            true/false  — no writes             (default: false)
       "allowlist":          ["pkg-a"]  — only these packages
       "denylist":           ["pkg-b"]  — skip these packages
    """
    # ------------------------------------------------------------------
    # Mode: list checkpoints
    # ------------------------------------------------------------------
    if event.get("list_checkpoints"):
        bucket = (event.get("checkpoint_s3_bucket", CHECKPOINT_S3_BUCKET)
                  or event.get("errors_s3_bucket", ERRORS_S3_BUCKET))
        checkpoints = _list_checkpoints(bucket)
        return _success(f"Found {len(checkpoints)} checkpoint(s)",
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

    # Package filtering — event values override env vars
    allowlist_raw = PACKAGE_ALLOWLIST
    denylist_raw = PACKAGE_DENYLIST
    event_allowlist = event.get("allowlist")
    event_denylist = event.get("denylist")
    if isinstance(event_allowlist, list):
        allowlist_raw = ",".join(event_allowlist)
    elif isinstance(event_allowlist, str):
        allowlist_raw = event_allowlist
    if isinstance(event_denylist, list):
        denylist_raw = ",".join(event_denylist)
    elif isinstance(event_denylist, str):
        denylist_raw = event_denylist

    allowlist = _load_filter_set(allowlist_raw)
    denylist = _load_filter_set(denylist_raw)
    if allowlist:
        logger.info("Allowlist active — %d packages", len(allowlist))
    if denylist:
        logger.info("Denylist active  — %d packages", len(denylist))
    if dry_run:
        logger.info("DRY-RUN mode — no packages will be written to CodeArtifact")

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
        ckpt_key = (event["_checkpoint_key"] if is_auto_continuation
                    else _checkpoint_key(run_id))
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
    # New session
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
        packages = []  # set below

        if event.get("setup_codeartifact", False):
            setup_codeartifact()

        if do_cleanup:
            cleanup_result = cleanup_garbage_packages()
            if cleanup_result["cleaned"] > 0:
                logger.info("Pre-run cleanup removed %d garbage packages",
                            cleanup_result["cleaned"])

        if is_update_existing:
            package_names = list_codeartifact_packages()
            logger.info("New session %s — update mode, %d existing packages",
                        run_id, len(package_names))
        else:
            # Load requirements.txt
            if "requirements_content" in event:
                req_content = event["requirements_content"]
            elif "s3_bucket" in event and "s3_key" in event:
                obj = s3_client.get_object(
                    Bucket=event["s3_bucket"], Key=event["s3_key"])
                req_content = obj["Body"].read().decode()
            else:
                return _error(
                    "Provide 'requirements_content', 's3_bucket'+'s3_key', "
                    "'update_existing', 'resume_run_id', or 'list_checkpoints'")

            packages = parse_requirements(req_content)
            if not packages:
                return _success("No pinned packages found in requirements.txt",
                                _empty_summary())

            package_names = unique_package_names(packages)
            logger.info("New session %s — %d unique packages from requirements.txt",
                        run_id, len(package_names))

        if not package_names:
            return _success("No packages to process", _empty_summary())

        # Apply allow/deny filters
        package_names = _apply_package_filters(package_names, allowlist, denylist)
        if not package_names:
            return _success("All packages were filtered out by allowlist/denylist",
                            _empty_summary())

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
        return _error(
            f"Exceeded max invocations ({MAX_INVOCATIONS}). "
            f'Resume with: {{"resume_run_id": "{run_id}"}}')

    # ==================================================================
    # PHASE 1: DISCOVERY — fetch Simple API indexes, build work items
    # ==================================================================
    if phase == "discovery":
        # Rebuild version map for lockfile mode continuations
        lockfile_versions: dict[str, set[str]] = {}
        if not is_update_existing and not (is_auto_continuation or is_manual_resume):
            for p in packages:
                lockfile_versions.setdefault(p["normalized_name"], set()).add(p["version"])
        elif is_auto_continuation or is_manual_resume:
            oe = original_event
            if not oe.get("update_existing", False):
                if oe.get("s3_bucket") and oe.get("s3_key"):
                    try:
                        obj = s3_client.get_object(
                            Bucket=oe["s3_bucket"], Key=oe["s3_key"])
                        reparsed = parse_requirements(obj["Body"].read().decode())
                        for p in reparsed:
                            lockfile_versions.setdefault(
                                p["normalized_name"], set()).add(p["version"])
                    except Exception:
                        logger.warning("Could not re-parse requirements for version filtering")

        is_update = is_update_existing or original_event.get("update_existing", False)
        logger.info("Discovery phase — starting at %d/%d",
                    discovery_index, len(package_names))

        try:
            for i in range(discovery_index, len(package_names)):
                if _is_time_running_out(context):
                    logger.warning("Low on time at discovery %d/%d — checkpointing",
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
                        f"Discovery checkpointed at {i}/{len(package_names)} — "
                        f"continuation #{invocation_number + 1} launched",
                        {"run_id": run_id, "invocation_number": invocation_number,
                         "phase": "discovery",
                         "discovery_progress": f"{i}/{len(package_names)}",
                         "work_items_so_far": len(work_items),
                         "status": "continuing", **counters})

                name = package_names[i]
                existing = list_codeartifact_versions(name)

                if is_update:
                    # Update mode: find all CGR versions not yet in CodeArtifact
                    for reg_key, reg_url in _STANDARD_REGISTRIES:
                        all_files = fetch_simple_index(name, reg_url, tracker)
                        if all_files is None:
                            continue
                        for f in all_files:
                            meta = _parse_filename(f["filename"])
                            if meta is None:
                                continue
                            ver = meta["version"]
                            if ver in existing:
                                counters["skipped_existing"] += 1
                                continue
                            if not _should_sync_file(f["filename"], meta):
                                continue
                            work_items.append({
                                "name": name, "version": ver,
                                "filename": f["filename"], "url": f["url"],
                                "sha256": f.get("sha256", ""),
                                "registry": reg_key,
                                "filetype": meta["filetype"],
                                "pyversion": meta["pyversion"],
                            })
                        break  # use first registry that has the package
                else:
                    # Lockfile mode: only the pinned versions
                    pinned = lockfile_versions.get(name, set())
                    for version in sorted(pinned):
                        if version in existing:
                            counters["skipped_existing"] += 1
                            logger.debug("  %s==%s — already in CodeArtifact", name, version)
                            continue
                        registries = _determine_registries(version)
                        found = False
                        for reg_key, reg_url in registries:
                            items = _files_for_package_version(
                                name, version, reg_key, reg_url, tracker)
                            if items:
                                work_items.extend(items)
                                logger.info("[%d/%d] %s==%s — %d file(s) from %s",
                                            i + 1, len(package_names), name,
                                            version, len(items), reg_key)
                                found = True
                                break
                        if not found:
                            not_found_list.append(f"{name}=={version}")
                            logger.info("[%d/%d] %s==%s — not in any Chainguard registry",
                                        i + 1, len(package_names), name, version)

        except Exception as exc:
            logger.error("Unexpected error during discovery at %d: %s", i, exc,
                         exc_info=True)
            _flush_errors(tracker, original_event)
            saved = _try_save_failure_checkpoint(
                run_id, checkpoint_bucket, original_event,
                "discovery", work_items, counters, not_found_list,
                tracker, invocation_number,
                package_names=package_names, discovery_index=i)
            msg = f"Discovery error at package {i}: {type(exc).__name__}: {str(exc)[:200]}"
            if saved:
                msg += f'. Resume with: {{"resume_run_id": "{saved}"}}'
            _send_sns_notification(
                f"[Chainguard Sync] FAILED — {run_id}",
                {"error": msg, "run_id": run_id}, run_id)
            return _error(msg)

        logger.info("Discovery complete — %d total work items (files)", len(work_items))
        phase = "processing"
        processing_index = 0

    # ==================================================================
    # PHASE 2: PROCESSING — download + publish in parallel batches
    # ==================================================================
    endpoint = ca_client.get_repository_endpoint(
        domain=DOMAIN_NAME, repository=REPO_NAME, format="pypi",
    )["repositoryEndpoint"]
    auth_token = _get_codeartifact_auth_token()

    upload_url_base = endpoint.rstrip('/') + "/"
    logger.info("CodeArtifact publish endpoint: %s", upload_url_base)
    logger.info("Auth token obtained (length=%d chars)", len(auth_token))

    # Pre-flight: verify the CodeArtifact HTTP endpoint is reachable via HTTPS.
    # Fetches the PyPI simple index root — a read operation that uses the same
    # network path as uploads.  If this fails, the issue is network/VPC.
    # If this succeeds but uploads return 404, the issue is IAM permissions
    # (missing codeartifact:PublishPackageVersion on the Lambda execution role).
    if not dry_run:
        _preflight_endpoint_check(endpoint, auth_token)

    current_index = processing_index
    timed_out = False
    lock = threading.Lock()
    workers = min(MAX_WORKERS, max(1, len(work_items) - processing_index))

    logger.info("Processing phase — %d work items starting at %d (workers=%d%s)",
                len(work_items), processing_index, workers,
                ", DRY-RUN" if dry_run else "")

    try:
        i = processing_index
        while i < len(work_items):
            if _is_time_running_out(context):
                logger.warning("Low on time at item %d/%d — checkpointing",
                               i, len(work_items))
                timed_out = True
                current_index = i
                break

            batch = work_items[i:i + workers]
            with ThreadPoolExecutor(max_workers=len(batch)) as executor:
                futures = [
                    executor.submit(
                        _process_work_item,
                        item, endpoint, auth_token,
                        tracker, lock, counters, not_found_list, dry_run,
                    )
                    for item in batch
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        logger.error("Worker raised unexpected exception: %s",
                                     exc, exc_info=True)
                        with lock:
                            counters["failed"] += 1

            i += len(batch)
            current_index = i
            logger.info("[%d/%d] batch done — synced=%d failed=%d not_found=%d",
                        current_index, len(work_items),
                        counters["found"], counters["failed"], counters["not_found"])

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
        _send_sns_notification(
            f"[Chainguard Sync] FAILED — {run_id}",
            {"error": msg, "run_id": run_id}, run_id)
        return _error(msg)

    # ------------------------------------------------------------------
    # Checkpoint if timed out — auto-continue
    # ------------------------------------------------------------------
    if timed_out and current_index < len(work_items):
        if not checkpoint_bucket:
            _flush_errors(tracker, original_event)
            return _error(
                f"Timed out at {current_index}/{len(work_items)} "
                "with no checkpoint bucket configured.")
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
        "total_work_items": len(work_items),  # individual files
        "synced_to_codeartifact": counters["found"],
        "skipped_existing": counters["skipped_existing"],
        "tarball_download_failed": counters["not_found"],
        "failed_to_publish": counters["failed"],
        "http_errors": tracker.count,
        "errors_file": errors_s3_uri,
        "errored_packages": tracker.package_names,
        "not_found_packages": not_found_list,
        "status": "dry_run_complete" if dry_run else "complete",
    }
    logger.info("Sync complete — %s", json.dumps(summary, indent=2))

    subject = (
        f"[{'DRY-RUN ' if dry_run else ''}Chainguard PyPI Sync] "
        f"{'Preview' if dry_run else 'Complete'} — "
        f"{counters['found']} synced, {counters['failed']} failed | {run_id}"
    )
    _send_sns_notification(subject, summary, run_id)
    _emit_cloudwatch_metrics(summary, run_id)

    return _success("Dry-run complete" if dry_run else "Sync complete", summary)


# ===========================================================================
# 14. Utility helpers
# ===========================================================================
def _flush_errors(tracker: ErrorTracker, event: dict) -> Optional[str]:
    if tracker.count == 0:
        return None
    bucket = event.get("errors_s3_bucket", ERRORS_S3_BUCKET)
    prefix = event.get("errors_s3_prefix", ERRORS_S3_PREFIX)
    return tracker.upload_to_s3(bucket, prefix)


def _empty_summary() -> dict:
    return {
        "total_work_items": 0, "synced_to_codeartifact": 0,
        "skipped_existing": 0, "tarball_download_failed": 0,
        "failed_to_publish": 0, "http_errors": 0,
        "errors_file": None, "errored_packages": [],
        "not_found_packages": [], "status": "complete",
    }


def _success(message: str, summary: dict) -> dict:
    return {"statusCode": 200, "body": {"message": message, "summary": summary}}


def _error(message: str) -> dict:
    logger.error(message)
    return {"statusCode": 400, "body": {"error": message}}


# ===========================================================================
# 15. CloudWatch metrics
# ===========================================================================
def _emit_cloudwatch_metrics(summary: dict, run_id: str = "") -> None:
    try:
        dimensions = [{"Name": "FunctionName", "Value": FUNCTION_NAME}]
        metric_data = [
            {"MetricName": "PackagesSynced",
             "Value": float(summary.get("synced_to_codeartifact", 0)),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "PackagesFailed",
             "Value": float(summary.get("failed_to_publish", 0)),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "PackagesSkipped",
             "Value": float(summary.get("skipped_existing", 0)),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "DownloadFailed",
             "Value": float(summary.get("tarball_download_failed", 0)),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "HttpErrors",
             "Value": float(summary.get("http_errors", 0)),
             "Unit": "Count", "Dimensions": dimensions},
            {"MetricName": "TotalWorkItems",
             "Value": float(summary.get("total_work_items", 0)),
             "Unit": "Count", "Dimensions": dimensions},
        ]
        cw_client.put_metric_data(Namespace=CLOUDWATCH_NAMESPACE,
                                  MetricData=metric_data)
        logger.info("CloudWatch metrics emitted to %s", CLOUDWATCH_NAMESPACE)
    except Exception as exc:
        logger.warning("Failed to emit CloudWatch metrics: %s", exc)


# ===========================================================================
# 16. SNS notifications
# ===========================================================================
def _send_sns_notification(subject: str, summary: dict, run_id: str = "") -> None:
    """Publish a sync summary to SNS. No-op if SNS_TOPIC_ARN is unset."""
    if not SNS_TOPIC_ARN:
        return
    try:
        message_body = json.dumps(
            {"run_id": run_id or summary.get("run_id", ""),
             "subject": subject, **summary},
            indent=2, default=str,
        )
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=subject[:100],
            Message=message_body,
        )
        logger.info("SNS notification sent: %s", subject)
    except Exception as exc:
        logger.warning("Failed to send SNS notification: %s", exc)