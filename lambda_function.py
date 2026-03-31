"""
AWS Lambda: Sync Chainguard NPM packages to AWS CodeArtifact

Combines three bash scripts into a single Lambda:
  1. CodeArtifact setup (domain, repos, external connection) — idempotent
  2. Package-lock.json parsing
  3. Fetch tarballs from Chainguard Libraries, publish to CodeArtifact

Trigger with an event like:
{
    "s3_bucket": "my-bucket",
    "s3_key": "path/to/package-lock.json",
    "setup_codeartifact": true          // optional, default false
}

Or inline:
{
    "lockfile_content": "{ ... raw JSON ... }",
    "setup_codeartifact": false
}

Environment variables:
    CGR_USERNAME            — Chainguard registry username  (required)
    CGR_TOKEN               — Chainguard registry token     (required)
    AWS_REGION              — AWS region           (default: us-east-2)
    CODEARTIFACT_DOMAIN     — domain name          (default: my-npm-domain)
    CODEARTIFACT_REPO       — repo name            (default: my-npm-repo)
    CODEARTIFACT_UPSTREAM   — upstream repo name   (default: npm-public)
    LOG_LEVEL               — DEBUG / INFO / WARN  (default: INFO)
"""

import json
import os
import base64
import hashlib
import logging
import urllib.request
import urllib.error
import urllib.parse
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

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# Reuse across invocations
ca_client = boto3.client("codeartifact", region_name=AWS_REGION)
s3_client = boto3.client("s3", region_name=AWS_REGION)


# ===================================================================
# 1. CodeArtifact setup  (idempotent — mirrors setup-codeartifact.sh)
# ===================================================================
def setup_codeartifact() -> str:
    """Create domain, upstream repo, external connection, and main repo.
    Returns the repository endpoint URL.
    """
    logger.info("Setting up CodeArtifact — domain=%s repo=%s upstream=%s",
                DOMAIN_NAME, REPO_NAME, UPSTREAM_REPO_NAME)

    # --- Domain -----------------------------------------------------------
    try:
        ca_client.create_domain(domain=DOMAIN_NAME)
        logger.info("Created domain '%s'", DOMAIN_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConflictException":
            logger.info("Domain '%s' already exists", DOMAIN_NAME)
        else:
            raise

    # --- Upstream repository ----------------------------------------------
    try:
        ca_client.create_repository(domain=DOMAIN_NAME, repository=UPSTREAM_REPO_NAME)
        logger.info("Created upstream repository '%s'", UPSTREAM_REPO_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConflictException":
            logger.info("Upstream repository '%s' already exists", UPSTREAM_REPO_NAME)
        else:
            raise

    # --- External connection to public NPM --------------------------------
    try:
        ca_client.associate_external_connection(
            domain=DOMAIN_NAME,
            repository=UPSTREAM_REPO_NAME,
            externalConnection="public:npmjs",
        )
        logger.info("Associated external connection public:npmjs")
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("ConflictException", "ResourceExistsException"):
            logger.info("External connection already associated")
        else:
            raise

    # --- Main repository with upstream ------------------------------------
    try:
        ca_client.create_repository(
            domain=DOMAIN_NAME,
            repository=REPO_NAME,
            upstreams=[{"repositoryName": UPSTREAM_REPO_NAME}],
        )
        logger.info("Created main repository '%s'", REPO_NAME)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "ConflictException":
            logger.info("Main repository '%s' already exists — updating upstreams", REPO_NAME)
            ca_client.update_repository(
                domain=DOMAIN_NAME,
                repository=REPO_NAME,
                upstreams=[{"repositoryName": UPSTREAM_REPO_NAME}],
            )
        else:
            raise

    # --- Endpoint ---------------------------------------------------------
    endpoint = ca_client.get_repository_endpoint(
        domain=DOMAIN_NAME,
        repository=REPO_NAME,
        format="npm",
    )["repositoryEndpoint"]
    logger.info("CodeArtifact endpoint: %s", endpoint)
    return endpoint


# ===================================================================
# 2. Package-lock.json parsing
# ===================================================================
def parse_lockfile(lockfile_json: dict) -> list[dict]:
    """Extract (name, version) pairs from a v2/v3 package-lock.json."""
    packages = lockfile_json.get("packages", {})
    results = []
    for key, meta in packages.items():
        if not key or not key.startswith("node_modules/"):
            continue
        name = key.removeprefix("node_modules/")
        version = meta.get("version")
        if name and version:
            results.append({"name": name, "version": version})
    logger.info("Parsed %d packages from lockfile", len(results))
    return results


# ===================================================================
# 3. Chainguard registry helpers
# ===================================================================
def _cgr_auth_header() -> str:
    """Return a Basic auth header value for the Chainguard npm registry."""
    credentials = f"{CGR_USERNAME}:{CGR_TOKEN}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return f"Basic {encoded}"


def fetch_chainguard_tarball(name: str, version: str) -> Optional[bytes]:
    """Attempt to download the package tarball from Chainguard Libraries.
    Returns tarball bytes on success, None if the package is not found.
    """
    # npm registry protocol: GET /{package} returns metadata JSON
    # which includes versions[version].dist.tarball URL
    encoded_name = urllib.parse.quote(name, safe="@")
    metadata_url = f"{CGR_REGISTRY}/{encoded_name}"
    auth = _cgr_auth_header()

    # Fetch metadata -------------------------------------------------------
    req = urllib.request.Request(metadata_url, headers={
        "Authorization": auth,
        "Accept": "application/json",
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            metadata = json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401, 403):
            logger.debug("Metadata not found for %s (%s)", name, exc.code)
            return None
        raise
    except urllib.error.URLError as exc:
        logger.warning("Network error fetching metadata for %s: %s", name, exc)
        return None

    # Locate tarball URL ---------------------------------------------------
    version_meta = metadata.get("versions", {}).get(version)
    if not version_meta:
        logger.debug("Version %s not found for %s in Chainguard", version, name)
        return None

    tarball_url = version_meta.get("dist", {}).get("tarball")
    if not tarball_url:
        logger.debug("No tarball URL for %s@%s", name, version)
        return None

    # Download tarball -----------------------------------------------------
    tar_req = urllib.request.Request(tarball_url, headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(tar_req, timeout=60) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        if exc.code in (404, 401, 403):
            logger.debug("Tarball download failed for %s@%s (%s)", name, version, exc.code)
            return None
        raise


# ===================================================================
# 4. CodeArtifact publish helpers
# ===================================================================
def _get_codeartifact_auth_token() -> str:
    return ca_client.get_authorization_token(
        domain=DOMAIN_NAME,
    )["authorizationToken"]


def _set_origin_controls(name: str):
    """Set package origin to ALLOW direct publish, BLOCK upstream.
    This ensures Chainguard packages take precedence over public NPM.
    """
    kwargs = dict(
        domain=DOMAIN_NAME,
        repository=REPO_NAME,
        format="npm",
        restrictions={"publish": "ALLOW", "upstream": "BLOCK"},
    )
    if name.startswith("@") and "/" in name:
        namespace, pkg = name.lstrip("@").split("/", 1)
        kwargs["namespace"] = namespace
        kwargs["package"] = pkg
    else:
        kwargs["package"] = name

    try:
        ca_client.put_package_origin_configuration(**kwargs)
    except ClientError:
        logger.debug("Could not set origin controls for %s (may already be set)", name)


def publish_to_codeartifact(
    name: str,
    version: str,
    tarball_bytes: bytes,
    endpoint: str,
    auth_token: str,
) -> bool:
    """Publish a tarball to CodeArtifact using the npm registry PUT protocol.
    Returns True on success (including 'already exists').
    """
    # Set origin controls first
    _set_origin_controls(name)

    # Build the npm publish payload ----------------------------------------
    tarball_b64 = base64.b64encode(tarball_bytes).decode()
    shasum = hashlib.sha1(tarball_bytes).hexdigest()
    integrity = "sha512-" + base64.b64encode(
        hashlib.sha512(tarball_bytes).digest()
    ).decode()

    # Attachment key: npm convention is {name}-{version}.tgz
    safe_name = name.replace("/", "-").lstrip("@-")
    attachment_key = f"{safe_name}-{version}.tgz"

    payload = {
        "_id": name,
        "name": name,
        "versions": {
            version: {
                "name": name,
                "version": version,
                "dist": {
                    "shasum": shasum,
                    "integrity": integrity,
                    "tarball": f"{endpoint.rstrip('/')}/{name}/-/{attachment_key}",
                },
            }
        },
        "_attachments": {
            attachment_key: {
                "content_type": "application/octet-stream",
                "data": tarball_b64,
                "length": len(tarball_bytes),
            }
        },
    }

    # PUT to the registry --------------------------------------------------
    encoded_name = urllib.parse.quote(name, safe="@")
    put_url = f"{endpoint.rstrip('/')}/{encoded_name}"
    body = json.dumps(payload).encode()

    req = urllib.request.Request(
        put_url,
        data=body,
        method="PUT",
        headers={
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            logger.info("Published %s@%s to CodeArtifact (%s)", name, version, resp.status)
            return True
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode(errors="replace")
        if exc.code == 409 or "already exists" in response_body.lower() \
                or "previously published" in response_body.lower() \
                or "EPUBLISHCONFLICT" in response_body:
            logger.info("Package %s@%s already exists in CodeArtifact", name, version)
            return True
        logger.error("Failed to publish %s@%s — HTTP %s: %s",
                      name, version, exc.code, response_body[:500])
        return False


# ===================================================================
# 5. Lambda handler
# ===================================================================
def lambda_handler(event, context):
    """
    Event schema:
        s3_bucket           (str)  — S3 bucket containing package-lock.json
        s3_key              (str)  — S3 key for package-lock.json
          — OR —
        lockfile_content    (str)  — raw JSON string of the lockfile
        setup_codeartifact  (bool) — run idempotent setup first (default False)
    """
    # Validate credentials
    if not CGR_USERNAME or not CGR_TOKEN:
        return _error("CGR_USERNAME and CGR_TOKEN environment variables are required")

    # Optional: run CodeArtifact setup
    if event.get("setup_codeartifact", False):
        endpoint = setup_codeartifact()
    else:
        endpoint = ca_client.get_repository_endpoint(
            domain=DOMAIN_NAME,
            repository=REPO_NAME,
            format="npm",
        )["repositoryEndpoint"]

    # Load lockfile --------------------------------------------------------
    if "lockfile_content" in event:
        lockfile = json.loads(event["lockfile_content"])
    elif "s3_bucket" in event and "s3_key" in event:
        obj = s3_client.get_object(Bucket=event["s3_bucket"], Key=event["s3_key"])
        lockfile = json.loads(obj["Body"].read())
    else:
        return _error("Provide either 'lockfile_content' or 's3_bucket'+'s3_key'")

    packages = parse_lockfile(lockfile)
    if not packages:
        return _success(message="No packages found in lockfile", summary=_empty_summary())

    # Get CodeArtifact auth token
    auth_token = _get_codeartifact_auth_token()

    # Process packages -----------------------------------------------------
    found, not_found, failed, already_exists = 0, 0, 0, 0
    not_found_list: list[str] = []

    for pkg in packages:
        name, version = pkg["name"], pkg["version"]
        logger.info("Processing %s@%s", name, version)

        tarball = fetch_chainguard_tarball(name, version)
        if tarball is None:
            not_found += 1
            not_found_list.append(f"{name}@{version}")
            logger.info("  Not found in Chainguard — will use NPM fallback")
            continue

        found += 1
        ok = publish_to_codeartifact(name, version, tarball, endpoint, auth_token)
        if not ok:
            failed += 1

    summary = {
        "total_packages": len(packages),
        "found_in_chainguard": found,
        "not_found_npm_fallback": not_found,
        "failed_to_publish": failed,
        "not_found_packages": not_found_list,
    }

    logger.info("Sync complete — %s", json.dumps(summary, indent=2))
    return _success(message="Sync complete", summary=summary)


# ===================================================================
# Helpers
# ===================================================================
def _empty_summary():
    return {
        "total_packages": 0,
        "found_in_chainguard": 0,
        "not_found_npm_fallback": 0,
        "failed_to_publish": 0,
        "not_found_packages": [],
    }


def _success(message: str, summary: dict):
    return {"statusCode": 200, "body": {"message": message, "summary": summary}}


def _error(message: str):
    logger.error(message)
    return {"statusCode": 400, "body": {"error": message}}
