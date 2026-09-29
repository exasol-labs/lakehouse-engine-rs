#!/usr/bin/env python3
"""Local AWS STS `AssumeRole` stub for the assume-role E2E suite (issue #139).

MinIO's own `AssumeRole` ignores `RoleArn` and inherits the caller's policy —
it cannot model role assumption. This stub stands in for `sts.amazonaws.com`
in front of the stack's MinIO: it verifies the caller's SigV4 signature
independently of the engine's own signer (service `sts`, the configured base
identity), checks `RoleArn` and `ExternalId`, then mints a real MinIO STS
session as the configured role user and hands it back verbatim in the AWS
`AssumeRoleResponse` shape. Python standard library only; every identity and
endpoint is configured through environment variables (see docker-compose.yml's
`sts-stub` service).

Endpoints:
  GET  /health       -> 200, liveness probe
  GET  /__requests   -> 200, {"count": N} AssumeRole attempts seen so far
  GET or POST (form) with Action=AssumeRole -> AWS AssumeRoleResponse or
      ErrorResponse (403 AccessDenied / SignatureDoesNotMatch)
"""

import datetime
import hashlib
import hmac
import http.server
import json
import os
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from xml.sax.saxutils import escape as xml_escape

PORT = int(os.environ.get("STS_STUB_PORT", "8080"))
BASE_ACCESS_KEY = os.environ["STS_STUB_BASE_ACCESS_KEY"]
BASE_SECRET_KEY = os.environ["STS_STUB_BASE_SECRET_KEY"]
ROLE_ARN = os.environ["STS_STUB_ROLE_ARN"]
EXTERNAL_ID = os.environ.get("STS_STUB_EXTERNAL_ID") or None
MINIO_ENDPOINT = os.environ["STS_STUB_MINIO_ENDPOINT"]
ROLE_ACCESS_KEY = os.environ["STS_STUB_ROLE_ACCESS_KEY"]
ROLE_SECRET_KEY = os.environ["STS_STUB_ROLE_SECRET_KEY"]

# AWS STS's default AssumeRole session lifetime, which the engine relies on (it sends no DurationSeconds).
SESSION_DURATION_SECONDS = "3600"
ROLE_SESSION_NAME = "lakehouse-sts-stub"

_request_count = 0
_count_lock = threading.Lock()


def _record_assume_role_request():
    global _request_count
    with _count_lock:
        _request_count += 1
        return _request_count


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sign(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def _signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _sign(f"AWS4{secret_key}".encode(), date_stamp)
    k_region = _sign(k_date, region)
    k_service = _sign(k_region, service)
    return _sign(k_service, "aws4_request")


_AUTH_RE = re.compile(
    r"^AWS4-HMAC-SHA256 Credential=(?P<access_key>[^/]+)/(?P<date>[0-9]+)/"
    r"(?P<region>[^/]+)/(?P<service>[^/]+)/aws4_request, ?"
    r"SignedHeaders=(?P<signed_headers>[^,]+), ?Signature=(?P<signature>[0-9a-f]+)$"
)


class AssumeRoleError(Exception):
    """A rejected AssumeRole attempt: `code` is an AWS STS error code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _verify_signature(
    method: str,
    canonical_query: str,
    headers: Message,
    body: bytes,
) -> None:
    """Recomputes the SigV4 signature independently of the engine's own signer
    and raises `AssumeRoleError("SignatureDoesNotMatch", ...)` on any mismatch."""
    auth_header = headers.get("Authorization")
    if not auth_header:
        raise AssumeRoleError("SignatureDoesNotMatch", "request carries no Authorization header")
    match = _AUTH_RE.match(auth_header)
    if not match:
        raise AssumeRoleError("SignatureDoesNotMatch", "Authorization header is not a well-formed SigV4 header")
    if match.group("access_key") != BASE_ACCESS_KEY:
        raise AssumeRoleError("SignatureDoesNotMatch", "the request is not signed by the configured base access key")
    if match.group("service") != "sts":
        raise AssumeRoleError("SignatureDoesNotMatch", "the request is not signed for service sts")

    signed_header_names = match.group("signed_headers").split(";")
    canonical_headers = "".join(
        f"{name}:{(headers.get(name) or '').strip()}\n" for name in signed_header_names
    )
    payload_hash = (
        headers.get("x-amz-content-sha256")
        if "x-amz-content-sha256" in signed_header_names
        else _sha256_hex(body)
    )
    canonical_request = "\n".join(
        [
            method,
            "/",
            canonical_query,
            canonical_headers,
            ";".join(signed_header_names),
            payload_hash or "",
        ]
    )
    date_stamp = match.group("date")
    region = match.group("region")
    service = match.group("service")
    amz_date = headers.get("x-amz-date") or ""
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        ["AWS4-HMAC-SHA256", amz_date, credential_scope, _sha256_hex(canonical_request.encode())]
    )
    expected_signature = hmac.new(
        _signing_key(BASE_SECRET_KEY, date_stamp, region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(expected_signature, match.group("signature")):
        raise AssumeRoleError("SignatureDoesNotMatch", "the request signature does not match")


def _canonical_query_from_pairs(pairs: list[tuple[str, str]]) -> str:
    """SigV4 canonical query string: the received (already percent-encoded)
    pairs, sorted by key — sorting, not re-encoding, is all that is needed
    since every value arrives already canonically encoded."""
    return "&".join(f"{k}={v}" for k, v in sorted(pairs))


def _parse_pairs(raw: str) -> list[tuple[str, str]]:
    pairs = []
    for part in raw.split("&"):
        if not part:
            continue
        key, _, value = part.partition("=")
        pairs.append((key, value))
    return pairs


def _decoded(pairs: list[tuple[str, str]]) -> dict[str, str]:
    return {k: urllib.parse.unquote(v) for k, v in pairs}


def _assume_role_via_minio() -> bytes:
    """Mints a session as the configured MinIO role user and returns MinIO's
    raw `AssumeRoleResponse` XML bytes verbatim — MinIO already answers in the
    exact shape the engine's `quick-xml` parser expects."""
    host = MINIO_ENDPOINT.split("://", 1)[1]
    body = (
        "Action=AssumeRole&Version=2011-06-15"
        f"&DurationSeconds={SESSION_DURATION_SECONDS}&RoleSessionName={ROLE_SESSION_NAME}"
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = _sha256_hex(body.encode())
    region, service = "us-east-1", "sts"

    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"POST\n/\n\ncontent-type:application/x-www-form-urlencoded\nhost:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\nx-amz-date:{stamp}\n\n"
        f"{signed_headers}\n{payload_hash}"
    )
    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = (
        f"AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{_sha256_hex(canonical_request.encode())}"
    )
    signature = hmac.new(
        _signing_key(ROLE_SECRET_KEY, date_stamp, region, service),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()

    request = urllib.request.Request(
        MINIO_ENDPOINT + "/",
        data=body.encode(),
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Host": host,
            "X-Amz-Content-Sha256": payload_hash,
            "X-Amz-Date": stamp,
            "Authorization": (
                f"AWS4-HMAC-SHA256 Credential={ROLE_ACCESS_KEY}/{scope}, "
                f"SignedHeaders={signed_headers}, Signature={signature}"
            ),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.read()
    except urllib.error.URLError as error:
        raise AssumeRoleError(
            "AccessDenied", f"the stub's own MinIO AssumeRole call failed: {error}"
        ) from error


def _error_response_xml(code: str, message: str) -> bytes:
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        "<ErrorResponse>"
        "<Error>"
        "<Type>Sender</Type>"
        f"<Code>{xml_escape(code)}</Code>"
        f"<Message>{xml_escape(message)}</Message>"
        "</Error>"
        "<RequestId>sts-stub</RequestId>"
        "</ErrorResponse>"
    ).encode()


def _handle_assume_role(method: str, params: dict[str, str], canonical_query: str, headers: Message, body: bytes):
    if params.get("Action") != "AssumeRole":
        return 400, _error_response_xml("InvalidAction", f"unsupported Action {params.get('Action')!r}")

    _record_assume_role_request()
    try:
        _verify_signature(method, canonical_query, headers, body)

        role_arn = params.get("RoleArn")
        if role_arn != ROLE_ARN:
            raise AssumeRoleError(
                "AccessDenied", f"role '{role_arn}' is not assumable by this identity"
            )
        if EXTERNAL_ID is not None and params.get("ExternalId") != EXTERNAL_ID:
            raise AssumeRoleError(
                "AccessDenied", f"role '{ROLE_ARN}' requires a matching ExternalId"
            )

        return 200, _assume_role_via_minio()
    except AssumeRoleError as error:
        return 403, _error_response_xml(error.code, error.message)


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "sts-stub/1.0"

    def _reply(self, status: int, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802 (BaseHTTPRequestHandler's naming convention)
        path, _, raw_query = self.path.partition("?")
        if path == "/health":
            self._reply(200, b'{"status":"ok"}', "application/json")
            return
        if path == "/__requests":
            with _count_lock:
                count = _request_count
            self._reply(200, json.dumps({"count": count}).encode(), "application/json")
            return

        pairs = _parse_pairs(raw_query)
        status, body = _handle_assume_role(
            "GET", _decoded(pairs), _canonical_query_from_pairs(pairs), self.headers, b""
        )
        self._reply(status, body, "text/xml")

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        pairs = _parse_pairs(body.decode("utf-8", errors="replace"))
        status, response_body = _handle_assume_role("POST", _decoded(pairs), "", self.headers, body)
        self._reply(status, response_body, "text/xml")

    def log_message(self, format_str, *args):  # noqa: A002, N802
        # Keep container logs readable; the stub's own /__requests endpoint is
        # the suite's real observability channel, not stdout access logs.
        pass


def main():
    # NOSONAR python:S5332: local-only test double; the stub asserts SigV4, not transport security.
    http.server.ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()  # NOSONAR


if __name__ == "__main__":
    main()
