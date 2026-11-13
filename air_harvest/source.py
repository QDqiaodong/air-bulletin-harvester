"""Bounded acquisition from an explicit file or loopback HTTP endpoint.

The HTTP transport here is deliberately narrow: the raw caller string is only
used to address a caller-controlled loopback fixture.  Scheme, host, port,
credentials, fragments and redirects are checked before any socket opens, and
the response body is bounded by :data:`MAX_BYTES` with a short-read check.
"""
from __future__ import annotations

import hashlib
import http.client
import socket
from dataclasses import dataclass
from datetime import timezone
from email.utils import format_datetime, parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

MAX_BYTES = 2 * 1024 * 1024
TIMEOUT = 5.0


class AcquisitionError(ValueError):
    """The source could not be acquired under this module's contract."""


class InvalidResponse(AcquisitionError):
    """The response violates the HTTP or bulletin transport contract."""


class ResponseTooLarge(AcquisitionError):
    """The response body exceeds the 2 MiB bound."""


class StaleResponse(AcquisitionError):
    """A response arrived after the stored state for its URL had advanced."""


@dataclass(frozen=True)
class Validators:
    etag: str | None
    last_modified: str | None


@dataclass(frozen=True)
class Response:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes

    def __post_init__(self):
        object.__setattr__(
            self, "headers",
            tuple((str(name).lower(), str(value)) for name, value in self.headers))

    def values(self, name):
        wanted = name.lower()
        return [value for key, value in self.headers if key == wanted]

    def validators(self):
        return Validators(parse_etag_header(self), parse_last_modified_header(self))


def _check_url_text(source):
    if not isinstance(source, str) or not source:
        raise AcquisitionError("URL must be a non-empty string")
    for character in source:
        if ord(character) < 0x20 or ord(character) == 0x7F or character.isspace():
            raise AcquisitionError(
                "URL must not contain control characters or whitespace")
    if "#" in source:
        raise AcquisitionError("URL fragments are outside the local source contract")


LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _is_loopback(hostname):
    return hostname in LOOPBACK_HOSTS


def parse_url(source):
    """Validate a loopback HTTP URL and return its parsed components."""
    _check_url_text(source)
    try:
        parsed = urlsplit(source)
        port = parsed.port
    except ValueError as exc:
        raise AcquisitionError(f"invalid URL: {exc}") from exc
    if parsed.scheme != "http":
        raise AcquisitionError("only plain HTTP loopback URLs are accepted")
    if parsed.hostname is None or not _is_loopback(parsed.hostname.lower()):
        raise AcquisitionError("HTTP acquisition is limited to local fixture endpoints")
    if parsed.username is not None or parsed.password is not None:
        raise AcquisitionError("URL credentials are outside the local source contract")
    if parsed.fragment:
        raise AcquisitionError("URL fragments are outside the local source contract")
    if port is not None and not 0 < port < 65536:
        raise AcquisitionError("URL port must be between 1 and 65535")
    return parsed


def canonical_url(source):
    """Return a stable logical form of a loopback URL.

    The raw caller string is never used as an identity or a path.  Scheme,
    lowercase host, an explicit effective port, path and query are retained;
    the fragment is rejected by :func:`parse_url`.
    """
    parsed = parse_url(source)
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    port = parsed.port or 80
    return urlunsplit(("http", f"{host}:{port}", parsed.path or "/", parsed.query, ""))


def url_identity(source):
    """Return the SHA256 digest of the canonical URL (the URL key)."""
    return hashlib.sha256(canonical_url(source).encode("utf-8")).hexdigest()


def _request_target(parsed):
    return urlunsplit(("", "", parsed.path or "/", parsed.query, ""))


def parse_etag_header(response):
    values = response.values("etag")
    if not values:
        return None
    distinct = {value.strip() for value in values}
    if len(distinct) != 1:
        raise InvalidResponse("conflicting ETag response headers")
    raw = next(iter(distinct))
    opaque = raw[2:] if raw.startswith("W/") else raw
    if len(opaque) < 2 or not (opaque.startswith('"') and opaque.endswith('"')):
        raise InvalidResponse(f"malformed ETag response header: {raw!r}")
    interior = opaque[1:-1]
    for character in interior:
        if character == '"' or ord(character) < 0x21 or ord(character) == 0x7F:
            raise InvalidResponse(f"malformed ETag response header: {raw!r}")
    return raw


def parse_last_modified_header(response):
    values = response.values("last-modified")
    if not values:
        return None
    distinct = {value.strip() for value in values}
    if len(distinct) != 1:
        raise InvalidResponse("conflicting Last-Modified response headers")
    raw = next(iter(distinct))
    try:
        moment = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise InvalidResponse(f"malformed Last-Modified header: {raw!r}") from exc
    if moment is None or moment.tzinfo is None:
        raise InvalidResponse(f"malformed Last-Modified header: {raw!r}")
    return format_datetime(moment.astimezone(timezone.utc), usegmt=True)


def _content_length(response):
    values = response.values("content-length")
    if not values:
        return None
    distinct = {value.strip() for value in values}
    if len(distinct) != 1:
        raise InvalidResponse("conflicting Content-Length response headers")
    raw = next(iter(distinct))
    if not raw.isascii() or not raw.isdigit():
        raise InvalidResponse(f"malformed Content-Length response header: {raw!r}")
    length = int(raw)
    if length > MAX_BYTES:
        raise ResponseTooLarge("response Content-Length exceeds the 2 MiB limit")
    return length


def _check_content_type(response):
    values = response.values("content-type")
    for value in values:
        for parameter in value.split(";")[1:]:
            name, _, parameter_value = parameter.partition("=")
            if name.strip().lower() == "charset":
                charset = parameter_value.strip().strip('"').lower()
                if charset not in {"utf-8", "utf8"}:
                    raise InvalidResponse(
                        f"unsupported response charset {charset!r}; UTF-8 is required")
    encodings = {value.strip().lower() for value in response.values("content-encoding")}
    if encodings - {"identity"}:
        raise InvalidResponse("response content encoding is not supported")


def _read_body(connection, response):
    try:
        body = response.read(MAX_BYTES + 1)
    except http.client.IncompleteRead as exc:
        raise InvalidResponse(
            "short read: response ended before its declared Content-Length") from exc
    except (socket.timeout, TimeoutError) as exc:
        raise AcquisitionError("response read timed out") from exc
    except OSError as exc:
        raise AcquisitionError(f"response read failed: {exc}") from exc
    if len(body) > MAX_BYTES:
        raise ResponseTooLarge("response body exceeds the 2 MiB limit")
    return body


def fetch_response(source, conditional_headers=None):
    """Issue one bounded, non-redirecting GET against a loopback URL.

    No HTTP library redirect handling is used; a redirect status is returned
    to the caller as a normal (rejected) status.  The body is read only for a
    200 response; a 304 must not carry one and any other status carries an
    empty body.
    """
    parsed = parse_url(source)
    headers = {"Accept": "text/html", "Connection": "close"}
    headers.update(conditional_headers or {})
    connection = http.client.HTTPConnection(
        parsed.hostname, parsed.port or 80, timeout=TIMEOUT)
    try:
        connection.request("GET", _request_target(parsed), headers=headers)
        raw_response = connection.getresponse()
        status = raw_response.status
        response = Response(
            status,
            tuple((name.lower(), value) for name, value in raw_response.getheaders()),
            b"")
        if status == 200:
            declared = _content_length(response)
            _check_content_type(response)
            body = _read_body(connection, raw_response)
            if declared is not None and len(body) != declared:
                raise InvalidResponse(
                    "short read: response body length does not match Content-Length")
            return Response(status, response.headers, body)
        if status == 304:
            declared = _content_length(response)
            if declared not in (None, 0):
                raise InvalidResponse("a 304 response must not carry a body")
            # No body is read: a 304 has none, and reading it could block on a
            # keep-alive connection that sends no framing bytes.
            return response
        return response
    except AcquisitionError:
        raise
    except (http.client.HTTPException, OSError, TimeoutError) as exc:
        raise AcquisitionError(f"request failed: {exc}") from exc
    finally:
        connection.close()


def acquire(source, http=False):
    """Legacy bounded byte acquisition used by the original CLI and demo."""
    if http:
        response = fetch_response(str(source))
        if response.status == 304:
            raise InvalidResponse("unexpected 304 response to an unconditional request")
        if response.status != 200:
            raise InvalidResponse(f"unexpected HTTP status {response.status}")
        return response.body
    with Path(source).open("rb") as stream:
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("source exceeds 2 MiB limit")
    return raw
