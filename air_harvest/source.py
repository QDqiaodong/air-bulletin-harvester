"""Bounded acquisition from an explicit file or local fixture HTTP endpoint."""
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import build_opener, HTTPRedirectHandler

MAX_BYTES = 2 * 1024 * 1024


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("redirects are outside the local source contract")


def acquire(source, http=False):
    if http:
        parsed = urlsplit(source)
        if (parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
                or parsed.username or parsed.password):
            raise ValueError("HTTP acquisition is limited to local fixture endpoints")
        with build_opener(NoRedirects()).open(source, timeout=5) as response:
            raw = response.read(MAX_BYTES + 1)
    else:
        with Path(source).open("rb") as stream:
            raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES:
        raise ValueError("source exceeds 2 MiB limit")
    return raw
