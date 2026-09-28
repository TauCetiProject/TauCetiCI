"""A small GitHub REST client: stdlib only, token from GH_TOKEN, retries, rate-limit aware."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.github.com"


def _token() -> str:
    tok = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if tok:
        return tok
    # Local runs: borrow the gh CLI's credentials.
    return subprocess.run(["gh", "auth", "token"], capture_output=True, text=True, check=True).stdout.strip()


class GitHub:
    def __init__(self, min_remaining: int = int(os.environ.get("MIN_REMAINING", "100"))):
        self.token = _token()
        self.min_remaining = min_remaining
        self.calls = 0
        self.remaining: int | None = None
        self.reset: int | None = None
        self._lock = threading.Lock()

    def _count(self, headers: dict | None = None):
        with self._lock:
            self.calls += 1
            if headers and "x-ratelimit-remaining" in headers:
                self.remaining = int(headers["x-ratelimit-remaining"])
                self.reset = int(headers.get("x-ratelimit-reset", "0"))

    def _request(self, url: str, accept: str = "application/vnd.github+json") -> tuple[bytes, dict]:
        if not url.startswith("http"):
            url = f"{API}/{url.lstrip('/')}"
        for attempt in range(6):
            if self.remaining is not None and self.remaining < self.min_remaining and self.reset:
                wait = max(0, self.reset - time.time()) + 5
                print(f"rate limit low ({self.remaining}); sleeping {wait:.0f}s", file=sys.stderr)
                time.sleep(wait)
                self.remaining = None
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": accept,
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "tauceti-ci-collector",
            })
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._count(headers)
                    return resp.read(), headers
            except urllib.error.HTTPError as e:
                self._count()
                if e.code in (404, 410, 422):
                    raise
                if e.code in (403, 429) and e.headers.get("x-ratelimit-remaining") == "0":
                    wait = max(0, int(e.headers.get("x-ratelimit-reset", "0")) - time.time()) + 5
                    print(f"rate limited; sleeping {wait:.0f}s", file=sys.stderr)
                    time.sleep(wait)
                    continue
                if e.code in (403, 429) and e.headers.get("retry-after"):
                    time.sleep(int(e.headers["retry-after"]) + 1)
                    continue
                if e.code >= 500 or e.code in (403, 429):
                    time.sleep(2 ** attempt)
                    continue
                raise
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                time.sleep(2 ** attempt)
        raise RuntimeError(f"GitHub API request failed repeatedly: {url}")

    def get(self, path: str, **params) -> dict | list:
        url = path if not params else f"{path}?{urllib.parse.urlencode(params)}"
        body, _ = self._request(url)
        return json.loads(body)

    def paginate(self, path: str, key: str | None = None, **params):
        """Yield items across pages. `key` selects the list inside an object response."""
        params.setdefault("per_page", 100)
        url = f"{path}?{urllib.parse.urlencode(params)}"
        while url:
            body, headers = self._request(url)
            data = json.loads(body)
            yield from (data[key] if key else data)
            m = re.search(r'<([^>]+)>;\s*rel="next"', headers.get("link", ""))
            url = m.group(1) if m else None

    def text(self, path: str) -> str:
        """Fetch a job log (see `raw`)."""
        return self.raw(path).decode("utf-8", errors="replace")

    def raw(self, path: str) -> bytes:
        """Fetch a log or artifact archive. The API answers with a redirect to signed blob storage,
        which must be followed WITHOUT the Authorization header."""
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None
        opener = urllib.request.build_opener(NoRedirect)
        req = urllib.request.Request(f"{API}/{path.lstrip('/')}", headers={
            "Authorization": f"Bearer {self.token}", "User-Agent": "tauceti-ci-collector"})
        self._count()
        try:
            with opener.open(req, timeout=60) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code not in (301, 302, 303, 307, 308):
                raise
            location = e.headers["location"]
        with urllib.request.urlopen(location, timeout=120) as resp:
            return resp.read()
