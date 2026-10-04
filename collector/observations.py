"""Collect the minute measurements archived by the existing Cloudflare heartbeat."""
import datetime as dt
import json
import urllib.parse
import urllib.request

URL = "https://bors.taucetiproject.org/api/merge-observations"


def read_days(since, until):
    day = since.date()
    while day <= until.date():
        cursor, seen = None, set()
        while True:
            query = {"day": day.isoformat()}
            if cursor:
                query["cursor"] = cursor
            with urllib.request.urlopen(URL + "?" + urllib.parse.urlencode(query), timeout=30) as r:
                page = json.load(r)
            for obs in page["observations"]:
                when = dt.datetime.fromisoformat(obs["observed_at"].replace("Z", "+00:00"))
                if since <= when <= until:
                    yield obs
            if page["truncated"] is False:
                break
            cursor = page["cursor"]
            if not cursor or cursor in seen:
                raise RuntimeError("invalid observation pagination")
            seen.add(cursor)
        day += dt.timedelta(days=1)
