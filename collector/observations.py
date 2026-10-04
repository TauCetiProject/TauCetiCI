"""Collect the minute measurements archived by the existing Cloudflare heartbeat."""
import datetime as dt
import json
import urllib.parse
import urllib.request

URL = "https://bors.taucetiproject.org/api/merge-observations"


def batch_members(batch_id, head):
    url = f"https://bors.taucetiproject.org/repositories/1/active-batches?base=main&batch_id={int(batch_id)}"
    with urllib.request.urlopen(url, timeout=10) as response:
        data = json.load(response)
    batch = data.get("requested_batch")
    if (data.get("repo") != "TauCetiProject/TauCeti" or data.get("base") != "main"
            or not batch or batch.get("id") != batch_id or batch.get("head_sha") != head):
        raise RuntimeError("batch observation does not match the actual tested head")
    return batch["members"]


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
