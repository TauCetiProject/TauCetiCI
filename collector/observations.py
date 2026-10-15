"""Collect the minute measurements archived by the existing Cloudflare heartbeat."""
import datetime as dt
import json
import urllib.parse
import urllib.request

URL = "https://bors.taucetiproject.org/api/merge-observations"


def read_json(url, timeout):
    request = urllib.request.Request(url, headers={
        "User-Agent": "TauCetiCI/1.0", "Accept": "application/json",
        "Cache-Control": "no-cache",
    })
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def batch_members(batch_id, head):
    url = f"https://bors.taucetiproject.org/repositories/1/active-batches?base=main&batch_id={int(batch_id)}"
    data = read_json(url, timeout=10)
    batch = data.get("requested_batch")
    if (data.get("repo") != "TauCetiProject/TauCeti" or data.get("base") != "main"
            or not batch or batch.get("id") != batch_id or batch.get("head_sha") != head):
        raise RuntimeError("batch observation does not match the actual tested head")
    return batch["members"]


def read_days(since, until, *, before_request=None, day_done=None, resume=None, page_done=None):
    # R2 pages are immutable prefixes. Resume at the first uncommitted page;
    # replay the last page on the current day to discover newly appended rows.
    day = max(since.date(), dt.date.fromisoformat(resume['day'])) if resume else since.date()
    while day <= until.date():
        cursor = resume.get('cursor') if resume and resume['day'] == day.isoformat() else None
        seen = set()
        future_seen, replay_cursor = False, None
        while True:
            query = {"day": day.isoformat()}
            if cursor:
                query["cursor"] = cursor
            if before_request:
                before_request()
            page = read_json(URL + "?" + urllib.parse.urlencode(query), timeout=30)
            for obs in page["observations"]:
                when = dt.datetime.fromisoformat(obs["observed_at"].replace("Z", "+00:00"))
                if when > until and not future_seen:
                    # The archive can grow while this invocation is running.
                    # Keep the first page with future rows replayable even if
                    # additional pages follow it.
                    future_seen, replay_cursor = True, cursor
                if since <= when <= until:
                    yield obs
            if page_done:
                page_done({'day': day.isoformat(), 'cursor':
                           replay_cursor if future_seen else page['cursor'] if page['truncated'] else cursor})
            if page["truncated"] is False:
                break
            cursor = page["cursor"]
            if not cursor or cursor in seen:
                raise RuntimeError("invalid observation pagination")
            seen.add(cursor)
        if day_done:
            day_done(min(until, dt.datetime.combine(day + dt.timedelta(days=1), dt.time(), since.tzinfo)))
        day += dt.timedelta(days=1)
