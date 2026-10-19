#!/usr/bin/env python3
"""One-time resumable archive import. Uses the existing derived database, never GitHub.

Build the input once with collector.build_db. Replay the same database safely;
each event has a content identity and the disposable local receipt database
skips confirmed imports. Existing SQLite release/export remains available.
"""

import argparse
import hashlib
import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path


def events(db, since):
    db.row_factory = sqlite3.Row
    weights = {
        (r["workflow"], r["hour"]): (r["total"], r["fetched"] or 0)
        for r in db.execute(
            """
        SELECT workflow,substr(created_at,1,13) hour,COUNT(*) total,SUM(jobs_fetched) fetched
        FROM runs WHERE created_at>=? AND conclusion!='skipped' GROUP BY 1,2""",
            (since,),
        )
    }
    typical = {}
    for (
        workflow,
        duration,
    ) in db.execute("""SELECT r.workflow,j.run_s FROM jobs j JOIN runs r USING(repo,run_id)
        WHERE j.run_s IS NOT NULL AND j.runner_kind='github' ORDER BY r.workflow,j.run_s"""):
        typical.setdefault(workflow, []).append(duration)
    medians = {k: v[len(v) // 2] for k, v in typical.items()}
    for row in db.execute(
        "SELECT * FROM runs WHERE created_at>=? ORDER BY repo,run_id", (since,)
    ):
        r = dict(row)
        repo = "TauCetiProject/" + r["repo"]
        merge = db.execute(
            "SELECT * FROM merge_builds WHERE repo=? AND run_id=?",
            (r["repo"], r["run_id"]),
        ).fetchone()
        tested = (
            {
                "head_sha": merge["head_sha"],
                "batch_id": merge["batch_id"],
                "batch_members": json.loads(merge["members_json"] or "[]"),
            }
            if merge
            else {}
        )
        jobs = [
            dict(j)
            for j in db.execute(
                "SELECT * FROM jobs WHERE repo=? AND run_id=?", (r["repo"], r["run_id"])
            )
        ]
        grouped = {}
        for j in jobs:
            j["id"] = str(j.pop("job_id"))
            j["run_id"] = str(r["run_id"])
            j["assigned"] = j["run_s"] is not None
            j["labels"] = json.loads(j["labels"] or "[]")
            j["status"] = (
                "completed"
                if j["conclusion"]
                else "in_progress"
                if j["started_at"]
                else "queued"
            )
            j["steps"] = [
                dict(s)
                for s in db.execute(
                    "SELECT * FROM steps WHERE job_id=? ORDER BY n", (int(j["id"]),)
                )
            ]
            total, fetched = weights.get((r["workflow"], r["created_at"][:13]), (1, 1))
            j["weight"] = total / fetched if fetched else 1
            grouped.setdefault(j["run_attempt"] or 1, []).append(j)
        # Historical unsampled workflow-hours retain the previous running-only
        # duration estimate. They never establish queue or wait observations.
        total, fetched = weights.get((r["workflow"], r["created_at"][:13]), (1, 1))
        if (
            not jobs
            and not fetched
            and r["conclusion"] != "skipped"
            and r["updated_at"]
        ):
            from datetime import datetime, timedelta

            end = datetime.fromisoformat(r["updated_at"])
            created = datetime.fromisoformat(r["created_at"])
            start = max(
                created, end - timedelta(seconds=medians.get(r["workflow"], 15))
            )
            grouped[r["run_attempt"]] = [
                {
                    "id": "9" + str(r["run_id"]),
                    "run_id": str(r["run_id"]),
                    "run_attempt": r["run_attempt"],
                    "name": "estimated auxiliary job",
                    "status": "completed",
                    "conclusion": r["conclusion"],
                    "created_at": r["created_at"],
                    "started_at": start.isoformat(),
                    "completed_at": end.isoformat(),
                    "runner_kind": "github",
                    "assigned": True,
                    "estimated": True,
                }
            ]
        if not grouped and not merge:
            # Sample weights already represent these runs. Their original
            # metadata remains in git and the downloadable database.
            continue
        grouped.setdefault(r["run_attempt"], [])
        for attempt, attempt_jobs in sorted(grouped.items()):
            run = {
                **r,
                "id": str(r["run_id"]),
                "run_attempt": attempt,
                "tested": tested,
                "status": "completed" if r["conclusion"] else "queued",
            }
            # Only the latest run's conclusion is known in the legacy runs table;
            # job conclusions retain the results of every earlier execution.
            record = {
                "repo": repo,
                "run": run,
                "jobs": attempt_jobs,
                "verified_jobs": bool(attempt_jobs),
                "historical": True,
            }
            tel = db.execute(
                "SELECT phases_json FROM telemetry WHERE repo=? AND run_id=?",
                (r["repo"], r["run_id"]),
            ).fetchone()
            if tel and attempt == r["run_attempt"]:
                record["telemetry"] = {"phases": json.loads(tel[0] or "[]")}
            yield record
    for row in db.execute(
        "SELECT * FROM merge_observations WHERE observed_at>=? ORDER BY observed_at",
        (since,),
    ):
        o = dict(row)
        o["eligible"] = json.loads(o.pop("eligible_json") or "[]")
        metadata = db.execute(
            "SELECT * FROM queue_observation_metadata WHERE observed_at=?",
            (o["observed_at"],),
        ).fetchone()
        if metadata:
            o.update(dict(metadata))
        yield {"repo": "TauCetiProject/TauCeti", "observation": o, "historical": True}
    for row in db.execute(
        """SELECT c.pr,MIN(l.landed_at) at FROM main_commits c JOIN main_landings l ON l.head_sha=c.sha
        WHERE l.repo='TauCeti' AND c.pr IS NOT NULL GROUP BY c.pr HAVING MIN(l.landed_at)>=?""",
        (since,),
    ):
        from datetime import datetime

        yield {
            "repo": "TauCetiProject/TauCeti",
            "landing": {
                "pr": row["pr"],
                "at": int(datetime.fromisoformat(row["at"]).timestamp()),
            },
            "historical": True,
        }
    for row in db.execute(
        "SELECT repo,stream,since,complete_to FROM stream_coverage WHERE stream='inventory' AND complete_to>=?",
        (since,),
    ):
        from datetime import datetime

        yield {
            "repo": "TauCetiProject/" + row["repo"],
            "coverage": {
                "stream": row["stream"],
                "since": int(datetime.fromisoformat(row["since"]).timestamp()),
                "until": int(datetime.fromisoformat(row["complete_to"]).timestamp()),
            },
            "historical": True,
        }
    for row in db.execute("SELECT at,text FROM annotations WHERE at>=?", (since,)):
        from datetime import datetime

        yield {
            "repo": "TauCetiProject/TauCeti",
            "annotation": {
                "at": int(datetime.fromisoformat(row["at"]).timestamp()),
                "text": row["text"],
            },
            "historical": True,
        }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--since", default="2026-07-20T00:00:00Z")
    ap.add_argument("--url", default="https://ci-data.taucetiproject.org")
    ap.add_argument(
        "--token-file",
        type=Path,
        default=Path.home() / ".config/tauceti-live-ci/admin-token",
    )
    ap.add_argument(
        "--receipts",
        type=Path,
        default=Path.home() / ".local/state/tauceti-live-ci/import.sqlite",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    args.receipts.parent.mkdir(parents=True, exist_ok=True)
    receipts = sqlite3.connect(args.receipts)
    receipts.execute("CREATE TABLE IF NOT EXISTS imported(id TEXT PRIMARY KEY)")
    token = args.token_file.read_text().strip() if not args.dry_run else ""
    count = 0
    batch = []
    identities = []
    batch_bytes = 0

    def publish():
        nonlocal count, batch_bytes
        if not batch:
            return
        request = urllib.request.Request(
            args.url + "/admin/import",
            data=json.dumps(batch).encode(),
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                "User-Agent": "TauCetiCI-live/1.0",
            },
        )
        for attempt in range(6):
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    response.read()
                break
            except (urllib.error.URLError, TimeoutError):
                if attempt == 5:
                    raise
                time.sleep(min(30, 2**attempt))
        receipts.executemany(
            "INSERT OR IGNORE INTO imported VALUES (?)", [(v,) for v in identities]
        )
        receipts.commit()
        count += len(batch)
        batch.clear()
        identities.clear()
        batch_bytes = 0
        print("Imported", count, "events", flush=True)

    with sqlite3.connect(f"file:{args.db.resolve()}?mode=ro", uri=True) as db:
        for event in events(db, args.since):
            payload = json.dumps(event, sort_keys=True, separators=(",", ":"))
            identity = "bootstrap/" + hashlib.sha256(payload.encode()).hexdigest()
            if receipts.execute(
                "SELECT 1 FROM imported WHERE id=?", (identity,)
            ).fetchone():
                continue
            event["delivery"] = identity
            if args.dry_run:
                count += 1
                continue
            if batch and (len(batch) >= 50 or batch_bytes + len(payload) > 1_000_000):
                publish()
            batch.append(event)
            identities.append(identity)
            batch_bytes += len(payload)
    if not args.dry_run:
        publish()
    print(
        ("Would import" if args.dry_run else "Imported"),
        count,
        "events; GitHub requests: 0",
    )


if __name__ == "__main__":
    main()
