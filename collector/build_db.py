"""Build a SQLite database from the NDJSON records. The database is derived and never committed.

    python -m collector.build_db [--out db/ci.sqlite]

Tables
  runs(repo, run_id, run_attempt, workflow, event, trigger, conclusion, head_sha, created_at,
       updated_at, pr, queue_pr, base_sha, group_position, group_prs, merge_base, behind_by,
       ahead_by, diff_files, diff_additions, diff_deletions, patch_fingerprint, landed,
       jobs_fetched: 0 when a backfill recorded the run without its jobs)
  jobs(repo, run_id, job_id, run_attempt, name, conclusion, created_at, started_at, completed_at,
       wait_s, run_s, runner_kind, runner_size, labels, failure_class, failed_step, excerpt)
  steps(job_id, n, name, conclusion, started_at, completed_at, run_s)
  main_commits(sha, committed_at, pr, subject)
  settings(repo, observed_at, rulesets_json)
  telemetry(repo, run_id, phases_json, build_json, meta_json)   pr-build's in-sandbox phase timings
       and module counts (statistics only: written where candidate code runs)
  coverage(repo, complete_to)   every run of `repo` created before `complete_to` is recorded
  annotations(at, text)   hand-kept notes (records/annotations.json) for changes the API cannot see,
                          such as repository variables

`runs` holds the latest attempt seen for each run; `jobs` holds every attempt's jobs, deduplicated.
Times are ISO-8601 UTC text; durations are seconds.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RECORDS = ROOT / "records"

SCHEMA = """
CREATE TABLE runs (
  repo TEXT, run_id INTEGER, run_attempt INTEGER, workflow TEXT, event TEXT, trigger TEXT,
  conclusion TEXT, head_sha TEXT, created_at TEXT, updated_at TEXT,
  pr INTEGER, queue_pr INTEGER, base_sha TEXT, group_position INTEGER, group_prs TEXT,
  merge_base TEXT, behind_by INTEGER, ahead_by INTEGER,
  diff_files INTEGER, diff_additions INTEGER, diff_deletions INTEGER, patch_fingerprint TEXT,
  landed INTEGER, jobs_fetched INTEGER,
  PRIMARY KEY (repo, run_id)
);
CREATE TABLE jobs (
  repo TEXT, run_id INTEGER, job_id INTEGER PRIMARY KEY, run_attempt INTEGER, name TEXT,
  conclusion TEXT, created_at TEXT, started_at TEXT, completed_at TEXT, wait_s REAL, run_s REAL,
  runner_kind TEXT, runner_size TEXT, labels TEXT,
  failure_class TEXT, failed_step TEXT, excerpt TEXT
);
CREATE TABLE steps (
  job_id INTEGER, n INTEGER, name TEXT, conclusion TEXT, started_at TEXT, completed_at TEXT,
  run_s REAL, PRIMARY KEY (job_id, n)
);
CREATE TABLE main_commits (sha TEXT PRIMARY KEY, committed_at TEXT, pr INTEGER, subject TEXT);
CREATE TABLE settings (repo TEXT, observed_at TEXT, rulesets_json TEXT);
CREATE TABLE annotations (at TEXT, text TEXT);
CREATE TABLE coverage (repo TEXT PRIMARY KEY, complete_to TEXT);
CREATE TABLE telemetry (repo TEXT, run_id INTEGER, phases_json TEXT, build_json TEXT, meta_json TEXT,
  PRIMARY KEY (repo, run_id));
CREATE INDEX jobs_run ON jobs(repo, run_id);
CREATE INDEX jobs_created ON jobs(created_at);
CREATE INDEX runs_created ON runs(created_at);
CREATE INDEX runs_workflow ON runs(workflow, trigger);
"""


def seconds(a, b):
    if not a or not b:
        return None
    f = lambda s: dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return (f(b) - f(a)).total_seconds()


def runner_size(labels):
    for l in labels:
        m = re.search(r"-(\d+x\d+)", l)
        if l.startswith("nscloud-") and m:
            return m.group(1)
    return None


def records(kind: str):
    for f in sorted((RECORDS / kind).rglob("*.ndjson.gz")):
        with gzip.open(f, "rt") as fh:
            for line in fh:
                yield json.loads(line)


def build(out: Path):
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    db = sqlite3.connect(tmp)
    db.executescript(SCHEMA)

    main = {}
    for c in records("main"):
        main[c["sha"]] = c
    db.executemany("INSERT OR REPLACE INTO main_commits VALUES (?,?,?,?)",
                   [(c["sha"], c["committed_at"], c.get("pr"), c.get("subject")) for c in main.values()])

    for r in records("runs"):
        t = r.get("tested") or {}
        d = t.get("diff") or {}
        prev = db.execute("SELECT run_attempt FROM runs WHERE repo=? AND run_id=?",
                          (r["repo"], r["run_id"])).fetchone()
        if not prev or prev[0] <= r["run_attempt"]:
            db.execute("INSERT OR REPLACE INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                r["repo"], r["run_id"], r["run_attempt"], r["workflow"], r["event"], r["trigger"],
                r.get("conclusion"), r["head_sha"], r["created_at"], r.get("updated_at"),
                t.get("pr"), t.get("queue_pr"), t.get("base_sha"), t.get("group_position"),
                json.dumps(t["group_prs"]) if "group_prs" in t else None,
                t.get("merge_base"), t.get("behind_by"), t.get("ahead_by"),
                d.get("files"), d.get("additions"), d.get("deletions"), d.get("patch_fingerprint"),
                (1 if r["head_sha"] in main else 0) if r["trigger"] == "merge_queue" else None,
                0 if r.get("jobs_fetched") is False else 1,
            ))
        t = r.get("telemetry")
        if t:
            db.execute("INSERT OR REPLACE INTO telemetry VALUES (?,?,?,?,?)", (
                r["repo"], r["run_id"], json.dumps(t.get("phases") or []),
                json.dumps(t.get("build") or {}), json.dumps(t.get("meta") or {})))
        for j in r["jobs"]:
            f = j.get("failure") or {}
            db.execute("INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                r["repo"], r["run_id"], j["id"], j.get("run_attempt"), j["name"], j.get("conclusion"),
                j.get("created_at"), j.get("started_at"), j.get("completed_at"),
                seconds(j.get("created_at"), j.get("started_at")),
                seconds(j.get("started_at"), j.get("completed_at")),
                j.get("runner_kind"), runner_size(j.get("labels") or []), json.dumps(j.get("labels") or []),
                f.get("class"), f.get("failed_step"),
                "\n".join(f["excerpt"]) if f.get("excerpt") else None,
            ))
            for s in j.get("steps") or []:
                db.execute("INSERT OR REPLACE INTO steps VALUES (?,?,?,?,?,?,?)", (
                    j["id"], s["n"], s["name"], s.get("conclusion"), s.get("started_at"),
                    s.get("completed_at"), seconds(s.get("started_at"), s.get("completed_at"))))

    for f in sorted((RECORDS / "settings").rglob("*.json")):
        s = json.loads(f.read_text())
        db.execute("INSERT INTO settings VALUES (?,?,?)",
                   (s["repo"], s["observed_at"], json.dumps(s["rulesets"])))
    cursor = ROOT / "state" / "cursor.json"
    if cursor.exists():
        db.executemany("INSERT INTO coverage VALUES (?,?)", json.loads(cursor.read_text()).items())
    notes = RECORDS / "annotations.json"
    if notes.exists():
        db.executemany("INSERT INTO annotations VALUES (?,?)",
                       [(a["at"], a["text"]) for a in json.loads(notes.read_text())])
    db.commit()
    db.execute("VACUUM")
    db.close()
    tmp.replace(out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=str(ROOT / "db" / "ci.sqlite"))
    build(Path(ap.parse_args(argv).out))


if __name__ == "__main__":
    main()
