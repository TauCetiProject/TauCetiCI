"""Collect completed GitHub Actions runs for every repository in the organisation.

Each invocation lists runs created in [since, until], skips those already recorded, and writes the
new ones to a fresh, never-rewritten file:

    records/runs/<repo>/<yyyy>/<mm>/<dd>/<collected-at>.ndjson.gz   (dated by the run's created_at)

Every run carries its jobs (all attempts), with queue and execution times and the runner. Runs of
the build workflows in DETAIL_REPOS (TauCeti) also carry steps, the commits they tested,
merge-queue composition, and a classified failure with an excerpt of the failing step's log. See
schema/run.v1.json.

Main-branch history and merge-queue settings are recorded alongside, in records/main/ and
records/settings/, because the analyses join against both.

    python -m collector.collect                        # the hourly job: resume from state/cursor.json
    python -m collector.collect --since 2026-09-01 --until 2026-09-02   # a backfill window

Listing runs costs one API call per 100 runs, and TauCeti creates over 2,000 runs an hour, so the
hourly job does not re-list a fixed look-back. It resumes from a per-repository cursor, minus
OVERLAP to catch long runs that completed since. The cursor advances only past runs actually
recorded, so a collection that runs out of API budget leaves the rest for the next one.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from . import classify
from .gh import GitHub

ORG = "TauCetiProject"
DETAIL_REPOS = {"TauCeti"}
SCHEMA = "tauceti-ci.run/v1"
ROOT = Path(__file__).resolve().parent.parent
RECORDS = ROOT / "records"
MAX_LOGS_PER_COLLECTION = 300
CURSOR = ROOT / "state" / "cursor.json"
# Longer than any workflow's timeout (nightly-verify's is 4 hours), so a run created before the
# cursor but completed after it is still listed.
OVERLAP = dt.timedelta(hours=6)

# Steps, commits tested and failure excerpts are kept only for these workflows. The label,
# notification and merge-bot workflows fire thousands of times a day and need only their jobs.
BUILD_WORKFLOWS = {".github/workflows/pr-build.yml", ".github/workflows/ci.yml",
                   ".github/workflows/nightly-verify.yml", ".github/workflows/pr-profile.yml",
                   ".github/workflows/lint-full.yml", ".github/workflows/pages.yml"}

# Of the high-volume workflows' runs, fetch jobs for one in SAMPLE_SHORT (chosen by run id, so
# deterministically) and record the rest at run level. That keeps an unbiased sample of their
# queue waits while cutting the collector's API cost by about three quarters.
SAMPLE_SHORT = 4

UTC = dt.timezone.utc


def iso(t: dt.datetime) -> str:
    return t.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(s: str) -> dt.datetime:
    now = dt.datetime.now(UTC)
    m = re.fullmatch(r"(\d+)([hd])", s)
    if m:
        n = int(m.group(1))
        return now - (dt.timedelta(hours=n) if m.group(2) == "h" else dt.timedelta(days=n))
    t = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return t if t.tzinfo else t.replace(tzinfo=UTC)


def runner_kind(labels: list[str]) -> str:
    if any(l.startswith("nscloud-") for l in labels):
        return "namespace"
    if not labels:
        return "none"
    if any(l.startswith(("ubuntu-", "windows-", "macos-")) for l in labels):
        return "github"
    return "self-hosted"


def trigger_class(run: dict) -> str:
    ev = run["event"]
    if ev in ("pull_request", "pull_request_target"):
        return "pr"
    if ev == "merge_group":
        return "merge_queue"
    if ev == "push":
        return "main" if run.get("head_branch") == "main" else "push"
    if ev in ("issue_comment", "pull_request_review", "pull_request_review_comment"):
        return "comment"
    return ev  # schedule, workflow_dispatch, workflow_run, repository_dispatch, ...


# --- existing records --------------------------------------------------------------------------


def day_dirs(kind: str, repo: str | None, since: dt.datetime, until: dt.datetime):
    base = RECORDS / kind / repo if repo else RECORDS / kind
    d = since.date()
    while d <= until.date():
        yield base / f"{d:%Y/%m/%d}"
        d += dt.timedelta(days=1)


def seen_keys(kind: str, repo: str | None, since: dt.datetime, until: dt.datetime, key) -> set:
    out = set()
    for d in day_dirs(kind, repo, since - dt.timedelta(days=1), until + dt.timedelta(days=1)):
        for f in d.glob("*.ndjson.gz") if d.is_dir() else []:
            for line in gzip.open(f, "rt"):
                out.add(key(json.loads(line)))
    return out


def write_records(kind: str, repo: str | None, records: list[dict], date_field: str, stamp: str):
    by_day: dict[str, list[dict]] = {}
    for r in records:
        by_day.setdefault(r[date_field][:10], []).append(r)
    for day, rs in by_day.items():
        d = (RECORDS / kind / repo if repo else RECORDS / kind) / day.replace("-", "/")
        d.mkdir(parents=True, exist_ok=True)
        with gzip.open(d / f"{stamp}.ndjson.gz", "at") as f:
            for r in sorted(rs, key=lambda r: r[date_field]):
                f.write(json.dumps(r, separators=(",", ":"), sort_keys=True, ensure_ascii=False) + "\n")


# --- listing ------------------------------------------------------------------------------------


def list_completed_runs(gh: GitHub, repo: str, since: dt.datetime, until: dt.datetime):
    """All completed runs created in [since, until]. The search behind this endpoint returns at
    most 1000 runs per query, so a window holding more is split in half until each part fits."""
    path = f"repos/{ORG}/{repo}/actions/runs"
    q = f"{iso(since)}..{iso(until)}"
    total = gh.get(path, created=q, status="completed", per_page=1)["total_count"]
    if total >= 1000 and until - since > dt.timedelta(minutes=5):
        mid = since + (until - since) / 2
        yield from list_completed_runs(gh, repo, since, mid)
        yield from list_completed_runs(gh, repo, mid + dt.timedelta(seconds=1), until)
        return
    if total:
        yield from gh.paginate(path, key="workflow_runs", created=q, status="completed")


# --- enrichment (build workflows of DETAIL_REPOS) -----------------------------------------------


class Enricher:
    def __init__(self, gh: GitHub, repo: str):
        self.gh, self.repo = gh, repo
        self.pr_by_head: dict[tuple[str, str], list[dict]] = {}
        self.main_at_cache: dict[str, str | None] = {}
        self.logs_fetched = 0
        self._lock = threading.Lock()

    def pr_number(self, run: dict) -> int | None:
        if run.get("pull_requests"):
            return run["pull_requests"][0]["number"]
        head_repo = (run.get("head_repository") or {}).get("full_name")
        if not head_repo or not run.get("head_branch"):
            return None
        owner = head_repo.split("/")[0]
        key = (owner, run["head_branch"])
        if key not in self.pr_by_head:
            try:
                self.pr_by_head[key] = self.gh.get(f"repos/{ORG}/{self.repo}/pulls",
                                                   head=f"{owner}:{run['head_branch']}", state="all",
                                                   per_page=10)
            except Exception:
                self.pr_by_head[key] = []
        prs = self.pr_by_head[key]
        for pr in prs:
            if pr["head"]["sha"] == run["head_sha"]:
                return pr["number"]
        return prs[0]["number"] if prs else None

    def main_at(self, when: str) -> str | None:
        minute = when[:16]
        if minute not in self.main_at_cache:
            commits = self.gh.get(f"repos/{ORG}/{self.repo}/commits", sha="main", until=when, per_page=1)
            self.main_at_cache[minute] = commits[0]["sha"] if commits else None
        return self.main_at_cache[minute]

    def compare(self, base: str, head: str) -> dict | None:
        try:
            return self.gh.get(f"repos/{ORG}/{self.repo}/compare/{base}...{head}", per_page=100)
        except Exception as e:
            print(f"compare {base[:8]}...{head[:8]} failed: {e}", file=sys.stderr)
            return None

    @staticmethod
    def diff_summary(cmp: dict) -> dict:
        files = cmp.get("files") or []
        # A content fingerprint of the change, stable across a pure rebase when the base's changes
        # don't touch the same hunks: the sorted (path, patch) pairs, falling back to the new blob.
        h = hashlib.sha256()
        for f in sorted(files, key=lambda f: f["filename"]):
            h.update(f["filename"].encode())
            h.update((f.get("patch") or f.get("sha") or "").encode())
        return {
            "files": len(files),
            "files_truncated": len(files) >= 300,
            "additions": sum(f.get("additions", 0) for f in files),
            "deletions": sum(f.get("deletions", 0) for f in files),
            "paths": sorted(f["filename"] for f in files)[:300],
            "patch_fingerprint": h.hexdigest()[:16],
        }

    def tested(self, run: dict) -> dict:
        """What a build run tested."""
        tc = trigger_class(run)
        out: dict = {"head_sha": run["head_sha"]}
        if tc == "merge_queue":
            # gh-readonly-queue/main/pr-<N>-<sha of main the group was formed on>
            m = re.fullmatch(r"gh-readonly-queue/main/pr-(\d+)-([0-9a-f]{40})", run.get("head_branch") or "")
            if m:
                out["queue_pr"] = int(m.group(1))
                out["base_sha"] = m.group(2)
                cmp = self.compare(m.group(2), run["head_sha"])
                if cmp:
                    prs = []
                    for c in cmp.get("commits", []):
                        subj = c["commit"]["message"].splitlines()[0]
                        n = re.search(r"\(#(\d+)\)\s*$", subj)
                        prs.append(int(n.group(1)) if n else None)
                    out["group_prs"] = prs              # oldest first; this entry's PR is last
                    out["group_position"] = len(prs)   # entry k tests entries 1..k together
                    out["diff"] = self.diff_summary(cmp)
        elif tc == "pr" or run["event"] == "workflow_dispatch":
            if tc == "pr":
                out["pr"] = self.pr_number(run)
            base = self.main_at(run["created_at"])
            out["main_at_created"] = base
            if base:
                head = run["head_sha"]
                cmp = self.compare(base, head)
                if cmp:
                    out["merge_base"] = cmp["merge_base_commit"]["sha"]
                    out["behind_by"] = cmp["behind_by"]
                    out["ahead_by"] = cmp["ahead_by"]
                    # Diff against the merge base: what the PR itself changes.
                    mb = self.compare(cmp["merge_base_commit"]["sha"], head) if cmp["behind_by"] else cmp
                    if mb:
                        out["diff"] = self.diff_summary(mb)
        return out

    def failure(self, job: dict) -> dict | None:
        if job.get("conclusion") in (None, "success", "skipped", "neutral"):
            return None
        failed = next((s for s in job.get("steps", []) if s.get("conclusion") == "failure"), None)
        lines = None
        with self._lock:
            fetch = job["conclusion"] == "failure" and self.logs_fetched < MAX_LOGS_PER_COLLECTION
            if fetch:
                self.logs_fetched += 1
        if fetch:
            try:
                log = self.gh.text(f"repos/{ORG}/{self.repo}/actions/jobs/{job['id']}/logs")
                lines = classify.excerpt(log, failed and failed.get("started_at"),
                                         failed and failed.get("completed_at"))
            except Exception as e:  # logs expire after 90 days
                print(f"log for job {job['id']} unavailable: {e}", file=sys.stderr)
        return {
            "failed_step": failed and failed["name"],
            "class": classify.classify(job["conclusion"], failed and failed["name"], lines),
            "classifier": classify.CLASSIFIER_VERSION,
            "excerpt": lines,
        }


def job_record(job: dict, detail: bool) -> dict:
    """A job. Durations are left to the database (wait = started - created, run = completed -
    started); the high-volume workflows get only what occupancy and wait analyses need."""
    labels = job.get("labels") or []
    rec = {
        "id": job["id"],
        "name": job["name"],
        "run_attempt": job.get("run_attempt"),
        "conclusion": job.get("conclusion"),
        "created_at": job.get("created_at"),
        "started_at": job.get("started_at"),
        "completed_at": job.get("completed_at"),
        "labels": labels,
        "runner_kind": runner_kind(labels),
    }
    if detail:
        rec["runner_name"] = job.get("runner_name") or None
        rec["steps"] = [{
            "n": s["number"], "name": s["name"], "conclusion": s.get("conclusion"),
            "started_at": s.get("started_at"), "completed_at": s.get("completed_at"),
        } for s in job.get("steps", [])]
    return rec


def needs_jobs(run: dict, jobs_for: str) -> bool:
    if (run.get("path") or run.get("name")) in BUILD_WORKFLOWS:
        return True
    return jobs_for == "all" and run["id"] % SAMPLE_SHORT == 0


def run_record(gh: GitHub, repo: str, run: dict, enricher: Enricher | None, jobs_for: str) -> dict:
    workflow = run.get("path") or run.get("name")
    # A run skipped by its `if:` never reached a runner; its jobs carry nothing worth a call. With
    # `jobs_for="build"` (backfills of long periods) the high-volume workflows are recorded at run
    # level only, and otherwise a sample of them is (SAMPLE_SHORT); either way marked
    # `jobs_fetched: false`.
    fetch = run.get("conclusion") != "skipped" and needs_jobs(run, jobs_for)
    jobs = list(gh.paginate(f"repos/{ORG}/{repo}/actions/runs/{run['id']}/jobs", key="jobs",
                            filter="all")) if fetch else []
    detail = enricher is not None and workflow in BUILD_WORKFLOWS
    rec = {
        "schema": SCHEMA,
        "repo": repo,
        "run_id": run["id"],
        "run_attempt": run.get("run_attempt", 1),
        "workflow": workflow,
        "event": run["event"],
        "trigger": trigger_class(run),
        "conclusion": run.get("conclusion"),
        "head_sha": run["head_sha"],
        "created_at": run["created_at"],
        "updated_at": run.get("updated_at"),
        "jobs": [job_record(j, detail) for j in jobs],
    }
    if not fetch and run.get("conclusion") != "skipped":
        rec["jobs_fetched"] = False
    if detail:
        rec.update({
            "head_branch": run.get("head_branch"),
            "head_repo": (run.get("head_repository") or {}).get("full_name"),
            "title": (run.get("display_title") or "")[:200],
            "actor": (run.get("actor") or {}).get("login"),
            "run_started_at": run.get("run_started_at"),
        })
        try:
            rec["tested"] = enricher.tested(run)
        except Exception as e:
            print(f"enrich run {run['id']} failed: {e}", file=sys.stderr)
        for j, jr in zip(jobs, rec["jobs"]):
            f = enricher.failure(j)
            if f:
                jr["failure"] = f
    return rec


def collect_runs(gh: GitHub, repo: str, since, until, stamp: str, dry_run: bool, max_calls: int,
                 jobs_for: str = "all") -> tuple[int, dt.datetime]:
    """Record the completed runs not yet recorded. Build workflows go first; once `max_calls` API
    calls are spent the rest wait for the next collection, which looks back far enough to find them."""
    key = lambda r: (r["run_id"], r["run_attempt"])
    have = seen_keys("runs", repo, since, until, key)
    new = []
    for r in list_completed_runs(gh, repo, since, until):
        if gh.calls >= max_calls:
            print(f"{repo}: API budget reached while listing runs", file=sys.stderr)
            return 0, since
        if (r["id"], r.get("run_attempt", 1)) not in have:
            new.append(r)
    new.sort(key=lambda r: (r.get("path") not in BUILD_WORKFLOWS, r["created_at"]))
    enricher = Enricher(gh, repo) if repo in DETAIL_REPOS else None

    def one(r):
        needs_calls = r.get("conclusion") != "skipped" and needs_jobs(r, jobs_for)
        if needs_calls and gh.calls >= max_calls:
            return None
        return run_record(gh, repo, r, enricher, jobs_for)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, new))
    records = [r for r in results if r is not None]
    # Everything created before the earliest run left unrecorded is complete.
    left = [r["created_at"] for r, rec in zip(new, results) if rec is None]
    covered = min(parse_time(t) for t in left) if left else until
    if left:
        print(f"{repo}: API budget reached; {len(left)} runs left for next time", file=sys.stderr)
    if not dry_run and records:
        write_records("runs", repo, records, "created_at", stamp)
    return len(records), covered


def collect_main(gh: GitHub, repo: str, since, until, stamp: str, dry_run: bool) -> int:
    """First-parent history of main: which commits landed when, and from which PR. The merge queue
    fast-forwards main to a group's head, so a merge-queue run landed iff its head_sha is here."""
    have = seen_keys("main", repo, since, until, lambda r: r["sha"])
    out = []
    for c in gh.paginate(f"repos/{ORG}/{repo}/commits", sha="main", since=iso(since), until=iso(until)):
        if c["sha"] in have:
            continue
        subj = c["commit"]["message"].splitlines()[0]
        n = re.search(r"\(#(\d+)\)\s*$", subj)
        out.append({
            "schema": "tauceti-ci.main-commit/v1",
            "sha": c["sha"],
            "parents": [p["sha"] for p in c["parents"]],
            "committed_at": c["commit"]["committer"]["date"],
            "pr": int(n.group(1)) if n else None,
            "subject": subj[:200],
        })
    if not dry_run and out:
        write_records("main", repo, out, "committed_at", stamp)
    return len(out)


def snapshot_settings(gh: GitHub, repo: str, stamp: str, dry_run: bool) -> bool:
    """Record the repository's rulesets (the merge queue lives in one) whenever they change."""
    rules = []
    for rs in gh.get(f"repos/{ORG}/{repo}/rulesets"):
        full = gh.get(f"repos/{ORG}/{repo}/rulesets/{rs['id']}")
        rules.append({k: full.get(k) for k in ("id", "name", "enforcement", "rules", "updated_at")})
    d = RECORDS / "settings" / repo
    previous = sorted(d.glob("*.json")) if d.is_dir() else []
    if previous and json.loads(previous[-1].read_text()).get("rulesets") == rules:
        return False
    if not dry_run:
        d.mkdir(parents=True, exist_ok=True)
        snap = {"schema": "tauceti-ci.settings/v1", "repo": repo, "observed_at": stamp, "rulesets": rules}
        (d / f"{stamp}.json").write_text(json.dumps(snap, indent=1, sort_keys=True) + "\n")
    return True


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--since", default=None,
                    help="ISO time or NNh/NNd ago (default: each repository's cursor, minus overlap)")
    ap.add_argument("--until", default=None, help="ISO time (default now)")
    ap.add_argument("--repos", default=None, help="comma-separated repos (default: every repo in the org)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--max-calls", type=int, default=int(os.environ.get("MAX_CALLS", "4500")),
                    help="stop after this many API calls (default 4500, env MAX_CALLS), and never "
                         "spend more than the token has left this hour, less a reserve")
    ap.add_argument("--jobs-for", choices=["all", "build"], default="all",
                    help="fetch jobs for every run (default), or only for build workflows (backfill)")
    args = ap.parse_args(argv)

    gh = GitHub()
    # /rate_limit does not count against the limit. Keep a reserve for the push and for anything
    # else sharing the token.
    core = gh.get("rate_limit")["resources"]["core"]
    gh.calls = 0
    budget = min(args.max_calls, core["remaining"] - max(200, gh.min_remaining))
    print(f"token: {core['remaining']}/{core['limit']} calls left this hour; budget {budget}", file=sys.stderr)
    args.max_calls = budget
    cursor = json.loads(CURSOR.read_text()) if CURSOR.exists() else {}
    since = parse_time(args.since) if args.since else None
    until = parse_time(args.until) if args.until else dt.datetime.now(UTC)
    stamp = dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    repos = args.repos.split(",") if args.repos else [
        r["name"] for r in gh.paginate(f"orgs/{ORG}/repos") if not r.get("archived")]
    # The detail repositories first, so a tight API budget is spent where it matters most.
    repos.sort(key=lambda r: r not in DETAIL_REPOS)

    def since_for(repo):
        if since:
            return since
        c = cursor.get(repo)
        return parse_time(c) - OVERLAP if c else until - dt.timedelta(hours=24)

    for repo in DETAIL_REPOS & set(repos):
        n = collect_main(gh, repo, since_for(repo), until, stamp, args.dry_run)
        print(f"{repo} main: {n} new commits", file=sys.stderr)
        if snapshot_settings(gh, repo, stamp, args.dry_run):
            print(f"{repo}: rulesets changed", file=sys.stderr)
    for repo in repos:
        n, covered = collect_runs(gh, repo, since_for(repo), until, stamp, args.dry_run,
                                  args.max_calls, args.jobs_for)
        print(f"{repo}: {n} new runs, complete up to {iso(covered)}", file=sys.stderr)
        # Only the scheduled (cursor-driven) mode moves the cursor; backfill windows leave it alone.
        if not since and not args.dry_run:
            cursor[repo] = max(cursor.get(repo, ""), iso(covered))
    if not since and not args.dry_run:
        CURSOR.parent.mkdir(exist_ok=True)
        CURSOR.write_text(json.dumps(cursor, indent=1, sort_keys=True) + "\n")
    print(f"API calls: {gh.calls}, rate limit remaining: {gh.remaining}", file=sys.stderr)


if __name__ == "__main__":
    main()
