"""Modelling report: failure rates, rebase and batching risk, runner capacity, queue policy.

    python3 -m analysis.report [--db db/ci.sqlite] [--out analysis/REPORT.md]

Everything is computed from the database that collector/build_db.py builds. Estimates carry their
sample sizes and 95% intervals (Wilson for proportions), because several of them rest on a few
dozen events; read the numbers with those intervals, not as point truths.
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import json
import math
import random
import sqlite3
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PR_BUILD = ".github/workflows/pr-build.yml"
BUILD = "sandboxed-build"
UTC = dt.timezone.utc


def ts(s: str) -> float:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float, float]:
    if n == 0:
        return (float("nan"),) * 3
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return p, max(0.0, c - h), min(1.0, c + h)


def pct(k: int, n: int) -> str:
    p, lo, hi = wilson(k, n)
    return "n/a" if n == 0 else f"{p:.1%} ({lo:.1%}–{hi:.1%}; {k}/{n})"


def quantile(v: list[float], q: float) -> float:
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))] if v else float("nan")


def table(headers: list[str], rows: list[list]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


class Report:
    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self.parts: list[str] = []

    def h(self, text: str):
        self.parts.append(f"\n## {text}\n")

    def p(self, text: str):
        self.parts.append(text + "\n")

    # --- coverage ---------------------------------------------------------------------------

    def coverage(self):
        first, last, n = self.db.execute(f"""
            SELECT MIN(created_at), MAX(created_at), COUNT(*) FROM runs WHERE workflow = '{PR_BUILD}'""").fetchone()
        mq = self.db.execute("SELECT COUNT(*) FROM runs WHERE trigger = 'merge_queue'").fetchone()[0]
        tel = self.db.execute("SELECT COUNT(*) FROM telemetry").fetchone()[0]
        self.h("Data")
        self.p(f"`pr-build` runs from {first} to {last}: {n} runs, of which {mq} merge-queue builds. "
               f"{tel} runs carry in-sandbox phase telemetry.")

    # --- 1. base failure rates ----------------------------------------------------------------

    def failure_rates(self):
        self.h("1. How often PR builds fail, and why")
        rows = self.db.execute(f"""
            SELECT r.run_id, r.pr, r.head_sha, r.diff_additions, j.conclusion, j.failure_class, j.created_at
            FROM runs r JOIN jobs j USING (repo, run_id)
            WHERE r.workflow = '{PR_BUILD}' AND r.trigger = 'pr' AND j.name = '{BUILD}'
              AND j.conclusion IN ('success', 'failure')""").fetchall()
        n = len(rows)
        failed = [r for r in rows if r[4] == "failure"]
        self.p(f"PR builds that finished: {n}. Failure rate: {pct(len(failed), n)}.")
        by_class = defaultdict(int)
        for r in failed:
            by_class[r[5] or "unclassified"] += 1
        self.p(table(["cause", "failed builds", "share of failures"],
                     [[c, k, f"{k/len(failed):.0%}"] for c, k in sorted(by_class.items(), key=lambda x: -x[1])]))
        buckets = [(0, 50), (50, 200), (200, 500), (500, 10**9)]
        rows_b = []
        for lo, hi in buckets:
            sel = [r for r in rows if r[3] is not None and lo <= r[3] < hi]
            k = sum(1 for r in sel if r[4] == "failure")
            rows_b.append([f"{lo}–{hi if hi < 10**9 else '∞'} lines added", pct(k, len(sel))])
        self.p("\nBy size of change:\n\n" + table(["size", "failure rate"], rows_b))
        # Flaky: the same commit failed, then passed on a later build.
        by_sha = defaultdict(list)
        for r in rows:
            by_sha[r[2]].append((r[6], r[4], r[5]))
        flaky, retried = 0, 0
        for runs in by_sha.values():
            runs.sort()
            for (t1, c1, _), (t2, c2, _) in zip(runs, runs[1:]):
                if c1 == "failure":
                    retried += 1
                    flaky += c2 == "success"
        self.p(f"\nA failure on a commit that was built again: {retried}. Of those, the next build of the "
               f"same commit passed in {pct(flaky, retried)}. That is the rate at which a failure is not "
               f"the change's fault (infrastructure, a flaky test, a timeout).")

    # --- 2. rebase risk ----------------------------------------------------------------------

    def main_index(self) -> dict[str, int]:
        shas = [s for (s,) in self.db.execute("SELECT sha FROM main_commits ORDER BY committed_at")]
        return {s: i for i, s in enumerate(shas)}

    def rebase_risk(self):
        self.h("2. Does rebasing break a green PR?")
        idx = self.main_index()
        rows = self.db.execute(f"""
            SELECT r.pr, r.created_at, r.patch_fingerprint, r.merge_base, j.conclusion
            FROM runs r JOIN jobs j USING (repo, run_id)
            WHERE r.workflow = '{PR_BUILD}' AND r.trigger = 'pr' AND j.name = '{BUILD}'
              AND j.conclusion IN ('success', 'failure') AND r.pr IS NOT NULL
            ORDER BY r.pr, r.created_at""").fetchall()
        by_pr = defaultdict(list)
        for r in rows:
            by_pr[r[0]].append(r)
        buckets = defaultdict(lambda: [0, 0])
        for runs in by_pr.values():
            for a, b in zip(runs, runs[1:]):
                if a[4] != "success" or a[2] != b[2] or a[3] == b[3]:
                    continue  # want: green, then the same change on a new base
                if a[3] not in idx or b[3] not in idx:
                    continue
                moved = idx[b[3]] - idx[a[3]]
                key = "1–10" if moved <= 10 else "11–50" if moved <= 50 else "51–200" if moved <= 200 else ">200"
                buckets[key][0] += b[4] == "failure"
                buckets[key][1] += 1
        order = ["1–10", "11–50", "51–200", ">200"]
        self.p("A PR that built green, then the same change (same patch fingerprint) rebuilt on a newer "
               "main. How often does the rebuilt one fail, by how far main moved?\n")
        self.p(table(["main moved by (commits)", "rebuild fails"], [[k, pct(*buckets[k])] for k in order]))

        # Merge queue: own failures (nothing ahead failed), against how stale the PR's green build was.
        mq = self.db.execute("""
            SELECT queue_pr, created_at, base_sha, conclusion, ahead_failed, queue_position
            FROM runs WHERE trigger = 'merge_queue' AND queue_pr IS NOT NULL""").fetchall()
        green = defaultdict(list)
        for pr, created, fp, mb, concl in rows:
            if concl == "success":
                green[pr].append((created, mb))
        stale = defaultdict(lambda: [0, 0])
        for pr, created, base, concl, ahead_failed, pos in mq:
            if ahead_failed or concl not in ("success", "failure"):
                continue
            prior = [g for g in green.get(pr, []) if g[0] < created]
            if not prior:
                continue
            _, mb = max(prior)
            # How far main's tip at enqueue is past the merge base the green build had.
            tip = self.db.execute("SELECT sha FROM main_commits WHERE committed_at <= ? ORDER BY committed_at DESC LIMIT 1",
                                  (created,)).fetchone()
            if not tip or mb not in idx or tip[0] not in idx:
                continue
            moved = idx[tip[0]] - idx[mb]
            key = "0–10" if moved <= 10 else "11–50" if moved <= 50 else "51–200" if moved <= 200 else ">200"
            stale[key][0] += concl == "failure"
            stale[key][1] += 1
        self.p("\nIn the merge queue, excluding builds that failed only because an entry ahead did: how "
               "often a PR fails there, by how far main had moved since the merge base of its last green "
               "PR build.\n")
        self.p(table(["main moved by (commits)", "merge-queue build fails"],
                     [[k, pct(*stale[k])] for k in ["0–10", "11–50", "51–200", ">200"]]))

    # --- 3. batching in the merge queue --------------------------------------------------------

    def queue(self) -> dict:
        self.h("3. The merge queue: position, cascades, and batching risk")
        rows = self.db.execute("""
            SELECT queue_position, conclusion, ahead_failed, landed FROM runs
            WHERE trigger = 'merge_queue' AND queue_position IS NOT NULL
              AND conclusion IN ('success', 'failure')""").fetchall()
        by_pos = defaultdict(lambda: [0, 0, 0, 0])
        for pos, concl, ahead, landed in rows:
            b = by_pos[pos]
            b[0] += 1
            b[1] += concl == "failure" and not ahead
            b[2] += concl == "failure" and bool(ahead)
            b[3] += bool(ahead) or 0
        out = []
        own_total = [0, 0]
        for pos in sorted(by_pos):
            n, own, casc, ahead = by_pos[pos]
            clean = n - ahead
            own_total[0] += own
            own_total[1] += clean
            out.append([pos, n, pct(own, clean), casc])
        self.p("Each merge-queue entry builds its PR together with every entry ahead of it. An entry's "
               "*own* failure is one where nothing ahead failed; a *cascade* failure is one that failed "
               "because something ahead did.\n")
        self.p(table(["position", "builds", "own failure rate (nothing ahead failed)", "cascade failures"], out))
        p_own = wilson(*own_total)
        self.p(f"\nOverall own-failure rate: {pct(*own_total)}. If PRs failed independently, the own "
               "failure rate would not depend on position; a rise with position would measure "
               "interaction between PRs built together.")
        casc = sum(v[2] for v in by_pos.values())
        mins = self.db.execute("""
            SELECT SUM(j.run_s) / 60 FROM runs r JOIN jobs j USING (repo, run_id)
            WHERE r.trigger = 'merge_queue' AND r.ahead_failed = 1 AND r.conclusion = 'failure'
              AND j.name = 'sandboxed-build'""").fetchone()[0] or 0
        self.p(f"\nCascade failures: {casc} builds, {mins:.0f} Namespace build-minutes, each spent on a "
               "group already doomed by a failure ahead of it.")
        durations = [r[0] / 60 for r in self.db.execute("""
            SELECT j.run_s FROM runs r JOIN jobs j USING (repo, run_id)
            WHERE r.trigger = 'merge_queue' AND j.name = 'sandboxed-build' AND j.conclusion = 'success'
              AND j.run_s IS NOT NULL""")]
        # Arrival rate from complete days of merge-queue data only: coverage has gaps.
        per_day = [n for (n,) in self.db.execute("""
            SELECT COUNT(*) FROM runs WHERE trigger = 'merge_queue' AND landed = 1
            GROUP BY substr(created_at, 1, 10) HAVING COUNT(*) >= 20""")]
        lam = quantile(per_day, 0.5) / 24 if per_day else 1.0
        return {"p_own": p_own[0], "build_min": durations, "arrivals_per_hour": lam}

    # --- 4. queue policy simulation -----------------------------------------------------------

    def simulate(self, params: dict):
        self.h("4. Merge-queue settings, simulated")
        p, durs, lam = params["p_own"], params["build_min"], params["arrivals_per_hour"]
        if not durs or math.isnan(p):
            self.p("Not enough merge-queue data yet.")
            return
        self.p(f"Inputs, from the data above: PRs arriving at {lam:.1f} an hour; each PR fails in the queue "
               f"on its own with probability {p:.1%}; a build takes the recorded merge-queue build times "
               f"(median {quantile(durs, 0.5):.1f} min). The simulation assumes failures are independent "
               "between PRs, which section 3 tests.\n")
        rows = []
        for policy, depth, merge in [("GitHub queue, build 1", 1, 1), ("GitHub queue, build 3", 3, 1),
                                     ("GitHub queue, build 5 (current)", 5, 2), ("GitHub queue, build 8", 8, 2),
                                     ("bors batch of 4, bisect on failure", 4, "bors"),
                                     ("bors batch of 8, bisect on failure", 8, "bors")]:
            r = simulate_queue(lam, p, durs, depth, merge, hours=24 * 30, seed=1)
            rows.append([policy, f"{r['latency_p50']:.0f}", f"{r['latency_p90']:.0f}",
                         f"{r['builds_per_pr']:.2f}", f"{r['minutes_per_pr']:.1f}"])
        self.p(table(["policy", "median minutes to land", "90th pct", "builds per landed PR",
                      "build-minutes per landed PR"], rows))
        self.p("\nA deeper speculative queue lands PRs sooner but wastes builds when an entry fails, because "
               "everything behind it is rebuilt. Bors-style batching builds several PRs as one and halves "
               "the batch on failure, which is cheap when failures are rare and slow when they are not.")

    # --- 5. GitHub-hosted capacity ------------------------------------------------------------

    def capacity(self):
        self.h("5. Who fills the 20 GitHub-hosted slots?")
        rows = self.db.execute("""
            SELECT r.workflow, r.created_at, j.run_s, r.jobs_fetched FROM jobs j JOIN runs r USING (repo, run_id)
            WHERE j.runner_kind = 'github' AND j.run_s IS NOT NULL""").fetchall()
        counts = {}
        for w, hour, total, fetched in self.db.execute("""
                SELECT workflow, substr(created_at, 1, 13), COUNT(*), SUM(jobs_fetched) FROM runs
                WHERE conclusion != 'skipped' GROUP BY 1, 2"""):
            counts[(w, hour)] = total / fetched if fetched else 0
        by_wf = defaultdict(float)
        hours = set()
        per_hour = defaultdict(float)
        for w, created, run_s, _ in rows:
            weight = counts.get((w, created[:13]), 1) or 1
            by_wf[w] += run_s * weight
            per_hour[created[:13]] += run_s * weight
            hours.add(created[:13])
        total_h = max(len(hours), 1)
        total = sum(by_wf.values())
        self.p(f"Over {total_h} hours with recorded GitHub-hosted jobs, the organisation used on average "
               f"{total / 3600 / total_h:.1f} of the 20 slots (slot-hours per hour). Busiest hour: "
               f"{max(per_hour.values(), default=0) / 3600:.1f}. By workflow (sampled workflows weighted "
               "up to their full run counts):\n")
        top = sorted(by_wf.items(), key=lambda x: -x[1])[:12]
        self.p(table(["workflow", "slot-hours per hour", "share"],
                     [[w.rsplit("/", 1)[-1], f"{v / 3600 / total_h:.2f}", f"{v / total:.0%}"] for w, v in top]))
        # Saturation shows as waiting, not as more than 20 slots in use: time-in-use cannot exceed 20.
        waits = defaultdict(list)
        for hour, w in self.db.execute("""
                SELECT substr(j.created_at, 1, 13), j.wait_s FROM jobs j
                WHERE j.runner_kind = 'github' AND j.wait_s IS NOT NULL"""):
            waits[hour].append(w)
        sat = [h for h, v in waits.items() if len(v) >= 10 and quantile(v, 0.9) > 120]
        self.p(f"\nSlot time in use cannot exceed 20 an hour, so saturation shows up as waiting instead: in "
               f"{len(sat)} of {len(waits)} hours, one GitHub-hosted job in ten waited more than two "
               "minutes for a runner.")

    # --- 6. batching PR builds ------------------------------------------------------------------

    def batched_pr_builds(self, p_fail: float):
        self.h("6. Would batching PR builds together save money?")
        rows = self.db.execute("SELECT phases_json FROM telemetry").fetchall()
        fixed, variable = [], []
        for (pj,) in rows:
            ph = {x["phase"]: x["seconds"] for x in json.loads(pj) if x.get("seconds") is not None}
            if "build" in ph:
                variable.append(ph["build"] / 60)
                fixed.append(sum(v for k, v in ph.items() if k != "build") / 60)
        steps = self.db.execute(f"""
            SELECT s.name, s.run_s FROM steps s JOIN jobs j USING (job_id) JOIN runs r USING (repo, run_id)
            WHERE r.workflow = '{PR_BUILD}' AND r.trigger = 'pr' AND j.name = '{BUILD}' AND j.conclusion = 'success'
            """).fetchall()
        job_total = defaultdict(float)
        outside = sum(s for n, s in steps if s and "Build exact candidate" not in n)
        inside = sum(s for n, s in steps if s and "Build exact candidate" in n)
        share_outside = outside / (outside + inside) if outside + inside else float("nan")
        if not variable:
            self.p(f"No phase telemetry yet. From step timings alone, {share_outside:.0%} of a PR build's time "
                   "is outside the sandboxed build step (checkouts, guards, caches), which batching would "
                   "share. The split of the sandboxed step waits for telemetry.")
            return
        f_in = quantile(fixed, 0.5)
        v = quantile(variable, 0.5)
        self.p(f"From {len(variable)} builds with telemetry: inside the sandbox, the median build phase takes "
               f"{v:.1f} min and the audits and lints {f_in:.1f} min; {share_outside:.0%} of the whole job is "
               "outside the sandbox. Batching N PRs shares everything but the build phase, whose work grows "
               "roughly with the modules the PRs touch.\n")
        rows = []
        per_pr_single = None
        for n in (1, 2, 4, 8):
            p_batch = 1 - (1 - p_fail) ** n
            # A failed batch is bisected: about log2(N) further rounds of two builds.
            extra = p_batch * (2 * math.log2(n)) if n > 1 else 0
            base = (f_in / (1 - share_outside)) if share_outside < 1 else f_in
            per_build = base + v * n
            per_pr = per_build * (1 + extra) / n
            per_pr_single = per_pr_single or per_pr
            rows.append([n, f"{p_batch:.0%}", f"{per_pr:.1f}", f"{per_pr / per_pr_single:.0%}"])
        self.p(table(["PRs per batch", "batch fails", "build-minutes per PR", "vs one at a time"], rows))
        self.p("\nThe saving is real only while batches rarely fail; with the current failure rate, the "
               "bisection cost overtakes the shared overhead quickly. Batching also delays each PR's own "
               "result until the batch finishes.")


def simulate_queue(lam: float, p: float, durs: list[float], depth: int, merge, hours: int, seed: int) -> dict:
    """Discrete-time simulation (1-minute steps) of a merge queue.

    GitHub's queue: entries are built speculatively up to `depth` at a time, each on top of those
    ahead; an entry passes if its own PR and every PR ahead are good. When the front entry finishes
    good it lands (with up to `merge` passing entries behind it); when it fails, that PR is removed
    and every entry behind it is rebuilt. Bors ("bors"): take up to `depth` waiting PRs as one batch;
    if it fails, split it in half and retry each half, until single PRs fail on their own."""
    rng = random.Random(seed)
    t, T = 0.0, hours * 60.0
    arrivals = []
    while t < T:
        t += rng.expovariate(lam / 60)
        arrivals.append(t)
    good = {i: rng.random() >= p for i in range(len(arrivals))}
    dur = lambda: rng.choice(durs)
    builds, minutes, landed_at = 0, 0.0, {}
    if merge == "bors":
        waiting = []
        now, ai = 0.0, 0
        stack = []
        while now < T or stack:
            while ai < len(arrivals) and arrivals[ai] <= now:
                waiting.append(ai)
                ai += 1
            if not stack:
                if not waiting:
                    if ai >= len(arrivals):
                        break
                    now = arrivals[ai]
                    continue
                stack.append(waiting[:depth])
                waiting = waiting[depth:]
            batch = stack.pop()
            d = dur()
            builds += 1
            minutes += d
            now += d
            if all(good[i] for i in batch):
                for i in batch:
                    landed_at[i] = now
            elif len(batch) > 1:
                h = len(batch) // 2
                stack += [batch[h:], batch[:h]]
    else:
        queue = []  # PR ids in order
        building = {}  # position key: (PR tuple, finish time)
        now, ai = 0.0, 0
        while now < T + 24 * 60:
            while ai < len(arrivals) and arrivals[ai] <= now:
                queue.append(ai)
                ai += 1
            # Start builds for the first `depth` entries that have none.
            for k in range(min(depth, len(queue))):
                key = tuple(queue[:k + 1])
                if key not in building:
                    d = dur()
                    building[key] = now + d
                    builds += 1
                    minutes += d
            # Resolve the front when its build is done.
            if queue:
                front = (queue[0],)
                if front in building and building[front] <= now:
                    if good[queue[0]]:
                        n_land = 1
                        # Land passing entries behind, up to `merge`, whose builds are also done.
                        for k in range(2, min(merge, len(queue)) + 1):
                            key = tuple(queue[:k])
                            if key in building and building[key] <= now and all(good[i] for i in key):
                                n_land = k
                        for i in queue[:n_land]:
                            landed_at[i] = now
                        queue = queue[n_land:]
                        building = {tuple(k[n_land:]): v for k, v in building.items() if len(k) > n_land
                                    and all(good[i] for i in k[:n_land])}
                    else:
                        queue = queue[1:]
                        building = {}
                    continue
            if ai >= len(arrivals) and not queue:
                break
            now += 1.0
    lat = [landed_at[i] - arrivals[i] for i in landed_at]
    n_landed = max(len(landed_at), 1)
    return {"latency_p50": quantile(lat, 0.5), "latency_p90": quantile(lat, 0.9),
            "builds_per_pr": builds / n_landed, "minutes_per_pr": minutes / n_landed}


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=str(ROOT / "db" / "ci.sqlite"))
    ap.add_argument("--out", default=str(ROOT / "analysis" / "REPORT.md"))
    args = ap.parse_args(argv)
    db = sqlite3.connect(args.db)
    r = Report(db)
    r.parts.append(f"# TauCeti CI: modelling report\n\nGenerated {dt.datetime.now(UTC):%Y-%m-%d %H:%M} UTC by "
                   "`analysis/report.py` from the TauCetiCI database. Intervals are 95% Wilson intervals.\n")
    r.coverage()
    r.failure_rates()
    r.rebase_risk()
    params = r.queue()
    r.simulate(params)
    r.capacity()
    n, k = db.execute(f"""SELECT COUNT(*), SUM(j.conclusion = 'failure') FROM runs r JOIN jobs j USING (repo, run_id)
                          WHERE r.workflow = '{PR_BUILD}' AND r.trigger = 'pr' AND j.name = '{BUILD}'
                            AND j.conclusion IN ('success', 'failure')""").fetchone()
    r.batched_pr_builds((k or 0) / n if n else 0.1)
    Path(args.out).write_text("\n".join(r.parts))
    print(args.out)


if __name__ == "__main__":
    main()
