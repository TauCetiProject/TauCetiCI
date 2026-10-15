"""Deterministic, offline advisory replay. No scheduler or GitHub write capability.

python -m analysis.replay --db db/ci.sqlite --out replay.json
All times are seconds; cost is modelled runner wall minutes, not dollars or CPU.
"""
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass, replace
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import statistics

SCHEMA = "tauceti-ci.advisory-replay/v1"


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, int((len(values) - 1) * fraction))] if values else None


def draw(key):
    """Common random numbers across policies; independent of Python hash/iteration order."""
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big") / 2**64


@dataclass(frozen=True)
class Head:
    pr: int
    sha: str
    lo: float
    hi: float
    work: float
    modules: tuple[str, ...] = ()
    behind: int | None = None
    dependency: bool | None = None
    stale_at: float | None = None
    bundle: str | None = None
    missing: bool = False
    work_imputed: bool = True

    @property
    def key(self):
        return f"{self.pr}@{self.sha}"

    @property
    def risk(self):
        # An explicitly unvalidated ordering hint, never a failure verdict.
        return .02 + .08 * bool(self.dependency) + .001 * min(self.behind or 0, 200)


@dataclass(frozen=True)
class Model:
    fixed_s: float = 300
    per_work_s: float = 1
    default_work: float = 100
    dependency_multiplier: float = 3


def calibration(db, cutoff):
    """Fit only completed, successful ordinary staging attempts before the window.

    Candidate module counts are statistical hints. Failed/cancelled jobs never
    train successful build duration; unknown/dependency cohorts stay separate.
    """
    samples, cohorts, verdicts, sources = [], {}, {}, []
    rows = db.execute("""SELECT r.run_id,r.updated_at,r.conclusion,a.cohort,
        j.run_s,t.build_json,m.members_json,j.runner_size,j.runner_kind
        FROM merge_builds m JOIN runs r USING(repo,run_id)
        JOIN run_attempts a ON a.repo=r.repo AND a.run_id=r.run_id AND a.run_attempt=r.run_attempt
        JOIN jobs j ON j.repo=r.repo AND j.run_id=r.run_id AND j.run_attempt=r.run_attempt
        LEFT JOIN telemetry t ON t.repo=r.repo AND t.run_id=r.run_id
        WHERE m.repo='TauCeti' AND m.engine='bors' AND j.name='sandboxed-build'
          AND r.updated_at<=? ORDER BY r.updated_at,r.run_id""", (cutoff,))
    per_head = []
    runners = set()
    for run, at, conclusion, cohort, seconds, build, members, size, kind in rows:
        cohorts[cohort] = cohorts.get(cohort, 0) + 1
        verdict = "infrastructure" if conclusion in ("cancelled", "timed_out") else conclusion
        verdicts[verdict] = verdicts.get(verdict, 0) + 1
        built = (json.loads(build or "{} ").get("jobs_by_verb") or {}).get("Built")
        if (conclusion == "success" and cohort == "ordinary" and seconds and
                isinstance(built, (int, float)) and 0 < built <= 20000):
            samples.append((built, seconds))
            sources.append(run)
            runners.add(f"{kind}/{size or 'unknown-size'}")
            n = len(json.loads(members or "[]"))
            if n:
                per_head.append(built / n)
    model = Model()
    if len(samples) >= 3 and len({x for x, _ in samples}) > 1:
        xs, ys = zip(*samples)
        mx, my = statistics.mean(xs), statistics.mean(ys)
        slope = max(0, sum((x - mx) * (y - my) for x, y in samples) /
                    sum((x - mx)**2 for x in xs))
        # Round fit noise so Python's differing summation implementations do
        # not perturb event times and trace hashes across supported versions.
        model = Model(round(max(1, my - slope * mx), 9), round(slope, 9),
                      round(statistics.median(per_head), 9) if per_head else 100)
    return model, {"cutoff": cutoff, "successful_ordinary_samples": len(samples),
                   "cohorts": cohorts, "conclusions": verdicts, "run_ids": sources,
                   "runners": sorted(runners), "fallback": len(samples) < 3 or len({x for x, _ in samples}) < 2,
                   "model": asdict(model)}


def capture(db, hours=24):
    last = db.execute("SELECT MAX(observed_at) FROM merge_observations").fetchone()[0]
    if not last:
        return [], Model(), {"unavailable": "No captured eligibility observations"}
    end = timestamp(last)
    start = end - hours * 3600
    since = dt.datetime.fromtimestamp(start, dt.timezone.utc).isoformat().replace("+00:00", "Z")
    model, fit = calibration(db, since)
    rows = db.execute("""SELECT observed_at,eligible_json FROM merge_observations
        WHERE observed_at>=? ORDER BY observed_at""", (since,)).fetchall()
    heads, first, previous, current = {}, None, None, {}
    gaps = initial = 0
    features = db.execute("SELECT COUNT(*) FROM sqlite_master WHERE name='replay_features'").fetchone()[0]
    for at, payload in rows:
        now = timestamp(at)
        if first is None:
            first = now
        if previous is not None and now - previous > 120:
            gaps += 1
        for item in sorted(json.loads(payload), key=lambda p: (p["pr"], p["head_sha"])):
            pr, sha = item["pr"], item["head_sha"]
            key = f"{pr}@{sha}"
            old = current.get(pr)
            if old and old != key and old in heads:
                heads[old] = replace(heads[old], stale_at=now - first)
            current[pr] = key
            if item.get("ready") is False:
                if key in heads:
                    heads[key] = replace(heads[key], stale_at=now - first)
                continue
            if key in heads:
                continue
            feature = None
            if features:
                feature = db.execute("""SELECT payload_json FROM replay_features
                    WHERE repo='TauCeti' AND pr=? AND head_sha=? AND available_at<=?
                    ORDER BY available_at DESC,run_id DESC,run_attempt DESC LIMIT 1""",
                    (pr, sha, dt.datetime.fromtimestamp(previous or now, dt.timezone.utc).isoformat().replace("+00:00", "Z"))).fetchone()
            f = json.loads(feature[0]) if feature else {}
            diff = f.get("diff") or {}
            paths = diff.get("paths")
            known = isinstance(paths, list) and not diff.get("files_truncated")
            modules = tuple(sorted(p for p in (paths or []) if p.endswith(".lean")))
            built = (f.get("build", {}).get("jobs_by_verb") or {}).get("Built")
            measured_work = isinstance(built, (int, float)) and math.isfinite(built) and 0 < built <= 20000
            work = built if measured_work else max(model.default_work, len(modules))
            dependency = any(p in {"lakefile.lean", "lakefile.toml", "lake-manifest.json", "lean-toolchain"}
                             for p in paths) if known else None
            heads[key] = Head(pr, sha, max(0, (previous or now) - first), now - first,
                              work, modules, f.get("behind_by"), dependency,
                              bundle=str(item["bundle_id"]) if item.get("bundle_id") else None,
                              missing=not known, work_imputed=not measured_work)
            initial += previous is None
        previous = now
    duration = (end - first) if first is not None else 0
    new = len(heads) - initial
    observed = observed_baseline(db, since, last)
    return sorted(heads.values(), key=lambda h: (h.hi, h.key)), model, {
        "since": since, "until": last, "window_s": duration, "snapshots": len(rows),
        "sample_gaps_over_120s": gaps, "heads": len(heads), "initial_left_censored_backlog": initial,
        "new_heads_per_hour": new * 3600 / duration if duration else None,
        "admission_bounds": "preceding/current eligibility sample; initial backlog age unknown; not actual admission",
        "missing_features": sum(h.missing for h in heads.values()),
        "imputed_build_work": sum(h.work_imputed for h in heads.values()),
        "unknown_staleness": sum(h.behind is None for h in heads.values()),
        "unknown_dependency": sum(h.dependency is None for h in heads.values()),
        "known_bundled_heads": sum(h.bundle is not None for h in heads.values()),
        "calibration": fit, "observed_baseline": observed}


def observed_baseline(db, since, until):
    covered = db.execute("SELECT complete_to FROM coverage WHERE repo='TauCeti'").fetchone()
    runs = db.execute("""SELECT r.run_id,r.conclusion,r.jobs_fetched FROM runs r
        JOIN merge_builds m USING(repo,run_id) WHERE m.repo='TauCeti' AND m.engine='bors'
        AND r.created_at>=? AND r.created_at<=? ORDER BY r.run_id""", (since, until)).fetchall()
    minutes = db.execute("""SELECT SUM(j.run_s)/60 FROM jobs j JOIN runs r USING(repo,run_id)
        JOIN merge_builds m USING(repo,run_id) WHERE m.repo='TauCeti' AND m.engine='bors'
        AND r.created_at>=? AND r.created_at<=?""", (since, until)).fetchone()[0]
    members = set()
    for (payload,) in db.execute("""SELECT m.members_json FROM merge_builds m
        JOIN main_landings l ON m.repo=l.repo AND m.head_sha=l.head_sha
        WHERE m.repo='TauCeti' AND m.engine='bors' AND l.landed_at>=? AND l.landed_at<=?""", (since, until)):
        members.update(item['pr'] for item in json.loads(payload or "[]") if item.get('pr'))
    return {"attempted_runs": len(runs), "missing_job_runs": sum(not r[2] and r[1] is not None for r in runs),
            "recorded_validation_runner_minutes": minutes, "accepted_distinct_prs": len(members),
            "recorded_accepted_prs_per_hour": len(members) * 3600 / max(1, timestamp(until) - timestamp(since)),
            "collection_complete_to": covered[0] if covered else None,
            "full_window_covered": bool(covered and timestamp(covered[0]) >= timestamp(until)),
            "run_ids": [r[0] for r in runs],
            "limitation": "Recorded attempts/all jobs, actual main-push landing bounds; incomplete collection. "
                          "Attempt and landing windows differ, so no causal cost/PR ratio. Observed PRs differ from replay heads."}


def units_for(heads, bound):
    groups = {}
    for head in sorted(heads, key=lambda h: h.key):
        groups.setdefault(head.bundle or head.key, []).append(head)
    # An atomic bundle is ready only when ALL its members are ready.
    return sorted(groups.values(), key=lambda u: (max(getattr(h, bound) for h in u), u[0].key))


def duration(units, model, overlap=0.5, work_scale=1):
    seen, work, dependency = set(), 0, False
    for h in sorted((h for u in units for h in u), key=lambda h: h.key):
        shared = len(seen.intersection(h.modules)) / len(h.modules) if h.modules else 0
        work += h.work * (1 - overlap * shared) * (1 + min(h.behind or 0, 500) / 500)
        seen.update(h.modules)
        dependency |= bool(h.dependency)
    return (model.fixed_s + model.per_work_s * work * work_scale) * (
        model.dependency_multiplier if dependency else 1)


def form(pending, now, cap, policy, bound, model, age_limit, overlap, work_budget):
    """Oldest unit anchors EVERY batch; overdue units bypass work/risk ordering.

    Thus a finite backlog cannot starve. This bounds bypass, not elapsed time
    under overload/outages. Oversized atomic units run alone, matching bors.
    """
    order = lambda u: (max(getattr(h, bound) for h in u), u[0].key)
    pending = sorted(pending, key=order)
    selected = [pending[0]]
    if len(selected[0]) >= cap:
        return selected
    rest = pending[1:]
    if policy == "work-risk":
        def score(unit):
            overdue = now - max(getattr(h, bound) for h in unit) >= age_limit
            marginal = duration(selected + [unit], model, overlap) - duration(selected, model, overlap)
            return (not overdue, order(unit) if overdue else
                    (max(h.risk for h in unit), marginal, unit[0].key))
        rest.sort(key=score)
    for unit in rest:
        if sum(map(len, selected)) + len(unit) > cap:
            continue
        overdue = now - max(getattr(h, bound) for h in unit) >= age_limit
        if (policy == "work-risk" and not overdue and
                duration(selected + [unit], model, overlap) > work_budget):
            continue
        selected.append(unit)
    return selected


def replay(heads, model, *, cap=128, policy="fifo", bound="hi", proof_rate=.02,
           infra_rate=.02, overlap=.5, work_scale=1, age_limit=3600,
           work_budget=1200, max_infra_retries=2):
    if cap < 1 or policy not in ("fifo", "work-risk") or bound not in ("lo", "hi"):
        raise ValueError("invalid replay policy")
    if not (0 <= proof_rate <= 1 and 0 <= infra_rate <= 1 and 0 <= overlap <= 1):
        raise ValueError("rates/overlap must be in [0,1]")
    future = units_for(heads, bound)
    now, pending, retries, trace = 0.0, [], [], []
    landed, rejected, expired, deferred = set(), set(), set(), set()
    costs = {"total_s": 0.0, "isolation_s": 0.0, "infrastructure_s": 0.0, "stale_s": 0.0}
    waits, latency = [], []
    attempts = proof_failures = infra_failures = 0
    while future or pending or retries:
        if not pending and not retries and future:
            now = max(now, max(getattr(h, bound) for h in future[0]))
        while future and max(getattr(h, bound) for h in future[0]) <= now:
            pending.append(future.pop(0))
        # Frozen retries have priority and cannot absorb new admissions.
        if retries:
            batch, retry = retries.pop(0)
            isolation = True
            viable = []
            for unit in batch:
                if any(h.stale_at is not None and h.stale_at <= now for h in unit):
                    expired.update(h.key for h in unit)
                else:
                    viable.append(unit)
            batch = viable
            if not batch:
                continue
        else:
            viable = []
            for unit in pending:
                if any(h.stale_at is not None and h.stale_at <= now for h in unit):
                    expired.update(h.key for h in unit)
                else:
                    viable.append(unit)
            pending = viable
            if not pending:
                continue
            batch = form(pending, now, cap, policy, bound, model, age_limit, overlap, work_budget)
            for unit in batch:
                pending.remove(unit)
            retry, isolation = 0, False
        members = sorted(h.key for u in batch for h in u)
        seconds = duration(batch, model, overlap, work_scale)
        attempts += 1
        costs["total_s"] += seconds
        if isolation:
            costs["isolation_s"] += seconds
        start = now
        now += seconds
        if draw("infra/" + "/".join(members) + f"/{retry}") < infra_rate:
            verdict = "infrastructure"
            infra_failures += 1
            costs["infrastructure_s"] += seconds
            if retry < max_infra_retries:
                retries.insert(0, (batch, retry + 1))
            else:
                deferred.update(members)
        elif any(h.stale_at is not None and h.stale_at <= now for u in batch for h in u):
            verdict = "stale"
            costs["stale_s"] += seconds
            for unit in batch:
                if any(h.stale_at is not None and h.stale_at <= now for h in unit):
                    expired.update(h.key for h in unit)
                else:
                    pending.append(unit)
        elif any(draw("proof/" + h.key) < proof_rate for u in batch for h in u):
            verdict = "proof-scenario"
            proof_failures += 1
            if len(batch) == 1:
                rejected.update(members)  # an atomic bundle is never split
            else:
                middle = len(batch) // 2
                retries[0:0] = [(batch[:middle], 0), (batch[middle:], 0)]
        else:
            verdict = "success-scenario"
            landed.update(members)
            for unit in batch:
                for h in unit:
                    waits.append(start - getattr(h, bound))
                    latency.append(now - getattr(h, bound))
        trace.append({"start_s": start, "end_s": now, "members": members,
                      "units": [sorted(h.key for h in u) for u in batch],
                      "verdict": verdict, "retry": retry, "isolation": isolation})
    elapsed = max(now, 1)
    return {"cap": cap, "policy": policy, "bound": bound, "attempts": attempts,
            "landed": len(landed), "rejected": len(rejected), "expired": len(expired),
            "deferred_infrastructure": len(deferred), "proof_scenario_failures": proof_failures,
            "infrastructure_failures": infra_failures, "elapsed_s": now,
            "landed_per_hour": len(landed) * 3600 / elapsed,
            "runner_minutes_per_landed": costs["total_s"] / 60 / len(landed) if landed else None,
            "p90_latency_min": percentile(latency, .9) / 60 if latency else None,
            "max_success_wait_min": max(waits) / 60 if waits else None,
            "costs": costs, "trace": trace}


def compare(heads, model, coverage):
    scenarios = {
        "low-failure": {"proof_rate": .01, "infra_rate": .01},
        "high-failure": {"proof_rate": .20, "infra_rate": .05},
        "infra-outage": {"proof_rate": 0, "infra_rate": 1},
        "no-overlap": {"overlap": 0}, "full-overlap": {"overlap": 1},
        "half-work": {"work_scale": .5}, "double-work": {"work_scale": 2},
    }
    # Explicit stress cohorts, not claims that captured heads had these traits.
    variants = {
        "dependency-bump-stress": [replace(h, dependency=i % 20 == 0) for i, h in enumerate(heads)],
        "stale-base-stress": [replace(h, behind=500) for h in heads],
        "atomic-pairs-stress": [replace(h, bundle=f"stress-{i//2}") for i, h in enumerate(heads)],
        "missing-telemetry-stress": [replace(h, work=model.default_work, modules=(), behind=None,
                                              dependency=None, missing=True) for h in heads],
    }
    scenarios.update({name: {} for name in variants})
    results = []
    policies = [(128, "fifo")] + [(cap, policy) for cap in (8, 16, 32) for policy in ("fifo", "work-risk")]
    for scenario, options in scenarios.items():
        for bound in ("lo", "hi"):
            for cap, policy in policies:
                result = replay(variants.get(scenario, heads), model, cap=cap, policy=policy, bound=bound, **options)
                # Full traces remain available through --trace. Hash commits to
                # unit composition, retry decisions and event times for parity.
                trace = result.pop("trace")
                result["trace_sha256"] = hashlib.sha256(json.dumps(trace, sort_keys=True).encode()).hexdigest()
                results.append({"scenario": scenario, **result})
    inputs = [asdict(h) for h in sorted(heads, key=lambda h: h.key)]
    return {"schema": SCHEMA, "adaptive_scheduling_enabled": False, "coverage": coverage,
            "inputs": inputs, "model": asdict(model),
            "input_sha256": hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest(),
            "assumptions": [
                "Single serial validation lane; no speculative promotion, parallelism or exact bors queue parity.",
                "FIFO cap128 references the production configuration captured 2026-10-14; it is not a live setting lookup or measured production throughput.",
                "No fill delay; oldest atomic unit anchors each batch, overdue units bypass the advisory work budget.",
                "Finite-backlog bypass bound only; elapsed starvation bound requires bounded service and arrivals.",
                "Unknown bundles treated as singletons; unknown modules get calibrated median work.",
                "Work hints/module overlap are untrusted statistics, not transitive rebuild graphs or proof evidence.",
                "All failure verdicts and head landings are scenarios; head versions are separate arrivals, not unique accepted PRs. Cancellation/infra exhaustion never rejects a head.",
                "No inference of individual proof culprits from observed group failures or candidate logs.",
                "Cost is runner wall time, not CPU, dollars, network bytes or a canary benefit.",
                "No optimum claimed. Keep policy disabled pending matched replay and production canary benefit."],
            "results": results}


def report_section(db):
    heads, model, coverage = capture(db)
    if not heads:
        return "## Advisory workload replay\n\nNo captured workload; adaptive scheduling remains disabled.\n"
    result = compare(heads, model, coverage)
    lines = ["## Advisory workload replay (adaptive scheduling disabled)", "",
             "Captured eligibility workload, deterministic serial-lane counterfactual; simulated landings are not production results.",
             "", "```json", json.dumps(coverage, sort_keys=True), "```", "",
             "| Scenario / admission bound | Policy | Cap | Landed/h | Runner min/landed | p90 latency min | Isolation min | Infra min |",
             "| --- | --- | --- | --- | --- | --- | --- | --- |"]
    fmt = lambda v: f"{v:.2f}" if v is not None else "unknown"
    for r in result["results"]:
        lines.append(f"| {r['scenario']} / {r['bound']} | {r['policy']} | {r['cap']} | "
                     f"{fmt(r['landed_per_hour'])} | {fmt(r['runner_minutes_per_landed'])} | "
                     f"{fmt(r['p90_latency_min'])} | {fmt(r['costs']['isolation_s']/60)} | "
                     f"{fmt(r['costs']['infrastructure_s']/60)} |")
    lines += ["", *result["assumptions"], "",
              "Advisory: retain current production policy. Prioritize telemetry coverage, preserve atomic bundles and oldest-first service, "
              "and compare caps 8/16/32 in matched canaries only after review. Low/high failure and work/overlap sensitivities "
              "are conditional estimates, not evidence of an optimal cap. No baseline-duration gate.",
              "Reproduce full machine-readable evidence: `python -m analysis.replay --db ci.sqlite --out replay.json`."]
    return "\n".join(lines) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--trace", action="store_true", help="include default scenario traces for every cap/policy")
    args = ap.parse_args(argv)
    if not math.isfinite(args.hours) or args.hours <= 0:
        ap.error("--hours must be finite and positive")
    with sqlite3.connect(args.db) as db:
        heads, model, coverage = capture(db, args.hours)
        result = compare(heads, model, coverage)
        if args.trace:
            result["traces"] = [replay(heads, model, cap=cap, policy=policy)
                                for cap, policy in [(128, "fifo")] + [(n, p) for n in (8, 16, 32) for p in ("fifo", "work-risk")]]
    Path(args.out).write_text(json.dumps(result, indent=2, sort_keys=True, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
