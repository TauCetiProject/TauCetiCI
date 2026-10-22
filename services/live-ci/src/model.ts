/** Pure, reversible metric contributions. Times and durations are seconds. */
import { classify } from "./failure";
export type Row = {
  day: string;
  bucket: number;
  metric: string;
  value: number;
  amount: number;
};
export type Run = {
  repo: string;
  id: string;
  attempt: number;
  workflow: string;
  event: string;
  trigger: string;
  status: string;
  conclusion: string | null;
  created: number;
  started: number;
  updated: number;
  head: string;
  branch: string;
  url: string;
  testedHead?: string;
  batch?: number;
  members?: number[];
  telemetry?: Telemetry;
  archiveSampling?: { until: number; weight: number };
};
export type Job = {
  id: string;
  run: string;
  attempt: number;
  name: string;
  status: string;
  conclusion: string | null;
  created: number;
  started: number | null;
  completed: number | null;
  kind: string;
  labels?: string[];
  runnerName?: string;
  assigned: boolean;
  steps: {
    name: string;
    started: number | null;
    completed: number | null;
    conclusion: string | null;
  }[];
  failure?: string;
  weight: number;
  estimated: boolean;
  updated: number;
};
export type Telemetry = {
  phases?: { phase: string; seconds: number | null }[];
  failure?: string;
  classifier?: string;
  job_failures?: Record<string, string>;
  coverage?: string;
};
export const DAY = 86400,
  BUILD = "sandboxed-build",
  WORKFLOW = ".github/workflows/pr-build.yml";
export const time = (v: any): number =>
  typeof v === "number"
    ? v
    : Number.isFinite(Date.parse(v))
      ? Date.parse(v) / 1000
      : 0;
export const iso = (v: number) =>
  new Date(v * 1000).toISOString().replace(".000Z", "Z");
export const day = (v: number) => iso(v).slice(0, 10);
export const rank = (status: string) =>
  status === "completed" ? 3 : status === "in_progress" ? 2 : 1;
// Public report aliases only; fleet credentials, admission and dispatch keep
// their permanent identities and operator names. Unknown hosts stay separate.
const fleetNames: Record<string, string> = {
  "4fab2fed9a5145f894d066c7bb28ace4": "eastanglia",
  "11e2756fc11042d798f759410573a935": "leanfro",
};
const hostNames: Record<string, string> = {
  "chris-server": "eastanglia", chris: "eastanglia", tauchonki: "leanfro",
};
export const RUNNERS = ["github", "namespace", "eastanglia", "leanfro", "self-hosted"];
export const runnerLabel = (kind: string) => ({
  github: "GitHub", namespace: "Namespace", "self-hosted": "Other self-hosted",
  merge: "Merge validation", other: "Other GitHub jobs", cancelled: "Cancelled PR builds",
}[kind] || kind);
export function reportingKind(kind: string, labels: string[] = [], name = ""): string {
  if (kind !== "self-hosted") return kind;
  const names = new Set(labels.flatMap((label) => {
    const found = Object.hasOwn(fleetNames, label.slice(6)) && label.startsWith("fleet-")
      ? fleetNames[label.slice(6)]
      : label.startsWith("fleet-host-") && Object.hasOwn(hostNames, label.slice(11)) ? hostNames[label.slice(11)] : undefined;
    return found ? [found] : [];
  }));
  // Older webhook jobs may only identify the host in its runner name.
  for (const [id, alias] of Object.entries(fleetNames))
    if (new RegExp(`^fleet-${id.slice(0, 12)}-[a-f0-9]{16}$`).test(name)) names.add(alias);
  return names.size === 1 ? [...names][0] : kind;
}
export const runner = (labels: string[]) =>
  labels.some((x) => x.startsWith("nscloud-"))
    ? "namespace"
    : labels.includes("self-hosted")
      ? "self-hosted"
      : labels.some((x) => /^(ubuntu-|windows-|macos-)/.test(x))
        ? "github"
        : labels.length ? "self-hosted" : "unknown";
export function normalizeRun(repo: string, r: any): Run {
  const event = r.event || "",
    workflow = r.path || r.workflow || "";
  let trigger =
    r.trigger ||
    (["pull_request", "pull_request_target"].includes(event)
      ? "pr"
      : event === "merge_group"
        ? "merge_queue"
        : event);
  const title =
    /^bors branch=(\S+) batch=(\d+) head=([0-9a-f]{40}) base=([0-9a-f]{40})$/.exec(
      r.display_title || r.title || "",
    );
  if (!r.trigger && event === "repository_dispatch" && workflow === WORKFLOW)
    trigger = title?.[1] === "main" ? "bors_staging" : "bors_unknown";
  const members = r.tested?.batch_members;
  return {
    repo,
    id: String(r.id ?? r.run_id),
    attempt: Number(r.run_attempt || 1),
    workflow,
    event,
    trigger,
    status: r.status || (r.conclusion ? "completed" : "queued"),
    conclusion: r.conclusion || null,
    created: time(r.created_at),
    started: time(r.run_started_at || r.created_at),
    updated: time(r.updated_at || r.created_at),
    archiveSampling: r.archive_sampling,
    head: r.head_sha || "",
    branch: r.head_branch || "",
    url:
      r.html_url ||
      `https://github.com/${repo}/actions/runs/${r.id ?? r.run_id}`,
    testedHead: r.tested?.head_sha || title?.[3],
    batch: r.tested?.batch_id || (title ? Number(title[2]) : undefined),
    members: Array.isArray(members)
      ? [
          ...new Set<number>(
            members.map((x: any) => Number(x.pr)).filter((x: number) => x > 0),
          ),
        ]
      : undefined,
  };
}
export function normalizeJob(j: any): Job {
  const steps = (j.steps || []).map((s: any) => ({
    name: String(s.name),
    started: time(s.started_at) || null,
    completed: time(s.completed_at) || null,
    conclusion: s.conclusion || null,
  }));
  const labels = Array.isArray(j.labels) ? j.labels : [];
  const assigned = !!(
    j.runner_id ||
    j.runner_name ||
    j.status === "in_progress" ||
    j.assigned ||
    steps.some((s: any) => s.started)
  );
  return {
    id: String(j.id ?? j.job_id),
    run: String(j.run_id),
    attempt: Number(j.run_attempt || 1),
    name: String(j.name || ""),
    status: j.status || (j.conclusion ? "completed" : "queued"),
    conclusion: j.conclusion || null,
    created: time(j.created_at),
    started: time(j.started_at) || null,
    completed: time(j.completed_at) || null,
    kind: reportingKind(j.runner_kind || runner(labels), labels, j.runner_name),
    labels,
    runnerName: j.runner_name || "",
    assigned,
    steps,
    failure:
      j.failure?.class ||
      j.failure_class ||
      (j.failure_excerpt !== undefined
        ? classify(
            j.conclusion,
            steps.find((s: any) => s.conclusion === "failure")?.name || "",
            j.failure_excerpt,
          )
        : undefined),
    weight: Number(j.weight ?? 1),
    estimated: !!j.estimated,
    updated: time(j.completed_at || j.started_at || j.created_at),
  };
}
export function newer<T extends { status: string; updated: number }>(
  old: T | undefined,
  next: T,
): T {
  if (!old) return next;
  if (rank(next.status) < rank(old.status) || next.updated < old.updated)
    return old;
  return { ...old, ...next };
}
const phases: Record<string, string> = {
  "stage-cache": "build",
  build: "build",
  "audit-duplicates": "audits",
  "audit-axioms": "audits",
  "audit-module-system": "audits",
  "lint-env": "lints",
  "lint-style": "lints",
};
export function stepPhase(name: string): string {
  if (
    /Report|Summarise CI telemetry|Upload CI telemetry|Post |Complete job/.test(
      name,
    )
  )
    return "reporting";
  if (
    /Mathlib's compressed|Fetch Mathlib|Require a complete Mathlib/.test(name)
  )
    return "Mathlib cache";
  if (/Lake artifact cache|TauCeti's own oleans|Lake archive/.test(name))
    return "Lake cache";
  if (name.includes("Build exact candidate")) return "build, audits, lints";
  if (/watchdog toolchain|environment-lint|Dot-notation/.test(name))
    return "lint preparation";
  return "setup and checks";
}
export function contributions(run: Run, j: Job, until: number): Row[] {
  const rows: Row[] = [];
  const add = (
    when: number,
    bucket: number,
    metric: string,
    value: number,
    amount: number,
  ) => {
    if (amount && Number.isFinite(amount) && Number.isFinite(value))
      rows.push({ day: day(when), bucket, metric, value, amount });
  };
  const daily = (metric: string, value = 0, amount = 1, when = j.created) =>
    add(when, 0, metric, value, amount);
  const isBuild = [BUILD, "verify", "build", "performance-gate"].includes(
    j.name,
  );
  const series =
    ["eastanglia", "leanfro", "self-hosted"].includes(j.kind)
      ? j.kind
      : j.kind === "github"
      ? isBuild
        ? ["pr", "workflow_dispatch"].includes(run.trigger)
          ? "gh-build-pr"
          : "gh-build-other"
        : "gh-other"
      : ["merge_queue", "bors_staging"].includes(run.trigger)
        ? "ns-mq"
        : run.trigger === "pr"
          ? "ns-pr"
          : "ns-other";
  // A terminal parent proves that an unobserved child is no longer running.
  // Keep its conclusion unknown until the authoritative job delivery arrives.
  const end = Math.min(
    j.completed ?? (run.status === "completed" ? run.updated : until),
    until,
  );
  const created = Math.max(j.created, until - 30 * DAY);
  const started = j.started && j.assigned ? j.started : null;
  // The archive's sampled cohort represents all jobs before cutover. After
  // cutover each observed job contributes once, including unsampled runs.
  const weightAt = (at: number) =>
    run.archiveSampling
      ? at < run.archiveSampling.until ? run.archiveSampling.weight : 1
      : j.weight;
  const interval = (
    a: number,
    b: number,
    kind: "running" | "queued",
    step: number,
  ) => {
    if (b <= a) return;
    const cutoff = run.archiveSampling?.until;
    const spans = cutoff && a < cutoff && cutoff < b
      ? [[a, cutoff], [cutoff, b]] : [[a, b]];
    for (const [since, until] of spans) {
      const weight = weightAt(since);
      if (!weight) continue;
      for (let bucket = Math.floor(since / step) * step; bucket < until; bucket += step) {
        const lo = Math.max(since, bucket),
          hi = Math.min(until, bucket + step);
        if (kind === "running")
          add(bucket, bucket, `fleet/${step}/running/${series}`, 0,
            (weight * (hi - lo)) / step);
        else {
          add(bucket, bucket, `fleet/${step}/queued/${j.kind}`, lo, weight);
          add(bucket, bucket, `fleet/${step}/queued/${j.kind}`, hi, -weight);
        }
      }
    }
  };
  if (RUNNERS.includes(j.kind))
    for (const step of [600, 7200]) {
      if (started) interval(Math.max(created, started), end, "running", step);
      if (!j.estimated)
        interval(created, Math.min(started ?? end, end), "queued", step);
      if (!j.estimated && started && started >= until - 30 * DAY) {
        const window = Math.max(step, 1800),
          last = Math.floor((started + window) / step) * step;
        for (
          let bucket = Math.floor((started - 0.000001) / step) * step;
          bucket < last;
          bucket += step
        ) {
          if (bucket + step >= started && bucket + step < started + window &&
            weightAt(Math.max(started, bucket + step - 0.000001)) > 0)
            add(
              bucket,
              bucket,
              `fleet/${step}/wait/${isBuild ? "build" : "other"}`,
              Math.max(0, started - j.created) / 60,
              1,
            );
        }
      }
      if (j.kind === "namespace" && run.trigger === "pr" && j.name === BUILD) {
        const b = Math.floor(j.created / step) * step;
        add(b, b, `fleet/${step}/picked`, j.created, 1);
      }
    }
  const seconds =
    started && j.completed ? Math.max(0, j.completed - started) : null;
  const wait = started ? Math.max(0, started - j.created) : null;
  if (
    !j.estimated &&
    !["sandboxed-build", "verify", "build"].includes(j.name) &&
    j.kind === "github" &&
    wait !== null && weightAt(started!) > 0
  )
    daily("wait/other", wait / 60);
  if (!run.repo.endsWith("/TauCeti")) return rows;
  const prBuild =
    run.workflow === WORKFLOW &&
    j.name === BUILD &&
    ["pr", "merge_queue", "bors_staging"].includes(run.trigger);
  if (prBuild && RUNNERS.includes(j.kind)) {
    if (run.trigger === "pr") {
      if (j.conclusion === "success" && seconds !== null)
        daily(`duration/${j.kind}`, seconds / 60);
      if (wait !== null) daily(`wait/${j.kind}`, wait / 60);
      if (["success", "failure", "timed_out"].includes(j.conclusion || ""))
        daily("pr-total");
      daily(`picker/${j.kind}`);
      if (["failure", "timed_out"].includes(j.conclusion || ""))
        daily(
          `failure/${j.conclusion === "timed_out" ? "timeout" : j.failure || run.telemetry?.job_failures?.[j.id] || run.telemetry?.failure || "pending"}`,
        );
      if (j.conclusion === "success") {
        daily("phase-count");
        for (const s of j.steps) {
          if (!s.started || !s.completed) continue;
          const seconds = Math.max(0, s.completed - s.started),
            phase = stepPhase(s.name);
          const split = (run.telemetry?.phases || []).filter(
            (p) => p.seconds !== null && p.seconds >= 0 && phases[p.phase],
          );
          const total = split.reduce((n, p) => n + p.seconds!, 0);
          if (phase === "build, audits, lints" && total > 0) {
            const factor = Math.min(1, seconds / total);
            for (const p of split)
              daily(`phase/${phases[p.phase]}`, 0, (p.seconds! * factor) / 60);
            daily(
              "phase/sandbox, unattributed",
              0,
              Math.max(0, seconds - total) / 60,
            );
          } else daily(`phase/${phase}`, 0, seconds / 60);
        }
      }
    } else {
      if (j.conclusion === "success" && seconds !== null)
        daily("duration/merge", seconds / 60);
      if (wait !== null) daily("wait/merge", wait / 60);
      if (j.conclusion)
        daily(
          `merge/${["success", "failure", "cancelled"].includes(j.conclusion) ? j.conclusion : "other"}`,
        );
    }
  }
  if (isBuild && seconds !== null && started) {
    daily(`minutes/${j.kind}`, 0, seconds / 60, started);
    if (run.trigger === "pr" && j.conclusion === "cancelled")
      daily("minutes/cancelled", 0, seconds / 60, started);
  }
  return rows;
}
export function difference(before: Row[], after: Row[]): Row[] {
  const totals = new Map<string, Row>();
  for (const [sign, rows] of [
    [-1, before],
    [1, after],
  ] as const)
    for (const r of rows) {
      const k = JSON.stringify([r.day, r.bucket, r.metric, r.value]),
        old = totals.get(k);
      totals.set(k, { ...r, amount: (old?.amount || 0) + sign * r.amount });
    }
  return [...totals.values()].filter((r) => Math.abs(r.amount) > 1e-9);
}
export function quantile(
  values: { value: number; amount: number }[],
  q: number,
): number | null {
  const sorted = values
      .filter((x) => x.amount > 0)
      .sort((a, b) => a.value - b.value),
    total = sorted.reduce((s, x) => s + x.amount, 0);
  if (!total) return null;
  const target = Math.max(1, Math.ceil(total * q));
  let n = 0;
  for (const row of sorted) {
    n += row.amount;
    if (n >= target) return row.value;
  }
  return sorted.at(-1)!.value;
}
export function summarize(rows: Omit<Row, "day">[]): any {
  const daily: Record<string, number | Record<string, number | null>> = {},
    fleet: Record<string, any> = {};
  const hist = new Map<string, { value: number; amount: number }[]>();
  for (const r of rows) {
    if (r.metric.startsWith("fleet/")) {
      const [, step, type, key = ""] = r.metric.split("/"),
        id = `${step}/${r.bucket}`;
      const b = (fleet[id] ??= {
        at: r.bucket,
        step: Number(step),
        running: {},
        queued: {},
        wait: {},
        picked: [],
        events: {},
      });
      if (type === "running") b.running[key] = (b.running[key] || 0) + r.amount;
      if (type === "queued") (b.events[key] ??= []).push(r);
      if (type === "wait" && r.amount > 0)
        b.wait[key] = Math.max(b.wait[key] || 0, r.value);
      if (type === "picked" && r.amount > 0) b.picked.push(r.value);
    } else if (
      r.metric.startsWith("duration/") ||
      r.metric.startsWith("wait/")
    ) {
      const values = hist.get(r.metric) || [];
      values.push(r);
      hist.set(r.metric, values);
    } else daily[r.metric] = Number(daily[r.metric] || 0) + r.amount;
  }
  for (const [key, values] of hist)
    daily[key] = { median: quantile(values, 0.5), p90: quantile(values, 0.9) };
  for (const b of Object.values(fleet)) {
    for (const [key, events] of Object.entries(b.events) as [string, any[]][]) {
      events.sort((a, b) => a.value - b.value || a.amount - b.amount);
      let depth = 0,
        peak = 0;
      for (const event of events) {
        depth += event.amount;
        peak = Math.max(peak, depth);
      }
      b.queued[key] = peak;
    }
    delete b.events;
  }
  return {
    daily,
    fleet: Object.values(fleet).sort((a, b) => a.at - b.at || a.step - b.step),
  };
}
export function validTelemetry(v: any): v is Telemetry {
  return (
    v &&
    typeof v === "object" &&
    (!v.phases ||
      (Array.isArray(v.phases) &&
        v.phases.length <= 100 &&
        v.phases.every(
          (p: any) =>
            typeof p.phase === "string" &&
            p.phase.length <= 80 &&
            (p.seconds === null ||
              (Number.isFinite(p.seconds) &&
                p.seconds >= 0 &&
                p.seconds <= 7 * DAY)),
        ))) &&
    (!v.failure || /^[a-z][a-z0-9-]{0,60}$/.test(v.failure))
  );
}
