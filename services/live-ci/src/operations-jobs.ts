/** Projection from stored webhook facts, never a GitHub request or CI verdict. */
import { reportingKind, type Job, type Run, WORKFLOW } from "./model";
const text = (v: unknown, n = 300) =>
  typeof v === "string"
    ? Array.from(
        v.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, ""),
      )
        .slice(0, n)
        .join("")
    : "";
const stamp = (v: number | null | undefined) =>
  v && Number.isFinite(v) ? new Date(v * 1000).toISOString() : null;
const id = (v: unknown) =>
  typeof v === "string" && /^\d{1,20}$/.test(v) ? v : null;
export function publicJob(run: Run, job: Job) {
  if (
    job.estimated ||
    !id(job.id) ||
    !id(run.id) ||
    job.attempt !== run.attempt
  )
    return null;
  const labels = (job.labels || []).slice(0, 30).map((v) => text(v, 100));
  const kind = reportingKind(job.kind, labels, job.runnerName);
  const build = run.workflow === WORKFLOW && job.name === "sandboxed-build";
  const active = job.status !== "completed" && run.status !== "completed";
  const workflow = run.workflow.split("/").at(-1) || "";
  const control =
    run.workflow === WORKFLOW ||
    [
      "pick-runner.yml",
      "pr-status.yml",
      "pr-labels.yml",
      "auto-merge.yml",
      "merge-sweep.yml",
      "ci-live-summary.yml",
      "ready-to-merge.yml",
      "queue-release.yml",
    ].includes(workflow);
  const maintenance = [
    "ci.yml",
    "tests.yml",
    "live-ci-tests.yml",
    "website-tests.yml",
    "nightly-verify.yml",
    "warm-dependencies.yml",
    "lint-repair.yml",
    "pages.yml",
  ].includes(workflow);
  return {
    repository: run.repo,
    id: job.id,
    run_id: run.id,
    run_attempt: job.attempt,
    workflow: text(run.workflow),
    name: text(job.name),
    trigger: text(run.trigger, 60),
    job_class: build
      ? run.trigger === "pr"
        ? "pr_build"
        : ["bors_staging", "merge_queue"].includes(run.trigger)
          ? "staging_build"
          : "other_build"
      : control
        ? "control"
        : maintenance
          ? "maintenance"
          : "other",
    status: job.status,
    conclusion: job.conclusion,
    active,
    created_at: stamp(job.created),
    started_at: stamp(job.started),
    completed_at: stamp(job.completed),
    observed_at: stamp(job.updated),
    assigned: job.assigned,
    pool: kind,
    labels,
    check_run_id: id(job.checkRunId),
    runner_id: id(job.runnerId),
    runner_name: text(job.runnerName),
    reported_candidate: /^[a-f0-9]{40}$/.test(run.testedHead || "")
      ? run.testedHead
      : null,
    candidate_binding: "unverified_run_metadata",
    // A repository_dispatch workflow head is NOT the candidate being tested.
    workflow_head: /^[a-f0-9]{40}$/.test(run.head) ? run.head : null,
    url: `https://github.com/${run.repo}/actions/runs/${run.id}/job/${job.id}`,
    run_url: `https://github.com/${run.repo}/actions/runs/${run.id}/attempts/${job.attempt}`,
    steps_truncated: job.steps.length > 12,
    steps: [
      ...job.steps.filter((s) => s.started && !s.completed),
      ...job.steps.filter((s) => !s.started || s.completed).slice(-11),
    ]
      .slice(0, 12)
      .map((s) => ({
        name: text(s.name),
        started_at: stamp(s.started),
        completed_at: stamp(s.completed),
        conclusion: text(s.conclusion, 40) || null,
      })),
    progress: {
      coverage: "steps_only",
      heartbeat_at: null,
      last_progress_at: null,
      completed_modules: null,
      active_compilers: null,
      cpu_seconds: null,
    },
    resources: {
      coverage: "unknown",
      cpu_used: null,
      memory_used_bytes: null,
      requested_profile: labels.find((l) => l.startsWith("nscloud-")) || null,
    },
  };
}
