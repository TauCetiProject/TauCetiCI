import { DAY, WORKFLOW, type Run } from './model';

export const BUILD_WORKFLOWS = new Set([
  WORKFLOW, '.github/workflows/ci.yml', '.github/workflows/nightly-verify.yml',
  '.github/workflows/pr-profile.yml', '.github/workflows/lint-full.yml',
  '.github/workflows/pages.yml',
]);
export const DETAIL_REQUESTS_PER_HOUR = 60;
export const recoveryLifetime = DAY;
export const rawLifetime = (run: Run) => (run.workflow === WORKFLOW ? 90 : 30) * DAY;
export const retained = (run: Run, clock: number) => run.created >= clock - rawLifetime(run);

const pick = (value: any, fields: string[]) => Object.fromEntries(
  fields.filter((field) => value[field] !== undefined).map((field) => [field, value[field]]),
);
/** Replay needs metric/identity evidence, not a copy of GitHub's full API objects. */
export function compactEvent(event: any): any {
  const value = { ...event };
  if (event.run) value.run = pick(event.run, [
    'id', 'run_id', 'run_attempt', 'path', 'workflow', 'event', 'trigger', 'status',
    'conclusion', 'created_at', 'run_started_at', 'updated_at', 'head_sha',
    'head_branch', 'html_url', 'display_title', 'title', 'tested', 'archive_sampling',
  ]);
  if (event.jobs) value.jobs = event.jobs.map((job: any) => ({
    ...pick(job, ['id', 'job_id', 'run_id', 'run_attempt', 'name', 'status', 'conclusion',
      'created_at', 'started_at', 'completed_at', 'labels', 'runner_id', 'runner_name',
      'runner_kind', 'assigned', 'weight', 'estimated', 'failure', 'failure_class', 'failure_excerpt']),
    steps: (job.steps || []).map((step: any) => pick(step, ['name', 'number', 'n', 'started_at', 'completed_at', 'conclusion'])),
  }));
  if (event.telemetry) value.telemetry = {
    ...pick(event.telemetry, ['coverage', 'failure', 'classifier', 'job_failures']),
    ...(event.telemetry.phases ? { phases: event.telemetry.phases.map((phase: any) => pick(phase, ['phase', 'seconds'])) } : {}),
  };
  return value;
}

export function needsJobs(status: any): boolean {
  const run = status.run;
  if (!run) return true;
  if (status.verified || ['skipped', 'cancelled'].includes(run.conclusion)) return false;
  return !status.jobs || status.job_rows.some((j: any) => j.status !== 'completed') ||
    (run.workflow === WORKFLOW && !status.job_rows.some((j: any) => j.name === 'sandboxed-build'));
}

export function repairReason(kind: string, run: Run | undefined, clock: number): string | null {
  if (!run) return null;
  if (!retained(run, clock)) return 'outside raw fact retention';
  if (kind === 'members') return run.created < clock - 30 * DAY ? 'outside staging chart window' : null;
  if (kind === 'run') return null;
  if (!BUILD_WORKFLOWS.has(run.workflow)) return 'auxiliary jobs use webhook observations';
  if (run.updated < clock - recoveryLifetime) return 'recovery expired after 24 hours';
  if (kind === 'details') {
    if (run.workflow !== WORKFLOW || run.trigger !== 'pr' ||
        !['success', 'failure', 'timed_out'].includes(run.conclusion || ''))
      return 'no PR phase or failure chart consumer';
    if (run.conclusion === 'success' && BigInt(run.id) % 20n !== 0n)
      return 'successful phase timings sampled one in twenty';
  }
  return null;
}
