/** Read-only backlog summary of retained facts, before public detail limits. */
import { STALE_WAITING_SECONDS, staleAt } from "./operations-activity";

const STATES = ["pending", "queued", "in_progress", "stuck", "unconfirmed", "stale"] as const;
const HOUR = 3600;
const group = () => ({ pending: 0, queued: 0, in_progress: 0, stuck: 0, unconfirmed: 0, stale: 0, oldest_waiting_at: null as number | null,
  /** Workflows that may be known GitHub zombies, as [hour, state, count]: a
   * merge counts them as `stale` once that hour is older than
   * STALE_WAITING_SECONDS by its own clock, and as `state` before that. */
  aging: [] as [number, string, number][],
  /** The waiting ones among them, as [hour, earliest workflow creation], for
   * the oldest wait while they are not stale. */
  waiting_runs: [] as [number, number][] });
export const backlogCounts = () => ({ pr_builds: group(), other_workflows: group(), waiting_jobs: 0, running_jobs: 0,
  stale_waiting_jobs: 0,
  /** Waiting jobs by creation hour, classified as stale or waiting by the merge. */
  waiting_created: [] as [number, number][] });
const bin = (bins: [number, number][], hour: number, n: number) => {
  const found = bins.find(b => b[0] === hour);
  if (found) found[1] += n; else bins.push([hour, n]);
};

export function backlogSnapshot(sql: SqlStorage, clock: number) {
  const counts = backlogCounts();
  const rows = sql.exec<any>(`SELECT r.data, m.value verified,
    SUM(CASE WHEN j.active=1 THEN 1 ELSE 0 END) active_jobs,
    SUM(CASE WHEN j.active=1 AND json_extract(j.data,'$.estimated')=0 THEN 1 ELSE 0 END) actual_jobs,
    SUM(CASE WHEN j.active=1 AND json_extract(j.data,'$.estimated')=0
      AND json_extract(j.data,'$.status')='in_progress' AND json_extract(j.data,'$.assigned')=1 THEN 1 ELSE 0 END) running_jobs,
    json_group_array(CASE WHEN j.active=1 AND json_extract(j.data,'$.estimated')=0
      AND NOT (json_extract(j.data,'$.status')='in_progress' AND json_extract(j.data,'$.assigned')=1)
      THEN json_extract(j.data,'$.created') END) waiting_created
    FROM runs r LEFT JOIN jobs j ON j.run=r.id AND j.attempt=r.attempt
    LEFT JOIN meta m ON m.key='verified/'||r.id||'/'||r.attempt
    WHERE json_extract(r.data,'$.status')!='completed'
      AND NOT EXISTS (SELECT 1 FROM runs newer WHERE newer.id=r.id AND newer.attempt>r.attempt)
    GROUP BY r.id,r.attempt`).toArray();
  for (const row of rows) {
    const run = JSON.parse(row.data);
    const target = run.workflow === '.github/workflows/pr-build.yml' ? counts.pr_builds : counts.other_workflows;
    counts.running_jobs += row.running_jobs;
    // Jobs with an unknown creation time are never stale.
    const created = (JSON.parse(row.waiting_created || '[]') as (number | null)[]).filter(c => c !== null) as number[];
    for (const c of created) if (c > 0) bin(counts.waiting_created, Math.floor(c / HOUR) * HOUR, 1); else counts.waiting_jobs++;
    counts.waiting_jobs += row.actual_jobs - row.running_jobs - created.length;
    // No age-only exclusion: require an enumerated empty/terminal attempt and
    // no active jobs, including synthetic placeholders. A live pending job
    // always keeps its place regardless of its age.
    const settled = !['pending', 'waiting', 'requested'].includes(run.status) &&
      row.verified === 'true' && row.active_jobs === 0 &&
      clock - Math.max(run.created || 0, run.updated || 0, run.started || 0) >= 5400;
    let state: (typeof STATES)[number];
    if (settled) state = 'stuck';
    else if (row.running_jobs > 0) state = 'in_progress';
    else if (['pending', 'waiting', 'requested'].includes(run.status) ||
        run.status === 'in_progress' && row.actual_jobs > 0) state = 'pending';
    else if (run.status === 'queued') state = 'queued';
    else if (run.status === 'in_progress') state = 'pending';
    else state = 'unconfirmed';
    // Known GitHub zombies never start and hold no runner: a workflow queued
    // with no jobs, or one whose only remaining jobs are all queued. Both
    // become stale once that whole hour is older than STALE_WAITING_SECONDS.
    // Pending, waiting and requested workflows keep their place at any age.
    let since: number | null = null;
    if (run.status === 'queued' && row.actual_jobs === 0 && (state === 'queued' || state === 'stuck'))
      since = run.created > 0 ? run.created : null;
    else if (['queued', 'in_progress'].includes(run.status) && row.running_jobs === 0 && row.actual_jobs > 0 &&
        created.length === row.actual_jobs && created.every(c => c > 0))
      since = Math.max(...created);
    if (since === null) target[state]++;
    else {
      const hour = Math.floor(since / HOUR) * HOUR;
      const found = target.aging.find(b => b[0] === hour && b[1] === state);
      if (found) found[2]++; else target.aging.push([hour, state, 1]);
    }
    if (!settled && row.running_jobs === 0 && run.created > 0 && (['pending', 'waiting', 'requested'].includes(run.status) ||
        ['queued', 'in_progress'].includes(run.status) && row.actual_jobs > 0)) {
      if (since === null) target.oldest_waiting_at = Math.min(target.oldest_waiting_at ?? run.created, run.created);
      else {
        const hour = Math.floor(since / HOUR) * HOUR;
        const found = target.waiting_runs.find(b => b[0] === hour);
        if (found) found[1] = Math.min(found[1], run.created); else target.waiting_runs.push([hour, run.created]);
      }
    }
  }
  return { ...counts, accounting_version: 3 };
}

/** Merges partition snapshots, classifying known GitHub zombies by `clock`:
 * jobs queued and workflows queued without jobs for over STALE_WAITING_SECONDS
 * are counted separately (`stale_waiting_jobs`, `stale`) and never in waiting
 * counts or the oldest wait. */
export function mergeBacklogs(partitions: any[], pending: boolean, clock: number) {
  const staleBefore = clock - STALE_WAITING_SECONDS;
  const repositories: Record<string, any> = {};
  let partial = pending;
  for (const partition of partitions) {
    const repo = partition.partition?.split('/').slice(0, 2).join('/');
    if (!repo || !partition.backlog) { partial = true; continue; }
    const source = partition.backlog, target = repositories[repo] ||= backlogCounts();
    for (const category of ['pr_builds', 'other_workflows'] as const) {
      const from = source[category], into = target[category];
      for (const state of STATES) into[state] += from[state] || 0;
      for (const [hour, state, n] of from.aging || [])
        into[staleAt(hour, staleBefore) ? 'stale' : state] += n;
      const live = (from.waiting_runs || []).filter(([hour]: [number]) => !staleAt(hour, staleBefore)).map(([, at]: [number, number]) => at);
      for (const oldest of [from.oldest_waiting_at, ...live])
        if (oldest !== null && oldest !== undefined) into.oldest_waiting_at = Math.min(into.oldest_waiting_at ?? oldest, oldest);
    }
    target.waiting_jobs += source.waiting_jobs;
    target.running_jobs += source.running_jobs;
    for (const [hour, n] of source.waiting_created || [])
      if (staleAt(hour, staleBefore)) target.stale_waiting_jobs += n; else target.waiting_jobs += n;
  }
  for (const target of Object.values(repositories)) {
    delete target.waiting_created;
    for (const category of ['pr_builds', 'other_workflows']) { delete target[category].aging; delete target[category].waiting_runs; }
  }
  return { schema: 'tauceti.operations.backlog/v1', coverage: partial ? 'partial' : 'observed', repositories };
}
