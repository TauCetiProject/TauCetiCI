/** Read-only backlog summary of retained facts, before public detail limits. */
const STATES = ["pending", "queued", "in_progress", "stuck", "unconfirmed"] as const;
const group = () => ({ pending: 0, queued: 0, in_progress: 0, stuck: 0, unconfirmed: 0, oldest_waiting_at: null as number | null });
export const backlogCounts = () => ({ pr_builds: group(), other_workflows: group(), waiting_jobs: 0, running_jobs: 0 });

export function backlogSnapshot(sql: SqlStorage, clock: number) {
  const counts = backlogCounts();
  const rows = sql.exec<any>(`SELECT r.data, m.value verified,
    SUM(CASE WHEN j.active=1 THEN 1 ELSE 0 END) active_jobs,
    SUM(CASE WHEN j.active=1 AND json_extract(j.data,'$.estimated')=0 THEN 1 ELSE 0 END) actual_jobs,
    SUM(CASE WHEN j.active=1 AND json_extract(j.data,'$.estimated')=0
      AND json_extract(j.data,'$.status')='in_progress' AND json_extract(j.data,'$.assigned')=1 THEN 1 ELSE 0 END) running_jobs
    FROM runs r LEFT JOIN jobs j ON j.run=r.id AND j.attempt=r.attempt
    LEFT JOIN meta m ON m.key='verified/'||r.id||'/'||r.attempt
    WHERE json_extract(r.data,'$.status')!='completed'
      AND NOT EXISTS (SELECT 1 FROM runs newer WHERE newer.id=r.id AND newer.attempt>r.attempt)
    GROUP BY r.id,r.attempt`).toArray();
  for (const row of rows) {
    const run = JSON.parse(row.data);
    const target = run.workflow === '.github/workflows/pr-build.yml' ? counts.pr_builds : counts.other_workflows;
    counts.running_jobs += row.running_jobs;
    counts.waiting_jobs += row.actual_jobs - row.running_jobs;
    // No age-only exclusion: require an enumerated empty/terminal attempt and
    // no active jobs, including synthetic placeholders. A live pending job
    // always keeps its place regardless of its age.
    const settled = row.verified === 'true' && row.active_jobs === 0 &&
      clock - Math.max(run.created || 0, run.updated || 0, run.started || 0) >= 5400;
    if (settled) target.stuck++;
    else if (row.running_jobs > 0) target.in_progress++;
    else if (['pending', 'waiting', 'requested'].includes(run.status) ||
        run.status === 'in_progress' && row.actual_jobs > 0) target.pending++;
    else if (row.actual_jobs > 0 && run.status === 'queued') target.queued++;
    else target.unconfirmed++;
    if (!settled && row.running_jobs === 0 && (['pending', 'waiting', 'requested'].includes(run.status) ||
        ['queued', 'in_progress'].includes(run.status) && row.actual_jobs > 0) && run.created > 0)
      target.oldest_waiting_at = Math.min(target.oldest_waiting_at ?? run.created, run.created);
  }
  return counts;
}

export function mergeBacklogs(partitions: any[], pending: boolean) {
  const repositories: Record<string, ReturnType<typeof backlogCounts>> = {};
  let partial = pending;
  for (const partition of partitions) {
    const repo = partition.partition?.split('/').slice(0, 2).join('/');
    if (!repo || !partition.backlog) { partial = true; continue; }
    const source = partition.backlog, target = repositories[repo] ||= backlogCounts();
    for (const category of ['pr_builds', 'other_workflows'] as const) {
      for (const state of STATES) target[category][state] += source[category][state];
      const oldest = source[category].oldest_waiting_at;
      if (oldest !== null) target[category].oldest_waiting_at = Math.min(target[category].oldest_waiting_at ?? oldest, oldest);
    }
    target.waiting_jobs += source.waiting_jobs;
    target.running_jobs += source.running_jobs;
  }
  return { schema: 'tauceti.operations.backlog/v1', coverage: partial ? 'partial' : 'observed', repositories };
}
