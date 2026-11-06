import { publicJob } from "./operations-jobs";
import { RUNNERS, type Job, type Run } from "./model";

const BACKFILL_PAGE = 100;
const WINDOWS = [3600, 10800, 86400];
const RESULTS = ["success", "failure", "cancelled", "skipped", "other"];
export const activityScope = () => ({
  running: 0, waiting: 0,
  latest_completion: null as any,
  oldest_running: null as any,
  oldest_waiting: null as any,
});
const earlier = (a: any, b: any, field: string) => {
  if (!a || !Number.isFinite(Date.parse(b[field]))) return b;
  if (!Number.isFinite(Date.parse(a[field]))) return a;
  return Date.parse(b[field]) < Date.parse(a[field]) ? b : a;
};
const later = (a: any, b: any, field: string) =>
  !a || Date.parse(b[field]) > Date.parse(a[field]) ? b : a;

/** Rebuildable numeric index of retained Facts, never a second job authority.
 * Counts precede the public job-list limits. Minute bins allow the publisher
 * to advance rolling windows without revisiting quiet source partitions. */
export class ActivityIndex {
  constructor(private sql: SqlStorage) {
    sql.exec(`CREATE TABLE IF NOT EXISTS ops_activity (
      id TEXT PRIMARY KEY,pool TEXT,build INTEGER,active INTEGER,running INTEGER,
      created REAL,started REAL,completed REAL,result TEXT,ref TEXT);
      CREATE INDEX IF NOT EXISTS ops_activity_completed ON ops_activity(completed,pool,build,result);
      CREATE INDEX IF NOT EXISTS ops_activity_pool_completed ON ops_activity(pool,build,completed);
      CREATE INDEX IF NOT EXISTS ops_activity_current ON ops_activity(pool,active,running,started,created);
      CREATE TABLE IF NOT EXISTS ops_activity_pools(pool TEXT PRIMARY KEY);`);
  }
  record(run: Run, job: Job, active: number) {
    const value = publicJob(run, job);
    if (!value) { this.remove(job.id); return; }
    const pool = RUNNERS.includes(value.pool) ? value.pool : "unknown";
    const build = ["pr_build", "staging_build", "other_build"].includes(value.job_class) ? 1 : 0;
    const ref = JSON.stringify({ id: value.id, run_id: value.run_id,
      run_attempt: value.run_attempt, name: value.name, url: value.url,
      created_at: value.created_at, started_at: value.started_at,
      completed_at: value.completed_at, status: value.status, conclusion: value.conclusion });
    this.sql.exec(`INSERT INTO ops_activity VALUES (?,?,?,?,?,?,?,?,?,?)
      ON CONFLICT(id) DO UPDATE SET pool=excluded.pool,build=excluded.build,
      active=excluded.active,running=excluded.running,created=excluded.created,
      started=excluded.started,completed=excluded.completed,result=excluded.result,ref=excluded.ref
      WHERE pool IS NOT excluded.pool OR build IS NOT excluded.build
        OR active IS NOT excluded.active OR running IS NOT excluded.running
        OR created IS NOT excluded.created OR started IS NOT excluded.started
        OR completed IS NOT excluded.completed OR result IS NOT excluded.result OR ref IS NOT excluded.ref`,
    job.id, pool, build, active, job.status === "in_progress" && job.assigned ? 1 : 0,
    job.created, job.assigned ? job.started : null,
    job.status === "completed" ? job.completed : null,
    job.conclusion === "timed_out" ? "failure" : RESULTS.includes(job.conclusion || "") ? job.conclusion : "other",
    ref);
    this.sql.exec("INSERT OR IGNORE INTO ops_activity_pools VALUES (?)", pool);
  }
  remove(id: string) { this.sql.exec("DELETE FROM ops_activity WHERE id=?", id); }
  removeRun(run: string, attempt: number) {
    this.sql.exec("DELETE FROM ops_activity WHERE id IN (SELECT id FROM jobs WHERE run=? AND attempt=?)", run, attempt);
  }
  backfill(resolve: (id: string, attempt: number) => Run | undefined) {
    const read = (key: string) => {
      const row = this.sql.exec<any>("SELECT value FROM meta WHERE key=?", key).toArray()[0];
      return row ? JSON.parse(row.value) : null;
    };
    if (read("activity_index_complete")) return false;
    const rows = this.sql.exec<any>(`SELECT id,run,attempt,active,data FROM jobs WHERE id>? ORDER BY id LIMIT ${BACKFILL_PAGE+1}`,
      read("activity_index_cursor") || "").toArray();
    for (const row of rows.slice(0, BACKFILL_PAGE)) {
      const run = resolve(row.run, row.attempt);
      if (!run) throw new Error("Activity source parent missing");
      this.record(run, JSON.parse(row.data), row.active);
    }
    const key = rows.length > BACKFILL_PAGE ? "activity_index_cursor" : "activity_index_complete";
    this.sql.exec("INSERT OR REPLACE INTO meta VALUES (?,?)", key,
      JSON.stringify(rows.length > BACKFILL_PAGE ? rows[BACKFILL_PAGE-1].id : true));
    return true;
  }
  snapshot(clock: number) {
    const ready = this.sql.exec<any>("SELECT value FROM meta WHERE key='activity_index_complete'").toArray()[0];
    const pools = this.sql.exec<any>("SELECT pool FROM ops_activity_pools ORDER BY pool").toArray().map(r => ({
      pool: r.pool, all: activityScope(), builds: activityScope(), bins: [] as number[][],
    }));
    const byPool = new Map(pools.map(p => [p.pool, p]));
    const active = this.sql.exec<any>(`SELECT pool,build,running,COUNT(*) n FROM ops_activity
      WHERE active=1 GROUP BY pool,build,running`).toArray();
    for (const row of active) {
      const pool = byPool.get(row.pool)!;
      for (const scope of row.build ? [pool.all, pool.builds] : [pool.all])
        scope[row.running ? "running" : "waiting"] += row.n;
    }
    for (const pool of pools) for (const [scope, build] of [[pool.all, false], [pool.builds, true]] as const) {
      const extra = build ? " AND build=1" : "";
      for (const [field, condition, order] of [
        ["oldest_running", "active=1 AND running=1", "started ASC,created ASC"],
        ["oldest_waiting", "active=1 AND running=0", "created ASC"],
      ]) {
        const row = this.sql.exec<any>(`SELECT ref FROM ops_activity WHERE pool=?${extra} AND ${condition} ORDER BY ${order},id LIMIT 1`,
          pool.pool).toArray()[0];
        (scope as any)[field] = row ? JSON.parse(row.ref) : null;
      }
      for (const kind of build ? [1] : [0, 1]) {
        const row = this.sql.exec<any>("SELECT ref FROM ops_activity WHERE pool=? AND build=? AND completed IS NOT NULL AND completed<=? ORDER BY completed DESC,id LIMIT 1", pool.pool, kind, clock).toArray()[0];
        if (row) scope.latest_completion = later(scope.latest_completion, JSON.parse(row.ref), "completed_at");
      }
    }
    const bins = new Map<string, Map<number, number[]>>();
    for (const row of this.sql.exec<any>(`SELECT pool,CAST(completed/60 AS INTEGER)*60 minute,build,result,COUNT(*) n
      FROM ops_activity WHERE completed>=? AND completed<=? GROUP BY pool,minute,build,result`,
      Math.floor(clock / 60) * 60 - 86400, clock).toArray()) {
      const group = bins.get(row.pool) || new Map<number, number[]>(); bins.set(row.pool, group);
      const bin = group.get(row.minute) || [row.minute, ...Array(10).fill(0)]; group.set(row.minute, bin);
      const result = RESULTS.indexOf(row.result);
      bin[1 + result] += row.n;
      if (row.build) bin[6 + result] += row.n;
    }
    for (const pool of pools) pool.bins = [...(bins.get(pool.pool)?.values() || [])].sort((a,b) => a[0]-b[0]);
    return { complete: !!ready && JSON.parse(ready.value) === true, computed_at: new Date(clock * 1000).toISOString(), pools };
  }
}

export function mergeActivity(facts: any[], clock: number, missing: boolean) {
  const through = Math.floor(clock / 60) * 60;
  const pools = new Map<string, any>();
  for (const fact of facts) for (const row of fact.activity?.pools || []) {
    let pool = pools.get(row.pool);
    if (!pool) { pool = { pool: row.pool, all: activityScope(), builds: activityScope() }; pools.set(row.pool, pool); }
    for (const name of ["all", "builds"]) {
      const scope = pool[name], incoming = row[name];
      scope.running += incoming.running; scope.waiting += incoming.waiting;
      if (incoming.latest_completion) scope.latest_completion = later(scope.latest_completion, incoming.latest_completion, "completed_at");
      if (incoming.oldest_running) scope.oldest_running = earlier(scope.oldest_running, incoming.oldest_running, "started_at");
      if (incoming.oldest_waiting) scope.oldest_waiting = earlier(scope.oldest_waiting, incoming.oldest_waiting, "created_at");
      scope.windows ||= WINDOWS.map(seconds => ({ seconds, finished: 0, success: 0, failure: 0, cancelled: 0, skipped: 0, other: 0 }));
      for (const bin of row.bins) for (const window of scope.windows) {
        if (bin[0] < through - window.seconds || bin[0] >= through) continue;
        RESULTS.forEach((result, index) => { const amount = bin[(name === "all" ? 1 : 6) + index]; window[result] += amount; window.finished += amount; });
      }
    }
  }
  return { coverage: !facts.length ? "unavailable" : missing || facts.some(f => !f.activity?.complete) ? "partial" : "retained_observations",
    through: new Date(through * 1000).toISOString(), interval: "minute_aligned",
    pools: [...pools.values()].sort((a,b) => a.pool.localeCompare(b.pool)) };
}
