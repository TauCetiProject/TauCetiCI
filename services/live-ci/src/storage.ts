import {
  contributions,
  difference,
  normalizeJob,
  reportingKind,
  normalizeRun,
  newer,
  day,
  DAY,
  iso,
  WORKFLOW,
  validTelemetry,
  type Row,
  type Run,
  type Job,
} from "./model";
import { call, json, now, type Env } from "./types";
async function scheduleSoon(ctx: DurableObjectState) {
  const next = Date.now() + 1000;
  const scheduled = await ctx.storage.getAlarm();
  // A busy partition must not keep postponing its notification indefinitely.
  if (scheduled === null || scheduled > next) await ctx.storage.setAlarm(next);
}
async function archiveRead(env: Env, key: string): Promise<any> {
  const object = await env.ARCHIVE.get(key + ".gz");
  return object
    ? new Response(
        object.body.pipeThrough(new DecompressionStream("gzip")),
      ).json()
    : null;
}
async function archiveWrite(env: Env, key: string, value: any) {
  const compressed = await new Response(
    new Blob([JSON.stringify(value)])
      .stream()
      .pipeThrough(new CompressionStream("gzip")),
  ).arrayBuffer();
  await env.ARCHIVE.put(key + ".gz", compressed, {
    httpMetadata: { contentType: "application/json", contentEncoding: "gzip" },
  });
}

/** Sharded locator: workflow_job does not carry its parent run creation date. */
export class RunDirectory {
  constructor(
    private ctx: DurableObjectState,
    private env: Env,
  ) {
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY,partition TEXT,seen INTEGER)",
    );
  }
  async fetch(request: Request) {
    const b: any = await request.json();
    if (new URL(request.url).pathname === "/put")
      this.ctx.storage.sql.exec(
        "INSERT INTO runs VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET partition=excluded.partition,seen=excluded.seen",
        b.id,
        b.partition,
        now(),
      );
    return json(
      this.ctx.storage.sql
        .exec("SELECT partition FROM runs WHERE id=?", b.id)
        .toArray()[0] || {},
    );
  }
}

/** One repository/day. Facts, prior contributions and transactional outbox commit together. */
export class FactPartition {
  private sql: SqlStorage;
  constructor(
    private ctx: DurableObjectState,
    private env: Env,
  ) {
    this.sql = ctx.storage.sql;
    this.sql
      .exec(`CREATE TABLE IF NOT EXISTS runs (id TEXT,attempt INTEGER,data TEXT,PRIMARY KEY(id,attempt));
      CREATE INDEX IF NOT EXISTS runs_active ON runs(id,attempt) WHERE json_extract(data,'$.status')!='completed';
      CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY,run TEXT,attempt INTEGER,active INTEGER,data TEXT,contribution TEXT);
      CREATE INDEX IF NOT EXISTS jobs_run ON jobs(run,attempt);
      CREATE INDEX IF NOT EXISTS jobs_estimated ON jobs(run,attempt) WHERE json_extract(data,'$.estimated')=1;
      CREATE INDEX IF NOT EXISTS jobs_active ON jobs(active);
      CREATE INDEX IF NOT EXISTS jobs_archive ON jobs(active,run,id);
      CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT,day TEXT,data TEXT);
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT);
      CREATE TABLE IF NOT EXISTS deliveries (id TEXT PRIMARY KEY,at INTEGER);
      CREATE INDEX IF NOT EXISTS deliveries_at ON deliveries(at);`);
  }
  meta(k: string): any {
    const r = this.sql
      .exec<{ value: string }>("SELECT value FROM meta WHERE key=?", k)
      .toArray()[0];
    return r ? JSON.parse(r.value) : undefined;
  }
  set(k: string, v: any) {
    this.sql.exec(
      "INSERT OR REPLACE INTO meta VALUES (?,?)",
      k,
      JSON.stringify(v),
    );
  }
  run(id: string, attempt: number): Run | undefined {
    const r = this.sql
      .exec<{
        data: string;
      }>("SELECT data FROM runs WHERE id=? AND attempt=?", id, attempt)
      .toArray()[0];
    return r ? JSON.parse(r.data) : undefined;
  }
  replaceJob(run: Run, j: Job, clock: number) {
    const before = this.sql
      .exec<{
        data: string;
        contribution: string;
        active: number;
      }>("SELECT data,contribution,active FROM jobs WHERE id=?", j.id)
      .toArray()[0];
    const old: Job | undefined = before ? JSON.parse(before.data) : undefined;
    const next = before ? newer<Job>(old, j) : j;
    if (old) {
      next.failure = next.failure || old.failure;
      if (!next.steps.length) next.steps = old.steps;
      if (next.kind === "unknown") next.kind = old.kind;
      if (!next.runnerName) next.runnerName = old.runnerName;
      if (!next.labels?.length) next.labels = old.labels;
      next.kind = reportingKind(next.kind, next.labels, next.runnerName);
      // Job identity is immutable even when its archived execution snapshot is
      // older. Learn a missing host without rolling back status or timings.
      const archivedKind = reportingKind("self-hosted", j.labels, j.runnerName);
      if (["unknown", "self-hosted"].includes(next.kind) &&
          ["eastanglia", "leanfro"].includes(archivedKind)) {
        next.kind = archivedKind;
        next.labels = j.labels;
        next.runnerName = j.runnerName || next.runnerName;
      }
    }
    const after = contributions(run, next, clock),
      delta = difference(before ? JSON.parse(before.contribution) : [], after);
    const active =
      next.status !== "completed" && run.status !== "completed" ? 1 : 0;
    if (
      !delta.length &&
      before &&
      before.data === JSON.stringify(next) &&
      before.active === active
    )
      return;
    this.sql.exec(
      "INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?)",
      next.id,
      run.id,
      run.attempt,
      active,
      JSON.stringify(next),
      JSON.stringify(after),
    );
    const groups = new Map<string, Row[]>();
    for (const r of delta) {
      const group = groups.get(r.day) || [];
      group.push(r);
      groups.set(r.day, group);
    }
    for (const [date, rows] of groups)
      for (let i = 0; i < rows.length; i += 200)
        this.sql.exec(
          "INSERT INTO outbox(day,data) VALUES (?,?)",
          date,
          JSON.stringify(rows.slice(i, i + 200)),
        );
  }
  async flush(notify = false) {
    const partition = this.meta("partition");
    const pending = this.sql
      .exec<{
        id: number;
        day: string;
        data: string;
      }>("SELECT * FROM outbox ORDER BY id LIMIT 40")
      .toArray();
    const groups = new Map<string, typeof pending>();
    for (const row of pending) {
      const group = groups.get(row.day) || [];
      group.push(row);
      groups.set(row.day, group);
    }
    for (const [date, rows] of groups) {
      const deltas = rows.map((row) => ({
        id: `${partition}/${row.id}`,
        day: date,
        rows: JSON.parse(row.data),
      }));
      if (deltas.length === 1)
        await call(this.env.DAYS, date, "/delta", deltas[0]);
      else {
        try {
          await call(this.env.DAYS, date, "/delta-batch", {
            day: date,
            deltas,
          });
        } catch (error) {
          // Durable Objects can adopt new code at different times. Old days
          // still accept individual receipts during deployment and rollback.
          if (!/Internal \/delta-batch: 404/.test(String(error))) throw error;
          for (const delta of deltas)
            await call(this.env.DAYS, date, "/delta", delta);
        }
      }
      for (const row of rows)
        this.sql.exec("DELETE FROM outbox WHERE id=?", row.id);
      this.set("outbox_last_drained", now());
    }
    const active =
      this.sql
        .exec<{ n: number }>("SELECT COUNT(*) n FROM jobs WHERE active=1")
        .one().n +
      this.sql
        .exec<{
          n: number;
        }>(
          "SELECT COUNT(*) n FROM runs WHERE json_extract(data,'$.status')!='completed'",
        )
        .one().n;
    const revision = (this.meta("partition_revision") || 0) + 1;
    this.set("partition_revision", revision);
    this.set("partition_pending", revision);
    // Notify from an alarm, whose request context starts a fresh call chain.
    // Calling Control here during Control -> Facts -> Days ingestion creates
    // reentrant chains that exhaust the production Workers depth budget.
    if (notify) {
      await call(this.env.CONTROL, "control", "/partition", {
        partition,
        active,
        revision,
      });
      if (this.meta("partition_pending") === revision)
        this.set("partition_pending", null);
    }
    const stillActive =
      this.sql.exec("SELECT 1 FROM jobs WHERE active=1 LIMIT 1").toArray()
        .length ||
      this.sql
        .exec(
          "SELECT 1 FROM runs WHERE json_extract(data,'$.status')!='completed' LIMIT 1",
        )
        .toArray().length;
    const stillPending =
      this.meta("partition_pending") ||
      this.sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length ||
      this.meta("alarm_cursor");
    // Persist the refresh deadline independently of notification/drain wakes.
    // Otherwise a steady stream of deliveries can postpone refresh forever.
    if (stillActive && !this.meta("alarm_refresh_due"))
      this.set("alarm_refresh_due", now() + 60);
    if (stillPending) await scheduleSoon(this.ctx);
    else if (stillActive)
      await this.ctx.storage.setAlarm(
        Math.max(Date.now() + 1000, this.meta("alarm_refresh_due") * 1000),
      );
    else await this.ctx.storage.deleteAlarm();
  }
  async alarm() {
    // Persist the next retry before any network call, including during cold
    // aggregate restoration or an extended downstream outage.
    await this.ctx.storage.setAlarm(Date.now() + 60000);
    this.ctx.storage.transactionSync(() => {
      // Drain old receipts before producing another page of interval deltas.
      // A drain wake must not regenerate up to 1000 updates while delivering
      // only 40: that turns a temporary burst into a self-sustaining backlog.
      if (this.sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length)
        return;
      const cursor = this.meta("alarm_cursor") || "";
      if (!cursor && (this.meta("alarm_refresh_due") || 0) > now())
        return;
      const rows = this.sql
        .exec<{
          id: string;
          data: string;
          run: string;
          attempt: number;
        }>(
          "SELECT id,data,run,attempt FROM jobs WHERE active=1 AND id>? ORDER BY id LIMIT 1000",
          cursor,
        )
        .toArray();
      for (const row of rows) {
        const run = this.run(row.run, row.attempt);
        if (run) {
          const job = JSON.parse(row.data);
          job.kind = reportingKind(job.kind, job.labels, job.runnerName);
          this.replaceJob(run, job, now());
        }
      }
      const full = rows.length === 1000;
      this.set("alarm_cursor", full ? rows.at(-1)!.id : "");
      this.set("alarm_last_refreshed", now());
      // Continue a paginated sweep only after its existing receipts drain.
      // Start the next sweep a minute after this one finishes, not once per
      // drain wake. Each page uses its current clock so newer event facts
      // cannot be rolled back to an older sweep timestamp.
      if (!full) this.set("alarm_refresh_due", now() + 60);
    });
    await this.flush(true);
  }
  async fetch(request: Request) {
    const path = new URL(request.url).pathname,
      b: any = await request.json();
    if (path === "/backlog")
      return json({
        partition: this.meta("partition"),
        outbox: this.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM outbox").one().n,
        outbox_sequence: this.sql.exec<{ seq: number }>(
          "SELECT seq FROM sqlite_sequence WHERE name='outbox'",
        ).toArray()[0]?.seq || 0,
        oldest: this.sql.exec("SELECT id,day FROM outbox ORDER BY id LIMIT 1").toArray()[0] || null,
        active_jobs: this.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM jobs WHERE active=1").one().n,
        last_drained: this.meta("outbox_last_drained") || null,
        last_refreshed: this.meta("alarm_last_refreshed") || null,
        refresh_due: this.meta("alarm_refresh_due") || null,
        refresh_cursor: this.meta("alarm_cursor") || null,
      });
    if (path === "/archive") {
      const partition = this.meta("partition");
      if (!partition) return json({ done: true });
      if (this.sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length)
        return json({ done: false, reason: "outbox pending" });
      const cutoff = now() - (b.builds ? 90 : 30) * DAY;
      // Archive by complete run, in bounded pages. The previous contribution is
      // retained in the archive, so restoring it does not re-add any metrics.
      const eligible = this.sql
        .exec<{ id: string; attempt: number; data: string }>(
          `SELECT * FROM runs WHERE (id>? OR (id=? AND attempt>?)) AND json_extract(data,'$.created')<?
        AND (?=1 OR json_extract(data,'$.workflow')!=?)
        AND NOT EXISTS(SELECT 1 FROM jobs WHERE jobs.run=runs.id AND jobs.attempt=runs.attempt AND jobs.active=1)
        ORDER BY id,attempt LIMIT ?`,
          b.cursor?.id || "",
          b.cursor?.id || "",
          b.cursor?.attempt || 0,
          cutoff,
          b.builds ? 1 : 0,
          WORKFLOW,
          Math.min(100, b.limit || 20),
        )
        .toArray();
      for (const r of eligible) {
        const jobs = this.sql
          .exec("SELECT * FROM jobs WHERE run=? AND attempt=?", r.id, r.attempt)
          .toArray();
        const archived = { partition, run: r, jobs };
        const key = `facts/${partition}/${r.id}/${r.attempt}.json`;
        await archiveWrite(this.env, key, archived);
        this.ctx.storage.transactionSync(() => {
          // A concurrent correction must not be erased by an older archive.
          const current = this.sql
            .exec<{
              data: string;
            }>(
              "SELECT data FROM runs WHERE id=? AND attempt=?",
              r.id,
              r.attempt,
            )
            .toArray()[0];
          const currentJobs = this.sql
            .exec(
              "SELECT * FROM jobs WHERE run=? AND attempt=?",
              r.id,
              r.attempt,
            )
            .toArray();
          if (
            current?.data !== r.data ||
            JSON.stringify(currentJobs) !== JSON.stringify(jobs) ||
            this.sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length
          )
            return;
          this.sql.exec(
            "DELETE FROM jobs WHERE run=? AND attempt=?",
            r.id,
            r.attempt,
          );
          this.sql.exec(
            "DELETE FROM runs WHERE id=? AND attempt=?",
            r.id,
            r.attempt,
          );
        });
      }
      const last = eligible.at(-1);
      return json({
        done: eligible.length < Math.min(100, b.limit || 20),
        cursor: last
          ? { id: last.id, attempt: last.attempt }
          : b.cursor || null,
      });
    }
    if (path === "/restore") {
      if (this.run(b.id, b.attempt)) return json({ restored: false });
      const archive = await archiveRead(
        this.env,
        `facts/${b.partition}/${b.id}/${b.attempt}.json`,
      );
      if (!archive) return json({ restored: false });
      this.ctx.storage.transactionSync(() => {
        if (this.run(b.id, b.attempt)) return;
        this.set("partition", b.partition);
        this.sql.exec(
          "INSERT INTO runs VALUES (?,?,?)",
          archive.run.id,
          archive.run.attempt,
          archive.run.data,
        );
        for (const j of archive.jobs)
          this.sql.exec(
            "INSERT OR IGNORE INTO jobs VALUES (?,?,?,?,?,?)",
            j.id,
            j.run,
            j.attempt,
            j.active,
            j.data,
            j.contribution,
          );
      });
      return json({ restored: true });
    }
    if (path === "/current") {
      const jobs = this.sql
        .exec(
          `SELECT id,run,attempt,json_extract(data,'$.name') name,json_extract(data,'$.status') status,
        json_extract(data,'$.kind') kind,json_extract(data,'$.assigned') assigned,json_extract(data,'$.updated') updated
        FROM jobs WHERE active=1 LIMIT 1000`,
        )
        .toArray();
      const runs = this.sql
        .exec<{ data: string }>(
          "SELECT data FROM runs WHERE json_extract(data,'$.status')!='completed' LIMIT 500",
        )
        .toArray()
        .map((r) => JSON.parse(r.data));
      const counts = this.sql
        .exec(
          `SELECT json_extract(data,'$.kind') kind,
        CASE WHEN json_extract(data,'$.status')='in_progress' AND json_extract(data,'$.assigned')=1 THEN 'running' ELSE 'queued' END state,COUNT(*) count
        FROM jobs WHERE active=1 GROUP BY 1,2`,
        )
        .toArray();
      return json({
        jobs,
        runs,
        counts,
        truncated: jobs.length === 1000 || runs.length === 500,
      });
    }
    if (path === "/status")
      return json({
        run: this.run(b.id, b.attempt),
        jobs: this.sql
          .exec<{
            n: number;
          }>(
            "SELECT COUNT(*) n FROM jobs WHERE run=? AND attempt=?",
            b.id,
            b.attempt,
          )
          .one().n,
        job_rows: this.sql
          .exec<{ data: string }>(
            "SELECT data FROM jobs WHERE run=? AND attempt=?",
            b.id,
            b.attempt,
          )
          .toArray()
          .map((r) => JSON.parse(r.data)),
        verified: this.meta(`verified/${b.id}/${b.attempt}`) || false,
      });
    if (path === "/verified") {
      const active = this.sql
        .exec<{
          n: number;
        }>(
          "SELECT COUNT(*) n FROM jobs WHERE run=? AND attempt=? AND active=1",
          b.id,
          b.attempt,
        )
        .one().n;
      if (!active) this.set(`verified/${b.id}/${b.attempt}`, true);
      return json({ verified: !active });
    }
    if (path === "/members") {
      const run = this.run(b.id, b.attempt);
      if (run && run.batch === b.batch && run.testedHead === b.head) {
        run.members = b.members;
        this.sql.exec(
          "UPDATE runs SET data=? WHERE id=? AND attempt=?",
          JSON.stringify(run),
          b.id,
          b.attempt,
        );
      }
      return json({ ok: true });
    }
    if (path === "/export")
      return json({
        partition: this.meta("partition"),
        runs: this.sql.exec("SELECT * FROM runs").toArray(),
        jobs: this.sql.exec("SELECT * FROM jobs").toArray(),
      });
    if (path === "/tick") {
      await this.alarm();
      return json({ ok: true });
    }
    if (path === "/recompute") {
      if (!this.meta("partition")) return json({ processed: 0, changed: 0, unidentified: 0, cursor: null });
      const rows = this.sql.exec<{ id: string; run: string; attempt: number; data: string }>(
        "SELECT id,run,attempt,data FROM jobs WHERE id>? AND json_extract(data,'$.kind') IN ('self-hosted','eastanglia','leanfro') ORDER BY id LIMIT 40",
        b.cursor || "",
      ).toArray();
      let changed = 0, unidentified = 0;
      this.ctx.storage.transactionSync(() => {
        for (const row of rows) {
          const run = this.run(row.run, row.attempt);
          if (run) {
            const job = JSON.parse(row.data);
            const kind = reportingKind(job.kind, job.labels, job.runnerName);
            if (kind !== job.kind) changed++;
            if (kind === "self-hosted") unidentified++;
            job.kind = kind;
            this.replaceJob(run, job, now());
          }
        }
      });
      await this.flush(true);
      return json({ processed: rows.length, changed, unidentified, cursor: rows.length === 40 ? rows.at(-1)!.id : null });
    }
    if (path === "/event") {
      const runID = String(
          b.run?.id ?? b.run?.run_id ?? b.run_id ?? b.jobs?.[0]?.run_id,
        ),
        attempt =
          b.run?.run_attempt ?? b.attempt ?? b.jobs?.[0]?.run_attempt ?? 1;
      if (!this.run(runID, attempt)) {
        const archived = await archiveRead(
          this.env,
          `facts/${b.partition}/${runID}/${attempt}.json`,
        );
        if (archived) {
          this.ctx.storage.transactionSync(() => {
            if (this.run(runID, attempt)) return;
            this.sql.exec(
              "INSERT INTO runs VALUES (?,?,?)",
              archived.run.id,
              archived.run.attempt,
              archived.run.data,
            );
            for (const j of archived.jobs)
              this.sql.exec(
                "INSERT OR IGNORE INTO jobs VALUES (?,?,?,?,?,?)",
                j.id,
                j.run,
                j.attempt,
                j.active,
                j.data,
                j.contribution,
              );
          });
        }
      }
      this.ctx.storage.transactionSync(() => {
        this.set("partition", b.partition);
        if (
          this.sql
            .exec("SELECT 1 FROM deliveries WHERE id=?", b.delivery)
            .toArray().length
        )
          return;
        if (b.run) {
          const next = normalizeRun(b.repo, b.run),
            old = this.run(next.id, next.attempt);
          const run = newer(old, next);
          if (next.archiveSampling || old?.archiveSampling)
            run.archiveSampling = next.archiveSampling || old!.archiveSampling;
          if (old?.telemetry) run.telemetry = old.telemetry;
          if (
            old?.members &&
            !run.members &&
            old.batch === run.batch &&
            old.testedHead === run.testedHead
          )
            run.members = old.members;
          if (b.telemetry && validTelemetry(b.telemetry))
            run.telemetry = b.telemetry;
          if (b.verified_jobs)
            this.set(`verified/${run.id}/${run.attempt}`, true);
          this.sql.exec(
            "INSERT OR REPLACE INTO runs VALUES (?,?,?)",
            run.id,
            run.attempt,
            JSON.stringify(run),
          );
          for (const row of this.sql
            .exec<{
              data: string;
            }>(
              "SELECT data FROM jobs WHERE run=? AND attempt=?",
              run.id,
              run.attempt,
            )
            .toArray())
            this.replaceJob(run, JSON.parse(row.data), now());
        }
        if (b.telemetry && !b.run) {
          const run = this.run(String(b.run_id), b.attempt);
          if (!run) throw new Error("Run metadata not present");
          run.telemetry = { ...run.telemetry, ...b.telemetry };
          this.sql.exec(
            "UPDATE runs SET data=? WHERE id=? AND attempt=?",
            JSON.stringify(run),
            run.id,
            run.attempt,
          );
          for (const row of this.sql
            .exec<{
              data: string;
            }>(
              "SELECT data FROM jobs WHERE run=? AND attempt=?",
              run.id,
              run.attempt,
            )
            .toArray())
            this.replaceJob(run, JSON.parse(row.data), now());
        }
        for (const raw of b.jobs || []) {
          const j = normalizeJob(raw),
            run = this.run(j.run, j.attempt);
          if (!run) throw new Error("Job parent metadata missing");
          const siblings = this.sql
            .exec<{
              id: string;
              data: string;
            }>(
              "SELECT id,data FROM jobs WHERE run=? AND attempt=? AND json_extract(data,'$.estimated')=1",
              j.run,
              j.attempt,
            )
            .toArray();
          if (
            j.estimated &&
            this.sql
              .exec(
                "SELECT 1 FROM jobs WHERE run=? AND attempt=? AND json_extract(data,'$.estimated')=0 LIMIT 1",
                j.run,
                j.attempt,
              )
              .toArray().length
          )
            continue;
          if (!j.estimated)
            for (const sibling of siblings) {
              const old: Job = JSON.parse(sibling.data);
              if (old.estimated) {
                this.replaceJob(run, { ...old, weight: 0 }, now());
                this.sql.exec("DELETE FROM jobs WHERE id=?", old.id);
              }
            }
          this.replaceJob(run, j, now());
        }
        this.sql.exec("INSERT INTO deliveries VALUES (?,?)", b.delivery, now());
        this.sql.exec("DELETE FROM deliveries WHERE at<?", now() - 14 * DAY);
      });
      await scheduleSoon(this.ctx);
      await this.flush();
      return json({ ok: true });
    }
    return json({ error: "unknown operation" }, 404);
  }
}

/** Global UTC day: delta receipts make cross-object outbox replay exactly once. */
export class DayAggregate {
  private sql: SqlStorage;
  private restoration?: Promise<boolean>;
  constructor(
    private ctx: DurableObjectState,
    private env: Env,
  ) {
    this.sql = ctx.storage.sql;
    this.sql
      .exec(`CREATE TABLE IF NOT EXISTS metrics (bucket INTEGER,metric TEXT,value REAL,amount REAL,PRIMARY KEY(bucket,metric,value));
      CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS observations (at INTEGER PRIMARY KEY,data TEXT);
      CREATE TABLE IF NOT EXISTS landings (pr INTEGER PRIMARY KEY,at INTEGER);
      CREATE TABLE IF NOT EXISTS staging (id TEXT PRIMARY KEY,attempt INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS buckets (bucket INTEGER PRIMARY KEY,data TEXT);
      CREATE TABLE IF NOT EXISTS dirty_buckets (bucket INTEGER PRIMARY KEY);
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY,value TEXT);`);
  }
  meta(key: string): any {
    const r = this.sql
      .exec<{ value: string }>("SELECT value FROM meta WHERE key=?", key)
      .toArray()[0];
    return r ? JSON.parse(r.value) : undefined;
  }
  set(key: string, value: any) {
    this.sql.exec(
      "INSERT OR REPLACE INTO meta VALUES (?,?)",
      key,
      JSON.stringify(value),
    );
  }
  summary() {
    // SQLite performs distribution scans; JavaScript receives only quantiles
    // and compact bins. Large days never materialize all interval boundaries
    // or timing samples in the Worker heap.
    if (!this.meta("projection_initialized")) {
      this.sql.exec(
        "INSERT OR IGNORE INTO dirty_buckets SELECT DISTINCT bucket FROM metrics",
      );
      this.set("projection_initialized", true);
    }
    for (const { bucket } of this.sql
      .exec<{
        bucket: number;
      }>("SELECT bucket FROM dirty_buckets ORDER BY bucket")
      .toArray()) {
      const data: any = bucket === 0 ? { daily: {} } : { fleet: [] };
      const metrics = this.sql
        .exec<{ metric: string }>(
          "SELECT DISTINCT metric FROM metrics WHERE bucket=?",
          bucket,
        )
        .toArray()
        .map((r) => r.metric);
      if (bucket === 0) {
        for (const metric of metrics) {
          if (metric.startsWith("duration/") || metric.startsWith("wait/")) {
            const total =
              this.sql
                .exec<{
                  n: number;
                }>(
                  "SELECT SUM(amount) n FROM metrics WHERE bucket=0 AND metric=? AND amount>0",
                  metric,
                )
                .one().n || 0;
            const quantile = (q: number) =>
              this.sql
                .exec<{ value: number }>(
                  `SELECT value FROM
              (SELECT value,SUM(amount) OVER (ORDER BY value ROWS UNBOUNDED PRECEDING) position FROM metrics WHERE bucket=0 AND metric=? AND amount>0)
              WHERE position>=? ORDER BY value LIMIT 1`,
                  metric,
                  Math.max(1, Math.ceil(total * q)),
                )
                .toArray()[0]?.value ?? null;
            data.daily[metric] = { median: quantile(0.5), p90: quantile(0.9) };
          } else
            data.daily[metric] =
              this.sql
                .exec<{
                  n: number;
                }>(
                  "SELECT SUM(amount) n FROM metrics WHERE bucket=0 AND metric=?",
                  metric,
                )
                .one().n || 0;
        }
      } else {
        const byStep = new Map<string, any>();
        for (const metric of metrics) {
          const [, step, type, key = ""] = metric.split("/");
          const b = byStep.get(step) || {
            at: bucket,
            step: Number(step),
            running: {},
            queued: {},
            wait: {},
            picked: [],
          };
          byStep.set(step, b);
          if (type === "running")
            b.running[key] =
              this.sql
                .exec<{
                  n: number;
                }>(
                  "SELECT SUM(amount) n FROM metrics WHERE bucket=? AND metric=?",
                  bucket,
                  metric,
                )
                .one().n || 0;
          if (type === "queued")
            b.queued[key] = Math.max(
              0,
              this.sql
                .exec<{
                  n: number;
                }>(
                  `SELECT MAX(depth) n FROM (SELECT SUM(amount) OVER (ORDER BY value ROWS UNBOUNDED PRECEDING) depth FROM metrics WHERE bucket=? AND metric=?)`,
                  bucket,
                  metric,
                )
                .one().n || 0,
            );
          if (type === "wait")
            b.wait[key] =
              this.sql
                .exec<{
                  n: number;
                }>(
                  "SELECT MAX(value) n FROM metrics WHERE bucket=? AND metric=? AND amount>0",
                  bucket,
                  metric,
                )
                .one().n || 0;
          if (type === "picked")
            b.picked = this.sql
              .exec<{ value: number }>(
                "SELECT value FROM metrics WHERE bucket=? AND metric=? AND amount>0 ORDER BY value",
                bucket,
                metric,
              )
              .toArray()
              .map((r) => r.value);
        }
        data.fleet = [...byStep.values()];
      }
      this.sql.exec(
        "INSERT OR REPLACE INTO buckets VALUES (?,?)",
        bucket,
        JSON.stringify(data),
      );
      this.sql.exec("DELETE FROM dirty_buckets WHERE bucket=?", bucket);
    }
    const dailyRow = this.sql
      .exec<{ data: string }>("SELECT data FROM buckets WHERE bucket=0")
      .toArray()[0];
    const fleet = this.sql
      .exec<{ data: string }>(
        "SELECT data FROM buckets WHERE bucket>0 ORDER BY bucket",
      )
      .toArray()
      .flatMap((r) => JSON.parse(r.data).fleet);
    return {
      daily: dailyRow ? JSON.parse(dailyRow.data).daily : {},
      fleet,
      observations: this.sql
        .exec<{ data: string }>("SELECT data FROM observations ORDER BY at")
        .toArray()
        .map((r) => JSON.parse(r.data)),
      staging: this.sql
        .exec<{ data: string }>("SELECT data FROM staging")
        .toArray()
        .map((r) => JSON.parse(r.data)),
      landings: this.sql.exec("SELECT * FROM landings").toArray(),
    };
  }
  async restore(): Promise<boolean> {
    if (this.restoration) return this.restoration;
    this.restoration = this.restorePage();
    try {
      return await this.restoration;
    } finally {
      this.restoration = undefined;
    }
  }
  private async restorePage(): Promise<boolean> {
    const cold = this.meta("cold");
    if (!cold) return true;
    let position = this.meta("restore") || 0;
    for (let i = 0; i < 20 && position < cold.pages.length; i++, position++) {
      const page = await archiveRead(this.env, cold.pages[position]);
      if (!page) throw new Error("Cold aggregate page missing");
      this.ctx.storage.transactionSync(() => {
        for (const row of page.rows) {
          const { archive_rowid, ...values } = row,
            columns = Object.keys(values);
          this.sql.exec(
            `INSERT OR REPLACE INTO ${page.table}(${columns.join(",")}) VALUES (${columns.map(() => "?").join(",")})`,
            ...(Object.values(values) as any[]),
          );
        }
        this.set("restore", position + 1);
      });
    }
    if (position >= cold.pages.length) {
      this.set("cold", null);
      this.set("restore", 0);
      return true;
    }
    await this.ctx.storage.setAlarm(Date.now() + 1000);
    return false;
  }
  async alarm() {
    await this.ctx.storage.setAlarm(Date.now() + 60000);
    const restored = await this.restore();
    const pending = this.meta("notification");
    if (pending) {
      await call(this.env.CONTROL, "control", "/dirty", { day: pending.day });
      if (this.meta("notification")?.revision === pending.revision)
        this.set("notification", null);
    }
    if (restored && !this.meta("notification"))
      await this.ctx.storage.deleteAlarm();
    else await this.ctx.storage.setAlarm(Date.now() + 1000);
  }
  async compact(date: string) {
    if (date >= day(now() - 90 * DAY))
      return { done: false, reason: "inside detail retention" };
    if (this.meta("notification"))
      return { done: false, reason: "notification pending" };
    if (this.meta("cold")) return { done: true };
    const revision = this.meta("revision") || 0;
    let state = this.meta("compaction");
    if (!state || state.revision !== revision)
      state = { revision, table: 0, cursor: 0, pages: [] };
    const tables = [
      "metrics",
      "receipts",
      "observations",
      "landings",
      "staging",
      "buckets",
      "dirty_buckets",
    ];
    for (let i = 0; i < 2 && state.table < tables.length; i++) {
      const table = tables[state.table],
        rows = this.sql
          .exec<any>(
            `SELECT rowid archive_rowid,* FROM ${table} WHERE rowid>? ORDER BY rowid LIMIT 500`,
            state.cursor,
          )
          .toArray();
      if (rows.length) {
        const key = `aggregates/${date}/${revision}/${table}/${state.cursor}.json`;
        await archiveWrite(this.env, key, { table, rows });
        state.pages.push(key);
        state.cursor = rows.at(-1).archive_rowid;
      }
      if (rows.length < 500) {
        state.table++;
        state.cursor = 0;
      }
    }
    if ((this.meta("revision") || 0) !== revision) {
      this.set("compaction", null);
      return { done: false };
    }
    if (state.table === tables.length) {
      await this.env.ARCHIVE.put(
        `summaries/${date}.json`,
        JSON.stringify(this.summary()),
      );
      await archiveWrite(this.env, `aggregates/${date}/manifest.json`, state);
      this.ctx.storage.transactionSync(() => {
        if ((this.meta("revision") || 0) !== revision) return;
        for (const table of tables) this.sql.exec(`DELETE FROM ${table}`);
        this.set("cold", state);
        this.set("compaction", null);
      });
      return { done: !!this.meta("cold") };
    }
    this.set("compaction", state);
    return { done: false };
  }
  async fetch(request: Request) {
    const path = new URL(request.url).pathname,
      b: any = await request.json();
    if (path === "/tick") {
      await this.alarm();
      return json({ ok: true });
    }
    if (path === "/compact") return json(await this.compact(b.day));
    if (path === "/summary" && this.meta("cold")) {
      const object = await this.env.ARCHIVE.get(`summaries/${b.day}.json`);
      if (object)
        return new Response(object.body, {
          headers: { "Content-Type": "application/json" },
        });
      throw new Error("Cold summary missing");
    }
    if (path !== "/summary" && !(await this.restore()))
      throw new Error("Aggregate restoration in progress");
    if (path === "/delta" || path === "/delta-batch") {
      const deltas = path === "/delta" ? [b] : b.deltas;
      if (
        !Array.isArray(deltas) ||
        deltas.length > 40 ||
        deltas.reduce((n, delta) => n + (delta.rows?.length || 0), 0) > 8000
      )
        return json({ error: "Delta batch exceeds limit" }, 400);
      this.ctx.storage.transactionSync(() => {
        for (const delta of deltas) {
          if (
            this.sql
              .exec("SELECT 1 FROM receipts WHERE id=?", delta.id)
              .toArray().length
          )
            continue;
          for (const row of delta.rows) {
            this.sql.exec(
              "INSERT OR IGNORE INTO dirty_buckets VALUES (?)",
              row.bucket,
            );
            this.sql.exec(
              "INSERT INTO metrics VALUES (?,?,?,?) ON CONFLICT(bucket,metric,value) DO UPDATE SET amount=amount+excluded.amount",
              row.bucket,
              row.metric,
              row.value,
              row.amount,
            );
            this.sql.exec(
              "DELETE FROM metrics WHERE bucket=? AND metric=? AND value=? AND ABS(amount)<0.000000001",
              row.bucket,
              row.metric,
              row.value,
            );
          }
          this.sql.exec("INSERT INTO receipts VALUES (?)", delta.id);
        }
      });
    } else if (path === "/observation")
      this.sql.exec(
        "INSERT OR REPLACE INTO observations VALUES (?,?)",
        b.at,
        JSON.stringify(b),
      );
    else if (path === "/landing" || path === "/unlanding") {
      const key = `landing/${b.pr}`,
        revision = Number(b.revision) || 0;
      if (revision >= (this.meta(key) || 0)) {
        if (path === "/landing")
          this.sql.exec(
            "INSERT INTO landings VALUES (?,?) ON CONFLICT(pr) DO UPDATE SET at=MIN(at,excluded.at)",
            b.pr,
            b.at,
          );
        else this.sql.exec("DELETE FROM landings WHERE pr=?", b.pr);
        this.set(key, revision);
      }
    } else if (path === "/staging") {
      const prior = this.sql
          .exec<{ data: string }>("SELECT data FROM staging WHERE id=?", b.id)
          .toArray()[0],
        old = prior ? JSON.parse(prior.data) : null;
      if (
        !old ||
        b.attempt > old.attempt ||
        (b.attempt === old.attempt &&
          b.updated >= old.updated &&
          (old.status !== "completed" || b.status === "completed"))
      ) {
        if (
          !b.members &&
          old?.batch === b.batch &&
          old?.testedHead === b.testedHead
        )
          b.members = old.members;
        this.sql.exec(
          "INSERT OR REPLACE INTO staging VALUES (?,?,?)",
          b.id,
          b.attempt,
          JSON.stringify(b),
        );
      }
    } else if (path === "/summary") {
      return json(this.summary());
    } else return json({ error: "unknown operation" }, 404);
    const revision = (this.meta("revision") || 0) + 1;
    this.set("revision", revision);
    this.set("notification", {
      day: b.day || day(b.at),
      revision,
    });
    await scheduleSoon(this.ctx);
    return json({ ok: true });
  }
}
