import {
  contributions,
  difference,
  normalizeJob,
  normalizeRun,
  newer,
  summarize,
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
      }>("SELECT data,contribution FROM jobs WHERE id=?", j.id)
      .toArray()[0];
    const old: Job | undefined = before ? JSON.parse(before.data) : undefined;
    const next = before ? newer<Job>(old, j) : j;
    if (old) {
      next.failure = next.failure || old.failure;
      if (!next.steps.length) next.steps = old.steps;
      if (next.kind === "unknown") next.kind = old.kind;
    }
    const after = contributions(run, next, clock),
      delta = difference(before ? JSON.parse(before.contribution) : [], after);
    if (!delta.length && before && before.data === JSON.stringify(next)) return;
    this.sql.exec(
      "INSERT OR REPLACE INTO jobs VALUES (?,?,?,?,?,?)",
      next.id,
      run.id,
      run.attempt,
      next.status === "completed" ? 0 : 1,
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
  async flush() {
    const partition = this.meta("partition");
    for (const row of this.sql
      .exec<{
        id: number;
        day: string;
        data: string;
      }>("SELECT * FROM outbox ORDER BY id LIMIT 40")
      .toArray()) {
      await call(this.env.DAYS, row.day, "/delta", {
        id: `${partition}/${row.id}`,
        day: row.day,
        rows: JSON.parse(row.data),
      });
      this.sql.exec("DELETE FROM outbox WHERE id=?", row.id);
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
    await call(this.env.CONTROL, "control", "/partition", {
      partition,
      active,
      revision,
    });
    const stillActive =
      this.sql.exec("SELECT 1 FROM jobs WHERE active=1 LIMIT 1").toArray()
        .length ||
      this.sql
        .exec(
          "SELECT 1 FROM runs WHERE json_extract(data,'$.status')!='completed' LIMIT 1",
        )
        .toArray().length;
    if (
      stillActive ||
      this.sql.exec("SELECT 1 FROM outbox LIMIT 1").toArray().length
    )
      await this.ctx.storage.setAlarm(Date.now() + 60000);
    else await this.ctx.storage.deleteAlarm();
  }
  async alarm() {
    // Persist the next retry before any network call, including during cold
    // aggregate restoration or an extended downstream outage.
    await this.ctx.storage.setAlarm(Date.now() + 60000);
    let full = false;
    this.ctx.storage.transactionSync(() => {
      const rows = this.sql
        .exec<{
          id: string;
          data: string;
          run: string;
          attempt: number;
        }>(
          "SELECT id,data,run,attempt FROM jobs WHERE active=1 AND id>? ORDER BY id LIMIT 1000",
          this.meta("alarm_cursor") || "",
        )
        .toArray();
      for (const row of rows) {
        const run = this.run(row.run, row.attempt);
        if (run) this.replaceJob(run, JSON.parse(row.data), now());
      }
      full = rows.length === 1000;
      this.set("alarm_cursor", full ? rows.at(-1)!.id : "");
    });
    await this.flush();
    if (full) await this.ctx.storage.setAlarm(Date.now() + 1000);
  }
  async fetch(request: Request) {
    const path = new URL(request.url).pathname,
      b: any = await request.json();
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
            }>("SELECT id,data FROM jobs WHERE run=? AND attempt=? AND json_extract(data,'$.estimated')=1", j.run, j.attempt)
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
      await this.ctx.storage.setAlarm(Date.now() + 1000);
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
    return {
      ...summarize(
        this.sql.exec<Omit<Row, "day">>("SELECT * FROM metrics").toArray(),
      ),
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
    if (await this.restore()) await this.ctx.storage.deleteAlarm();
  }
  async compact(date: string) {
    if (date >= day(now() - 90 * DAY))
      return { done: false, reason: "inside detail retention" };
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
    if (path === "/delta")
      this.ctx.storage.transactionSync(() => {
        if (
          this.sql.exec("SELECT 1 FROM receipts WHERE id=?", b.id).toArray()
            .length
        )
          return;
        for (const row of b.rows) {
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
        this.sql.exec("INSERT INTO receipts VALUES (?)", b.id);
      });
    else if (path === "/observation")
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
    this.set("revision", (this.meta("revision") || 0) + 1);
    await call(this.env.CONTROL, "control", "/dirty", {
      day: b.day || day(b.at),
    });
    return json({ ok: true });
  }
}
