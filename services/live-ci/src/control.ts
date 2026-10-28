import {
  allowed,
  historicalRepo,
  call,
  digest,
  directory,
  json,
  now,
  type Env,
} from "./types";
import { DAY, day, iso, normalizeRun, normalizeJob, WORKFLOW } from "./model";
import { renderCharts } from "./charts";
import { BUILD_WORKFLOWS, DETAIL_REQUESTS_PER_HOUR, needsJobs, repairReason, retained } from "./policy";
import { headers, installationToken, boundedBytes } from "./github";
import { unzipSync } from "fflate";
import { classify } from "./failure";
import { env as runtimeEnv } from "cloudflare:workers";

export class Control {
  private sql: SqlStorage;
  private ticking?: Promise<void>;
  private publishing?: Promise<void>;
  // Binding-only deployments can reuse an existing isolate. Read the current
  // bindings rather than retaining the constructor's pre-installation env.
  private get env(): Env {
    return runtimeEnv as Env;
  }
  constructor(private ctx: DurableObjectState) {
    this.sql = ctx.storage.sql;
    this.sql
      .exec(`CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
      CREATE TABLE IF NOT EXISTS partitions(name TEXT PRIMARY KEY,active INTEGER,seen INTEGER);
      CREATE INDEX IF NOT EXISTS partitions_active ON partitions(active);
      CREATE INDEX IF NOT EXISTS partitions_seen ON partitions(active,seen);
      CREATE TABLE IF NOT EXISTS dirty(day TEXT PRIMARY KEY,revision INTEGER);
      CREATE TABLE IF NOT EXISTS summaries(day TEXT PRIMARY KEY,data TEXT);
      CREATE TABLE IF NOT EXISTS repairs(key TEXT PRIMARY KEY,repo TEXT,kind TEXT,args TEXT,due INTEGER,failures INTEGER DEFAULT 0);
      CREATE INDEX IF NOT EXISTS repairs_due ON repairs(due);
      CREATE INDEX IF NOT EXISTS repairs_attempt ON repairs(repo,kind,CAST(json_extract(args,'$.id') AS TEXT),json_extract(args,'$.attempt'));
      CREATE TABLE IF NOT EXISTS gaps(key TEXT PRIMARY KEY,kind TEXT,reason TEXT,at INTEGER);
      CREATE TABLE IF NOT EXISTS attempts(at INTEGER,id INTEGER PRIMARY KEY AUTOINCREMENT,status INTEGER);
      CREATE INDEX IF NOT EXISTS attempts_at ON attempts(at);
      CREATE TABLE IF NOT EXISTS coverage(repo TEXT,since INTEGER,until INTEGER,PRIMARY KEY(repo,since,until));
      CREATE TABLE IF NOT EXISTS deliveries(id TEXT PRIMARY KEY,at INTEGER);
      CREATE INDEX IF NOT EXISTS deliveries_at ON deliveries(at);
      CREATE TABLE IF NOT EXISTS leases(publisher TEXT PRIMARY KEY,data TEXT,expires INTEGER);
      CREATE TABLE IF NOT EXISTS comparisons(at INTEGER PRIMARY KEY,passed INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS annotations(at INTEGER PRIMARY KEY,text TEXT);
      CREATE TABLE IF NOT EXISTS witnesses(repo TEXT,id TEXT,attempt INTEGER,partition TEXT,updated INTEGER,checked INTEGER DEFAULT 0,PRIMARY KEY(repo,id,attempt));
      CREATE INDEX IF NOT EXISTS witnesses_checked ON witnesses(checked,updated);
      CREATE TABLE IF NOT EXISTS landings(repo TEXT,pr INTEGER,at INTEGER,PRIMARY KEY(repo,pr));`);
    this.sql.exec(
      "CREATE TABLE IF NOT EXISTS landing_outbox(id INTEGER PRIMARY KEY AUTOINCREMENT,day TEXT,path TEXT,data TEXT)",
    );
    if (!this.get("started")) this.set("started", now());
  }
  get(k: string): any {
    const row = this.sql
      .exec<{ value: string }>("SELECT value FROM meta WHERE key=?", k)
      .toArray()[0];
    return row ? JSON.parse(row.value) : undefined;
  }
  set(k: string, v: any) {
    this.sql.exec(
      "INSERT OR REPLACE INTO meta VALUES (?,?)",
      k,
      JSON.stringify(v),
    );
  }
  enqueue(repo: string, kind: string, args: any, due = now()) {
    const identity = ["run", "jobs", "details", "members"].includes(kind)
      ? { id: args.id, attempt: args.attempt, page: args.page }
      : args;
    const key = `${repo}/${kind}/${JSON.stringify(identity)}`;
    if (this.sql.exec("SELECT 1 FROM gaps WHERE key=?", key).toArray().length)
      return;
    this.sql.exec(
      `INSERT INTO repairs(key,repo,kind,args,due) VALUES (?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET
      due=MIN(due,excluded.due),args=CASE WHEN json_extract(excluded.args,'$.archival') IS NOT 1 THEN excluded.args ELSE repairs.args END`,
      key,
      repo,
      kind,
      JSON.stringify(args),
      due,
    );
  }
  retire(row: { key: string; kind: string }, reason: string) {
    this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
    const counts = this.get("retired_repairs") || {};
    const key = `${row.kind}: ${reason}`;
    counts[key] = (counts[key] || 0) + 1;
    this.set("retired_repairs", counts);
  }
  async pruneRepairs(cursor = "", limit = 100) {
    limit = Math.max(1, Math.min(200, Number(limit) || 100));
    const rows = this.sql
      .exec<{ key: string; repo: string; kind: string; args: string; due: number }>(
        "SELECT key,repo,kind,args,due FROM repairs WHERE key>? AND kind IN ('run','jobs','details','members') ORDER BY key LIMIT ?",
        cursor,
        limit,
      )
      .toArray();
    let removed = 0, retired = 0;
    for (let i = 0; i < rows.length; i += 4)
      await Promise.all(
        rows.slice(i, i + 4).map(async (row) => {
          const args = JSON.parse(row.args);
          const location = await call(
            this.env.DIRECTORIES,
            directory(row.repo, args.id),
            "/get",
            { id: args.id },
          );
          if (!location.partition) {
            if (row.due < now() - DAY) { this.retire(row, "unresolved parent expired"); retired++; }
            return;
          }
          const status = await call(
            this.env.FACTS,
            location.partition,
            "/status",
            { id: String(args.id), attempt: args.attempt || 1 },
          );
          const reason = repairReason(row.kind, status.run, now());
          if (reason) { this.retire(row, reason); retired++; return; }
          const satisfied =
            row.kind === "run"
              ? status.run?.status === "completed"
              : row.kind === "jobs"
                ? status.verified || ((!args.page || args.page === 1) && !needsJobs(status))
                : row.kind === 'members' ? status.run?.members?.length : status.run?.telemetry?.coverage === "complete" ||
                  status.run?.telemetry?.phases?.length;
          if (satisfied) {
            this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
            removed++;
          }
        }),
      );
    return {
      scanned: rows.length,
      removed,
      retired,
      cursor: rows.length === limit ? rows.at(-1)!.key : null,
    };
  }
  async ingest(b: any) {
    if (
      this.sql.exec("SELECT 1 FROM deliveries WHERE id=?", b.delivery).toArray()
        .length
    )
      return;
    const repo = b.repo;
    if (
      !allowed(this.env).includes(repo) &&
      !(b.historical && historicalRepo(repo))
    )
      throw new Error("Repository is not allowed");
    if (b.run) {
      const run = normalizeRun(repo, b.run);
      if (!retained(run, now())) return;
      const partition = `${repo}/${day(run.created)}`;
      let needsMembers = !run.members?.length;
      await call(this.env.DIRECTORIES, directory(repo, run.id), "/put", {
        id: run.id,
        partition,
      });
      await call(this.env.FACTS, partition, "/event", { ...b, partition });
      // Metadata and trusted summaries satisfy repairs that were queued while
      // those deliveries were still in flight. Do not spend quota on them later.
      this.sql.exec(
        `DELETE FROM repairs WHERE repo=? AND kind='run'
        AND CAST(json_extract(args,'$.id') AS TEXT)=?
        AND (json_extract(args,'$.attempt')=? OR json_extract(args,'$.attempt') IS NULL)`,
        repo,
        run.id,
        run.attempt,
      );
      if (b.verified_jobs)
        this.sql.exec(
          `DELETE FROM repairs WHERE repo=? AND kind='jobs'
          AND CAST(json_extract(args,'$.id') AS TEXT)=? AND json_extract(args,'$.attempt')=?`,
          repo,
          run.id,
          run.attempt,
        );
      if (b.telemetry?.coverage === "complete" || b.telemetry?.phases?.length)
        this.sql.exec(
          `DELETE FROM repairs WHERE repo=? AND kind='details'
          AND CAST(json_extract(args,'$.id') AS TEXT)=? AND json_extract(args,'$.attempt')=?`,
          repo,
          run.id,
          run.attempt,
        );
      if (run.trigger === "bors_staging" && run.created >= now() - 30 * DAY) {
        const stored = await call(this.env.FACTS, partition, "/status", {
          id: run.id,
          attempt: run.attempt,
        });
        await call(this.env.DAYS, day(run.created), "/staging", {
          ...stored.run,
          day: day(run.created),
        });
        needsMembers = !stored.run?.members?.length;
        if (!needsMembers)
          this.sql.exec(
            "DELETE FROM repairs WHERE repo=? AND kind='members' AND CAST(json_extract(args,'$.id') AS TEXT)=? AND json_extract(args,'$.attempt')=?",
            repo, run.id, run.attempt,
          );
      }
      if (!b.historical && run.status === "completed" && BUILD_WORKFLOWS.has(run.workflow)) {
        const status = await call(this.env.FACTS, partition, "/status", { id: run.id, attempt: run.attempt });
        const args = { id: run.id, attempt: run.attempt, created: run.created, updated: run.updated };
        if (!repairReason("jobs", status.run, now()) && needsJobs(status))
          this.enqueue(repo, "jobs", args, now() + 180);
        if (!repairReason("details", status.run, now()) &&
            !status.run?.telemetry?.coverage && !status.run?.telemetry?.phases?.length)
          this.enqueue(repo, "details", args, now() + 600);
      }
      if (run.trigger === "bors_staging" && run.batch && needsMembers && run.created >= now() - 30 * DAY)
        this.enqueue(repo, "members", {
          id: run.id,
          attempt: run.attempt,
          batch: run.batch,
          head: run.head,
          created: run.created,
        });
      if (!b.historical && run.status === "completed")
        this.sql.exec(
          "INSERT INTO witnesses(repo,id,attempt,partition,updated) VALUES (?,?,?,?,?) ON CONFLICT(repo,id,attempt) DO UPDATE SET updated=excluded.updated",
          repo,
          run.id,
          run.attempt,
          partition,
          run.updated,
        );
    } else if (b.jobs?.length || b.telemetry) {
      if (b.jobs?.length && b.jobs.every((j: any) => Date.parse(j.created_at) / 1000 < now() - 90 * DAY)) return;
      const id = String(b.run_id ?? b.jobs[0].run_id);
      const location = await call(
        this.env.DIRECTORIES,
        directory(repo, id),
        "/get",
        { id },
      );
      if (!location.partition) {
        if (b.jobs?.length && b.jobs.every((j: any) => Date.parse(j.created_at) / 1000 < now() - DAY)) return;
        this.enqueue(repo, "run", {
          id,
          attempt: b.attempt || b.jobs?.[0]?.run_attempt || 1,
        });
        throw new Error("Run metadata not available yet; retry delivery");
      }
      const stored = await call(this.env.FACTS, location.partition, "/status", { id, attempt: b.attempt || b.jobs?.[0]?.run_attempt || 1 });
      if (stored.run && !retained(stored.run, now())) return;
      if (!stored.run && location.partition.slice(-10) < day(now() - 30 * DAY)) return;
      try {
        await call(this.env.FACTS, location.partition, "/event", {
          ...b,
          partition: location.partition,
        });
      } catch (error) {
        this.enqueue(repo, "run", {
          id,
          attempt: b.attempt || b.jobs?.[0]?.run_attempt || 1,
        });
        throw error;
      }
    } else if (b.annotation) {
      this.sql.exec(
        "INSERT OR REPLACE INTO annotations VALUES (?,?)",
        b.annotation.at,
        b.annotation.text,
      );
    } else if (b.bootstrap) {
      if (!b.historical || !Number.isFinite(Date.parse(b.bootstrap.since)))
        throw new Error("Invalid bootstrap completion");
      const previous = this.get("history_since");
      const since = Date.parse(b.bootstrap.since) / 1000;
      this.set("history_since", previous ? Math.min(previous, since) : since);
      this.set("charts_at", 0);
    } else if (b.coverage) {
      if (b.coverage.stream === "inventory")
        this.sql.exec(
          "INSERT OR IGNORE INTO coverage VALUES (?,?,?)",
          repo,
          b.coverage.since,
          b.coverage.until,
        );
    } else if (b.staging) {
      if (b.staging.created < now() - 30 * DAY) return;
      await call(this.env.DAYS, day(b.staging.created), "/staging", {
        ...b.staging,
        day: day(b.staging.created),
      });
    } else if (b.observation) {
      const o = b.observation,
        at = Math.floor(Date.parse(o.observed_at) / 1000);
      if (at < now() - 30 * DAY) return;
      if (!Number.isFinite(at))
        throw new Error("Invalid observation timestamp");
      const eligible = o.eligible || o.eligible_prs || [];
      const count = Number.isInteger(o.eligible_count)
        ? o.eligible_count
        : new Set(
            eligible
              .filter((p: any) => p.ready || p.queued)
              .map((p: any) => p.pr),
          ).size;
      // An in-progress scan reuses the last completed candidate snapshot.
      // Only the first scan after a restart has no trustworthy baseline.
      const complete =
        (o.candidate_scan_complete !== false &&
          o.candidate_scan_complete !== 0) ||
        !!o.candidate_snapshot_at;
      await call(this.env.DAYS, day(at), "/observation", {
        at,
        count,
        complete,
        day: day(at),
      });
      if (at >= (this.get("bors_observed") || 0)) {
        this.set("bors_observed", at);
        this.set("bors_current", {
          at: iso(at),
          count,
          complete,
        });
      }
    } else if (b.landing) {
      const { pr, at } = b.landing;
      if (at < now() - 90 * DAY) return;
      this.ctx.storage.transactionSync(() => {
        const prior = this.sql
          .exec<{
            at: number;
          }>("SELECT at FROM landings WHERE repo=? AND pr=?", repo, pr)
          .toArray()[0];
        if (!prior || at < prior.at) {
          const revision = (this.get("landing_revision") || 0) + 1;
          this.set("landing_revision", revision);
          if (prior)
            this.sql.exec(
              "INSERT INTO landing_outbox(day,path,data) VALUES (?,?,?)",
              day(prior.at),
              "/unlanding",
              JSON.stringify({ pr, at: prior.at, revision }),
            );
          this.sql.exec(
            "INSERT INTO landing_outbox(day,path,data) VALUES (?,?,?)",
            day(at),
            "/landing",
            JSON.stringify({ pr, at, revision }),
          );
          this.sql.exec(
            "INSERT OR REPLACE INTO landings VALUES (?,?,?)",
            repo,
            pr,
            at,
          );
        }
      });
      await this.flushLandings();
    }
    this.sql.exec(
      "INSERT OR IGNORE INTO deliveries VALUES (?,?)",
      b.delivery,
      now(),
    );
    this.set("event_observed", now());
  }
  async flushLandings() {
    for (const row of this.sql
      .exec<{
        id: number;
        day: string;
        path: string;
        data: string;
      }>("SELECT * FROM landing_outbox ORDER BY id LIMIT 50")
      .toArray()) {
      await call(this.env.DAYS, row.day, row.path, JSON.parse(row.data));
      this.sql.exec("DELETE FROM landing_outbox WHERE id=?", row.id);
    }
  }
  async github(
    path: string,
    binary = false,
    optional = false,
  ): Promise<{ response: Response; data: any }> {
    if (
      !this.env.GITHUB_APP_ID ||
      !this.env.GITHUB_INSTALLATION_ID ||
      !this.env.GITHUB_PRIVATE_KEY
    )
      throw new Error("Dedicated GitHub App is not configured");
    const clock = now(),
      used = this.sql
        .exec<{
          n: number;
        }>("SELECT COUNT(*) n FROM attempts WHERE at>?", clock - 3600)
        .one().n;
    if (used >= Math.min(500, Number(this.env.GITHUB_HOURLY_BUDGET) || 500))
      throw new Error("Rolling GitHub request budget exhausted");
    if ((this.get("github_pause") || 0) > clock)
      throw new Error("GitHub quota cooldown");
    let grant = this.get("installation");
    if (!grant || Date.parse(grant.expires_at) / 1000 < clock + 120) {
      // Token exchange is charged to our request-attempt ceiling too.
      this.sql.exec("INSERT INTO attempts(at,status) VALUES (?,0)", clock);
      grant = await installationToken(this.env);
      this.set("installation", grant);
      if (
        used + 1 >=
        Math.min(500, Number(this.env.GITHUB_HOURLY_BUDGET) || 500)
      )
        throw new Error("Rolling GitHub request budget exhausted");
    }
    if (
      this.sql
        .exec<{
          n: number;
        }>("SELECT COUNT(*) n FROM attempts WHERE at>?", now() - 3600)
        .one().n >= Math.min(500, Number(this.env.GITHUB_HOURLY_BUDGET) || 500)
    )
      throw new Error("Rolling GitHub request budget exhausted");
    if (optional) {
      const recent = (this.get("detail_requests") || []).filter((at: number) => at > now() - 3600);
      if (recent.length >= DETAIL_REQUESTS_PER_HOUR) throw new Error("Optional detail budget exhausted");
      this.set("detail_requests", [...recent, now()]);
    }
    const reserved = this.sql
      .exec<{
        id: number;
      }>("INSERT INTO attempts(at,status) VALUES (?,0) RETURNING id", now())
      .one().id;
    const response = await fetch("https://api.github.com/" + path, {
      headers: headers(grant.token),
      redirect: "manual",
      signal: AbortSignal.timeout(15000),
    });
    this.sql.exec(
      "UPDATE attempts SET status=? WHERE id=?",
      response.status,
      reserved,
    );
    const remaining = Number(
      response.headers.get("x-ratelimit-remaining") ?? 5000,
    );
    if (remaining < 200)
      this.set(
        "github_pause",
        Number(response.headers.get("x-ratelimit-reset")) || clock + 3600,
      );
    if (response.status === 403 || response.status === 429) {
      const retries = (this.get("secondary_retries") || 0) + 1;
      this.set("secondary_retries", retries);
      this.set(
        "github_pause",
        Math.max(
          this.get("github_pause") || 0,
          clock +
            Number(
              response.headers.get("retry-after") ||
                Math.min(3600, 60 * 2 ** retries),
            ),
        ),
      );
    }
    if (binary && [301, 302, 303, 307, 308].includes(response.status)) {
      const location = new URL(response.headers.get("location") || "");
      if (
        location.protocol !== "https:" ||
        !/(^|\.)githubusercontent\.com$|(^|\.)blob\.core\.windows\.net$/.test(
          location.hostname,
        )
      )
        throw new Error("Untrusted artifact redirect");
      const blob = await fetch(location, {
        signal: AbortSignal.timeout(15000),
      });
      if (!blob.ok) throw new Error(`Artifact HTTP ${blob.status}`);
      return { response, data: await boundedBytes(blob, 24 * 1024 * 1024) };
    }
    if (!response.ok) throw new Error(`GitHub HTTP ${response.status}`);
    this.set("secondary_retries", 0);
    return {
      response,
      data: binary
        ? await boundedBytes(response, 24 * 1024 * 1024)
        : await response.json(),
    };
  }
  async repair(row: any) {
    const args = JSON.parse(row.args),
      base = `repos/${row.repo}/actions`;
    if (["jobs", "details", "members"].includes(row.kind)) {
      const location = await call(this.env.DIRECTORIES, directory(row.repo, args.id), "/get", { id: args.id });
      if (location.partition) {
        const status = await call(this.env.FACTS, location.partition, "/status", { id: String(args.id), attempt: args.attempt || 1 });
        const reason = repairReason(row.kind, status.run, now());
        if (reason) { this.retire(row, reason); return; }
      }
    }
    if (row.kind === "details") {
      const location = await call(
        this.env.DIRECTORIES,
        directory(row.repo, args.id),
        "/get",
        { id: args.id },
      );
      if (!location.partition) throw new Error("Missing details parent");
      const status = await call(this.env.FACTS, location.partition, "/status", {
        id: args.id,
        attempt: args.attempt,
      });
      if (
        status.run?.telemetry?.coverage === "complete" ||
        status.run?.telemetry?.phases?.length
      ) {
        this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
        return;
      }
      const telemetry: any = { job_failures: {}, coverage: "complete" };
      if (status.run.conclusion === "success") {
        const { data } = await this.github(
          `${base}/runs/${args.id}/artifacts?name=ci-telemetry&per_page=100`, false, true,
        );
        const artifact = data.artifacts?.find(
          (a: any) =>
            !a.expired &&
            a.size_in_bytes < 24 * 1024 * 1024 &&
            Date.parse(a.created_at) / 1000 >= status.run.started &&
            Date.parse(a.created_at) / 1000 <= status.run.updated,
        );
        if (artifact) {
          const { data: zip } = await this.github(
            `${base}/artifacts/${artifact.id}/zip`,
            true, true,
          );
          const files = unzipSync(zip, {
            filter: (entry) =>
              entry.name === "telemetry.json" &&
              entry.originalSize <= 1024 * 1024,
          });
          if (files["telemetry.json"]) {
            const value = JSON.parse(
              new TextDecoder().decode(files["telemetry.json"]),
            );
            if (value.schema === "tauceti-ci.telemetry/v1")
              telemetry.phases = value.phases;
          }
        }
      }
      for (const j of status.job_rows
        .filter((j: any) => j.name === "sandboxed-build" && j.conclusion === "failure")
        .slice(0, 2)) {
        const { data: bytes } = await this.github(
          `${base}/jobs/${j.id}/logs`,
          true, true,
        );
        const failed = j.steps.find((s: any) => s.conclusion === "failure");
        const lines = new TextDecoder()
          .decode(bytes)
          .split("\n")
          .filter((l) => !l.includes("\x1b[36;1m"))
          .filter(
            (l) =>
              !failed?.started ||
              !/^\d{4}-/.test(l) ||
              (Date.parse(l.slice(0, l.indexOf(" "))) / 1000 >=
                failed.started &&
                Date.parse(l.slice(0, l.indexOf(" "))) / 1000 <=
                  failed.completed),
          )
          .map((l) =>
            l
              .replace(/^\uFEFF?\d{4}-\d\d-\d\dT[\d:.]+Z /, "")
              .replace(/\x1b\[[0-9;]*m/g, "")
              .trim(),
          )
          .filter(
            (l) =>
              /error|FAIL|✖|timed out|Killed/i.test(l) &&
              !/^(?:✔(?:\s|$)|\+\+?\s)/.test(l) &&
              !l.includes("Process completed with exit code"),
          )
          .slice(0, 30);
        telemetry.job_failures[j.id] = classify(
          j.conclusion,
          failed?.name || "",
          lines,
        );
      }
      await this.ingest({
        repo: row.repo,
        delivery: `detail/${args.id}/${args.attempt}`,
        run_id: args.id,
        attempt: args.attempt,
        telemetry,
      });
    } else if (row.kind === "members") {
      const partition = `${row.repo}/${day(args.created)}`;
      const status = await call(this.env.FACTS, partition, "/status", {
        id: args.id,
        attempt: args.attempt,
      });
      if (!status.run || status.run.batch !== args.batch)
        throw new Error("Missing or mismatched staging run");
      if (status.run.members?.length) {
        this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
        return;
      }
      const response = await fetch(
        `https://bors.taucetiproject.org/repositories/1/active-batches?base=main&batch_id=${args.batch}`,
        { signal: AbortSignal.timeout(10000) },
      );
      if (!response.ok)
        throw new Error(`Batch membership HTTP ${response.status}`);
      const data: any = await response.json(),
        batch = data.requested_batch;
      if (data.repo !== row.repo || !batch || batch.id !== args.batch ||
          status.run.testedHead !== batch.head_sha)
        throw new Error("Batch identity not available");
      {
        const members = [...new Set<number>((batch.members || [])
          .map((m: any) => m.pr)
          .filter((pr: any) => Number.isInteger(pr) && pr > 0))];
        if (!members.length) throw new Error("Batch membership unavailable");
        await call(this.env.FACTS, partition, "/members", {
          id: args.id,
          attempt: args.attempt,
          batch: args.batch,
          head: batch.head_sha,
          members,
        });
        await call(this.env.DAYS, day(args.created), "/staging", {
          ...status.run,
          members,
          day: day(args.created),
        });
        this.sql.exec(
          "INSERT INTO dirty VALUES (?,1) ON CONFLICT(day) DO UPDATE SET revision=revision+1",
          day(args.created),
        );
      }
    } else if (row.kind === "inventory") {
      const { data } = await this.github(
        `${base}/runs?per_page=100&page=${args.page || 1}&created=${encodeURIComponent(iso(args.since) + ".." + iso(args.until - 1))}`,
      );
      if (data.total_count > 1000) {
        if (args.until - args.since <= 1)
          throw new Error(
            "Dense inventory second exceeds GitHub enumeration cap",
          );
        const mid = Math.floor((args.since + args.until) / 2);
        this.enqueue(row.repo, "inventory", { since: args.since, until: mid });
        this.enqueue(row.repo, "inventory", { since: mid, until: args.until });
      } else {
        for (const run of data.workflow_runs || []) {
          await this.ingest({
            repo: row.repo,
            delivery: `repair/run/${run.id}/${run.run_attempt}/${run.updated_at}`,
            run,
          });
          if (run.status === "completed") {
            const status = await call(
              this.env.FACTS,
              `${row.repo}/${day(Date.parse(run.created_at) / 1000)}`,
              "/status",
              { id: String(run.id), attempt: run.run_attempt || 1 },
            );
            if (
              !repairReason("jobs", status.run, now()) && needsJobs(status)
            )
              this.enqueue(
                row.repo,
                "jobs",
                {
                  id: String(run.id),
                  attempt: run.run_attempt || 1,
                  archival: Date.parse(run.updated_at) / 1000 < now() - 3600,
                },
                now() + 180,
              );
          }
        }
        if (
          (data.workflow_runs || []).length === 100 &&
          100 * (args.page || 1) < data.total_count
        )
          this.enqueue(row.repo, "inventory", {
            ...args,
            page: (args.page || 1) + 1,
          });
        else
          this.sql.exec(
            "INSERT OR IGNORE INTO coverage VALUES (?,?,?)",
            row.repo,
            args.since,
            args.until,
          );
      }
    } else if (row.kind === "run") {
      const location = await call(
        this.env.DIRECTORIES,
        directory(row.repo, args.id),
        "/get",
        { id: args.id },
      );
      if (location.partition) {
        const status = await call(
          this.env.FACTS,
          location.partition,
          "/status",
          { id: String(args.id), attempt: args.attempt || 1 },
        );
        if (status.run?.status === "completed") {
          this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
          return;
        }
      }
      const { data } = await this.github(
        `${base}/runs/${args.id}${args.attempt ? "/attempts/" + args.attempt : ""}`,
      );
      await this.ingest({
        repo: row.repo,
        delivery: `repair/run/${data.id}/${data.run_attempt}/${data.updated_at}`,
        run: data,
      });

    } else if (row.kind === "jobs") {
      const location = await call(
        this.env.DIRECTORIES,
        directory(row.repo, args.id),
        "/get",
        { id: args.id },
      );
      if (location.partition) {
        const status = await call(
          this.env.FACTS,
          location.partition,
          "/status",
          { id: args.id, attempt: args.attempt },
        );
        if (!status.run) {
          this.enqueue(row.repo, "run", { id: args.id, attempt: args.attempt });
          throw new Error("Missing attempt metadata; repair requested");
        }
        if (status.verified || ((!args.page || args.page === 1) && !needsJobs(status))) {
          this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
          return;
        }
      }
      const { data } = await this.github(
        `${base}/runs/${args.id}/attempts/${args.attempt}/jobs?per_page=100&page=${args.page || 1}`,
      );
      await this.ingest({
        repo: row.repo,
        delivery: `repair/jobs/${row.key}/${await digest(JSON.stringify(data.jobs))}`,
        jobs: data.jobs || [],
        run_id: args.id,
      });
      if (
        (data.jobs || []).length === 100 &&
        100 * (args.page || 1) < data.total_count
      )
        this.enqueue(row.repo, "jobs", { ...args, page: (args.page || 1) + 1 });
      else if (location.partition)
        await call(this.env.FACTS, location.partition, "/verified", {
          id: args.id,
          attempt: args.attempt,
        });
      // Auxiliary workflows are observed through job webhooks. Builds receive
      // one final attempt-specific enumeration to recover omitted jobs/steps.
    }
    this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
  }
  async repairMembers() {
    const deadline = Date.now() + 10000;
    let repaired = 0;
    for (const row of this.sql.exec(
      "SELECT * FROM repairs WHERE kind='members' AND due<=? ORDER BY due LIMIT 8", now(),
    ).toArray()) {
      if (Date.now() > deadline) break;
      try {
        await this.repair(row);
        repaired++;
        this.set("members_error", null);
      } catch (error) {
        this.set("members_error", String(error));
        this.sql.exec(
          "UPDATE repairs SET failures=failures+1,due=? WHERE key=?",
          now() + Math.min(3600, 60 * 2 ** Math.min(6, Number(row.failures))), row.key,
        );
      }
    }
    if (repaired) this.set("charts_at", 0);
    return { repaired, pending: this.sql.exec<{ n: number }>(
      "SELECT COUNT(*) n FROM repairs WHERE kind='members'",
    ).one().n };
  }
  async bors() {
    const clock = now(),
      position = this.get("bors_position") || { day: day(clock), cursor: null };
    const url = new URL(
      "https://bors.taucetiproject.org/api/merge-observations",
    );
    url.searchParams.set("day", position.day);
    if (position.cursor) url.searchParams.set("cursor", position.cursor);
    const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
    if (!response.ok)
      throw new Error(`Bors observations HTTP ${response.status}`);
    const page: any = await response.json();
    for (const o of page.observations || [])
      await this.ingest({
        repo: "TauCetiProject/TauCeti",
        delivery: `bors/${o.observed_at}`,
        observation: o,
      });
    if (page.truncated && page.cursor)
      this.set("bors_position", { day: position.day, cursor: page.cursor });
    else if (position.day < day(clock))
      this.set("bors_position", {
        day: day(Date.parse(position.day) / 1000 + DAY),
        cursor: null,
      });
    // Replay the current tail page so newly appended observations are seen.
    this.set("bors_checked", clock);
  }
  async publish() {
    if (this.publishing) return this.publishing;
    this.publishing = this.performPublish();
    try {
      return await this.publishing;
    } finally {
      this.publishing = undefined;
    }
  }
  private async performPublish() {
    const clock = now();
    // Materialize only changed days, bounded across ticks. A revision test
    // preserves dirtiness if an event arrived during the cross-object read.
    for (const row of this.sql
      .exec<{
        day: string;
        revision: number;
      }>("SELECT * FROM dirty ORDER BY day DESC LIMIT 12")
      .toArray()) {
      const summary = await call(this.env.DAYS, row.day, "/summary", {
        day: row.day,
      });
      this.sql.exec(
        "INSERT OR REPLACE INTO summaries VALUES (?,?)",
        row.day,
        JSON.stringify(summary),
      );
      await this.env.ARCHIVE.put(
        `summaries/${row.day}.json`,
        JSON.stringify(summary),
      );
      this.sql.exec(
        "DELETE FROM dirty WHERE day=? AND revision=?",
        row.day,
        row.revision,
      );
    }
    const active = this.sql
      .exec<{
        name: string;
      }>(
        "SELECT name FROM partitions WHERE active>0 AND seen>? LIMIT 200",
        clock - 30 * DAY,
      )
      .toArray();
    const current: any = {
      running: {},
      queued: {},
      truncated: active.length === 200,
    };
    for (const p of active) {
      const snapshot = await call(this.env.FACTS, p.name, "/current");
      current.truncated = current.truncated || snapshot.truncated;
      for (const c of snapshot.counts)
        current[c.state][c.kind] = (current[c.state][c.kind] || 0) + c.count;
      const repo = p.name.split("/").slice(0, 2).join("/");
      for (const r of [
        ...snapshot.runs.map((r: any) => ({
          id: r.id,
          attempt: r.attempt,
          updated: r.updated,
        })),
        ...snapshot.jobs.map((j: any) => ({
          id: j.run,
          attempt: j.attempt,
          updated: j.updated,
        })),
      ]) {
        const key = `active_checked/${repo}/${r.id}/${r.attempt}`;
        if (clock - r.updated > 900)
          // Upgrade an existing repair even while the 15-minute admission
          // timer is running. Preserve its retry backoff and request count.
          this.sql.exec(
            `UPDATE repairs SET args=json_set(args,'$.urgent',json('true'))
            WHERE repo=? AND kind='run' AND CAST(json_extract(args,'$.id') AS TEXT)=?
            AND json_extract(args,'$.attempt')=?`,
            repo,
            String(r.id),
            r.attempt,
          );
        if (clock - r.updated > 900 && clock - (this.get(key) || 0) > 900) {
          this.enqueue(repo, "run", {
            id: r.id,
            attempt: r.attempt,
            urgent: true,
          });
          this.set(key, clock);
        }
      }
    }
    const previousObject = await this.env.ARCHIVE.get(
      "public/v1/ci/manifest.json",
    );
    const manifest: any = previousObject
      ? await previousObject.json()
      : { schema: "tauceti-ci.manifest/v1", groups: {}, assets: {} };
    const coverage = allowed(this.env).map((repo) => ({
      repo,
      intervals: this.sql
        .exec(
          "SELECT since,until FROM coverage WHERE repo=? AND until>? ORDER BY since",
          repo,
          clock - 30 * DAY,
        )
        .toArray(),
    }));
    const latest = coverage.map((c) =>
      Math.max(0, ...c.intervals.map((x: any) => x.until)),
    );
    const source = latest.length ? Math.min(...latest) : 0;
    const mainSource = Math.max(
      0,
      ...(
        coverage.find((c) => c.repo === "TauCetiProject/TauCeti")?.intervals ||
        []
      ).map((x: any) => x.until),
    );
    Object.assign(manifest, {
      mode: this.env.PUBLICATION_MODE || "shadow",
      generated_at: iso(clock),
      observed_at: this.get("event_observed")
        ? iso(this.get("event_observed"))
        : null,
      current: {
        ...current,
        ready_to_merge: this.get("bors_current") || null,
        as_of: source ? iso(source) : null,
        computed_at: iso(clock),
        coverage: "webhook_observed",
      },
      coverage,
      github: {
        requests_last_hour: this.sql
          .exec<{
            n: number;
          }>("SELECT COUNT(*) n FROM attempts WHERE at>?", now() - 3600)
          .one().n,
        budget: Math.min(500, Number(this.env.GITHUB_HOURLY_BUDGET) || 500),
        paused_until: this.get("github_pause") || null,
      },
      pending_repairs: this.sql
        .exec<{ n: number }>("SELECT COUNT(*) n FROM repairs")
        .one().n,
      collection_policy: {
        auxiliary_jobs: "webhook observations",
        recovery_hours: 24,
        successful_phase_sample: "one in twenty",
        optional_requests_per_hour: DETAIL_REQUESTS_PER_HOUR,
        raw_general_days: 30, raw_pr_build_days: 90,
        historical_recovery_retired: Object.values(this.get("retired_repairs") || {}).reduce((n: number, value: any) => n + Number(value), 0),
      },
      history: {
        status:
          this.get("history_since") <= clock - 89 * DAY ? "ready" : "importing",
        since: this.get("history_since")
          ? iso(this.get("history_since"))
          : null,
      },
    });
    if (clock - (this.get("charts_at") || 0) >= 600) {
      const days = this.sql
        .exec<{ day: string; data: string }>(
          "SELECT * FROM summaries WHERE day>=? ORDER BY day",
          day(clock - 89 * DAY),
        )
        .toArray()
        .map((r) => ({ day: r.day, data: JSON.parse(r.data) }));
      if (days.length) {
        const errors: Record<string, string> = {};
        let assets: Record<string, string> = {};
        try {
          assets = renderCharts(
            days,
            clock,
            this.sql
              .exec<{
                at: number;
                text: string;
              }>("SELECT * FROM annotations WHERE at>?", clock - 30 * DAY)
              .toArray(),
            errors,
            (coverage.find((c) => c.repo === "TauCetiProject/TauCeti")
              ?.intervals || []) as { since: number; until: number }[],
          );
        } catch (error) {
          errors.all = String(error);
        }
        const pending = this.sql.exec<{ n: number }>("SELECT COUNT(*) n FROM repairs WHERE kind='details'").one().n;
        const missingMembers = days.flatMap((d) => d.data.staging || [])
          .filter((r) => r.created >= clock - 30 * DAY && !r.members?.length).length;
        const membershipRepairs = this.sql.exec<{ n: number }>(
          "SELECT COUNT(*) n FROM repairs WHERE kind='members'",
        ).one().n;
        const gaps = this.sql
          .exec<{
            kind: string;
            count: number;
          }>("SELECT kind,COUNT(*) count FROM gaps GROUP BY kind")
          .toArray();
        const groups: Record<string, string[]> = {
          fleet: ["ci-fleet-72h.svg", "ci-fleet-30d.svg"],
          daily: [
            "ci-build-duration.svg",
            "ci-build-wait.svg",
            "ci-runner-minutes.svg",
            "ci-merge-queue.svg",
            "ci-picker.svg",
          ],
          details: ["ci-build-phases.svg", "ci-failures.svg"],
          queue: ["ci-queue-size.svg"],
          staging: ["ci-bors-staging.svg"],
        };
        for (const [group, files] of Object.entries(groups)) {
          const through =
            group === "queue"
              ? this.get("bors_observed") || 0
              : group === "fleet"
                ? source
                : mainSource;
          const metadata = {
            source_through: through ? iso(through) : null,
            coverage: "webhook_observed_with_bounded_recovery",
            complete: false,
            pending: group === "details" ? pending :
              group === "staging" ? Math.max(missingMembers, membershipRepairs) : 0,
            gaps: gaps
              .filter((g) =>
                group === "details"
                  ? g.kind === "details"
                  : g.kind !== "details",
              )
              .reduce((n, g) => n + g.count, 0),
          };
          try {
            const replacements: any = {};
            for (const file of files) {
              const svg = assets[file];
              if (
                !svg ||
                errors[file] ||
                errors.all ||
                !svg.startsWith("<svg") ||
                !svg.endsWith("</svg>")
              )
                throw new Error(
                  errors[file] ||
                    errors.all ||
                    "Report asset missing or invalid",
                );
              const revision = await digest(svg),
                key = `public/v1/ci/assets/${revision}/${file}`;
              await this.env.ARCHIVE.put(key, svg, {
                httpMetadata: { contentType: "image/svg+xml" },
              });
              replacements[file] = {
                url: `/v1/ci/assets/${revision}/${file}`,
                revision,
              };
            }
            Object.assign(manifest.assets, replacements);
            manifest.groups[group] = {
              ...metadata,
              published_at: iso(clock),
              published_through: metadata.source_through,
              status:
                !through || clock - through > 1800
                  ? "stale"
                  : metadata.pending
                    ? "pending"
                    : "updated",
            };
          } catch (error) {
            manifest.groups[group] = {
              ...manifest.groups[group],
              ...metadata,
              status: "error",
              reason: String(error),
            };
          }
        }
        delete manifest.series_url;
        this.set("charts_at", clock);
      }
    }
    // Live activity can launch before historical import finishes. Keep the
    // site's complete static charts until the incremental history is ready.
    // Their replacements are rendered again as soon as bootstrap completes.
    if (manifest.mode === "live" && manifest.history.status !== "ready") {
      manifest.assets = {};
      manifest.series_url = undefined;
      for (const group of Object.values(manifest.groups) as any[])
        group.status = "importing";
    }
    // Publish pointer last; readers see a complete immutable generation.
    await this.env.ARCHIVE.put(
      "public/v1/ci/manifest.json",
      JSON.stringify(manifest),
      { httpMetadata: { contentType: "application/json" } },
    );
    this.set("published", clock);
  }
  async compare() {
    const samples = this.sql
      .exec(
        "SELECT * FROM witnesses WHERE updated<? AND updated>? ORDER BY checked,updated DESC LIMIT 3",
        now() - 120,
        now() - DAY,
      )
      .toArray();
    if (!samples.length) return;
    const mismatches: any[] = [];
    for (const sample of samples) {
      // Independent read: take the stored facts first and never repair them
      // with this reference response before comparing.
      const stored = await call(
        this.env.FACTS,
        String(sample.partition),
        "/status",
        { id: sample.id, attempt: sample.attempt },
      );
      const reference = await this.github(
        `repos/${sample.repo}/actions/runs/${sample.id}/attempts/${sample.attempt}/jobs?per_page=100`,
      );
      if (reference.data.total_count > 100)
        throw new Error("Shadow sample requires pagination");
      const have = new Map(stored.job_rows.map((j: any) => [j.id, j]));
      for (const raw of reference.data.jobs) {
        const j = normalizeJob(raw),
          observed: any = have.get(j.id);
        for (const key of [
          "status",
          "conclusion",
          "created",
          "started",
          "completed",
          "kind",
        ] as const)
          if (!observed || observed[key] !== j[key])
            mismatches.push({
              repo: sample.repo,
              run: sample.id,
              job: j.id,
              field: key,
            });
      }
      if (have.size !== reference.data.jobs.length)
        mismatches.push({
          repo: sample.repo,
          run: sample.id,
          field: "job_count",
        });
      this.sql.exec(
        "UPDATE witnesses SET checked=? WHERE repo=? AND id=? AND attempt=?",
        now(),
        sample.repo,
        sample.id,
        sample.attempt,
      );
    }
    const object = await this.env.ARCHIVE.get("public/v1/ci/manifest.json"),
      manifest: any = object ? await object.json() : {};
    const fresh =
      Object.values(manifest.groups || {}).length === 5 &&
      Object.values(manifest.groups || {}).every(
        (g: any) => g.status === "updated",
      );
    const result = {
      samples: samples.length,
      passed: !mismatches.length && fresh,
      mismatches,
      fresh,
    };
    this.sql.exec(
      "INSERT OR REPLACE INTO comparisons VALUES (?,?,?)",
      now(),
      result.passed ? 1 : 0,
      JSON.stringify(result),
    );
    await this.env.ARCHIVE.put(
      `validation/${iso(now())}.json`,
      JSON.stringify(result),
    );
    this.set("compared", now());
  }
  async tick() {
    if (this.ticking) return this.ticking;
    this.ticking = this.performTick();
    try {
      return await this.ticking;
    } finally {
      this.ticking = undefined;
    }
  }
  private async performTick() {
    if ((this.get("lease") || 0) > now()) return;
    this.set("lease", now() + 90);
    try {
      const clock = now();
      await this.flushLandings();
      const pruning = await this.pruneRepairs(this.get("repair_prune_cursor") || "");
      this.set("repair_prune_cursor", pruning.cursor || "");
      const repositories = allowed(this.env), shard = Math.floor(clock / 60) % Math.max(1, repositories.length * 64);
      if (repositories.length)
        await call(this.env.DIRECTORIES, `${repositories[Math.floor(shard / 64)]}/${shard % 64}`, "/prune");
      if (clock - (this.get("inventory_scheduled") || 0) >= 900) {
        const previous = this.get("inventory_scheduled"),
          since = previous || clock - 900;
        for (const repo of allowed(this.env))
          this.enqueue(repo, "inventory", {
            since: Math.max(since - 120, clock - 3600),
            until: clock,
          });
        if (!previous)
          for (const repo of allowed(this.env))
            for (let i = 1; i <= 6; i++)
              this.enqueue(
                repo,
                "inventory",
                {
                  since: clock - i * 3600,
                  until: clock - (i - 1) * 3600,
                  archival: true,
                },
                clock + 600,
              );
        this.set("inventory_scheduled", clock);
      }
      if (clock - (this.get("bors_checked") || 0) >= 60)
        try {
          await this.bors();
          this.set("bors_error", null);
        } catch (error) {
          this.set("bors_error", String(error));
        }
      // Bors reads have their own bounded lane: GitHub quota cannot block them.
      await this.repairMembers();
      const deadline = Date.now() + 20000;
      for (const row of this.sql
        .exec(
          `SELECT * FROM repairs WHERE due<=? AND kind!='members' AND (kind!='details' OR ?=1)
          ORDER BY CASE
            WHEN kind='run' AND json_extract(args,'$.urgent')=1 THEN 0
            WHEN kind='inventory' AND COALESCE(json_extract(args,'$.archival'),0)=0
              AND json_extract(args,'$.until')>=? THEN 0
            WHEN kind='details' THEN 3
            WHEN COALESCE(json_extract(args,'$.archival'),0)=0 THEN 1 ELSE 2 END,
            CASE kind WHEN 'inventory' THEN 0 WHEN 'run' THEN 1 WHEN 'jobs' THEN 2 ELSE 3 END,
            COALESCE(json_extract(args,'$.urgent'),0) DESC,
            CASE WHEN kind='inventory' THEN -json_extract(args,'$.until') ELSE due END,due LIMIT 12`,
          clock,
          (this.get("detail_requests") || []).filter((at: number) => at > clock - 3600).length < DETAIL_REQUESTS_PER_HOUR ? 1 : 0,
          clock - 1800,
        )
        .toArray()) {
        if (Date.now() > deadline) break;
        try {
          await this.repair(row);
          this.set("repair_error", null);
        } catch (error) {
          this.set("repair_error", String(error));
          if (
            /GitHub HTTP (404|410)/.test(String(error)) ||
            (Number(row.failures) >= 2 &&
              /GitHub HTTP 422|Invalid telemetry|Response size limit/.test(
                String(error),
              ))
          ) {
            this.sql.exec(
              "INSERT OR REPLACE INTO gaps VALUES (?,?,?,?)",
              row.key,
              row.kind,
              String(error),
              now(),
            );
            this.sql.exec("DELETE FROM repairs WHERE key=?", row.key);
            continue;
          }
          // Admission deferrals are not failures of this repair. The quota
          // owner still gates every read, so retry promptly as slots reopen.
          const deferred = /budget|quota/.test(String(error));
          this.sql.exec(
            "UPDATE repairs SET failures=failures+?,due=? WHERE key=?",
            deferred ? 0 : 1,
            now() +
              (deferred
                ? 60
                : Math.min(3600, 60 * 2 ** Math.min(6, Number(row.failures)))),
            row.key,
          );
          if (/Optional detail budget/.test(String(error))) continue;
          if (/budget|quota|configured|authentication/.test(String(error)))
            break;
        }
      }
      await this.publish();
      if (clock - (this.get("compared") || 0) >= 3600)
        try {
          await this.compare();
          this.set("comparison_error", null);
        } catch (error) {
          this.set("comparison_error", String(error));
        }
      // Up to 400 runs per tick accommodates the modeled tenfold arrival
      // rate; every object still performs a bounded hundred-run quantum.
      const candidates = this.sql
        .exec<{
          name: string;
        }>(
          "SELECT name FROM partitions WHERE active=0 AND substr(name,-10)<? ORDER BY seen LIMIT 4",
          day(clock - 30 * DAY),
        )
        .toArray();
      for (const candidate of candidates) {
        const builds = candidate.name.slice(-10) < day(clock - 90 * DAY),
          key = `archive/${candidate.name}/${builds}`;
        const result = await call(this.env.FACTS, candidate.name, "/archive", {
          builds,
          cursor: this.get(key),
          limit: 100,
        });
        this.set(key, result.done ? null : result.cursor);
        this.sql.exec(
          "UPDATE partitions SET seen=? WHERE name=?",
          clock,
          candidate.name,
        );
        if (builds) {
          const compacted = await call(this.env.DAYS, candidate.name.slice(-10), "/compact", {
            day: candidate.name.slice(-10),
          });
          if (result.done && compacted.done) {
            this.sql.exec("DELETE FROM partitions WHERE name=?", candidate.name);
            this.sql.exec("DELETE FROM meta WHERE key IN (?,?)", key, `partition_revision/${candidate.name}`);
          }
        }
      }
      this.sql.exec("DELETE FROM attempts WHERE at<?", clock - 2 * DAY);
      this.sql.exec("DELETE FROM deliveries WHERE at<?", clock - 14 * DAY);
      this.sql.exec("DELETE FROM summaries WHERE day<?", day(clock - 90 * DAY));
      this.sql.exec("DELETE FROM leases WHERE expires<?", clock - DAY);
      this.sql.exec("DELETE FROM witnesses WHERE updated<?", clock - 2 * DAY);
      this.sql.exec("DELETE FROM comparisons WHERE at<?", clock - 30 * DAY);
      this.sql.exec("DELETE FROM gaps WHERE at<?", clock - 90 * DAY);
      this.sql.exec("DELETE FROM coverage WHERE until<?", clock - 30 * DAY);
      this.sql.exec("DELETE FROM annotations WHERE at<?", clock - 90 * DAY);
      this.sql.exec("DELETE FROM landings WHERE at<?", clock - 90 * DAY);
      this.sql.exec("DELETE FROM meta WHERE key LIKE 'active_checked/%' AND CAST(value AS INTEGER)<?", clock - 90 * DAY);
    } finally {
      this.set("lease", 0);
    }
  }
  async fetch(request: Request) {
    const path = new URL(request.url).pathname,
      b: any = await request.json();
    if (path === "/event") {
      await this.ingest(b);
      return json({ ok: true });
    }
    if (path === "/inbox-position") {
      if (b.save) this.set("inbox_cursor", b.cursor);
      return json({ cursor: this.get("inbox_cursor") || null });
    }
    if (path === "/partition") {
      const key = `partition_revision/${b.partition}`,
        revision = Number(b.revision) || 0;
      if (revision >= (this.get(key) || 0)) {
        this.sql.exec(
          "INSERT INTO partitions VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET active=excluded.active,seen=excluded.seen",
          b.partition,
          b.active,
          now(),
        );
        this.set(key, revision);
      }
      return json({ ok: true });
    }
    if (path === "/dirty") {
      this.sql.exec(
        "INSERT INTO dirty VALUES (?,1) ON CONFLICT(day) DO UPDATE SET revision=revision+1",
        b.day,
      );
      return json({ ok: true });
    }
    if (path === "/tick") {
      await this.tick();
      return json({ ok: true });
    }
    if (path === "/refresh-day") {
      // Finish an older publication before invalidating its derived snapshot.
      await this.publish();
      this.sql.exec("INSERT INTO dirty VALUES (?,1) ON CONFLICT(day) DO UPDATE SET revision=revision+1", b.day);
      this.set("charts_at", 0);
      await this.publish();
      const row = this.sql.exec<{ data: string }>("SELECT data FROM summaries WHERE day=?", b.day).toArray()[0];
      return json({ day: b.day, daily: row ? JSON.parse(row.data).daily : {} });
    }
    if (path === "/publish") {
      await this.publish();
      return json({ ok: true });
    }
    if (path === "/repair-members")
      return json(await this.repairMembers());
    if (path === "/repairs")
      return json(
        this.sql
          .exec(
            `SELECT repo,kind,args,due,failures FROM repairs
        WHERE (? IS NULL OR kind=?) ORDER BY due LIMIT 20`,
            b.kind || null,
            b.kind || null,
          )
          .toArray()
          .map((row) => ({ ...row, args: JSON.parse(String(row.args)) })),
      );
    if (path === "/prune-repairs")
      return json(await this.pruneRepairs(b.cursor || "", b.limit));
    if (path === "/backlog") {
      const rows = this.sql.exec<{ name: string }>(
        "SELECT name FROM partitions WHERE name>? ORDER BY name LIMIT 10",
        b.cursor || "",
      ).toArray();
      const results = await Promise.allSettled(rows.map((row) =>
        call(this.env.FACTS, row.name, "/backlog")));
      return json({
        // Inactive partitions can still have undelivered receipts. Include
        // them, but do not let one unavailable object hide the rest of a page.
        partitions: results.map((result, i) => result.status === "fulfilled"
          ? { ...result.value, partition: rows[i].name }
          : { partition: rows[i].name, error: "partition unavailable" }),
        cursor: rows.length === 10 ? rows.at(-1)!.name : null,
      });
    }
    if (path === "/health")
      return json({
        version: this.env.CF_VERSION?.id,
        github_configured: !!(
          this.env.GITHUB_APP_ID &&
          this.env.GITHUB_INSTALLATION_ID &&
          this.env.GITHUB_PRIVATE_KEY
        ),
        started: this.get("started"),
        published: this.get("published"),
        repair_error: this.get("repair_error"),
        bors_error: this.get("bors_error"),
        members_error: this.get("members_error"),
        comparison_error: this.get("comparison_error"),
        last_comparison: this.get("compared"),
        active_partitions: this.sql
          .exec<{
            n: number;
          }>("SELECT COUNT(*) n FROM partitions WHERE active>0")
          .one().n,
        retired_repairs: this.get("retired_repairs") || {},
        detail_requests_last_hour: (this.get("detail_requests") || []).filter((at: number) => at > now() - 3600).length,
        pending: this.sql
          .exec(
            "SELECT kind,COUNT(*) count,MIN(due) oldest FROM repairs GROUP BY kind",
          )
          .toArray(),
        dirty: this.sql.exec("SELECT COUNT(*) count FROM dirty").one(),
        requests: this.sql
          .exec(
            "SELECT status,COUNT(*) count FROM attempts WHERE at>? GROUP BY status",
            now() - 3600,
          )
          .toArray(),
      });
    if (path === "/comparison") {
      this.sql.exec(
        "INSERT OR REPLACE INTO comparisons VALUES (?,?,?)",
        now(),
        b.passed ? 1 : 0,
        JSON.stringify(b),
      );
      return json({ ok: true });
    }
    if (path === "/activation-check") {
      const rows = this.sql
        .exec<{
          at: number;
          passed: number;
        }>(
          "SELECT at,passed FROM comparisons WHERE at>=? ORDER BY at",
          now() - 8 * DAY,
        )
        .toArray();
      const span = rows.length ? rows.at(-1)!.at - rows[0].at : 0;
      const maxGap = rows.reduce(
        (n, r, i) => Math.max(n, i ? r.at - rows[i - 1].at : 0),
        0,
      );
      return json({
        ready: !!(
          this.env.GITHUB_APP_ID &&
          this.env.GITHUB_INSTALLATION_ID &&
          this.env.GITHUB_PRIVATE_KEY &&
          this.get("published") &&
          now() - this.get("published") < 300
        ),
        publication_age_seconds: this.get("published")
          ? now() - this.get("published")
          : null,
        latest_comparison_passed: rows.at(-1)?.passed === 1,
        comparison_count: rows.length,
        span_seconds: span,
        max_gap_seconds: maxGap,
      });
    }
    if (path === "/repair") {
      this.enqueue(b.repo, b.kind, b.args);
      return json({ ok: true });
    }
    if (path === "/lease") {
      const prior = this.sql
          .exec<{
            data: string;
          }>("SELECT data FROM leases WHERE publisher=?", b.publisher)
          .toArray()[0],
        existing = prior ? JSON.parse(prior.data) : null;
      if (existing) return json(existing);
      const clock = now(),
        budget = Math.min(500, Number(this.env.GITHUB_HOURLY_BUDGET) || 500),
        used = this.sql
          .exec<{
            n: number;
          }>("SELECT COUNT(*) n FROM attempts WHERE at>?", clock - 3600)
          .one().n,
        reserved = this.sql
          .exec<{ n: number }>(
            "SELECT COUNT(*) n FROM attempts WHERE at>? AND status=-1",
            clock - 3600,
          )
          .one().n;
      if (
        used + 6 > budget ||
        // At 500/hour, admit at most 60 six-read summaries. Leave 140 reads
        // for inventory, stale active runs, comparisons and token exchanges.
        reserved + 6 > Math.floor((budget * 0.72) / 6) * 6 ||
        (this.get("github_pause") || 0) > clock
      )
        return json({ granted: false, retry_after: 300 });
      this.ctx.storage.transactionSync(() => {
        for (let i = 0; i < 6; i++)
          this.sql.exec("INSERT INTO attempts(at,status) VALUES (?,-1)", clock);
      });
      const lease = {
        granted: true,
        run_id: b.run_id,
        attempt: b.attempt,
        expires: clock + 600,
        max_requests: 6,
      };
      this.sql.exec(
        "INSERT INTO leases VALUES (?,?,?)",
        b.publisher,
        JSON.stringify(lease),
        lease.expires,
      );
      return json(lease);
    }
    if (path === "/check-lease") {
      const row = this.sql
          .exec<{
            data: string;
          }>("SELECT data FROM leases WHERE publisher=?", b.publisher)
          .toArray()[0],
        l = row ? JSON.parse(row.data) : null;
      return json({
        valid:
          !!l &&
          l.expires >= now() &&
          l.run_id === b.run_id &&
          l.attempt === b.attempt,
      });
    }
    if (path === "/publisher-quota") {
      const lease = this.sql
        .exec<{
          expires: number;
        }>("SELECT expires FROM leases WHERE publisher=?", b.publisher)
        .toArray()[0];
      if (!lease || lease.expires < now()) return json({ accepted: false });
      const until =
        b.remaining === "0"
          ? Number(b.reset) || now() + 3600
          : now() + Math.max(60, Number(b.retry_after) || 60);
      this.set(
        "github_pause",
        Math.max(this.get("github_pause") || 0, Math.min(now() + 86400, until)),
      );
      return json({ accepted: true });
    }
    return json({ error: "unknown operation" }, 404);
  }
}
