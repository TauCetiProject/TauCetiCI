/** Prepared operations publication. Public GETs never enter this object. */
import { historyFrame } from "./operations-history";
import { hostFrame, publicHosts, retainHostFacts } from "./operations-hosts";
import { PUBLIC_POOLS } from "./runner-registry";
import { env as bindings } from "cloudflare:workers";
import { call, digest, json, now, operationsScope, type Env } from "./types";
import {
  canonical,
  QUEUE_SCHEMA,
  queueFrame,
  progressFrame,
  type QueueFrame,
} from "./operations-model";

const PREFIX = "/v1/operations/objects/";
type SourceRow = {
  id: string;
  epoch: number;
  sequence: number;
  committed_epoch: number;
  committed_sequence: number;
  snapshot: string | null;
  metadata: string | null;
  received: number;
};

export class Operations {
  private sql: SqlStorage;
  private publishing?: Promise<void>;
  private get env(): Env {
    return bindings as Env;
  }

  constructor(private ctx: DurableObjectState) {
    this.sql = ctx.storage.sql;
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS ops_meta(key TEXT PRIMARY KEY,value TEXT);
      CREATE TABLE IF NOT EXISTS ops_sources(id TEXT PRIMARY KEY,epoch INTEGER,sequence INTEGER,
        committed_epoch INTEGER,committed_sequence INTEGER,snapshot TEXT,metadata TEXT,received INTEGER);
      CREATE TABLE IF NOT EXISTS ops_frames(snapshot TEXT,part INTEGER,parts INTEGER,source TEXT,
        epoch INTEGER,sequence INTEGER,data TEXT,received INTEGER,hash TEXT,bytes INTEGER,metadata TEXT,base TEXT,PRIMARY KEY(snapshot,part));
      CREATE INDEX IF NOT EXISTS ops_frames_source ON ops_frames(source,epoch,sequence);
      CREATE INDEX IF NOT EXISTS ops_frames_age ON ops_frames(received);
      CREATE TABLE IF NOT EXISTS ops_changes(snapshot TEXT,collection TEXT,id TEXT,batch TEXT,pr_bucket INTEGER,
        data TEXT,part INTEGER,PRIMARY KEY(snapshot,collection,id));
      CREATE TABLE IF NOT EXISTS ops_entities(source TEXT,collection TEXT,id TEXT,batch TEXT,pr_bucket INTEGER,
        data TEXT,PRIMARY KEY(source,collection,id));
      CREATE INDEX IF NOT EXISTS ops_entities_batch ON ops_entities(source,collection,batch,id);
      CREATE INDEX IF NOT EXISTS ops_entities_pr ON ops_entities(source,collection,pr_bucket,id);
      CREATE TABLE IF NOT EXISTS ops_objects(hash TEXT PRIMARY KEY,referenced INTEGER);
      CREATE TABLE IF NOT EXISTS ops_links(parent TEXT,child TEXT,PRIMARY KEY(parent,child));
      CREATE TABLE IF NOT EXISTS ops_dirty(kind TEXT,id TEXT,revision INTEGER,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_views(kind TEXT,id TEXT,data TEXT,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_partitions(name TEXT PRIMARY KEY,revision INTEGER,pending INTEGER,received INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS ops_progress(repository TEXT,run TEXT,attempt INTEGER,check_run TEXT,session TEXT,sequence INTEGER,received INTEGER,data TEXT,PRIMARY KEY(repository,run,attempt,check_run));
      CREATE INDEX IF NOT EXISTS ops_progress_received ON ops_progress(received);
      CREATE TABLE IF NOT EXISTS ops_hosts(machine TEXT PRIMARY KEY,source TEXT,epoch INTEGER,sequence INTEGER,received INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS ops_job_facts(partition TEXT,id TEXT,repository TEXT,active INTEGER,observed REAL,data TEXT,PRIMARY KEY(partition,id));
      CREATE INDEX IF NOT EXISTS ops_job_facts_current ON ops_job_facts(active DESC,observed DESC,id ASC);
      CREATE INDEX IF NOT EXISTS ops_job_facts_repo ON ops_job_facts(repository,active,observed);
      CREATE TABLE IF NOT EXISTS ops_failures(kind TEXT,id TEXT,failures INTEGER,due INTEGER,error TEXT,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_runner_hosts(runner TEXT PRIMARY KEY,machine TEXT,instance TEXT,slot INTEGER,seen INTEGER);
      CREATE TABLE IF NOT EXISTS ops_host_samples(machine TEXT,bucket INTEGER,resolution INTEGER,data TEXT,PRIMARY KEY(machine,bucket,resolution));
      CREATE INDEX IF NOT EXISTS ops_host_samples_age ON ops_host_samples(resolution,bucket);
    `);
  }
  private get(key: string): any {
    const row = this.sql
      .exec<{ value: string }>("SELECT value FROM ops_meta WHERE key=?", key)
      .toArray()[0];
    return row ? JSON.parse(row.value) : undefined;
  }
  private set(key: string, value: any) {
    this.sql.exec(
      "INSERT OR REPLACE INTO ops_meta VALUES (?,?)",
      key,
      JSON.stringify(value),
    );
  }
  private repositories(): string[] {
    return JSON.parse(this.env.OPERATIONS_PUBLIC_REPOSITORIES || "[]");
  }
  private async schedule(delay = 10_000) {
    const next = Math.max(
        Date.now() + delay,
        (this.get("publication_retry") || 0) * 1000,
      ),
      old = await this.ctx.storage.getAlarm();
    if (old === null || old <= Date.now() || old > next)
      await this.ctx.storage.setAlarm(next);
  }
  private source(id: string) {
    return this.sql
      .exec<SourceRow>("SELECT * FROM ops_sources WHERE id=?", id)
      .toArray()[0];
  }

  private stage(frame: QueueFrame, fingerprint: string): Record<string, any> {
    if (frame.base !== null && this.get("queue_resync"))
      return { error: "Public scope changed", code: "resync", status: 409 };
    const currentId = this.get("queue_source");
    if (currentId && currentId !== frame.source.id)
      return {
        error: "Source identity changed",
        code: "source_changed",
        status: 409,
      };
    const previous = this.source(frame.source.id);
    const { epoch, sequence, id } = frame.source;
    if (
      previous?.snapshot === frame.snapshot &&
      previous.committed_epoch === epoch &&
      previous.committed_sequence === sequence
    ) {
      if (this.get("queue_receipts")?.[String(frame.part)] !== fingerprint)
        return { error: "Committed snapshot frame changed", status: 409 };
      return { committed: true, snapshot: frame.snapshot };
    }
    if (
      frame.base !== null &&
      (!previous?.metadata ||
        frame.base.epoch !== previous.committed_epoch ||
        frame.base.sequence !== previous.committed_sequence)
    )
      return {
        error: "Delta base is unavailable",
        code: "resync",
        status: 409,
      };
    if (
      previous &&
      (epoch < previous.epoch ||
        (epoch === previous.epoch && sequence < previous.sequence))
    )
      return {
        error: "Superseded observation",
        code: "superseded",
        status: 409,
      };
    const pending = this.get("queue_pending");
    if (
      pending?.epoch === epoch &&
      pending?.sequence === sequence &&
      pending.snapshot !== frame.snapshot
    )
      return { error: "Sequence already has a snapshot", status: 409 };
    const wire = JSON.stringify({ frame, hash: fingerprint });
    const existing = this.sql
      .exec<{ data: string }>(
        "SELECT data FROM ops_frames WHERE snapshot=? AND part=?",
        frame.snapshot,
        frame.part,
      )
      .toArray()[0];
    if (existing && existing.data !== wire)
      return { error: "Snapshot frame changed", status: 409 };
    if (
      previous &&
      epoch === previous.committed_epoch &&
      sequence <= previous.committed_sequence
    )
      return { error: "Sequence was already committed", status: 409 };
    if (!previous) {
      this.sql.exec(
        "INSERT INTO ops_sources VALUES (?,?,?,0,0,NULL,NULL,0)",
        id,
        epoch,
        sequence,
      );
      this.set("queue_source", id);
    } else if (epoch > previous.epoch || sequence > previous.sequence) {
      this.sql.exec(
        "UPDATE ops_sources SET epoch=?,sequence=? WHERE id=?",
        epoch,
        sequence,
        id,
      );
      // Fence a delayed frame from an earlier process or abandoned upload as
      // soon as a newer observation starts. Its last published data survives.
      this.sql.exec(
        "DELETE FROM ops_changes WHERE snapshot IN (SELECT snapshot FROM ops_frames WHERE source=? AND (epoch<? OR (epoch=? AND sequence<?)))",
        id,
        epoch,
        epoch,
        sequence,
      );
      this.sql.exec(
        "DELETE FROM ops_frames WHERE source=? AND (epoch<? OR (epoch=? AND sequence<?))",
        id,
        epoch,
        epoch,
        sequence,
      );
    }
    this.set("queue_pending", { epoch, sequence, snapshot: frame.snapshot });
    if (!existing)
      for (const change of frame.changes) {
        const value = change.value;
        const bucket =
          typeof value?.pr === "string" ? Number(BigInt(value.pr) % 64n) : null;
        // The primary key rejects an identity repeated in a different part.
        this.sql.exec(
          "INSERT INTO ops_changes VALUES (?,?,?,?,?,?,?)",
          frame.snapshot,
          change.collection,
          change.id,
          value?.batch_id ??
            (change.collection === "batches" ? change.id : null),
          bucket,
          JSON.stringify(value),
          frame.part,
        );
      }
    this.sql.exec(
      "INSERT OR IGNORE INTO ops_frames VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
      frame.snapshot,
      frame.part,
      frame.parts,
      id,
      epoch,
      sequence,
      wire,
      now(),
      fingerprint,
      new TextEncoder().encode(wire).length,
      frame.metadata ? JSON.stringify(frame.metadata) : null,
      JSON.stringify(frame.base),
    );
    const staged = this.sql
      .exec<{ count: number; bytes: number }>(
        "SELECT COUNT(*) count,COALESCE(SUM(bytes),0) bytes FROM ops_frames WHERE source=?",
        id,
      )
      .one();
    if (staged.bytes > 16_000_000 || staged.count > 2000)
      throw new Error("Snapshot staging limit exceeded");
    if (staged.count < frame.parts)
      return {
        committed: false,
        snapshot: frame.snapshot,
        received_parts: staged.count,
      };
    const parts = this.sql
      .exec<{
        part: number;
        parts: number;
        source: string;
        epoch: number;
        sequence: number;
        hash: string;
        metadata: string | null;
        base: string;
      }>(
        "SELECT part,parts,source,epoch,sequence,hash,metadata,base FROM ops_frames WHERE snapshot=? ORDER BY part",
        frame.snapshot,
      )
      .toArray();
    if (
      parts.length !== frame.parts ||
      parts.some(
        (p, i) =>
          p.part !== i ||
          p.parts !== frame.parts ||
          p.source !== id ||
          p.epoch !== epoch ||
          p.sequence !== sequence ||
          p.base !== JSON.stringify(frame.base),
      )
    )
      throw new Error("Inconsistent snapshot framing");
    const receipts: Record<string, string> = {};
    let metadata: Record<string, any> | undefined;
    for (const part of parts) {
      receipts[String(part.part)] = part.hash;
      if (part.metadata) metadata = JSON.parse(part.metadata);
    }
    if (!metadata) throw new Error("Missing snapshot commit");
    const scopes = metadata.scope.repositories as string[];
    if (
      !scopes.length ||
      this.sql
        .exec(
          `SELECT 1 FROM ops_changes WHERE snapshot=? AND collection IN ('batches','inputs','members')
       AND data!='null' AND json_extract(data,'$.repository') NOT IN (${scopes.map(() => "?").join(",")}) LIMIT 1`,
          frame.snapshot,
          ...scopes,
        )
        .toArray().length
    )
      throw new Error("Snapshot scope differs from its records");
    const revision = (this.get("revision") || 0) + 1;
    // Mark old AND new joins, including deletions, before mutating current state.
    const mark = (row: {
      collection: string;
      id: string;
      batch: string | null;
      pr_bucket: number | null;
    }) => {
      if (row.collection === "attempts")
        this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
      if (row.batch !== null) this.dirty("batch", row.batch, revision);
      if (row.pr_bucket !== null)
        this.dirty("pr", String(row.pr_bucket), revision);
    };
    if (frame.base === null)
      for (const collection of Object.keys(metadata.collection_complete)) {
        if (!metadata.collection_complete[collection]) continue;
        for (const row of this.sql
          .exec<any>(
            `SELECT collection,id,batch,pr_bucket FROM ops_entities WHERE source=? AND collection=?
        AND id NOT IN (SELECT id FROM ops_changes WHERE snapshot=? AND collection=? AND data!='null')`,
            id,
            collection,
            frame.snapshot,
            collection,
          )
          .toArray())
          mark(row);
        this.sql.exec(
          `DELETE FROM ops_entities WHERE source=? AND collection=? AND id NOT IN
        (SELECT id FROM ops_changes WHERE snapshot=? AND collection=? AND data!='null')`,
          id,
          collection,
          frame.snapshot,
          collection,
        );
      }
    for (const row of this.sql
      .exec<any>(
        `SELECT c.collection,c.id,c.batch,c.pr_bucket,e.batch old_batch,e.pr_bucket old_bucket FROM ops_changes c
      LEFT JOIN ops_entities e ON e.source=? AND e.collection=c.collection AND e.id=c.id
      WHERE c.snapshot=? AND (e.data IS NULL OR e.data!=c.data)`,
        id,
        frame.snapshot,
      )
      .toArray()) {
      mark(row);
      mark({
        ...row,
        batch: row.old_batch ?? null,
        pr_bucket: row.old_bucket ?? null,
      });
    }
    this.sql.exec(
      `DELETE FROM ops_entities WHERE source=? AND (collection,id) IN
      (SELECT collection,id FROM ops_changes WHERE snapshot=? AND data='null')`,
      id,
      frame.snapshot,
    );
    this.sql.exec(
      `INSERT INTO ops_entities(source,collection,id,batch,pr_bucket,data)
      SELECT ?,collection,id,batch,pr_bucket,data FROM ops_changes WHERE snapshot=? AND data!='null'
      ON CONFLICT(source,collection,id) DO UPDATE SET batch=excluded.batch,pr_bucket=excluded.pr_bucket,data=excluded.data
      WHERE ops_entities.data!=excluded.data`,
      id,
      frame.snapshot,
    );
    // Live detail retention is bounded independently of producer restarts/caps.
    // Long-term totals live in the separately prepared journal report.
    const retainedBatches = new Set(metadata.retained_batch_ids);
    for (const row of this.sql
      .exec<any>(
        "SELECT id FROM ops_entities WHERE source=? AND collection='batches' AND json_extract(data,'$.state') NOT IN ('head','open')",
        id,
      )
      .toArray()) {
      if (retainedBatches.has(row.id)) continue;
      this.dirty("batch", row.id, revision);
      this.sql.exec(
        "DELETE FROM ops_entities WHERE source=? AND collection='batches' AND id=?",
        id,
        row.id,
      );
    }
    for (const [collection, limit] of [
      ["events", 3000],
      ["tasks", 20000],
      ["attempts", 20000],
    ] as const) {
      for (const row of this.sql
        .exec<any>(
          "SELECT collection,id,batch,pr_bucket FROM ops_entities WHERE source=? AND collection=? ORDER BY CAST(id AS INTEGER) DESC LIMIT -1 OFFSET ?",
          id,
          collection,
          limit,
        )
        .toArray()) {
        mark(row);
        this.sql.exec(
          "DELETE FROM ops_entities WHERE source=? AND collection=? AND id=?",
          id,
          collection,
          row.id,
        );
      }
    }
    const terminalBatches = this.sql
      .exec<any>(
        "SELECT id,data FROM ops_entities WHERE source=? AND collection='batches' AND json_extract(data,'$.state') NOT IN ('head','open') ORDER BY CAST(id AS INTEGER) DESC",
        id,
      )
      .toArray();
    const horizon = Date.parse(metadata.observed_at) / 1000 - 90 * 86400;
    for (const [index, row] of terminalBatches.entries()) {
      const batch = JSON.parse(row.data),
        ended = Date.parse(batch.landed_at || batch.created_at) / 1000;
      if (index >= 500 || ended < horizon) {
        this.dirty("batch", row.id, revision);
        this.sql.exec(
          "DELETE FROM ops_entities WHERE source=? AND collection='batches' AND id=?",
          id,
          row.id,
        );
      }
    }
    const expiredInputs = this.sql
      .exec<any>(
        "SELECT collection,id,batch,pr_bucket FROM ops_entities WHERE source=? AND collection='inputs' AND json_extract(data,'$.state') NOT IN ('waiting','reserved') ORDER BY CAST(id AS INTEGER) DESC LIMIT -1 OFFSET 5000",
        id,
      )
      .toArray();
    for (const row of expiredInputs) {
      mark(row);
      this.sql.exec(
        "DELETE FROM ops_entities WHERE source=? AND collection='inputs' AND id=?",
        id,
        row.id,
      );
    }
    for (const row of this.sql
      .exec<any>(
        "SELECT collection,id,batch,pr_bucket FROM ops_entities WHERE source=? AND collection IN ('members','attempts','tasks','events') AND batch NOT IN (SELECT id FROM ops_entities WHERE source=? AND collection='batches')",
        id,
        id,
      )
      .toArray())
      mark(row);
    this.sql.exec(
      "DELETE FROM ops_entities WHERE source=? AND collection IN ('members','attempts','tasks','events') AND batch NOT IN (SELECT id FROM ops_entities WHERE source=? AND collection='batches')",
      id,
      id,
    );
    for (const row of this.sql
      .exec<any>(
        `SELECT collection,id,batch,pr_bucket FROM ops_entities m WHERE m.source=? AND m.collection='members'
      AND json_extract(m.data,'$.batch_version') != (SELECT json_extract(b.data,'$.version') FROM ops_entities b WHERE b.source=m.source AND b.collection='batches' AND b.id=m.batch)`,
        id,
      )
      .toArray()) {
      mark(row);
      this.sql.exec(
        "DELETE FROM ops_entities WHERE source=? AND collection='members' AND id=?",
        id,
        row.id,
      );
    }
    let retainedBytes = this.sql
      .exec<{ bytes: number }>(
        "SELECT COALESCE(SUM(length(CAST(data AS BLOB))),0) bytes FROM ops_entities",
      )
      .one().bytes;
    if (retainedBytes > 15_000_000) {
      // Retire detail, never a live batch identity or a queue state. Coverage is
      // explicitly partial and old member generations cannot count as current.
      const expendable = this.sql
        .exec<any>(
          `SELECT collection,id,batch,pr_bucket,length(CAST(data AS BLOB)) bytes FROM ops_entities
        WHERE collection!='batches' ORDER BY CASE WHEN collection='events' THEN 0 WHEN collection='tasks' THEN 1
        WHEN batch IN (SELECT id FROM ops_entities WHERE collection='batches' AND json_extract(data,'$.state')='head') THEN 3 ELSE 2 END,
        CAST(id AS INTEGER)`,
        )
        .toArray();
      for (const row of expendable) {
        if (retainedBytes <= 15_000_000) break;
        mark(row);
        this.sql.exec(
          "DELETE FROM ops_entities WHERE source=? AND collection=? AND id=?",
          id,
          row.collection,
          row.id,
        );
        retainedBytes -= row.bytes;
      }
      metadata.truncated = true;
      metadata.coverage = "partial";
    }
    for (const row of [...metadata.claims, ...metadata.conditions].filter(
      (r: any) => r.batch_id,
    ))
      if (
        !this.sql
          .exec(
            "SELECT 1 FROM ops_entities WHERE source=? AND collection='batches' AND id=?",
            id,
            row.batch_id,
          )
          .toArray().length
      )
        throw new Error("Metadata refers to an unpublished batch");
    for (const batch of metadata.active_batch_ids) {
      if (
        !this.sql
          .exec(
            "SELECT 1 FROM ops_entities WHERE source=? AND collection='batches' AND id=?",
            id,
            batch,
          )
          .toArray().length
      )
        throw new Error("Active batch was not transferred");
    }
    this.sql.exec(
      "UPDATE ops_sources SET committed_epoch=?,committed_sequence=?,snapshot=?,metadata=?,received=? WHERE id=?",
      epoch,
      sequence,
      frame.snapshot,
      JSON.stringify(metadata),
      now(),
      id,
    );
    this.sql.exec("DELETE FROM ops_frames WHERE source=?", id);
    this.sql.exec("DELETE FROM ops_changes WHERE snapshot=?", frame.snapshot);
    if (frame.base === null) this.set("queue_resync", false);
    this.set("queue_receipts", receipts);
    this.set("queue_pending", null);
    this.set("revision", revision);
    return { committed: true, snapshot: frame.snapshot };
  }

  private dirty(kind: string, id: string, revision: number) {
    this.sql.exec(
      "INSERT INTO ops_dirty VALUES (?,?,?) ON CONFLICT(kind,id) DO UPDATE SET revision=excluded.revision",
      kind,
      id,
      revision,
    );
  }
  private rows(
    source: string,
    collection: string,
    where = "",
    params: any[] = [],
  ) {
    return this.sql
      .exec<{ data: string }>(
        `SELECT data FROM ops_entities WHERE source=? AND collection=? ${where} ORDER BY id`,
        source,
        collection,
        ...params,
      )
      .toArray()
      .map((r) => JSON.parse(r.data));
  }
  private async object(value: any): Promise<{ url: string }> {
    const data = JSON.stringify(value),
      hash = (await operationsScope(this.env)) + "/" + (await digest(data)),
      clock = now();
    const exists = this.sql
      .exec("SELECT 1 FROM ops_objects WHERE hash=?", hash)
      .toArray().length;
    if (!exists) {
      await this.env.ARCHIVE.put("public" + PREFIX + hash + ".json", data, {
        httpMetadata: { contentType: "application/json" },
      });
      this.sql.exec(
        "INSERT OR IGNORE INTO ops_objects VALUES (?,?)",
        hash,
        clock,
      );
      for (const match of data.matchAll(
        /\/v1\/operations\/objects\/([a-f0-9]{64}\/[a-f0-9]{64})\.json/g,
      ))
        this.sql.exec(
          "INSERT OR IGNORE INTO ops_links VALUES (?,?)",
          hash,
          match[1],
        );
    } else
      this.sql.exec(
        "UPDATE ops_objects SET referenced=? WHERE hash=? AND referenced<?",
        clock,
        hash,
        clock - 3600,
      );
    return { url: PREFIX + hash + ".json" };
  }
  private async pages(schema: string, records: any[], size = 100) {
    const result = [];
    for (let start = 0; start < records.length; start += size)
      result.push({
        ...(await this.object({
          schema,
          records: records.slice(start, start + size),
        })),
        count: Math.min(size, records.length - start),
      });
    return result;
  }
  /** Capture all SQL facts synchronously before the first R2 await. Later commits
   * remain dirty for the next flush; a busy source cannot starve publication. */
  private captureQueue(source: SourceRow) {
    const views = this.sql
      .exec<{ kind: string; id: string; data: string }>(
        "SELECT * FROM ops_views",
      )
      .toArray();
    const cached = new Map(
      views.map((v) => [v.kind + ":" + v.id, JSON.parse(v.data)]),
    );
    const dirty = this.sql
      .exec<{ kind: string; id: string; revision: number }>(
        "SELECT * FROM ops_dirty",
      )
      .toArray();
    const batches = this.rows(source.id, "batches").filter((b) =>
      this.repositories().includes(b.repository),
    );
    const ids = new Set(batches.map((b) => b.id)),
      metadata = JSON.parse(source.metadata!);
    metadata.scope.repositories = metadata.scope.repositories.filter(
      (r: string) => this.repositories().includes(r),
    );
    metadata.active_batch_ids = metadata.active_batch_ids.filter((id: string) =>
      ids.has(id),
    );
    metadata.retained_batch_ids = metadata.retained_batch_ids.filter(
      (id: string) => ids.has(id),
    );
    metadata.claims = metadata.claims.filter((c: any) => ids.has(c.batch_id));
    metadata.conditions = metadata.conditions.filter(
      (c: any) => !c.batch_id || ids.has(c.batch_id),
    );
    source = { ...source, metadata: JSON.stringify(metadata) };
    const changed = batches
      .filter(
        (b) =>
          !cached.has("batch:" + b.id) ||
          dirty.some((d) => d.kind === "batch" && d.id === b.id),
      )
      .map((batch) => ({
        batch,
        members: this.rows(source.id, "members", "AND batch=?", [
          batch.id,
        ]).sort((a, b) => a.position - b.position),
        attempts: this.rows(source.id, "attempts", "AND batch=?", [batch.id]),
        tasks: this.rows(source.id, "tasks", "AND batch=?", [batch.id]),
        events: this.rows(source.id, "events", "AND batch=?", [batch.id]),
      }));
    const prs = Array.from({ length: 64 }, (_, bucket) => {
      if (
        cached.has("pr:" + bucket) &&
        !dirty.some((d) => d.kind === "pr" && d.id === String(bucket))
      )
        return null;
      return {
        bucket,
        inputs: this.rows(source.id, "inputs", "AND pr_bucket=?", [bucket]),
        members: this.rows(source.id, "members", "AND pr_bucket=?", [bucket]),
      };
    }).filter((p) => p !== null);
    return { source, cached, dirty, batches, changed, prs };
  }
  private async queueView(capture: ReturnType<Operations["captureQueue"]>) {
    const { source, cached, batches } = capture,
      meta = JSON.parse(source.metadata!);
    const updates: { kind: string; id: string; data: any }[] = [];
    const save = (kind: string, id: string, data: any) => {
      cached.set(kind + ":" + id, data);
      updates.push({ kind, id, data });
    };
    for (const { batch, members, attempts, tasks, events } of capture.changed) {
      const activeMembers = members.filter(
        (m) => !m.removed && m.batch_version === batch.version,
      );
      const detail = await this.object({
        schema: "tauceti.operations.batch/v1",
        source_id: source.id,
        batch,
        member_pages: await this.pages(
          "tauceti.operations.members/v1",
          members,
        ),
        attempt_pages: await this.pages(
          "tauceti.operations.attempts/v1",
          attempts,
          20,
        ),
        task_pages: await this.pages("tauceti.operations.tasks/v1", tasks),
        event_pages: await this.pages("tauceti.operations.events/v1", events),
        member_details_complete: activeMembers.length === batch.size,
        member_count: activeMembers.length,
        removed_count: members.length - activeMembers.length,
      });
      save("batch", batch.id, {
        ...batch,
        detail,
        member_count: activeMembers.length,
        latest_attempt:
          [...attempts].sort((a, b) =>
            BigInt(a.id) > BigInt(b.id) ? -1 : 1,
          )[0] || null,
        active_tasks: tasks.filter((t) => t.state === "open").slice(0, 10),
      });
    }
    for (const { bucket, inputs, members } of capture.prs) {
      const prs: Record<string, any> = {};
      for (const [kind, records] of [
        ["inputs", inputs],
        ["members", members],
      ] as const)
        for (const item of records) {
          const key = `${item.repository}#${item.pr}`;
          const pr = (prs[key] ||= {
            repository: item.repository,
            pr: item.pr,
            inputs: [],
            members: [],
          });
          pr[kind].push(item);
        }
      save("pr", String(bucket), {
        ref: await this.object({ schema: "tauceti.operations.prs/v1", prs }),
        waiting: inputs.filter((i) => ["waiting", "reserved"].includes(i.state))
          .length,
      });
    }
    const shards: any[][] = Array.from({ length: 64 }, () => []);
    let memberCount = 0;
    for (const batch of batches) {
      const view = cached.get("batch:" + batch.id);
      memberCount += view.member_count;
      shards[Number(BigInt(batch.id) % 64n)].push(view);
    }
    const batchIndex = [];
    for (const shard of shards)
      batchIndex.push(
        await this.object({
          schema: "tauceti.operations.batches/v1",
          batches: shard,
        }),
      );
    const active = new Set(meta.active_batch_ids);
    const recent = [...batches]
      .sort((a, b) => (BigInt(a.id) > BigInt(b.id) ? -1 : 1))
      .slice(0, 20);
    const summary = [
      ...new Map(
        [...batches.filter((b) => active.has(b.id)), ...recent].map((b) => [
          b.id,
          b,
        ]),
      ).values(),
    ];
    const ref = await this.object({
      ...meta,
      schema: QUEUE_SCHEMA,
      source_id: source.id,
      source_epoch: source.committed_epoch,
      source_sequence: source.committed_sequence,
      received_at: new Date(source.received * 1000).toISOString(),
      computed_at: new Date(now() * 1000).toISOString(),
      batches: summary.map((b) => ({
        ...b,
        latest_attempt: cached.get("batch:" + b.id).latest_attempt,
        active_tasks: cached.get("batch:" + b.id).active_tasks,
        detail: cached.get("batch:" + b.id).detail,
      })),
      counts: {
        batches: batches.length,
        members: memberCount,
        waiting: Array.from(
          { length: 64 },
          (_, i) => cached.get("pr:" + i).waiting,
        ).reduce((a, b) => a + b, 0),
      },
      batch_shards: batchIndex,
      pr_shards: Array.from(
        { length: 64 },
        (_, i) => cached.get("pr:" + i).ref,
      ),
    });
    return { ref, updates };
  }
  private partition(name: string, revision: number) {
    if (
      !/^[-A-Za-z0-9_.]+\/[-A-Za-z0-9_.]+\/\d{4}-\d{2}-\d{2}$/.test(name) ||
      !this.repositories().includes(name.split("/").slice(0, 2).join("/")) ||
      !Number.isSafeInteger(revision) ||
      revision < 0
    )
      return false;
    const old = this.sql
      .exec<any>("SELECT revision FROM ops_partitions WHERE name=?", name)
      .toArray()[0];
    if (old && old.revision >= revision) return false;
    this.sql.exec(
      `INSERT INTO ops_partitions VALUES (?,?,1,?,NULL) ON CONFLICT(name)
      DO UPDATE SET revision=excluded.revision,pending=1,received=excluded.received`,
      name,
      revision,
      now(),
    );
    this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
    this.set("revision", (this.get("revision") || 0) + 1);
    return true;
  }
  private async collectJobs() {
    const pending = this.sql
      .exec<{ name: string; revision: number }>(
        "SELECT p.name,p.revision FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name WHERE p.pending=1 AND (f.due IS NULL OR f.due<=?) ORDER BY p.received LIMIT 3",
        now(),
      )
      .toArray();
    await Promise.all(
      pending.map(async (p) => {
        try {
          const response = await this.env.FACTS.get(
            this.env.FACTS.idFromName(p.name),
          ).fetch("https://internal/operations", {
            method: "POST",
            body: "{}",
            signal: AbortSignal.timeout(3000),
          });
          if (!response.ok) throw new Error("Facts unavailable");
          const value: any = await response.json();
          if (
            value.schema !== "tauceti.operations.partition/v1" ||
            value.partition !== p.name
          )
            throw new Error("Facts version skew");
          this.ctx.storage.transactionSync(() => {
            this.sql.exec(
              "DELETE FROM ops_job_facts WHERE partition=?",
              p.name,
            );
            for (const job of value.jobs)
              this.sql.exec(
                "INSERT INTO ops_job_facts VALUES (?,?,?,?,?,?)",
                p.name,
                job.id,
                job.repository,
                job.active ? 1 : 0,
                Date.parse(job.observed_at) / 1000 || 0,
                JSON.stringify(job),
              );
            this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
            const { jobs: _jobs, ...metadata } = value;
            this.sql.exec(
              "UPDATE ops_partitions SET data=?,pending=CASE WHEN revision=? THEN 0 ELSE 1 END WHERE name=?",
              JSON.stringify(metadata),
              p.revision,
              p.name,
            );
            this.sql.exec(
              "DELETE FROM ops_failures WHERE kind='partition' AND id=?",
              p.name,
            );
          });
        } catch {
          const failures =
            (this.sql
              .exec<{ failures: number }>(
                "SELECT failures FROM ops_failures WHERE kind='partition' AND id=?",
                p.name,
              )
              .toArray()[0]?.failures || 0) + 1;
          this.sql.exec(
            "INSERT OR REPLACE INTO ops_failures VALUES ('partition',?,?,?,?)",
            p.name,
            failures,
            now() + Math.min(300, 10 * 2 ** Math.min(5, failures - 1)),
            "facts_observation_unavailable",
          );
          this.set("revision", (this.get("revision") || 0) + 1);
          console.warn("operations: partition observation unavailable");
        }
      }),
    );
    this.sql.exec(
      "DELETE FROM ops_partitions WHERE received<? AND pending=0",
      now() - 30 * 86400,
    );
    this.sql.exec(
      "DELETE FROM ops_job_facts WHERE partition NOT IN (SELECT name FROM ops_partitions)",
    );
  }
  private recordHost(host: any) {
    for (const slot of host.slots)
      if (slot.runner_id && slot.instance_id) {
        const old = this.sql
          .exec<{ machine: string; instance: string }>(
            "SELECT machine,instance FROM ops_runner_hosts WHERE runner=?",
            slot.runner_id,
          )
          .toArray()[0];
        if (
          old &&
          (old.machine !== host.machine_id || old.instance !== slot.instance_id)
        )
          continue;
        if (!old)
          this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
        this.sql.exec(
          "INSERT OR REPLACE INTO ops_runner_hosts VALUES (?,?,?,?,?)",
          slot.runner_id,
          host.machine_id,
          slot.instance_id,
          slot.slot,
          now(),
        );
      }
    this.sql.exec(
      "DELETE FROM ops_runner_hosts WHERE seen<?",
      now() - 30 * 86400,
    );
    const sampleBucket = Math.floor(now() / 15) * 15;
    if (
      this.sql
        .exec(
          "SELECT 1 FROM ops_host_samples WHERE machine=? AND bucket=? AND resolution=15",
          host.machine_id,
          sampleBucket,
        )
        .toArray().length
    )
      return;
    const fresh =
      host.connected &&
      Number.isFinite(Date.parse(host.observed_at)) &&
      Math.abs(now() - Date.parse(host.observed_at) / 1000) <= 60;
    const metrics: Record<string, number | null> = {
      cpu_idle_fraction: fresh ? host.cpu_idle_fraction : null,
      memory_available: fresh ? host.resources.memory_available : null,
      busy_slots:
        fresh && !host.slots_truncated
          ? host.slots.filter((s: any) => s.busy).length
          : null,
      workers_alive:
        fresh && !host.workers_truncated
          ? host.workers.filter((w: any) => w.alive).length
          : null,
      compiler_waiting_guests:
        fresh && host.compiler.healthy ? host.compiler.waiting_guests : null,
    };
    this.sql.exec(
      "INSERT OR REPLACE INTO ops_host_samples VALUES (?,?,15,?)",
      host.machine_id,
      Math.floor(now() / 15) * 15,
      JSON.stringify({
        observed_at: host.observed_at,
        received_at: now(),
        fresh,
        connected: host.connected,
        ...metrics,
      }),
    );
    for (const resolution of [300, 86400]) {
      const bucket = Math.floor(now() / resolution) * resolution;
      const previous = this.sql
        .exec<{ data: string }>(
          "SELECT data FROM ops_host_samples WHERE machine=? AND bucket=? AND resolution=?",
          host.machine_id,
          bucket,
          resolution,
        )
        .toArray()[0];
      const value = previous
        ? JSON.parse(previous.data)
        : { samples: 0, fresh_samples: 0, metrics: {} };
      value.samples++;
      value.fresh_samples += fresh ? 1 : 0;
      for (const [key, metric] of Object.entries(metrics)) {
        const m = (value.metrics[key] ||= {
          count: 0,
          sum: 0,
          min: null,
          max: null,
        });
        if (metric === null) continue;
        m.count++;
        m.sum += metric;
        m.min = m.min === null ? metric : Math.min(m.min, metric);
        m.max = m.max === null ? metric : Math.max(m.max, metric);
      }
      this.sql.exec(
        "INSERT OR REPLACE INTO ops_host_samples VALUES (?,?,?,?)",
        host.machine_id,
        bucket,
        resolution,
        JSON.stringify(value),
      );
    }
    if ((this.get("host_pruned") || 0) < now() - 3600) {
      const rows = this.sql
        .exec(
          "SELECT rowid FROM ops_host_samples WHERE (resolution=15 AND bucket<?) OR (resolution=300 AND bucket<?) OR (resolution=86400 AND bucket<?) LIMIT 1000",
          now() - 7 * 86400,
          now() - 90 * 86400,
          now() - 1095 * 86400,
        )
        .toArray();
      for (const row of rows)
        this.sql.exec("DELETE FROM ops_host_samples WHERE rowid=?", row.rowid);
      if (rows.length < 1000) this.set("host_pruned", now());
    }
  }
  private async hostHistory() {
    const cached = this.get("host_history");
    if (
      cached &&
      Math.floor((this.get("host_history_at") || 0) / 300) ===
        Math.floor(now() / 300)
    )
      return cached;
    // Capture the bounded five-minute history synchronously. Live host frames
    // can continue while immutable pages are uploaded.
    const registry = publicHosts(this.env.OPERATIONS_PUBLIC_HOSTS);
    const rows = registry.map((host) => ({
      host,
      records: this.sql
        .exec<{ bucket: number; data: string }>(
          "SELECT bucket,data FROM ops_host_samples WHERE machine=? AND resolution=300 AND bucket>=? ORDER BY bucket",
          host.machine_id,
          now() - 7 * 86400,
        )
        .toArray()
        .map((r) => ({
          at: new Date(r.bucket * 1000).toISOString(),
          ...JSON.parse(r.data),
        })),
    }));
    const hosts = [];
    for (const row of rows)
      hosts.push({
        ...row.host,
        pages: await this.pages(
          "tauceti.operations.host-history-page/v1",
          row.records,
          500,
        ),
      });
    const ref = await this.object({
      schema: "tauceti.operations.host-history/v1",
      observed_at: new Date(now() * 1000).toISOString(),
      resolution_seconds: 300,
      window_days: 7,
      hosts,
      coverage: "recorded_samples_only",
      before_activation: "unknown",
    });
    this.set("host_history", ref);
    this.set("host_history_at", now());
    return ref;
  }
  private async hostsView() {
    const frame = this.get("hosts"),
      registry = publicHosts(this.env.OPERATIONS_PUBLIC_HOSTS);
    const pushedAt = this.sql
      .exec<{ at: number | null }>("SELECT MAX(received) at FROM ops_hosts")
      .one().at;
    const value = {
      schema: "tauceti.operations.hosts/v1",
      coverage: frame || pushedAt ? "observed" : "unavailable",
      observed_at:
        frame?.observed_at ||
        (pushedAt ? new Date(pushedAt * 1000).toISOString() : null),
      received_at:
        this.get("hosts_received") ||
        (pushedAt ? new Date(pushedAt * 1000).toISOString() : null),
      computed_at: new Date(now() * 1000).toISOString(),
      source_revision: frame?.source || null,
      scope: { hosts: registry.map((h) => h.machine_id) },
      truncated: false,
      pools: [
        ...PUBLIC_POOLS.filter(
          (p) =>
            !("machine_id" in p) ||
            registry.some((h) => h.machine_id === p.machine_id),
        ),
        ...registry
          .filter(
            (h) =>
              !PUBLIC_POOLS.some(
                (p) => "machine_id" in p && p.machine_id === h.machine_id,
              ),
          )
          .map((h) => ({
            id: h.alias,
            label: h.alias,
            machine_id: h.machine_id,
            resources: "host_observer",
            inventory: "configured",
          })),
      ],
      hosts: registry.map((h) => {
        const pushed = this.sql
          .exec<{ data: string }>(
            "SELECT data FROM ops_hosts WHERE machine=?",
            h.machine_id,
          )
          .toArray()[0];
        const bridge = frame?.hosts.find(
          (v: any) => v.machine_id === h.machine_id,
        );
        const direct = pushed ? JSON.parse(pushed.data) : null;
        const selected = !direct
          ? bridge
          : !bridge
            ? direct
            : (Date.parse(bridge.observed_at) || 0) >
                  (Date.parse(direct.observed_at) || 0) ||
                (bridge.observed_at === direct.observed_at &&
                  (Date.parse(bridge.transport_at) || 0) >
                    (Date.parse(direct.transport_at) || 0))
              ? bridge
              : direct;
        return (
          selected || {
            ...h,
            coverage: "unavailable",
            observed_at: null,
            transport_at: null,
            connected: false,
          }
        );
      }),
    };
    return {
      ...(await this.object(value)),
      coverage: value.coverage,
      observed_at: value.observed_at,
      received_at: value.received_at,
    };
  }
  private async jobsView() {
    const partitions = this.sql
      .exec<{ name: string; data: string | null; pending: number }>(
        "SELECT name,data,pending FROM ops_partitions ORDER BY received DESC LIMIT 201",
      )
      .toArray();
    const facts = partitions
      .filter((p) =>
        this.repositories().includes(p.name.split("/").slice(0, 2).join("/")),
      )
      .flatMap((p) => (p.data ? [JSON.parse(p.data)] : []));
    const reporters = this.sql
      .exec<any>(
        "SELECT repository,run,attempt,check_run,received,data FROM ops_progress WHERE received>?",
        now() - 86400,
      )
      .toArray();
    const runnerHosts = this.sql
      .exec<{
        runner: string;
        machine: string;
        instance: string;
        slot: number;
      }>("SELECT runner,machine,instance,slot FROM ops_runner_hosts")
      .toArray();
    let registry: ReturnType<typeof publicHosts> = [];
    try {
      registry = publicHosts(this.env.OPERATIONS_PUBLIC_HOSTS);
    } catch {
      /* Host configuration is a separate domain. */
    }
    const queueAttempts = this.sql
      .exec<{ data: string }>(
        "SELECT data FROM ops_entities WHERE collection='attempts' AND json_extract(data,'$.run_id') IS NOT NULL",
      )
      .toArray()
      .map((r) => JSON.parse(r.data));
    const publicRepositories = this.repositories();
    const rawJobs = publicRepositories.length
      ? this.sql
          .exec<{ data: string }>(
            `SELECT data FROM ops_job_facts WHERE repository IN (${publicRepositories.map(() => "?").join(",")}) ORDER BY active DESC,observed DESC,id ASC LIMIT 2201`,
            ...publicRepositories,
          )
          .toArray()
          .map((r) => JSON.parse(r.data))
      : [];
    const hostsByRunner = new Map(runnerHosts.map((h) => [h.runner, h]));
    const hostsById = new Map(registry.map((h) => [h.machine_id, h]));
    const attemptsByRun = new Map(
      queueAttempts.map((a) => [a.run_id + "/" + a.run_attempt, a]),
    );
    const progressByJob = new Map(
      reporters.map((r) => [
        r.repository + "/" + r.run + "/" + r.attempt + "/" + r.check_run,
        r,
      ]),
    );
    const jobs = rawJobs
      .map((j) => {
        const candidateHost = hostsByRunner.get(j.runner_id);
        const host =
          candidateHost &&
          hostsById.has(candidateHost.machine) &&
          ((j.labels || []).includes("fleet-" + candidateHost.machine) ||
            j.runner_name ===
              "fleet-" +
                candidateHost.machine.slice(0, 12) +
                "-" +
                candidateHost.instance)
            ? candidateHost
            : undefined;
        j = {
          ...j,
          host_binding: host
            ? {
                machine_id: host.machine,
                alias: hostsById.get(host.machine)!.alias,
                instance_id: host.instance,
                slot: host.slot,
                evidence: "runner_id_and_fleet_identity",
              }
            : null,
        };
        const attempt = attemptsByRun.get(j.run_id + "/" + j.run_attempt);
        j = {
          ...j,
          queue_binding: attempt
            ? {
                batch_id: attempt.batch_id,
                version: attempt.version,
                candidate: attempt.candidate,
                attempt_id: attempt.id,
              }
            : null,
        };
        const reporter = progressByJob.get(
          j.repository +
            "/" +
            j.run_id +
            "/" +
            j.run_attempt +
            "/" +
            j.check_run_id,
        );
        if (!reporter) return j;
        const v = JSON.parse(reporter.data);
        return {
          ...j,
          progress: {
            ...v,
            coverage: "untrusted_counters_trusted_reporter",
            heartbeat_at: v.observed_at,
            received_at: new Date(reporter.received * 1000).toISOString(),
          },
        };
      })
      .sort(
        (a, b) =>
          Number(b.active) - Number(a.active) ||
          (b.observed_at || "").localeCompare(a.observed_at || ""),
      );
    const active = jobs.filter((j) => j.active),
      terminal = jobs.filter((j) => !j.active).slice(0, 200);
    const context = this.get("jobs_context");
    const received =
      facts
        .map((f) => f.received_at)
        .filter(Boolean)
        .sort()
        .at(-1) || null;
    const observed =
      [
        ...jobs.map((j) => j.observed_at),
        ...facts.flatMap((f) =>
          f.pending_workflows.map((w: any) => w.observed_at),
        ),
      ]
        .filter(Boolean)
        .sort()
        .at(-1) || null;
    const value = {
      schema: "tauceti.operations.jobs/v1",
      observed_at: observed,
      computed_at: new Date(now() * 1000).toISOString(),
      received_at: received,
      coverage: facts.length
        ? partitions.some((p) => p.pending)
          ? "partial"
          : "observed"
        : "unavailable",
      scope: { repositories: this.repositories() },
      truncated:
        partitions.length > 200 ||
        rawJobs.length > 2200 ||
        facts.some((f) => f.truncated) ||
        active.length > 2000,
      observer: {
        publication_at: new Date(now() * 1000).toISOString(),
        last_event_at: observed,
        inventory: context?.inventory || [],
        context_observed_at: context?.observed_at || null,
        pending_partitions: partitions.filter((p) => p.pending).length,
      },
      counts: {
        running: active.filter((j) => j.status === "in_progress" && j.assigned)
          .length,
        queued_jobs: active.filter(
          (j) => j.status !== "in_progress" || !j.assigned,
        ).length,
        pending_workflows: facts.reduce(
          (n, p) =>
            n + (p.pending_workflows_count ?? p.pending_workflows.length),
          0,
        ),
        active: active.length,
      },
      job_pages: await this.pages("tauceti.operations.job-page/v1", [
        ...active.slice(0, 2000),
        ...terminal,
      ]),
      workflow_pages: await this.pages(
        "tauceti.operations.workflow-page/v1",
        facts.flatMap((p) => p.pending_workflows).slice(0, 1000),
      ),
      partition_errors: partitions.filter((p) => p.pending).map((p) => p.name),
      github_budget: context?.github_budget || null,
    };
    return {
      ...(await this.object(value)),
      observed_at: observed,
      received_at: received,
      coverage: value.coverage,
      truncated: value.truncated,
    };
  }
  private async syncScope() {
    const scope = await operationsScope(this.env),
      previousScope = this.get("public_scope");
    if (previousScope === scope) return;
    const repositories = this.repositories(),
      publicSet = new Set(repositories);
    this.ctx.storage.transactionSync(() => {
      for (const row of this.sql
        .exec<{ source: string; collection: string; id: string; data: string }>(
          "SELECT source,collection,id,data FROM ops_entities WHERE collection IN ('batches','inputs','members')",
        )
        .toArray()) {
        if (!publicSet.has(JSON.parse(row.data).repository))
          this.sql.exec(
            "DELETE FROM ops_entities WHERE source=? AND collection=? AND id=?",
            row.source,
            row.collection,
            row.id,
          );
      }
      this.sql.exec(
        "DELETE FROM ops_entities WHERE collection IN ('attempts','tasks','events') AND batch NOT IN (SELECT id FROM ops_entities WHERE collection='batches')",
      );
      for (const row of this.sql
        .exec<{ name: string }>("SELECT name FROM ops_partitions")
        .toArray())
        if (!publicSet.has(row.name.split("/").slice(0, 2).join("/")))
          this.sql.exec("DELETE FROM ops_partitions WHERE name=?", row.name);
      this.sql.exec(
        "DELETE FROM ops_job_facts WHERE partition NOT IN (SELECT name FROM ops_partitions)",
      );
      for (const row of this.sql
        .exec<{ repository: string }>(
          "SELECT DISTINCT repository FROM ops_progress",
        )
        .toArray())
        if (!publicSet.has(row.repository))
          this.sql.exec(
            "DELETE FROM ops_progress WHERE repository=?",
            row.repository,
          );
      const history = this.get("history");
      if (history?.scope.repositories.some((r: string) => !publicSet.has(r)))
        this.set("history", null);
      this.sql.exec("DELETE FROM ops_meta WHERE key LIKE 'domain_cache/%'");
      this.sql.exec("DELETE FROM ops_views");
      this.set("host_history", null);
      this.set("public_scope", scope);
      if (previousScope !== undefined) {
        this.set("queue_resync", true);
        this.set("revision", (this.get("revision") || 0) + 1);
      }
    });
  }
  private async performPublish() {
    if ((this.get("publication_retry") || 0) > now()) return;
    await this.syncScope();
    const revision = this.get("revision") || 0,
      clock = now();
    if (
      revision === this.get("published_revision") &&
      !this.domainRetries().some((r) => r.due <= now()) &&
      (this.get("manifest_heartbeat") || 0) <= (this.get("published") || 0) &&
      !this.sql
        .exec(
          "SELECT 1 FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name WHERE p.pending=1 AND (f.due IS NULL OR f.due<=?) LIMIT 1",
          now(),
        )
        .toArray().length
    )
      return;
    const sourceId = this.get("queue_source"),
      source = sourceId && this.source(sourceId);
    const queueCache = this.get("domain_cache/queue"),
      queueVersion = source?.snapshot || "unavailable";
    const captured =
      source?.metadata &&
      this.repositories().length &&
      (queueCache?.version !== queueVersion || this.get("domain_error/queue"))
        ? this.captureQueue(source)
        : null;
    await this.collectJobs();
    const domains: Record<string, any> = {},
      caches: any[] = [];
    let updates: { kind: string; id: string; data: any }[] = [];
    const project = async (
      kind: string,
      version: any,
      build: () => Promise<any>,
    ) => {
      const previous = this.get("domain_cache/" + kind),
        scope = this.get("public_scope");
      if (
        previous?.scope === scope &&
        previous.version === version &&
        !this.get("domain_error/" + kind)
      ) {
        domains[kind] = previous.value;
        return;
      }
      const failed = this.get("domain_error/" + kind);
      if (!failed || failed.due <= now()) {
        try {
          const value = await build();
          domains[kind] = value;
          caches.push({ kind, version, scope, value });
          this.set("domain_error/" + kind, null);
          return;
        } catch {
          const failures = (failed?.failures || 0) + 1;
          this.set("domain_error/" + kind, {
            failures,
            due: now() + Math.min(300, 10 * 2 ** Math.min(5, failures - 1)),
            error: "projection_unavailable",
          });
          console.warn("operations: " + kind + " projection unavailable");
        }
      }
      domains[kind] = {
        ...(previous?.scope === scope
          ? previous.value
          : {
              url: null,
              coverage: "unavailable",
              observed_at: null,
              received_at: null,
            }),
        error: "projection_unavailable",
      };
    };
    const hostVersion = canonical({
      bridge: this.get("hosts")?.source || null,
      pushed: this.sql
        .exec("SELECT machine,epoch,sequence FROM ops_hosts ORDER BY machine")
        .toArray(),
    });
    await Promise.all([
      project("jobs", "2/" + (this.get("jobs_revision") || 0), () => this.jobsView()),
      project("hosts", hostVersion, () => this.hostsView()),
      project("host_history", Math.floor(now() / 300), async () => ({
        ...(await this.hostHistory()),
        coverage: "recorded_samples_only",
      })),
      project("queue", queueVersion, async () => {
        if (!source?.metadata || !this.repositories().length)
          return {
            coverage: "unavailable",
            observed_at: null,
            received_at: null,
            url: null,
          };
        const metadata = JSON.parse(source.metadata),
          result = await this.queueView(captured || this.captureQueue(source));
        updates = result.updates;
        return {
          ...result.ref,
          observed_at: metadata.observed_at,
          received_at: new Date(source.received * 1000).toISOString(),
          coverage: metadata.coverage,
          source_revision: metadata.source_revision,
          truncated: metadata.truncated,
        };
      }),
      project(
        "history",
        this.get("history")?.observed_at || "unavailable",
        async () => {
          const history = this.get("history");
          return history
            ? {
                ...(await this.object(history)),
                observed_at: history.observed_at,
                received_at: this.get("history_received"),
                coverage: history.coverage,
              }
            : {
                url: null,
                coverage: "unavailable",
                observed_at: null,
                received_at: null,
              };
        },
      ),
    ]);
    const manifest = {
      schema: "tauceti.operations.manifest/v1",
      scope: this.get("public_scope"),
      generated_at: new Date(clock * 1000).toISOString(),
      domains,
    };
    await this.env.ARCHIVE.put(
      "public/v1/operations/manifest.json",
      JSON.stringify(manifest),
      {
        httpMetadata: { contentType: "application/json" },
        customMetadata: { scope: this.get("public_scope") },
      },
    );
    this.ctx.storage.transactionSync(() => {
      for (const c of caches) this.set("domain_cache/" + c.kind, c);
      for (const v of updates)
        this.sql.exec(
          "INSERT OR REPLACE INTO ops_views VALUES (?,?,?)",
          v.kind,
          v.id,
          JSON.stringify(v.data),
        );
      this.sql.exec(
        "DELETE FROM ops_views WHERE kind='batch' AND id NOT IN (SELECT id FROM ops_entities WHERE collection='batches')",
      );
      this.sql.exec("DELETE FROM ops_dirty WHERE revision<=?", revision);
      this.set("published", clock);
      this.set("published_revision", revision);
      this.set("publication_error", false);
      this.set("publication_retry", null);
      this.set("publication_failures", 0);
      const roots = Object.values(domains)
        .filter((d) => d.url)
        .map((d) => d.url.slice(PREFIX.length, -5));
      if (roots.length)
        this.sql.exec(
          `WITH RECURSIVE live(hash) AS (
        SELECT hash FROM ops_objects WHERE hash IN (${roots.map(() => "?").join(",")})
        UNION SELECT child FROM ops_links JOIN live ON parent=live.hash)
        UPDATE ops_objects SET referenced=? WHERE hash IN (SELECT hash FROM live) AND referenced<?`,
          ...roots,
          clock,
          clock - 3600,
        );
    });
    for (const row of this.sql
      .exec<{ hash: string }>(
        "SELECT hash FROM ops_objects WHERE referenced<? LIMIT 20",
        clock - 7 * 86400,
      )
      .toArray()) {
      // Remove the existence receipt first. A crash can leave an orphan object,
      // but can never make a later projection skip uploading a missing object.
      this.sql.exec("DELETE FROM ops_objects WHERE hash=?", row.hash);
      this.sql.exec("DELETE FROM ops_links WHERE parent=?", row.hash);
      await this.env.ARCHIVE.delete("public" + PREFIX + row.hash + ".json");
    }
    this.sql.exec(
      "DELETE FROM ops_changes WHERE snapshot IN (SELECT snapshot FROM ops_frames WHERE received<?)",
      clock - 3600,
    );
    this.sql.exec("DELETE FROM ops_frames WHERE received<?", clock - 3600);
  }
  async publish() {
    if (this.publishing) return this.publishing;
    this.publishing = this.performPublish();
    try {
      await this.publishing;
    } finally {
      this.publishing = undefined;
    }
  }
  private domainRetries(): { due: number }[] {
    return this.sql
      .exec<{ value: string }>(
        "SELECT value FROM ops_meta WHERE key LIKE 'domain_error/%' AND value!='null'",
      )
      .toArray()
      .map((r) => JSON.parse(r.value));
  }
  async alarm() {
    await this.ctx.storage.setAlarm(Date.now() + 30_000);
    try {
      await this.publish();
    } catch {
      const failures = (this.get("publication_failures") || 0) + 1;
      this.set("publication_failures", failures);
      this.set("publication_error", true);
      this.set(
        "publication_retry",
        now() + Math.min(300, 10 * 2 ** Math.min(5, failures - 1)),
      );
      console.warn("operations: public pointer unavailable");
    }
    const partitionDue = this.sql
      .exec<{ due: number | null }>(
        "SELECT MIN(COALESCE(f.due,?)) due FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name WHERE p.pending=1",
        now() + 10,
      )
      .one().due;
    const retryAt = Math.min(
      ...this.domainRetries().map((r) => r.due),
      partitionDue ?? Infinity,
    );
    if ((this.get("revision") || 0) !== (this.get("published_revision") ?? -1))
      await this.schedule();
    else if (Number.isFinite(retryAt))
      await this.ctx.storage.setAlarm(
        Math.max(
          Date.now() + 1000,
          retryAt * 1000,
          (this.get("publication_retry") || 0) * 1000,
        ),
      );
    else await this.ctx.storage.deleteAlarm();
  }

  async fetch(request: Request) {
    const path = new URL(request.url).pathname;
    if (path === "/queue") {
      let frame: QueueFrame;
      try {
        frame = queueFrame(await request.json(), this.repositories());
      } catch {
        return json(
          { error: "Invalid public observation", code: "invalid_frame" },
          400,
        );
      }
      let result;
      const canonicalFrame = canonical(frame),
        fingerprint = await digest(canonicalFrame);
      frame = JSON.parse(canonicalFrame);
      try {
        result = this.ctx.storage.transactionSync(() =>
          this.stage(frame, fingerprint),
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : "";
        const code =
          message === "Snapshot staging limit exceeded"
            ? "staging_budget"
            : message.includes("batch")
              ? "missing_batch"
              : "invalid_snapshot";
        return json({ error: "Invalid or oversized snapshot", code }, 400);
      }
      if (result.committed) await this.schedule();
      return json(result, result.status || 200);
    }
    if (path === "/history") {
      let value;
      try {
        value = historyFrame(await request.json(), this.repositories());
      } catch {
        return json({ error: "Invalid public history" }, 400);
      }
      const old = this.get("history");
      if (old && Date.parse(value.observed_at) < Date.parse(old.observed_at))
        return json({ error: "Older report" }, 409);
      this.set("history", value);
      this.set("history_received", new Date(now() * 1000).toISOString());
      this.set("revision", (this.get("revision") || 0) + 1);
      await this.schedule();
      return json({ accepted: true });
    }
    if (path === "/progress") {
      let body: any, value;
      try {
        body = await request.json();
        value = progressFrame(body.value);
      } catch {
        return json({ error: "Invalid public progress" }, 400);
      }
      const c = body.claims;
      if (!this.repositories().includes(c.repository))
        return json({ error: "Repository not public" }, 403);
      const args = [c.repository, c.run_id, c.run_attempt, c.check_run_id];
      const old = this.sql
        .exec<any>(
          "SELECT session,sequence,received FROM ops_progress WHERE repository=? AND run=? AND attempt=? AND check_run=?",
          ...args,
        )
        .toArray()[0];
      if (old && now() - old.received < 10)
        return json(
          { error: "Progress interval is at least ten seconds" },
          429,
        );
      // One trusted wrapper session per immutable job. A delayed reporter cannot
      // overwrite a newer session merely by claiming a large wall-clock time.
      if (
        old &&
        (old.session !== value.session || old.sequence >= value.sequence)
      )
        return json({ error: "Superseded progress" }, 409);
      this.sql.exec(
        "INSERT OR REPLACE INTO ops_progress VALUES (?,?,?,?,?,?,?,?)",
        ...args,
        value.session,
        value.sequence,
        now(),
        JSON.stringify(value),
      );
      this.sql.exec("DELETE FROM ops_progress WHERE received<?", now() - 86400);
      this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
      this.set("revision", (this.get("revision") || 0) + 1);
      await this.schedule();
      return json({ accepted: true });
    }
    if (path === "/host") {
      let frame;
      try {
        const value: any = await request.json(),
          registry = publicHosts(this.env.OPERATIONS_PUBLIC_HOSTS);
        if (value.hosts?.length !== 1) throw new Error("Expected one machine");
        frame = hostFrame(
          value,
          registry.filter((h) => h.machine_id === value.hosts[0].machine_id),
        );
      } catch {
        return json({ error: "Invalid scoped host observation" }, 400);
      }
      let host = frame.hosts[0];
      const source = frame.source;
      const old = this.sql
        .exec<any>(
          "SELECT source,epoch,sequence,data FROM ops_hosts WHERE machine=?",
          host.machine_id,
        )
        .toArray()[0];
      if (
        old &&
        (old.source !== source.id ||
          old.epoch > source.epoch ||
          (old.epoch === source.epoch && old.sequence >= source.sequence))
      )
        return json({ error: "Superseded scoped host observation" }, 409);
      host = retainHostFacts(host, old ? JSON.parse(old.data) : null);
      this.ctx.storage.transactionSync(() => {
        this.sql.exec(
          "INSERT OR REPLACE INTO ops_hosts VALUES (?,?,?,?,?,?)",
          host.machine_id,
          source.id,
          source.epoch,
          source.sequence,
          now(),
          JSON.stringify(host),
        );
        this.recordHost(host);
      });
      this.set("revision", (this.get("revision") || 0) + 1);
      await this.schedule();
      return json({ accepted: true });
    }
    if (path === "/hosts") {
      let frame;
      try {
        frame = hostFrame(
          await request.json(),
          publicHosts(this.env.OPERATIONS_PUBLIC_HOSTS),
        );
      } catch {
        return json({ error: "Invalid public host observation" }, 400);
      }
      const previous = this.get("hosts");
      if (
        previous &&
        (previous.source.id !== frame.source.id ||
          previous.source.epoch > frame.source.epoch ||
          (previous.source.epoch === frame.source.epoch &&
            previous.source.sequence >= frame.source.sequence))
      )
        return json(
          { error: "Superseded host observation or changed source identity" },
          409,
        );
      frame.hosts = frame.hosts.map((host: any) =>
        retainHostFacts(
          host,
          previous?.hosts.find((old: any) => old.machine_id === host.machine_id),
        ),
      );
      this.ctx.storage.transactionSync(() => {
        this.set("hosts", frame);
        this.set("hosts_received", new Date(now() * 1000).toISOString());
        this.set("revision", (this.get("revision") || 0) + 1);
        for (const host of frame.hosts) this.recordHost(host);
      });
      await this.schedule();
      return json({ accepted: true });
    }
    if (path === "/partition") {
      const value: any = await request.json();
      if (this.partition(value.partition, value.revision))
        await this.schedule();
      return json({ ok: true });
    }
    if (path === "/tick") {
      this.set("manifest_heartbeat", now());
      if ((this.get("context_checked") || 0) < now() - 45 && this.env.CONTROL) {
        this.set("context_checked", now());
        try {
          const context = await call(
            this.env.CONTROL,
            "control",
            "/operations-context",
          );
          const { observed_at: _observed, ...meaning } = context;
          const meaningKey = canonical(meaning);
          if (this.get("jobs_context_key") !== meaningKey) {
            this.set("jobs_context_key", meaningKey);
            this.set("jobs_context", context);
            this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
            this.set("revision", (this.get("revision") || 0) + 1);
          }
          for (const p of context.partitions) {
            const known = this.sql
              .exec("SELECT 1 FROM ops_partitions WHERE name=?", p.name)
              .toArray().length;
            if (!known) this.partition(p.name, 0);
          }
          this.set("jobs_context_error", false);
        } catch {
          this.set("jobs_context_error", true);
        }
      }
      await this.alarm();
      return json({ ok: true });
    }
    if (path === "/health")
      return json({
        published_at: this.get("published") || null,
        publication_error: !!this.get("publication_error"),
        revision: this.get("revision") || 0,
        published_revision: this.get("published_revision") ?? null,
        queue_source: this.get("queue_source") || null,
        queue_fence: (() => {
          const row = this.source(this.get("queue_source") || "");
          return row ? { epoch: row.epoch, sequence: row.sequence } : null;
        })(),
        staged_frames: this.sql
          .exec<{ count: number }>("SELECT COUNT(*) count FROM ops_frames")
          .one().count,
      });
    return json({ error: "Not found" }, 404);
  }
}
