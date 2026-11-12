/** Prepared operations publication. Public GETs never enter this object. */
import { historyFrame } from "./operations-history";
import { mergeActivity } from "./operations-activity";
import { mergeBacklogs } from "./operations-backlog";
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
const BUILD_CLASSES = ["pr_build", "staging_build", "other_build"];
const BUILD_CLASS_SQL =
  "CASE WHEN json_extract(data,'$.job_class') IN ('pr_build','staging_build','other_build') THEN 1 ELSE 0 END";
type QueuePreparation = {
  scope: string;
  version: string;
  source_epoch: number;
  source_sequence: number;
  observed_at: string;
  started_at: number;
  capture_finished_at: number;
  graph_finished_at: number | null;
  ready_recorded_at: number | null;
  object_requests: number;
  cache_hits: number;
  shared_hits: number;
  put_attempts: number;
  puts: number;
  bytes_written: number;
  permit_wait_ms: number;
  put_ms: number;
  max_put_ms: number;
};
type Projection = {
  scope: string;
  lane: string;
  repositories: string[];
  hosts: string;
  timing?: QueuePreparation;
};
type Ready = { scope: string; version: string; value: any; revision: number };
const DOMAINS = ["jobs", "hosts", "host_history", "queue", "history"] as const;
type Domain = (typeof DOMAINS)[number];
type Preparation = {
  version: string;
  scope: string;
  started: number;
  promise: Promise<void>;
};
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
  private publicationRequested = false;
  private bootstrapRequested = false;
  private publicationStarted?: number;
  private preparing = new Map<string, Preparation>();
  private pointer?: Promise<void>;
  private pointerStarted?: number;
  private maintenance?: Promise<void>;
  private alarmUpdates: Promise<void> = Promise.resolve();
  private publicationPending = new Set<string>();
  private objectWrites = new Map<string, Promise<{ url: string }>>();
  private activeUploads = 0;
  private uploadWaiters: { lane: string; resolve: () => void }[] = [];
  private lastUploadLane?: string;
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
      CREATE TABLE IF NOT EXISTS ops_object_keys(logical TEXT PRIMARY KEY,hash TEXT,allocated INTEGER);
      CREATE UNIQUE INDEX IF NOT EXISTS ops_object_keys_hash ON ops_object_keys(hash);
      CREATE TABLE IF NOT EXISTS ops_links(parent TEXT,child TEXT,PRIMARY KEY(parent,child));
      CREATE TABLE IF NOT EXISTS ops_dirty(kind TEXT,id TEXT,revision INTEGER,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_views(kind TEXT,id TEXT,data TEXT,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_partitions(name TEXT PRIMARY KEY,revision INTEGER,pending INTEGER,received INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS ops_progress(repository TEXT,run TEXT,attempt INTEGER,check_run TEXT,session TEXT,sequence INTEGER,received INTEGER,data TEXT,PRIMARY KEY(repository,run,attempt,check_run));
      CREATE INDEX IF NOT EXISTS ops_progress_received ON ops_progress(received);
      CREATE TABLE IF NOT EXISTS ops_hosts(machine TEXT PRIMARY KEY,source TEXT,epoch INTEGER,sequence INTEGER,received INTEGER,data TEXT);
      CREATE TABLE IF NOT EXISTS ops_job_facts(partition TEXT,id TEXT,repository TEXT,active INTEGER,observed REAL,data TEXT,PRIMARY KEY(partition,id));
      CREATE INDEX IF NOT EXISTS ops_job_facts_current ON ops_job_facts(active DESC,observed DESC,id ASC);
      CREATE INDEX IF NOT EXISTS ops_job_facts_completed_class ON ops_job_facts(active,(${BUILD_CLASS_SQL}),observed DESC,id ASC);
      CREATE INDEX IF NOT EXISTS ops_job_facts_repo ON ops_job_facts(repository,active,observed);
      CREATE TABLE IF NOT EXISTS ops_failures(kind TEXT,id TEXT,failures INTEGER,due INTEGER,error TEXT,PRIMARY KEY(kind,id));
      CREATE TABLE IF NOT EXISTS ops_runner_hosts(runner TEXT PRIMARY KEY,machine TEXT,instance TEXT,slot INTEGER,seen INTEGER);
      CREATE TABLE IF NOT EXISTS ops_host_samples(machine TEXT,bucket INTEGER,resolution INTEGER,data TEXT,PRIMARY KEY(machine,bucket,resolution));
      CREATE INDEX IF NOT EXISTS ops_host_samples_age ON ops_host_samples(resolution,bucket);
    `);
    // Quiet historical sources will not emit another webhook. Revisit legacy
    // cached partitions through the existing bounded collector to seed activity.
    const legacy = this.sql.exec(`UPDATE ops_partitions SET pending=1
      WHERE pending=0 AND data IS NOT NULL AND
        (json_type(data,'$.activity') IS NULL OR json_type(data,'$.backlog') IS NULL)
      RETURNING name`).toArray();
    if (legacy.length) {
      this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
      this.set("revision", (this.get("revision") || 0) + 1);
    }
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
  private alarmUpdate(work: () => Promise<void>) {
    const next = this.alarmUpdates.then(work);
    this.alarmUpdates = next.catch(() => {});
    return next;
  }
  private async schedule() {
    // Recovery is durable before best-effort background work starts.
    await this.alarmUpdate(() => this.scheduleObservation());
    this.nudge();
  }
  private nudge(): void {
    // Completion callbacks must not await/return this pump: the jobs lane may
    // itself be waiting for the completing Facts worker's promise.
    this.ctx.waitUntil(
      this.publish().catch(() =>
        console.warn("operations: event publication unavailable"),
      ),
    );
  }
  private async scheduleObservation() {
    // Coalesce at the next allowed publication slot, rather than adding a
    // full interval after every idle arrival. The write cadence stays bounded.
    const lastStarted = Math.max(
      this.publicationStarted || 0,
      (this.get("published") || 0) * 1000,
    );
    const next = Math.max(
        Date.now() + 1000,
        lastStarted + 10_000,
        (this.get("publication_retry") || 0) * 1000,
      ),
      old = await this.ctx.storage.getAlarm();
    // A due alarm may be waiting for delivery. Incoming observations must
    // preserve it: resetting an overdue deadline can starve publication.
    if (old === null || old > next) await this.ctx.storage.setAlarm(next);
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
      .exec<{
        data: string;
      }>(
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
      .exec<{
        count: number;
        bytes: number;
      }>(
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
      .exec<{
        bytes: number;
      }>(
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
  /** A held put retains its permit. Pointer writes take the next available
   * permit; bulk lanes alternate when more than one lane is waiting. */
  private async upload<T>(lane: string, write: () => Promise<T>): Promise<T> {
    if (this.activeUploads >= 4)
      await new Promise<void>((resolve) =>
        this.uploadWaiters.push({ lane, resolve }),
      );
    else this.activeUploads++;
    this.lastUploadLane = lane;
    try {
      return await write();
    } finally {
      const pointer = this.uploadWaiters.findIndex(
        (w) => w.lane === "manifest",
      );
      const other = this.uploadWaiters.findIndex(
        (w) => w.lane !== this.lastUploadLane,
      );
      const index = pointer >= 0 ? pointer : other >= 0 ? other : 0;
      const next = this.uploadWaiters.splice(index, 1)[0];
      if (next) next.resolve();
      else this.activeUploads--;
    }
  }
  private async parallel<T, R>(
    values: T[],
    build: (value: T) => Promise<R>,
  ): Promise<R[]> {
    const result: R[] = [];
    let next = 0;
    const workers = await Promise.allSettled(
      Array.from({ length: Math.min(4, values.length) }, async () => {
        while (next < values.length) {
          const index = next++;
          result[index] = await build(values[index]);
        }
      }),
    );
    const failure = workers.find((r) => r.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
    return result;
  }
  /** Await every bounded sibling even if one fails. A held child continues to
   * own this preparation and its permit until storage actually settles. */
  private async settled<T>(promises: Promise<T>[]): Promise<T[]> {
    const results = await Promise.allSettled(promises);
    const failure = results.find((r) => r.status === "rejected");
    if (failure?.status === "rejected") throw failure.reason;
    return results.map((r) => (r as PromiseFulfilledResult<T>).value);
  }
  private async object(
    value: any,
    projection: Projection,
  ): Promise<{ url: string }> {
    const timing = projection.timing;
    if (timing) timing.object_requests++;
    const data = JSON.stringify(value),
      logical = projection.scope + "/" + (await digest(data));
    const pending = this.objectWrites.get(logical);
    if (pending) {
      if (timing) timing.shared_hits++;
      return pending;
    }
    const work = (async () => {
      let allocation = this.sql
        .exec<{
          hash: string;
        }>("SELECT hash FROM ops_object_keys WHERE logical=?", logical)
        .toArray()[0];
      // Existing deployments used the logical content hash as the physical key.
      // Reuse a live legacy receipt; after retirement never recreate that key.
      if (
        !allocation &&
        this.sql
          .exec("SELECT 1 FROM ops_objects WHERE hash=?", logical)
          .toArray().length
      ) {
        this.sql.exec(
          "UPDATE ops_objects SET referenced=? WHERE hash=? AND referenced<?",
          now(),
          logical,
          now() - 3600,
        );
        if (timing) timing.cache_hits++;
        return { url: PREFIX + logical + ".json" };
      }
      if (!allocation) {
        const hash =
          projection.scope +
          "/" +
          (await digest(logical + "/" + crypto.randomUUID()));
        // Persist allocation BEFORE upload so an ambiguous put/restart retries
        // the same incarnation instead of leaving a new orphan on every retry.
        this.sql.exec(
          "INSERT OR IGNORE INTO ops_object_keys VALUES (?,?,?)",
          logical,
          hash,
          now(),
        );
        allocation = this.sql
          .exec<{
            hash: string;
          }>("SELECT hash FROM ops_object_keys WHERE logical=?", logical)
          .one();
      }
      const hash = allocation.hash;
      if (
        !this.sql.exec("SELECT 1 FROM ops_objects WHERE hash=?", hash).toArray()
          .length
      ) {
        const queuedAt = Date.now();
        await this.upload(projection.lane, async () => {
          const startedAt = Date.now();
          if (timing) {
            timing.permit_wait_ms += startedAt - queuedAt;
            timing.put_attempts++;
          }
          try {
            const result = await this.env.ARCHIVE.put(
              "public" + PREFIX + hash + ".json",
              data,
              {
                httpMetadata: { contentType: "application/json" },
              },
            );
            if (timing) {
              timing.puts++;
              timing.bytes_written += new TextEncoder().encode(data).length;
            }
            return result;
          } finally {
            if (timing) {
              const elapsed = Date.now() - startedAt;
              timing.put_ms += elapsed;
              timing.max_put_ms = Math.max(timing.max_put_ms, elapsed);
            }
          }
        });
        this.ctx.storage.transactionSync(() => {
          this.sql.exec(
            "INSERT OR IGNORE INTO ops_objects VALUES (?,?)",
            hash,
            now(),
          );
          for (const match of data.matchAll(
            /\/v1\/operations\/objects\/([a-f0-9]{64}\/[a-f0-9]{64})\.json/g,
          ))
            this.sql.exec(
              "INSERT OR IGNORE INTO ops_links VALUES (?,?)",
              hash,
              match[1],
            );
        });
      } else {
        if (timing) timing.cache_hits++;
        this.sql.exec(
          "UPDATE ops_objects SET referenced=? WHERE hash=? AND referenced<?",
          now(),
          hash,
          now() - 3600,
        );
      }
      return { url: PREFIX + hash + ".json" };
    })();
    this.objectWrites.set(logical, work);
    try {
      return await work;
    } finally {
      if (this.objectWrites.get(logical) === work)
        this.objectWrites.delete(logical);
    }
  }
  private async pages(
    schema: string,
    records: any[],
    size = 100,
    projection: Projection,
  ) {
    const starts = Array.from(
      { length: Math.ceil(records.length / size) },
      (_, index) => index * size,
    );
    return this.parallel(starts, async (start) => ({
      ...(await this.object(
        { schema, records: records.slice(start, start + size) },
        projection,
      )),
      count: Math.min(size, records.length - start),
    }));
  }
  /** Capture all SQL facts synchronously before the first R2 await. Later commits
   * remain dirty for the next flush; a busy source cannot starve publication. */
  private captureQueue(source: SourceRow) {
    const views = this.sql
      .exec<{
        kind: string;
        id: string;
        data: string;
      }>("SELECT * FROM ops_views")
      .toArray();
    const cached = new Map(
      views.map((v) => [v.kind + ":" + v.id, JSON.parse(v.data)]),
    );
    const dirty = this.sql
      .exec<{
        kind: string;
        id: string;
        revision: number;
      }>("SELECT * FROM ops_dirty")
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
    const prs = Array.from({ length: 16 }, (_, group) => {
      const buckets = Array.from({ length: 4 }, (_, n) => group * 4 + n);
      if (
        buckets.every(
          (bucket) =>
            cached.get("pr:" + bucket)?.waiting_records &&
            cached.get("pr:" + bucket)?.lookup_group === 1 &&
            !dirty.some((d) => d.kind === "pr" && d.id === String(bucket)),
        )
      )
        return null;
      return buckets.map((bucket) => ({
        bucket,
        inputs: this.rows(source.id, "inputs", "AND pr_bucket=?", [bucket]),
        members: this.rows(source.id, "members", "AND pr_bucket=?", [bucket]),
      }));
    }).filter((p) => p !== null);
    return { source, cached, dirty, batches, changed, prs };
  }
  private async queueView(
    capture: ReturnType<Operations["captureQueue"]>,
    projection: Projection,
  ) {
    const object = (value: any) => this.object(value, projection),
      pages = (schema: string, records: any[], size = 100) =>
        this.pages(schema, records, size, projection);
    const { source, cached, batches } = capture,
      meta = JSON.parse(source.metadata!);
    const updates: { kind: string; id: string; data: any }[] = [];
    const save = (kind: string, id: string, data: any) => {
      cached.set(kind + ":" + id, data);
      updates.push({ kind, id, data });
    };
    const batchWork = this.parallel(
      capture.changed,
      async ({ batch, members, attempts, tasks, events }) => {
        const activeMembers = members.filter(
          (m) => !m.removed && m.batch_version === batch.version,
        );
        const [memberPages, attemptPages, taskPages, eventPages] =
          await this.settled([
            pages("tauceti.operations.members/v1", members),
            pages("tauceti.operations.attempts/v1", attempts, 20),
            pages("tauceti.operations.tasks/v1", tasks),
            pages("tauceti.operations.events/v1", events),
          ]);
        const detail = await object({
          schema: "tauceti.operations.batch/v1",
          source_id: source.id,
          batch,
          member_pages: memberPages,
          attempt_pages: attemptPages,
          task_pages: taskPages,
          event_pages: eventPages,
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
      },
    );
    const prWork = this.parallel(
      capture.prs,
      async (buckets) => {
        const prepared = buckets.map(({ bucket, inputs, members }) => {
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
          return {
            bucket,
            prs,
            waiting: inputs.filter((i) =>
              ["waiting", "reserved"].includes(i.state),
            ),
          };
        });
        // The existing reader selects a PR by key, not by object ownership.
        // Share one immutable object across up to four lookup slots, preserving
        // all member/version fields. Split large groups before upload so a
        // cold lookup never becomes larger than an existing individual bucket
        // or this 256 KiB grouping budget. Capture includes unchanged neighbors
        // too: a dirty bucket cannot erase another bucket's records.
        const packs: { buckets: typeof prepared; value: any }[] = [];
        const pack = (buckets: typeof prepared) => {
          const value = {
            schema: "tauceti.operations.prs/v1",
            prs: Object.assign({}, ...buckets.map((b) => b.prs)),
          };
          if (
            buckets.length > 1 &&
            new TextEncoder().encode(JSON.stringify(value)).length > 262144
          ) {
            const middle = Math.ceil(buckets.length / 2);
            pack(buckets.slice(0, middle));
            pack(buckets.slice(middle));
          } else packs.push({ buckets, value });
        };
        pack(prepared);
        await this.parallel(packs, async ({ buckets, value }) => {
          const ref = await object(value);
          for (const { bucket, waiting } of buckets)
            save("pr", String(bucket), {
              ref,
              lookup_group: 1,
              waiting: waiting.length,
              waiting_records: waiting,
            });
        });
      },
    );
    await this.settled([batchWork, prWork]);
    const shards: any[][] = Array.from({ length: 64 }, () => []);
    let memberCount = 0;
    for (const batch of batches) {
      const view = cached.get("batch:" + batch.id);
      memberCount += view.member_count;
      shards[Number(BigInt(batch.id) % 64n)].push(view);
    }
    const batchIndexWork = this.parallel(shards, (shard) =>
      object({
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
    const waiting = Array.from(
      { length: 64 },
      (_, i) => cached.get("pr:" + i).waiting_records,
    )
      .flat()
      .sort(
        (a, b) =>
          (a.approved_at || "").localeCompare(b.approved_at || "") ||
          a.id.localeCompare(b.id),
      );
    const [batchIndex, waitingPages] = await this.settled([
      batchIndexWork,
      pages("tauceti.operations.inputs/v1", waiting),
    ]);
    const ref = await object({
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
      waiting_pages: waitingPages,
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
  private async collectJobs(projection: Projection) {
    const eligible = `FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name
      WHERE p.pending=1 AND (f.due IS NULL OR f.due<=?)`;
    // Migration must not starve currently changing jobs. Reserve two of the
    // existing three source reads for recent/active partitions, oldest capture
    // first; the remaining lane always advances historical work.
    const hot = this.sql.exec<{name:string;revision:number}>(
      `SELECT p.name,p.revision ${eligible} AND (substr(p.name,-10)>=? OR
        EXISTS(SELECT 1 FROM ops_job_facts j WHERE j.partition=p.name AND j.active=1))
       ORDER BY COALESCE(json_extract(p.data,'$.computed_at'),''),p.received,p.name LIMIT 2`,
      now(),new Date((now()-86400)*1000).toISOString().slice(0,10)).toArray();
    const oldest = this.sql.exec<{name:string;revision:number}>(
      `SELECT p.name,p.revision ${eligible} ORDER BY p.received,p.name LIMIT 3`,now()).toArray();
    const pending = [...hot,...oldest.filter(p=>!hot.some(h=>h.name===p.name))].slice(0,3);
    await Promise.all(
      pending.map(async (p) => {
        try {
          const response = await this.env.FACTS.get(
            this.env.FACTS.idFromName(p.name),
          ).fetch("https://internal/operations", {
            method: "POST",
            // This is a bounded internal SQLite projection, not an upstream
            // discovery call. Cold reads may legitimately exceed three seconds.
            body: "{}",
          });
          if (!response.ok) throw new Error("Facts unavailable");
          const value: any = await response.json();
          if (
            value.schema !== "tauceti.operations.partition/v1" ||
            value.partition !== p.name
          )
            throw new Error("Facts version skew");
          if (
            projection.scope !== this.get("public_scope") ||
            projection.scope !== (await operationsScope(this.env)) ||
            !projection.repositories.includes(
              p.name.split("/").slice(0, 2).join("/"),
            ) ||
            !this.sql
              .exec("SELECT 1 FROM ops_partitions WHERE name=?", p.name)
              .toArray().length
          )
            return;
          this.ctx.storage.transactionSync(() => {
            const previous = new Set(
              this.sql
                .exec<{ id: string }>(
                  "SELECT id FROM ops_job_facts WHERE partition=?",
                  p.name,
                )
                .toArray()
                .map((row) => row.id),
            );
            for (const job of value.jobs) {
              previous.delete(job.id);
              this.sql.exec(
                `INSERT INTO ops_job_facts VALUES (?,?,?,?,?,?)
                 ON CONFLICT(partition,id) DO UPDATE SET repository=excluded.repository,
                   active=excluded.active,observed=excluded.observed,data=excluded.data
                 WHERE ops_job_facts.data!=excluded.data`,
                p.name,
                job.id,
                job.repository,
                job.active ? 1 : 0,
                Date.parse(job.observed_at) / 1000 || 0,
                JSON.stringify(job),
              );
            }
            for (const id of previous.keys())
              this.sql.exec(
                "DELETE FROM ops_job_facts WHERE partition=? AND id=?",
                p.name,
                id,
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
        } catch (error) {
          console.warn("operations Facts projection failed", p.name,
            error instanceof Error ? error.name + ": " + error.message.slice(0, 300) : "unknown error");
          if (
            !(await this.eligible(projection)) ||
            !this.sql
              .exec("SELECT 1 FROM ops_partitions WHERE name=?", p.name)
              .toArray().length
          )
            return;
          const failures =
            (this.sql
              .exec<{
                failures: number;
              }>(
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
          .exec<{
            machine: string;
            instance: string;
            slot: number;
          }>(
            "SELECT machine,instance,slot FROM ops_runner_hosts WHERE runner=?",
            slot.runner_id,
          )
          .toArray()[0];
        if (
          old &&
          (old.machine !== host.machine_id || old.instance !== slot.instance_id)
        )
          continue;
        if (!old || old.slot !== slot.slot)
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
    const pruned = this.sql.exec(
      "DELETE FROM ops_runner_hosts WHERE seen<?",
      now() - 30 * 86400,
    );
    if (pruned.rowsWritten)
      this.set("jobs_revision", (this.get("jobs_revision") || 0) + 1);
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
        .exec<{
          data: string;
        }>(
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
  private async hostHistory(projection: Projection) {
    const object = (value: any) => this.object(value, projection),
      pages = (schema: string, records: any[], size = 100) =>
        this.pages(schema, records, size, projection);
    // Capture the bounded five-minute history synchronously. Live host frames
    // can continue while immutable pages are uploaded.
    const registry = publicHosts(projection.hosts);
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
        pages: await pages(
          "tauceti.operations.host-history-page/v1",
          row.records,
          500,
        ),
      });
    const ref = await object({
      schema: "tauceti.operations.host-history/v1",
      observed_at: new Date(now() * 1000).toISOString(),
      resolution_seconds: 300,
      window_days: 7,
      hosts,
      coverage: "recorded_samples_only",
      before_activation: "unknown",
    });
    return ref;
  }
  private async hostsView(projection: Projection) {
    const object = (value: any) => this.object(value, projection),
      pages = (schema: string, records: any[], size = 100) =>
        this.pages(schema, records, size, projection);
    const frame = this.get("hosts"),
      registry = publicHosts(projection.hosts);
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
          .exec<{
            data: string;
          }>("SELECT data FROM ops_hosts WHERE machine=?", h.machine_id)
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
      ...(await object(value)),
      coverage: value.coverage,
      observed_at: value.observed_at,
      received_at: value.received_at,
    };
  }
  private async jobsView(projection: Projection) {
    const object = (value: any) => this.object(value, projection),
      pages = (schema: string, records: any[], size = 100) =>
        this.pages(schema, records, size, projection);
    const partitions = this.sql
      .exec<{
        name: string;
        data: string | null;
        pending: number;
      }>(
        "SELECT name,data,pending FROM ops_partitions ORDER BY received DESC LIMIT 201",
      )
      .toArray();
    const facts = partitions
      .filter((p) =>
        projection.repositories.includes(
          p.name.split("/").slice(0, 2).join("/"),
        ),
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
      registry = publicHosts(projection.hosts);
    } catch {
      /* Host configuration is a separate domain. */
    }
    const queueAttempts = this.sql
      .exec<{ data: string }>(
        "SELECT data FROM ops_entities WHERE collection='attempts' AND json_extract(data,'$.run_id') IS NOT NULL",
      )
      .toArray()
      .map((r) => JSON.parse(r.data));
    const publicRepositories = projection.repositories;
    // Select each bounded cohort using its index before merging. A burst of
    // skipped control jobs cannot consume the entire completed-build window.
    const cohort = (condition: string, limit: number) =>
      publicRepositories.length
        ? this.sql
            .exec<{ data: string }>(
              `SELECT data FROM ops_job_facts WHERE repository IN (${publicRepositories.map(() => "?").join(",")}) AND ${condition} ORDER BY observed DESC,id ASC LIMIT ${limit}`,
              ...publicRepositories,
            )
            .toArray()
            .map((r) => JSON.parse(r.data))
        : [];
    const rawJobs = [
      ...cohort("active=1", 2001),
      ...cohort(`active=0 AND (${BUILD_CLASS_SQL})=1`, 151),
      ...cohort(`active=0 AND (${BUILD_CLASS_SQL})=0`, 51),
    ];
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
      completedBuilds = jobs.filter(
        (j) => !j.active && BUILD_CLASSES.includes(j.job_class),
      ),
      completedOther = jobs.filter(
        (j) => !j.active && !BUILD_CLASSES.includes(j.job_class),
      ),
      terminal = [
        ...completedBuilds.slice(0, 150),
        ...completedOther.slice(0, 50),
      ].sort((a, b) =>
        (b.observed_at || "").localeCompare(a.observed_at || ""),
      );
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
      scope: { repositories: projection.repositories },
      truncated:
        partitions.length > 200 ||
        completedBuilds.length > 150 ||
        completedOther.length > 50 ||
        facts.some((f) => f.truncated) ||
        active.length > 2000,
      active_truncated: active.length > 2000,
      source_truncated:
        partitions.length > 200 || facts.some((f) => f.truncated),
      recent_completed: {
        builds: {
          limit: 150,
          returned: Math.min(150, completedBuilds.length),
          truncated: completedBuilds.length > 150,
        },
        other: {
          limit: 50,
          returned: Math.min(50, completedOther.length),
          truncated: completedOther.length > 50,
        },
      },
      pool_activity: (() => {
        // Activity has no job-list or 200-partition display limit. Cached
        // metadata contains compact bins/references, never the full job list.
        const sources = this.sql.exec<{name:string;data:string|null;pending:number}>(
          "SELECT name,data,pending FROM ops_partitions").toArray()
          .filter(p=>projection.repositories.includes(p.name.split("/").slice(0,2).join("/")));
        return mergeActivity(sources.flatMap(p=>p.data?[JSON.parse(p.data)]:[]),now(),
          sources.some(p=>p.pending || !p.data));
      })(),
      backlog: (() => {
        const sources = this.sql.exec<{name:string;data:string|null;pending:number}>(
          "SELECT name,data,pending FROM ops_partitions").toArray()
          .filter(p=>projection.repositories.includes(p.name.split('/').slice(0,2).join('/')));
        return mergeBacklogs(sources.flatMap(p=>p.data?[JSON.parse(p.data)]:[]),
          sources.some(p=>p.pending || !p.data));
      })(),
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
      job_pages: await pages("tauceti.operations.job-page/v1", [
        ...active.slice(0, 2000),
        ...terminal,
      ]),
      workflow_pages: await pages(
        "tauceti.operations.workflow-page/v1",
        facts.flatMap((p) => p.pending_workflows).slice(0, 1000),
      ),
      partition_errors: partitions.filter((p) => p.pending).map((p) => p.name),
      github_budget: context?.github_budget || null,
    };
    return {
      ...(await object(value)),
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
        .exec<{
          source: string;
          collection: string;
          id: string;
          data: string;
        }>(
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
        .exec<{
          repository: string;
        }>("SELECT DISTINCT repository FROM ops_progress")
        .toArray())
        if (!publicSet.has(row.repository))
          this.sql.exec(
            "DELETE FROM ops_progress WHERE repository=?",
            row.repository,
          );
      const history = this.get("history");
      if (history?.scope.repositories.some((r: string) => !publicSet.has(r)))
        this.set("history", null);
      this.sql.exec(
        "DELETE FROM ops_meta WHERE key LIKE 'domain_cache/%' OR key LIKE 'domain_ready/%' OR key LIKE 'domain_target/%' OR key LIKE 'domain_error/%'",
      );
      this.sql.exec("DELETE FROM ops_views");
      this.set("host_history", null);
      this.set("public_scope", scope);
      if (previousScope !== undefined) {
        this.set("queue_resync", true);
        this.set("revision", (this.get("revision") || 0) + 1);
      }
    });
  }
  private projection(lane: string): Projection {
    return {
      lane,
      scope: this.get("public_scope"),
      repositories: this.repositories(),
      hosts: this.env.OPERATIONS_PUBLIC_HOSTS || "[]",
    };
  }
  private targets(): Record<Domain, string> {
    const source = this.source(this.get("queue_source") || "");
    return {
      queue: source?.snapshot ? "3/" + source.snapshot : "unavailable",
      jobs: "5/" + (this.get("jobs_revision") || 0) + "/" + (this.sql.exec("SELECT 1 FROM ops_partitions LIMIT 1").toArray().length ? Math.floor(now() / 60) : 0),
      hosts: canonical({
        bridge: this.get("hosts")?.source || null,
        pushed: this.sql
          .exec("SELECT machine,epoch,sequence FROM ops_hosts ORDER BY machine")
          .toArray(),
      }),
      host_history: String(Math.floor(now() / 300)),
      history: this.get("history")
        ? canonical([
            this.get("history").observed_at,
            this.get("history_revision") || 0,
          ])
        : "unavailable",
    };
  }
  private state(kind: Domain, ready = true): Ready | undefined {
    const value =
      (ready && this.get("domain_ready/" + kind)) ||
      this.get("domain_cache/" + kind);
    return value?.scope === this.get("public_scope") ? value : undefined;
  }
  private queuePreparations(): QueuePreparation[] {
    const saved = this.get("queue_preparation"),
      scope = this.get("public_scope");
    // Accept the previous single-summary representation during a live update.
    return (Array.isArray(saved) ? saved : saved ? [saved] : [])
      .filter((row: QueuePreparation) => row.scope === scope)
      .slice(-8);
  }
  private changedReady(): boolean {
    return DOMAINS.some((kind) => {
      const ready = this.state(kind),
        committed = this.state(kind, false);
      return (
        ready &&
        canonical([ready.version, ready.value]) !==
          canonical([committed?.version, committed?.value])
      );
    });
  }
  private async eligible(projection: Projection) {
    return (
      projection.scope === this.get("public_scope") &&
      projection.scope === (await operationsScope(this.env))
    );
  }
  /** An alarm is a short scheduling pump. Slow preparations and puts retain
   * their own lifetime promises, rather than retaining the alarm handler. */
  private rearm() {
    return this.alarmUpdate(() => this.performRearm());
  }
  private async performRearm() {
    const targets = this.targets(),
      clock = Date.now(),
      times: number[] = [];
    for (const kind of DOMAINS) {
      const state = this.state(kind),
        failure = this.get("domain_error/" + kind);
      if (
        !this.preparing.has(kind) &&
        (state?.version !== targets[kind] || failure)
      )
        times.push(
          Math.max(
            clock + 1000,
            (this.get("domain_started/" + kind) || 0) + 10000,
            (failure?.due || 0) * 1000,
          ),
        );
    }
    if (!this.preparing.has("facts")) {
      const due = this.sql
        .exec<{
          due: number | null;
        }>(
          "SELECT MIN(COALESCE(f.due,?)) due FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name WHERE p.pending=1",
          now(),
        )
        .one().due;
      if (due !== null)
        times.push(
          Math.max(
            clock + 1000,
            due * 1000,
            (this.get("domain_started/facts") || 0) + 10000,
          ),
        );
    }
    if (
      !this.pointer &&
      (this.changedReady() ||
        (this.get("manifest_heartbeat") || 0) > (this.get("published") || 0))
    )
      times.push(
        Math.max(
          clock + 1000,
          (this.pointerStarted || (this.get("published") || 0) * 1000) + 10000,
          (this.get("publication_retry") || 0) * 1000,
        ),
      );
    // Durable retry survives an isolate restart, including loss of in-memory workers.
    if (this.preparing.size || this.pointer) times.push(clock + 30000);
    if (times.length) {
      const next = Math.min(...times),
        old = await this.ctx.storage.getAlarm();
      if (old === null || old > next) await this.ctx.storage.setAlarm(next);
    } else await this.ctx.storage.deleteAlarm();
  }
  private start(
    kind: string,
    version: string,
    projection: Projection,
    work: () => Promise<void>,
  ) {
    if (this.preparing.has(kind)) return;
    const entry: Preparation = {
      scope: projection.scope,
      version,
      started: Date.now(),
      promise: Promise.resolve(),
    };
    this.preparing.set(kind, entry);
    this.publicationPending.add(kind);
    entry.promise = Promise.resolve()
      .then(work)
      .catch(() => {
        console.warn("operations: background preparation unavailable");
      })
      .finally(async () => {
        if (this.preparing.get(kind) === entry) {
          this.preparing.delete(kind);
          this.publicationPending.delete(kind);
        }
        await this.rearm();
        this.nudge();
      });
    this.ctx.waitUntil(entry.promise);
  }
  private async prepare(kind: Domain, projection: Projection) {
    const startedAt = Date.now();
    const version = this.targets()[kind],
      revision = this.get("revision") || 0;
    this.set("domain_target/" + kind, {
      scope: projection.scope,
      version,
      revision,
    });
    // All SQL/input captures occur before their projection's first network await.
    const source = this.source(this.get("queue_source") || ""),
      capture =
        kind === "queue" && source?.metadata && projection.repositories.length
          ? this.captureQueue(source)
          : null;
    if (capture)
      projection.timing = {
        scope: projection.scope,
        version,
        source_epoch: capture.source.committed_epoch,
        source_sequence: capture.source.committed_sequence,
        observed_at: JSON.parse(capture.source.metadata!).observed_at,
        started_at: startedAt,
        capture_finished_at: Date.now(),
        graph_finished_at: null,
        ready_recorded_at: null,
        object_requests: 0,
        cache_hits: 0,
        shared_hits: 0,
        put_attempts: 0,
        puts: 0,
        bytes_written: 0,
        permit_wait_ms: 0,
        put_ms: 0,
        max_put_ms: 0,
      };
    try {
      let value: any,
        updates: { kind: string; id: string; data: any }[] = [];
      if (kind === "queue") {
        if (!capture)
          value = {
            url: null,
            coverage: "unavailable",
            observed_at: null,
            received_at: null,
          };
        else {
          const result = await this.queueView(capture, projection),
            metadata = JSON.parse(capture.source.metadata!);
          projection.timing!.graph_finished_at = Date.now();
          updates = result.updates;
          value = {
            ...result.ref,
            observed_at: metadata.observed_at,
            received_at: new Date(capture.source.received * 1000).toISOString(),
            coverage: metadata.coverage,
            source_revision: metadata.source_revision,
            truncated: metadata.truncated,
          };
        }
      } else if (kind === "jobs") value = await this.jobsView(projection);
      else if (kind === "hosts") value = await this.hostsView(projection);
      else if (kind === "host_history")
        value = {
          ...(await this.hostHistory(projection)),
          coverage: "recorded_samples_only",
        };
      else {
        const history = this.get("history"),
          received = this.get("history_received");
        value = history
          ? {
              ...(await this.object(history, projection)),
              observed_at: history.observed_at,
              received_at: received,
              coverage: history.coverage,
            }
          : {
              url: null,
              coverage: "unavailable",
              observed_at: null,
              received_at: null,
            };
      }
      if (!(await this.eligible(projection))) return;
      if (
        capture &&
        (this.get("queue_source") !== capture.source.id ||
          this.source(capture.source.id)?.committed_epoch !==
            capture.source.committed_epoch)
      )
        return;
      this.ctx.storage.transactionSync(() => {
        for (const v of updates)
          this.sql.exec(
            "INSERT OR REPLACE INTO ops_views VALUES (?,?,?)",
            v.kind,
            v.id,
            JSON.stringify(v.data),
          );
        if (capture) {
          const ids = new Set(capture.batches.map((b) => b.id));
          for (const row of this.sql
            .exec<{ id: string }>("SELECT id FROM ops_views WHERE kind='batch'")
            .toArray())
            if (!ids.has(row.id))
              this.sql.exec(
                "DELETE FROM ops_views WHERE kind='batch' AND id=?",
                row.id,
              );
          for (const d of capture.dirty)
            this.sql.exec(
              "DELETE FROM ops_dirty WHERE kind=? AND id=? AND revision=?",
              d.kind,
              d.id,
              d.revision,
            );
        }
        this.set("domain_ready/" + kind, {
          scope: projection.scope,
          version,
          value,
          revision,
        });
        this.set("domain_error/" + kind, null);
        if (projection.timing) {
          projection.timing.ready_recorded_at = Date.now();
          this.set(
            "queue_preparation",
            [...this.queuePreparations(), projection.timing].slice(-8),
          );
        }
      });
    } catch {
      if (!(await this.eligible(projection))) return;
      const previous = this.state(kind),
        failed = this.get("domain_error/" + kind),
        failures = (failed?.failures || 0) + 1;
      this.set("domain_error/" + kind, {
        failures,
        due: now() + Math.min(300, 10 * 2 ** Math.min(5, failures - 1)),
        error: "projection_unavailable",
      });
      this.set("domain_ready/" + kind, {
        scope: projection.scope,
        version,
        revision,
        value: {
          ...(previous?.value || {
            url: null,
            coverage: "unavailable",
            observed_at: null,
            received_at: null,
          }),
          error: "projection_unavailable",
        },
      });
      console.warn("operations: " + kind + " projection unavailable");
    }
  }
  private startPointer() {
    if (
      this.pointer ||
      (this.get("publication_retry") || 0) > now() ||
      Date.now() <
        (this.pointerStarted || (this.get("published") || 0) * 1000) + 10000 ||
      (!this.changedReady() &&
        (this.get("manifest_heartbeat") || 0) <= (this.get("published") || 0))
    )
      return;
    const projection = this.projection("manifest"),
      captured = Object.fromEntries(
        DOMAINS.map((kind) => [kind, this.state(kind)]),
      ) as Record<Domain, Ready | undefined>;
    const domains = Object.fromEntries(
      DOMAINS.map((kind) => [
        kind,
        captured[kind]?.value || {
          url: null,
          coverage: "unavailable",
          observed_at: null,
          received_at: null,
        },
      ]),
    );
    const clock = now();
    this.pointerStarted = Date.now();
    this.publicationPending.add("manifest");
    this.pointer = (async () => {
      try {
        if (!(await this.eligible(projection))) return;
        await this.upload("manifest", () =>
          this.env.ARCHIVE.put(
            "public/v1/operations/manifest.json",
            JSON.stringify({
              schema: "tauceti.operations.manifest/v1",
              scope: projection.scope,
              generated_at: new Date(clock * 1000).toISOString(),
              domains,
            }),
            {
              httpMetadata: { contentType: "application/json" },
              customMetadata: { scope: projection.scope },
            },
          ),
        );
        if (!(await this.eligible(projection))) return;
        this.ctx.storage.transactionSync(() => {
          for (const kind of DOMAINS)
            if (captured[kind])
              this.set("domain_cache/" + kind, captured[kind]);
          this.set("published", clock);
          const targets = this.targets();
          if (
            DOMAINS.every(
              (kind) =>
                captured[kind]?.version === targets[kind] &&
                !this.get("domain_error/" + kind),
            ) &&
            !this.sql
              .exec("SELECT 1 FROM ops_partitions WHERE pending=1 LIMIT 1")
              .toArray().length
          )
            this.set("published_revision", this.get("revision") || 0);
          this.set("publication_elapsed_ms", Date.now() - this.pointerStarted!);
          this.set("publication_error", false);
          this.set("publication_retry", null);
          this.set("publication_failures", 0);
        });
        this.startMaintenance();
      } catch {
        const failures = (this.get("publication_failures") || 0) + 1;
        this.set("publication_failures", failures);
        this.set("publication_error", true);
        this.set(
          "publication_retry",
          now() + Math.min(300, 10 * 2 ** Math.min(5, failures - 1)),
        );
        console.warn("operations: public pointer unavailable");
      } finally {
        this.pointer = undefined;
        this.publicationPending.delete("manifest");
        await this.rearm();
        this.nudge();
      }
    })();
    this.ctx.waitUntil(this.pointer);
  }
  private startMaintenance() {
    if (this.maintenance || (this.get("maintenance_at") || 0) > now() - 60)
      return;
    this.set("maintenance_at", now());
    this.maintenance = (async () => {
      const clock = now(),
        roots = DOMAINS.flatMap((kind) => [
          this.state(kind),
          this.state(kind, false),
        ])
          .filter((s) => s?.value.url)
          .map((s) => s!.value.url.slice(PREFIX.length, -5));
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
      for (const row of this.sql
        .exec<{ hash: string; referenced: number; logical: string | null }>(
          `SELECT o.hash,o.referenced,k.logical FROM ops_objects o LEFT JOIN ops_object_keys k ON k.hash=o.hash WHERE o.referenced<?
         UNION ALL SELECT k.hash,k.allocated referenced,k.logical FROM ops_object_keys k WHERE k.allocated<?
         AND NOT EXISTS(SELECT 1 FROM ops_objects o WHERE o.hash=k.hash) LIMIT 20`,
          clock - 7 * 86400,
          clock - 7 * 86400,
        )
        .toArray()) {
        // A preceding R2 await can make a selected candidate reachable again.
        // An active logical write also protects its pending physical allocation.
        const receipt = this.sql
          .exec<{
            referenced: number;
          }>("SELECT referenced FROM ops_objects WHERE hash=?", row.hash)
          .toArray()[0];
        const allocation = this.sql
          .exec<{
            logical: string;
            allocated: number;
          }>(
            "SELECT logical,allocated FROM ops_object_keys WHERE hash=?",
            row.hash,
          )
          .toArray()[0];
        if (
          (receipt ? receipt.referenced : allocation?.allocated) !==
            row.referenced ||
          this.objectWrites.has(allocation?.logical || row.hash)
        )
          continue;
        // Retirement is durable and atomic. A reconstruction gets a DIFFERENT
        // physical key even if this old delete finishes after an isolate restart.
        this.ctx.storage.transactionSync(() => {
          this.sql.exec("DELETE FROM ops_object_keys WHERE hash=?", row.hash);
          this.sql.exec("DELETE FROM ops_objects WHERE hash=?", row.hash);
          this.sql.exec("DELETE FROM ops_links WHERE parent=?", row.hash);
        });
        await this.upload("maintenance", () =>
          this.env.ARCHIVE.delete("public" + PREFIX + row.hash + ".json"),
        );
      }
      this.sql.exec(
        "DELETE FROM ops_changes WHERE snapshot IN (SELECT snapshot FROM ops_frames WHERE received<?)",
        clock - 3600,
      );
      this.sql.exec("DELETE FROM ops_frames WHERE received<?", clock - 3600);
    })()
      .catch(() => console.warn("operations: object maintenance unavailable"))
      .finally(() => {
        this.maintenance = undefined;
      });
    this.ctx.waitUntil(this.maintenance);
  }
  private async performPublish(allowBootstrap: boolean) {
    await this.syncScope();
    const targets = this.targets(),
      clock = Date.now();
    if (
      !this.preparing.has("facts") &&
      clock >= (this.get("domain_started/facts") || 0) + 10000 &&
      this.sql
        .exec(
          "SELECT 1 FROM ops_partitions p LEFT JOIN ops_failures f ON f.kind='partition' AND f.id=p.name WHERE p.pending=1 AND (f.due IS NULL OR f.due<=?) LIMIT 1",
          now(),
        )
        .toArray().length
    ) {
      const projection = this.projection("facts");
      this.set("domain_started/facts", clock);
      this.start(
        "facts",
        String(this.get("jobs_revision") || 0),
        projection,
        () => this.collectJobs(projection),
      );
    }
    for (const kind of DOMAINS) {
      const target = {
        scope: this.get("public_scope"),
        version: targets[kind],
        revision: this.get("revision") || 0,
      };
      if (
        this.get("domain_target/" + kind)?.version !== target.version ||
        this.get("domain_target/" + kind)?.scope !== target.scope
      )
        this.set("domain_target/" + kind, target);
      const failure = this.get("domain_error/" + kind);
      if (
        this.preparing.has(kind) ||
        (this.state(kind)?.version === targets[kind] && !failure) ||
        (failure?.due || 0) > now() ||
        clock < (this.get("domain_started/" + kind) || 0) + 10000
      )
        continue;
      this.publicationStarted = clock;
      this.set("domain_started/" + kind, clock);
      const projection = this.projection(kind);
      this.start(kind, targets[kind], projection, async () => {
        if (kind === "jobs") await this.preparing.get("facts")?.promise;
        await this.prepare(kind, projection);
      });
    }
    // An event can arrive before a cadence-deferred cold queue has even
    // started preparing. Empty in-memory workers do not establish readiness.
    const warm = DOMAINS.some((kind) => !!this.state(kind, false));
    const queueReady = this.state("queue")?.version === this.targets().queue;
    if (allowBootstrap || warm || queueReady) this.startPointer();
    await this.rearm();
  }
  async publish(allowBootstrap = false) {
    this.publicationRequested = true;
    this.bootstrapRequested ||= allowBootstrap;
    if (this.publishing) return this.publishing;
    this.publishing = (async () => {
      do {
        const bootstrap = this.bootstrapRequested;
        this.publicationRequested = false;
        this.bootstrapRequested = false;
        await this.performPublish(bootstrap);
      } while (this.publicationRequested);
    })();
    try {
      await this.publishing;
    } finally {
      this.publishing = undefined;
      // Preserve a kick delivered after the loop's last check but before this
      // continuation. There is only one pending bit, never a work queue.
      if (this.publicationRequested) this.nudge();
    }
  }
  async alarm() {
    await this.publish(true);
  }

  async fetch(request: Request) {
    const path = new URL(request.url).pathname;
    if (path === "/backlog") {
      const repo = "TauCetiProject/TauCeti";
      const source = this.source(this.get("queue_source") || "");
      const metadata = source?.metadata ? JSON.parse(source.metadata) : null;
      if (!this.repositories().includes(repo) ||
          !metadata?.scope.repositories.includes(repo) ||
          metadata.service.mode !== "production") return json({ observation: null });
      // Read only the last atomically committed generation. Waiting inputs and
      // current members overlap during insertion and a PR can have older heads.
      const count = this.sql.exec<{ n: number }>(`
        SELECT COUNT(*) n FROM (
          SELECT json_extract(data,'$.pr') pr FROM ops_entities
          WHERE source=? AND collection='inputs'
            AND json_extract(data,'$.repository')=?
            AND json_extract(data,'$.state') IN ('waiting','reserved')
          UNION
          SELECT json_extract(m.data,'$.pr') pr FROM ops_entities m
          JOIN ops_entities b ON b.source=m.source AND b.collection='batches' AND b.id=m.batch
          WHERE m.source=? AND m.collection='members'
            AND json_extract(m.data,'$.repository')=?
            AND json_extract(b.data,'$.state') IN ('head','open')
            AND json_extract(m.data,'$.batch_version')=json_extract(b.data,'$.version')
            AND json_extract(m.data,'$.removed')=0
            AND json_extract(m.data,'$.withdrawn')=0
        )`, source!.id, repo, source!.id, repo).one().n;
      const complete = metadata.coverage === "observed" && !metadata.truncated &&
        ["inputs", "members", "batches"].every((key) => metadata.collection_complete[key]);
      return json({ observation: {
        observed_at: metadata.observed_at,
        eligible_count: count,
        candidate_scan_complete: complete,
        candidate_snapshot_at: complete ? metadata.observed_at : null,
      } });
    }
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
      if (canonical(old) !== canonical(value))
        this.set("history_revision", (this.get("history_revision") || 0) + 1);
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
          previous?.hosts.find(
            (old: any) => old.machine_id === host.machine_id,
          ),
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
    if (path === "/health") {
      const queuePreparations = this.queuePreparations();
      return json({
        publisher_version: this.env.CF_VERSION?.id || null,
        alarm_at: await this.ctx.storage.getAlarm(),
        publication_in_flight:
          !!this.publishing || !!this.pointer || this.preparing.size > 0,
        domain_publication: Object.fromEntries(
          DOMAINS.map((kind) => [
            kind,
            {
              target: this.get("domain_target/" + kind)?.version || null,
              prepared: this.state(kind)?.version || null,
              committed: this.state(kind, false)?.version || null,
              started_at: this.preparing.get(kind)?.started || null,
              retry_at: this.get("domain_error/" + kind)?.due || null,
            },
          ]),
        ),
        queue_preparation: queuePreparations.at(-1) || null,
        queue_preparations: queuePreparations,
        publication_started_at: this.publicationStarted || null,
        publication_pending: [...this.publicationPending],
        publication_elapsed_ms: this.get("publication_elapsed_ms") ?? null,
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
    }
    return json({ error: "Not found" }, 404);
  }
}
