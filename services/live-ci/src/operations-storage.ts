/** Read-only storage report for the Operations Durable Object.
 * Every statement here is a SELECT, so the report still works when the
 * database has reached its size limit and every write fails. */

// Cloudflare's per-object SQLite limit on Workers Paid.
export const DATABASE_LIMIT_BYTES = 10 * 1024 ** 3;

// Columns per table. Byte figures are payload lengths (TEXT as UTF-8), not
// page usage: indexes and B-tree overhead are excluded.
export const OPERATIONS_TABLES: Record<string, string[]> = {
  ops_meta: ["key", "value"],
  ops_sources: ["id", "snapshot", "metadata"],
  ops_frames: ["snapshot", "source", "data", "hash", "metadata", "base"],
  ops_changes: ["snapshot", "collection", "id", "batch", "data"],
  ops_entities: ["source", "collection", "id", "batch", "data"],
  ops_evicted: ["source", "collection", "id", "batch"],
  ops_objects: ["hash"],
  ops_object_keys: ["logical", "hash"],
  ops_links: ["parent", "child"],
  ops_dirty: ["kind", "id"],
  ops_views: ["kind", "id", "data"],
  ops_partitions: ["name", "data"],
  ops_progress: ["repository", "run", "check_run", "session", "data"],
  ops_hosts: ["machine", "source", "data"],
  ops_job_facts: ["partition", "id", "repository", "data"],
  ops_failures: ["kind", "id", "error"],
  ops_runner_hosts: ["runner", "machine", "instance"],
  ops_host_samples: ["machine", "data"],
  ops_activity: ["id", "pool", "result", "ref"],
  ops_activity_pools: ["pool"],
};

const SAMPLE_POINTS = 8,
  SAMPLE_ROWS = 25;

function rowBytes(columns: string[]) {
  // Integer and real columns are counted as eight bytes each by the caller.
  return columns
    .map((c) => `coalesce(length(CAST(${c} AS BLOB)),0)`)
    .join("+");
}

/** By default each table reports its rowid range and a small descriptive
 * sample. `exact` names tables to COUNT(*) (one index scan each) and estimate
 * payload from the sample; `bytes` names tables whose payload is summed over
 * every row (a full table scan). */
export function storageReport(
  sql: SqlStorage,
  exact: Set<string>,
  bytes: Set<string>,
) {
  const database = (() => {
    try {
      return sql.databaseSize;
    } catch {
      return null;
    }
  })();
  const tables: Record<string, any> = {};
  for (const [table, columns] of Object.entries(OPERATIONS_TABLES)) {
    const started = Date.now();
    try {
      // Separate MIN and MAX queries each use SQLite's single-seek optimisation.
      const lo = sql
          .exec<{ v: number | null }>(`SELECT min(rowid) v FROM ${table}`)
          .one().v,
        hi = sql
          .exec<{ v: number | null }>(`SELECT max(rowid) v FROM ${table}`)
          .one().v;
      const report: any = {
        rowid_min: lo,
        rowid_max: hi,
        // An upper bound on the row count: deletes and INSERT OR REPLACE leave gaps.
        rowid_span: lo === null ? 0 : hi! - lo + 1,
      };
      if (lo !== null) {
        // Descriptive only: windows after evenly spaced rowids, never overlapping.
        let rows = 0,
          total = 0,
          next = lo;
        for (let i = 0; i < SAMPLE_POINTS; i++) {
          const at = Math.max(
            next,
            lo + Math.floor(((hi! - lo) * i) / Math.max(1, SAMPLE_POINTS - 1)),
          );
          const sample = sql
            .exec<{ n: number; b: number | null; last: number | null }>(
              `SELECT count(*) n,sum(b) b,max(r) last FROM (SELECT rowid r,${rowBytes(columns)} b FROM ${table} WHERE rowid>=? ORDER BY rowid LIMIT ?)`,
              at,
              SAMPLE_ROWS,
            )
            .one();
          rows += sample.n;
          total += sample.b || 0;
          if (sample.last === null) break;
          next = sample.last + 1;
        }
        report.sampled_rows = rows;
        report.sampled_mean_bytes = rows ? Math.round(total / rows) : null;
      }
      if (exact.has(table) || bytes.has(table)) {
        const counted = sql
          .exec<{ n: number; b: number | null }>(
            bytes.has(table)
              ? `SELECT count(*) n,sum(${rowBytes(columns)}) b FROM ${table}`
              : `SELECT count(*) n,NULL b FROM ${table}`,
          )
          .one();
        report.rows = counted.n;
        if (bytes.has(table)) report.payload_bytes = counted.b || 0;
        else if (report.sampled_mean_bytes != null)
          report.estimated_payload_bytes = Math.round(
            counted.n * report.sampled_mean_bytes,
          );
      }
      report.elapsed_ms = Date.now() - started;
      tables[table] = report;
    } catch (error) {
      const message = error instanceof Error ? error.message : "";
      tables[table] = message.includes("no such table")
        ? { missing: true }
        : { error: message.slice(0, 200) || "unavailable" };
    }
  }
  let meta_prefixes = null;
  if (bytes.has("ops_meta"))
    try {
      meta_prefixes = sql
        .exec<{ prefix: string; n: number; b: number }>(
          `SELECT CASE WHEN instr(key,'/')>0 THEN substr(key,1,instr(key,'/')) ELSE key END prefix,
            count(*) n,sum(length(CAST(value AS BLOB))) b
          FROM ops_meta GROUP BY prefix ORDER BY b DESC LIMIT 20`,
        )
        .toArray();
    } catch {
      meta_prefixes = null;
    }
  return {
    database_bytes: database,
    limit_bytes: DATABASE_LIMIT_BYTES,
    used_fraction:
      database === null ? null : Math.round((database / DATABASE_LIMIT_BYTES) * 1e4) / 1e4,
    tables,
    meta_prefixes,
  };
}
