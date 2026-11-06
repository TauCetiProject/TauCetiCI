# Queue and runner operations

The existing `/ci` website reads `/v1/operations/manifest.json` and immutable JSON
objects in the existing live-CI R2 bucket. Object URLs include the current public
scope hash before their content hash; scope withdrawal is checked before edge
cache reads. Previously downloaded data cannot be retracted. Browser reads never enter the
Operations Durable Object, call GitHub, open SSH connections or trigger discovery.
The chart manifest under `/v1/ci/` keeps its existing contract.

Queue facts remain authoritative for admission, candidate versions, verdict
bindings and landings. Job facts add exact run/attempt/job/runner identities. A
run-reported candidate is labelled unverified until its run and attempt match the
queue's retained attempt. A repository-dispatch workflow head is never used as
its tested candidate. Telemetry cannot change statuses, admission or promotion.

## Activation

Deploy this reader before starting any producer. The new SQLite Durable Object
binding/migration is in `wrangler.json`; ordinary `wrangler deploy` uses empty
public repository/host scopes. Explicit scope is mandatory.

The existing private setup directory is `~/.config/tauceti-live-ci` (see
`scripts/setup.py` for the authoritative location). Configure:

- `operations-repositories.json`: approved public repository names, selected
  from the installation's existing allowed repositories.
- `operations-hosts.json`: approved `{machine_id, alias}` rows, with full
  permanent machine identities. This is an allowlist, not a private inventory.
- `queue-monitor-token`: private, telemetry-only queue/history ingress token.
- `host-monitor-token`: distinct private host-observation ingress token. Give
  this to the trusted bridge or enrollment broker, never to a candidate build.

`scripts/setup.py` carries these explicit files into deployment variables and
secrets. Keep private files mode 0600. No admission, runner-count or drain setting
is part of activation. Existing deployments without producer secrets reject
telemetry and continue normal CI/chart operation.

Queue/history POSTs use `/ingest/operations/{queue,history}` with the queue grant.
The bridge uses `/ingest/operations/hosts`; the enrollment broker relays one
machine at a time through `/ingest/operations/host` with the host grant. These
credentials cannot invoke admin, runner management or queue mutation endpoints.
`GET /ingest/operations/queue/state` needs the queue grant and returns its source
identity/fence for deliberate recovery of a lost publisher state file.

Trusted progress POSTs use `/ingest/operations/progress` and short-lived GitHub
OIDC tokens with audience `tauceti-operations-progress`. The main-branch trusted
`pr-build.yml` workflow must grant `id-token: write` to its build job. The signed
repository/run/attempt/check-run identity determines the join. Caller-supplied
job IDs cannot broaden the grant. The wrapper stays outside bwrap; candidate
phase/module records are untrusted counters and never verdicts. No continuously
retrieved logs or new GitHub collector are involved.

## Publication and recovery

Queue updates consist of UTF-8 frames at most 256 KB, a source UUID, restart epoch,
monotonic sequence and snapshot UUID. Frames may arrive out of order. Only a
complete validated snapshot commits, in one SQLite transaction. Duplicate receipts
cannot acknowledge changed data. A newer process/upload fences delayed old ones.
The receiver retains the previous committed/public generation after partial uploads.

Deltas identify their acknowledged base. A missing receiver base returns HTTP 409
with `code: resync`; the producer clears its delta cache and sends current retained
facts in full. Explicit tombstones remove deleted records when source coverage is
complete. Missing rows in a capped source do not imply deletion. An unrelated new
source UUID cannot silently replace an established queue. The producer's explicit
`--recover-state` option requires a new private state-file path and the same
configured authenticated receiver; it recovers the receiver's source/fence.

Local queue detail is bounded to 90 days / 500 terminal batches and 5,000 terminal
inputs, plus active work. Old member/attempt/task/event children retire with their
batch. Source/receiver byte and record limits are enforced before public pointer
advancement; partial source detail has explicit coverage flags. Whole-pilot totals
come from the independent journal report, not the retained live detail window.

The publisher synchronously captures changed queue details before R2 awaits,
reuses cached immutable references and publishes that coherent generation even if
new facts arrive. New facts remain dirty for the next flush. Objects are uploaded
before the manifest pointer; a failed upload leaves the previous pointer intact.
Each domain caches unchanged roots and preserves its last successful generation
with an explicit error if another domain fails. Job facts occupy individual SQLite
rows, keeping every row below the platform limit even for a large fleet. Alarms
coalesce at ten seconds, survive failures and stop when idle. The existing
minute cron provides a backstop. GitHub partition notifications originate in a
FactPartition alarm and only schedule Operations; they never re-enter Facts in
the same ingestion call chain. Telemetry failures do not hold the metric outbox.

The manifest caches for five seconds; browser visible-tab polling is fifteen
seconds and pauses when hidden. Queue/host collection normally uses ten to fifteen
seconds, targeting visibility within thirty seconds. Receipt, source observation,
publication, inventory confirmation and actual work progress are separate times.
Quiet webhooks do not establish observer failure, and a new publication does not
establish complete upstream inventory. Capacity discovery keeps its existing
owner/cadence and is never invoked by this publisher or a browser.

Host samples retain 15-second points for seven days, five-minute aggregates for
ninety days, and daily aggregates for three years. Aggregates carry sample counts and
per-metric coverage; stale host facts contribute unknown metrics, never false
idle/free-capacity zeros. Public host-history pages contain bounded five-minute
samples and refresh at most every five minutes. Before activation is unknown.

Immutable objects remain live while reachable from the current manifest. Only
unreferenced objects older than seven days retire. A crash during retirement can
leave an orphan object but cannot leave an existence receipt for a missing object.

## Validation

The operations tests exercise the actual Durable Object against SQLite, the real
Python exporter fixture, malformed/private fields, source fencing, partial uploads,
base loss, deleted attempts, R2 failure, continuous ingestion, stopped idle alarms,
1,200 members, 1,000 jobs, exact identity joins and public cache/ETag/CORS reads.
A browser-load fixture makes zero upstream calls. OIDC tests verify signed identity
and reject wrong repositories, workflows, visibility, audiences and job identities.
The website's real-browser tests consume a compressed prepared-object fixture.

Use the existing GitHub repair budget and optional-detail allowance. The Operations
lane has no GitHub App installation-token code. Physical cloud-runner utilization
and account-wide Namespace usage remain unknown without a separately authorized
provider source; requested profiles are labelled accordingly.
