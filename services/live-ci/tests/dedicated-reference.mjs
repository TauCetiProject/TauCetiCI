// Extend the legacy report's independent reference with the runner class it
// omitted. Use direct interval intersections and a queue sweep, not the model.
export function includeDedicatedFleet(fixture) {
  const jobs = fixture.events.flatMap((e) => e.jobs)
    .filter((j) => j.runner_kind === "self-hosted");
  const time = (s) => Date.parse(s) / 1000;
  const running = [], queued = [];
  for (let i = 0; i < 144; i++) {
    const lo = fixture.fleet.since + i * 600, hi = lo + 600;
    let seconds = 0, wait = fixture.fleet.wait_min.build[i];
    const events = [];
    for (const j of jobs) {
      const created = time(j.created_at), start = time(j.started_at), end = time(j.completed_at);
      seconds += Math.max(0, Math.min(hi, end) - Math.max(lo, start));
      const a = Math.max(lo, created), b = Math.min(hi, start);
      if (b > a) events.push([a, 1], [b, -1]);
      if (start <= hi && start > hi - 1800)
        wait = Math.max(wait, Math.max(0, start - created) / 60);
    }
    let depth = 0, peak = 0;
    for (const [, delta] of events.sort((a, b) => a[0] - b[0] || a[1] - b[1]))
      peak = Math.max(peak, depth += delta);
    running.push(seconds / 600);
    queued.push(peak);
    fixture.fleet.wait_min.build[i] = wait;
  }
  fixture.fleet.running["self-hosted"] = running;
  fixture.fleet.queued["self-hosted"] = queued;
  return fixture;
}

/**
 * The same fixture moved forward by whole days so its day is two days ago. Ingestion drops jobs
 * from partitions more than 30 days old, so replaying the archived day unshifted stops exercising
 * the projections once it ages past that window. Whole-day shifts keep every bin and total.
 */
export function atRecentDay(fixture, now = Date.now()) {
  const DAY = 86400;
  const target = Math.floor(now / 1000 / DAY) * DAY - 2 * DAY;
  const offset = target - Date.parse(fixture.day + "T00:00:00Z") / 1000;
  const iso = /^\d{4}-\d\d-\d\dT[\d:.]+Z$/;
  const shift = (value, key) => {
    if (Array.isArray(value)) return value.map((v) => shift(v, key));
    if (value && typeof value === "object")
      return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, shift(v, k)]));
    if (typeof value === "string" && iso.test(value))
      return new Date(Date.parse(value) + offset * 1000).toISOString().replace(".000Z", "Z");
    if (key === "start" && typeof value === "number") return value + offset;
    return value;
  };
  return {
    ...fixture,
    day: new Date(target * 1000).toISOString().slice(0, 10),
    events: shift(fixture.events),
    fleet: { ...fixture.fleet, since: fixture.fleet.since + offset },
  };
}
