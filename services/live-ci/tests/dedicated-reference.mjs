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
