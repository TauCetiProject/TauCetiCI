import { DAY, day, iso } from "./model";
const colors = [
  "#5eead4",
  "#ff9d4d",
  "#60a5fa",
  "#fb7185",
  "#c4b5fd",
  "#86efac",
  "#facc15",
  "#94a3b8",
];
const escape = (s: any) =>
  String(s).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ]!,
  );
type Series = {
  name: string;
  values: (number | null)[];
  color?: string;
  dashed?: boolean;
};
const frame = (title: string, note: string, body: string, height = 440) =>
  `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 980 ${height}" role="img" aria-label="${escape(title)}"><style>text{font:12px system-ui;fill:#e2e8f0}.muted{fill:#94a3b8}.grid{stroke:#334155;stroke-width:1}.title{font-size:21px;font-weight:600}</style><rect width="980" height="${height}" fill="#111827" rx="12"/><text x="64" y="34" class="title">${escape(title)}</text><text x="64" y="58" class="muted">${escape(note)}</text>${body}</svg>`;
function panel(
  series: Series[],
  labels: string[],
  options: {
    top?: number;
    bottom?: number;
    stack?: boolean;
    max?: number;
    percent?: boolean;
    times?: number[];
    since?: number;
    until?: number;
  } = {},
): string {
  const top = options.top ?? 140,
    bottom = options.bottom ?? 380,
    left = 64,
    width = 864,
    n = Math.max(1, labels.length);
  const sums = labels.map((_, i) =>
    series.reduce((s, x) => s + (x.values[i] || 0), 0),
  );
  const max =
    options.max ??
    Math.max(
      1,
      ...(options.stack
        ? sums
        : series.flatMap((s) => s.values.map((v) => v || 0))),
    ) * 1.1;
  const y = (v: number) => bottom - ((bottom - top) * v) / max,
    x = (i: number) =>
      options.times
        ? left +
          (width * (options.times[i] - options.since!)) /
            (options.until! - options.since!)
        : left + ((i + 0.5) * width) / n;
  let out = "";
  for (let i = 0; i <= 4; i++) {
    const v = (max * i) / 4;
    out += `<path class="grid" d="M64 ${y(v)}H928"/><text x="56" y="${y(v) + 4}" text-anchor="end">${options.percent ? (100 * v).toFixed(0) + "%" : v.toFixed(v < 10 ? 1 : 0)}</text>`;
  }
  const totals = new Array(n).fill(0);
  series.forEach((s, k) => {
    const color = s.color || colors[k % colors.length];
    if (options.stack) {
      s.values.forEach((v, i) => {
        if (v === null) return;
        out += `<rect x="${left + (i * width) / n + 1}" y="${y(totals[i] + v)}" width="${Math.max(1, width / n - 2)}" height="${Math.max(0, y(totals[i]) - y(totals[i] + v))}" fill="${color}"/>`;
        totals[i] += v;
      });
    } else {
      let points: string[] = [];
      const flush = () => {
        if (points.length)
          out += `<polyline fill="none" stroke="${color}" stroke-width="2" ${s.dashed ? 'stroke-dasharray="5 4"' : ""} points="${points.join(" ")}"/>`;
        if (points.length === 1 && n > 90) {
          const [cx, cy] = points[0].split(",");
          out += `<circle cx="${cx}" cy="${cy}" r="2" fill="${color}"/>`;
        }
        points = [];
      };
      s.values.forEach((v, i) => {
        if (v === null) {
          flush();
          return;
        }
        points.push(`${x(i)},${y(v)}`);
      });
      flush();
      s.values.forEach((v, i) => {
        if (v !== null && n <= 90)
          out += `<circle cx="${x(i)}" cy="${y(v)}" r="2" fill="${color}"/>`;
      });
    }
  });
  const stride = Math.max(1, Math.ceil(n / 7));
  labels.forEach((label, i) => {
    if (i % stride === 0 || i === n - 1)
      out += `<text x="${x(i)}" y="${bottom + 20}" text-anchor="middle" class="muted">${escape(label)}</text>`;
  });
  return out;
}
function legend(series: Series[]): string {
  return series
    .map(
      (s, i) =>
        `<rect x="${64 + (i % 4) * 215}" y="${77 + Math.floor(i / 4) * 19}" width="12" height="3" fill="${s.color || colors[i % colors.length]}"/><text x="${81 + (i % 4) * 215}" y="${83 + Math.floor(i / 4) * 19}">${escape(s.name)}</text>`,
    )
    .join("");
}
export function renderCharts(
  days: { day: string; data: any }[],
  clock: number,
  annotations: { at: number; text: string }[] = [],
  errors: Record<string, string> = {},
  inventory: { since: number; until: number }[] = [],
): Record<string, string> {
  days = days
    .filter((d) => d.day >= day(clock - 89 * DAY) && d.day <= day(clock))
    .sort((a, b) => a.day.localeCompare(b.day));
  const labels = days.map(
    (d) => d.day.slice(5) + (d.day === day(clock) ? "*" : ""),
  );
  const note = `UTC days; * today is provisional. Updated ${iso(clock)}. Missing coverage is reported in the manifest.`;
  const out: Record<string, string> = {};
  const daily = (key: string) =>
    days.map((d) =>
      typeof d.data.daily[key] === "number" ? d.data.daily[key] : 0,
    );
  const render = (file: string, fn: () => string) => {
    try {
      out[file] = fn();
    } catch (error) {
      errors[file] = String(error);
    }
  };
  const plot = (
    file: string,
    title: string,
    series: Series[],
    opts: any = {},
  ) =>
    render(file, () =>
      frame(title, note, legend(series) + panel(series, labels, opts)),
    );
  for (const [metric, file, title] of [
    ["duration", "ci-build-duration.svg", "How long a successful build takes"],
    ["wait", "ci-build-wait.svg", "How long a job waits for a runner"],
  ]) {
    const series: Series[] = [];
    for (const [i, kind] of [
      "github",
      "namespace",
      "merge",
      ...(metric === "wait" ? ["other"] : []),
    ].entries())
      for (const q of ["median", "p90"])
        series.push({
          name: `${kind}, ${q}`,
          color: colors[i],
          dashed: q === "p90",
          values: days.map(
            (d) => d.data.daily[`${metric}/${kind}`]?.[q] ?? null,
          ),
        });
    plot(file, title + " (minutes)", series);
  }
  const keys = (prefix: string) =>
    [
      ...new Set<string>(
        days.flatMap((d) =>
          Object.keys(d.data.daily).filter((k) => k.startsWith(prefix)),
        ),
      ),
    ].sort();
  plot(
    "ci-build-phases.svg",
    "Mean minutes per successful PR build",
    keys("phase/").map((k) => ({
      name: k.slice(6),
      values: daily(k).map(
        (v, i) => v / (days[i].data.daily["phase-count"] || 1),
      ),
    })),
    { stack: true },
  );
  plot(
    "ci-failures.svg",
    "Failed PR builds by cause",
    keys("failure/").map((k) => ({ name: k.slice(8), values: daily(k) })),
    { stack: true },
  );
  // Separate rate plot avoids mixing counts and shares on one axis.
  const rate: Series = {
    name: "failure rate",
    values: days.map((d) =>
      d.data.daily["pr-total"]
        ? keys("failure/").reduce((n, k) => n + (d.data.daily[k] || 0), 0) /
          d.data.daily["pr-total"]
        : null,
    ),
  };
  render("ci-failures.svg", () =>
    frame(
      "Failed PR builds and failure rate",
      note,
      legend(keys("failure/").map((k) => ({ name: k.slice(8), values: [] }))) +
        panel(
          keys("failure/").map((k) => ({ name: k.slice(8), values: daily(k) })),
          labels,
          { top: 150, bottom: 330, stack: true },
        ) +
        panel([rate], labels, {
          top: 395,
          bottom: 480,
          max: 1,
          percent: true,
        }) +
        '<text x="64" y="380">Failure rate</text>',
      525,
    ),
  );
  plot(
    "ci-runner-minutes.svg",
    "Build minutes per day",
    ["namespace", "github", "cancelled"].map((k) => ({
      name: k,
      values: daily("minutes/" + k),
    })),
  );
  plot(
    "ci-picker.svg",
    "Share of PR builds on GitHub-hosted runners",
    [
      {
        name: "GitHub share",
        values: days.map((d) => {
          const gh = d.data.daily["picker/github"] || 0,
            ns = d.data.daily["picker/namespace"] || 0;
          return gh + ns ? gh / (gh + ns) : null;
        }),
      },
    ],
    { max: 1, percent: true },
  );
  const merge = ["success", "failure", "cancelled", "other"].map((k, i) => ({
    name: k,
    color: colors[[5, 3, 7, 4][i]],
    values: daily("merge/" + k),
  }));
  const landed: Series = {
    name: "PR arrivals on main",
    values: days.map((d) => d.data.landings?.length || 0),
  };
  render("ci-merge-queue.svg", () =>
    frame(
      "Merge validation and PR arrivals on main",
      note,
      legend(merge) +
        panel(merge, labels, { top: 130, bottom: 310, stack: true }) +
        panel([landed], labels, { top: 370, bottom: 470 }) +
        '<text x="64" y="350">PR arrivals on main (notification upper bound)</text>',
      515,
    ),
  );
  const fleet = days.flatMap((d) => d.data.fleet || []);
  for (const [name, step, hours] of [
    ["72h", 600, 72],
    ["30d", 7200, 720],
  ] as const) {
    const end = Math.ceil(clock / step) * step,
      start = end - hours * 3600;
    const bins = new Map(
      fleet.filter((b: any) => b.step === step).map((b: any) => [b.at, b]),
    );
    const data: any[] = Array.from(
      { length: (hours * 3600) / step },
      (_, i) => bins.get(start + i * step) || { at: start + i * step },
    );
    const labels = data.map((b) => iso(b.at).slice(5, 16).replace("T", " "));
    const names = [
      "PR builds on GitHub",
      "Other builds on GitHub",
      "Other GitHub jobs",
      "PR jobs on Namespace",
      "Merge validation on Namespace",
      "Other Namespace jobs",
    ];
    const running = [
      "gh-build-pr",
      "gh-build-other",
      "gh-other",
      "ns-pr",
      "ns-mq",
      "ns-other",
    ].map((k, i) => ({
      name: names[i],
      color: colors[i],
      values: data.map((b) => (b.running ? b.running[k] || 0 : null)),
    }));
    const queued = ["github", "namespace"].map((k, i) => ({
      name: k,
      color: colors[i],
      values: data.map((b) => (b.queued ? b.queued[k] || 0 : null)),
    }));
    const waits = ["build", "other"].map((k, i) => ({
      name: k,
      color: colors[i],
      values: data.map((b) => (b.wait ? b.wait[k] || 0 : null)),
    }));
    const runningMax =
      Math.max(
        22,
        ...data.map((b) =>
          Object.values(b.running || {}).reduce<number>(
            (n, v) => n + Number(v),
            0,
          ),
        ),
      ) * 1.1;
    const capY = 280 - (140 * 20) / runningMax;
    const marks = annotations
      .filter((a) => a.at >= start && a.at < end)
      .map(
        (a) =>
          `<path stroke="#94a3b8" stroke-dasharray="3 4" d="M${64 + (864 * (a.at - start)) / (end - start)} 140V660"><title>${escape(a.text)}</title></path>`,
      )
      .join("");
    const picks = data
      .flatMap((b) => b.picked || [])
      .map(
        (at) =>
          `<path stroke="#ff9d4d" d="M${64 + (864 * (at - start)) / (end - start)} 282v7"/>`,
      )
      .join("");
    render(`ci-fleet-${name}.svg`, () =>
      frame(
        `CI fleet, last ${name}`,
        `Average jobs running; peak jobs queued; longest trailing wait (minutes). ${iso(clock)}. Current bin is partial.`,
        legend(running) +
          '<text x="64" y="127">Running</text>' +
          panel(running, labels, {
            top: 140,
            bottom: 280,
            stack: true,
            max: runningMax,
          }) +
          `<path stroke="#facc15" stroke-dasharray="5 4" d="M64 ${capY}H928"/><text x="923" y="${capY - 4}" text-anchor="end">20 GitHub-hosted jobs</text>` +
          picks +
          '<text x="64" y="330">Queued</text>' +
          panel(queued, labels, { top: 345, bottom: 470 }) +
          '<text x="64" y="520">Longest wait (minutes)</text>' +
          panel(waits, labels, { top: 535, bottom: 660 }) +
          marks,
        710,
      ),
    );
  }
  const observations = days
    .flatMap((d) => d.data.observations || [])
    .filter(
      (o) => o.at >= clock - 30 * DAY && o.at <= clock,
    )
    .sort((a, b) => a.at - b.at);
  const queueLabels = observations.map((o) =>
    iso(o.at).slice(5, 16).replace("T", " "),
  );
  // Null points break the line across missing minute-observation intervals.
  const queueValues: (number | null)[] = [];
  const queueTimes: string[] = [];
  const queuePositions: number[] = [];
  observations.forEach((o, i) => {
    if (i && o.at - observations[i - 1].at > 180) {
      queueValues.push(null);
      queueTimes.push("gap");
      queuePositions.push(observations[i - 1].at + 1);
    }
    queueValues.push(o.complete === false ? null : o.count);
    queueTimes.push(queueLabels[i]);
    queuePositions.push(o.at);
  });
  render("ci-queue-size.svg", () =>
    frame(
      "Observed ready-to-merge PR backlog",
      `Distinct ready-labelled PRs, including staging; gaps are missing observations. ${iso(clock)}`,
      panel([{ name: "ready PRs", values: queueValues }], queueTimes, {
        times: queuePositions,
        since: clock - 30 * DAY,
        until: clock,
      }),
    ),
  );
  const staging = days
    .flatMap((d) => d.data.staging || [])
    .filter(
      (r) =>
        r.members?.length &&
        r.conclusion &&
        r.created >= clock - 30 * DAY &&
        r.created <= clock,
    );
  const lo = clock - 30 * DAY,
    max = Math.max(1, ...staging.map((r) => r.members.length));
  const dots = staging
    .map(
      (r) =>
        `<circle cx="${64 + 864 * Math.max(0, (r.created - lo) / (30 * DAY))}" cy="${380 - (240 * r.members.length) / max}" r="4" fill="${r.conclusion === "success" ? colors[5] : r.conclusion === "failure" ? colors[3] : colors[7]}"><title>${escape(r.url)}: ${r.members.length} PRs, ${escape(r.conclusion)}</title></circle>`,
    )
    .join("");
  let cursor = lo,
    shading = "";
  const shade = (a: number, b: number) => {
    if (b > a)
      shading += `<rect x="${64 + (864 * (a - lo)) / (30 * DAY)}" y="140" width="${(864 * (b - a)) / (30 * DAY)}" height="240" fill="#94a3b8" opacity=".16"><title>Incomplete run inventory</title></rect>`;
  };
  for (const interval of [...inventory].sort((a, b) => a.since - b.since)) {
    const start = Math.max(lo, interval.since),
      end = Math.min(clock, interval.until);
    if (end <= lo || start >= clock) continue;
    if (start > cursor) shade(cursor, start);
    cursor = Math.max(cursor, end);
  }
  shade(cursor, clock);
  render("ci-bors-staging.svg", () =>
    frame(
      "Bors staging runs",
      `Production main batches, latest attempt; shading marks incomplete inventory. ${iso(clock)}`,
      panel([], ["30 days ago", "now"], {
        max,
        times: [lo, clock],
        since: lo,
        until: clock,
      }) +
        shading +
        dots,
    ),
  );
  return out;
}
