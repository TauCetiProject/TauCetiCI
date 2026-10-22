import {
  allowed,
  historicalRepo,
  call,
  digest,
  equal,
  json,
  now,
  type Env,
} from "./types";
import { validTelemetry } from "./model";
import { verifyPublisher } from "./oidc";
export { RunDirectory, FactPartition, DayAggregate } from "./storage";
export { Control } from "./control";

async function body(request: Request, max = 2_000_000): Promise<string> {
  if (Number(request.headers.get("content-length") || 0) > max)
    throw new Error("Body too large");
  const reader = request.body?.getReader();
  if (!reader) return "";
  const chunks: Uint8Array[] = [];
  let n = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    n += value.byteLength;
    if (n > max) {
      await reader.cancel();
      throw new Error("Body too large");
    }
    chunks.push(value);
  }
  const out = new Uint8Array(n);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return new TextDecoder().decode(out);
}
async function verify(
  secret: string,
  text: string,
  signature: string,
): Promise<boolean> {
  if (!secret || !/^sha256=[a-f0-9]{64}$/.test(signature)) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  const bytes = Uint8Array.from(signature.slice(7).match(/../g)!, (v) =>
    parseInt(v, 16),
  );
  return crypto.subtle.verify(
    "HMAC",
    key,
    bytes,
    new TextEncoder().encode(text),
  );
}
function validateEvent(b: any, env: Env) {
  if (
    !b ||
    typeof b !== "object" ||
    (!allowed(env).includes(b.repo) &&
      !(b.historical && historicalRepo(b.repo)))
  )
    throw new Error("Invalid repository");
  if (typeof b.delivery !== "string" || b.delivery.length > 300)
    throw new Error("Invalid delivery ID");
  const id = b.run?.id ?? b.run?.run_id ?? b.run_id ?? b.jobs?.[0]?.run_id;
  if (id !== undefined && !/^\d+$/.test(String(id)))
    throw new Error("Invalid run ID");
  if (b.run && !Number.isFinite(Date.parse(b.run.created_at)))
    throw new Error("Invalid run timestamp");
  const sampling = b.run?.archive_sampling;
  if (sampling && (!b.historical || !Number.isFinite(sampling.until) ||
    !Number.isFinite(sampling.weight) || sampling.weight < 0))
    throw new Error("Invalid archive sampling");
  if (
    b.jobs &&
    (!Array.isArray(b.jobs) ||
      b.jobs.length > 100 ||
      b.jobs.some(
        (j: any) =>
          !/^\d+$/.test(String(j.id ?? j.job_id)) ||
          !Number.isFinite(Date.parse(j.created_at)),
      ))
  )
    throw new Error("Invalid jobs");
  if (b.telemetry && !validTelemetry(b.telemetry))
    throw new Error("Invalid telemetry");
}
async function archiveEvent(env: Env, event: any) {
  validateEvent(event, env);
  const key = `inbox/${new Date().toISOString().slice(0, 10)}/${await digest(event.delivery)}.json`;
  // Retain the body before enqueueing. A failed enqueue is safely retryable.
  await env.ARCHIVE.put(key, JSON.stringify(event), {
    customMetadata: { queued_at: String(now()), dispatches: "1" },
  });
  await env.EVENTS.send({ key });
}
async function handle(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const url = new URL(request.url),
    path = url.pathname;
  const cors = {
    "Access-Control-Allow-Origin":
      env.PUBLIC_ORIGIN || "https://taucetiproject.org",
    "Access-Control-Allow-Headers": "If-None-Match,Content-Type",
    "Access-Control-Allow-Methods": "GET,HEAD,OPTIONS",
    "Access-Control-Expose-Headers": "ETag",
    Vary: "Origin",
  };
  if (request.method === "OPTIONS")
    return new Response(null, { status: 204, headers: cors });
  if (path === "/healthz")
    return json({
      service: "tauceti-live-ci",
      version: env.CF_VERSION?.id || "development",
    });
  if (path === "/setup/register" && request.method === "GET") {
    const state = crypto.randomUUID();
    const manifest = {
      name: "tauceti-live-ci",
      url: "https://github.com/TauCetiProject/TauCetiCI",
      public: false,
      hook_attributes: { url: url.origin + "/webhooks/github" },
      redirect_url: url.origin + "/setup/callback",
      default_permissions: {
        actions: "read",
        contents: "read",
        metadata: "read",
      },
      default_events: ["workflow_run", "workflow_job", "push"],
    };
    const value = JSON.stringify(manifest)
      .replaceAll("&", "&amp;")
      .replaceAll('"', "&quot;")
      .replaceAll("<", "&lt;");
    return new Response(
      `<html lang="en"><meta charset="utf-8"><title>Register TauCeti live CI</title><h1>Register the read-only CI App</h1><p>This creates a dedicated Actions reader for TauCetiProject. Install it on all organisation repositories used by the fleet chart.</p><form method="post" action="https://github.com/organizations/TauCetiProject/settings/apps/new?state=${state}"><input type="hidden" name="manifest" value="${value}"><button>Create GitHub App</button></form></html>`,
      {
        headers: {
          "Content-Type": "text/html;charset=utf-8",
          "Cache-Control": "no-store",
          "Referrer-Policy": "no-referrer",
          "Set-Cookie": `ci_setup=${state}; HttpOnly; Secure; SameSite=Lax; Max-Age=1800; Path=/setup/`,
        },
      },
    );
  }
  if (path === "/setup/callback" && request.method === "GET") {
    const code = url.searchParams.get("code");
    if (!code || !/^[-a-zA-Z0-9]{1,200}$/.test(code))
      return json({ error: "Invalid registration code" }, 400);
    const cookie = /\bci_setup=([-a-f0-9]+)\b/.exec(
      request.headers.get("cookie") || "",
    )?.[1];
    if (!cookie || !(await equal(cookie, url.searchParams.get("state") || "")))
      return json({ error: "Registration state mismatch" }, 403);
    const existing = await env.ARCHIVE.head("private/registration.json");
    if (existing) return json({ error: "Registration already saved" }, 409);
    const response = await fetch(
      `https://api.github.com/app-manifests/${code}/conversions`,
      {
        method: "POST",
        headers: {
          Accept: "application/vnd.github+json",
          "User-Agent": "TauCetiCI-live/1.0",
        },
        signal: AbortSignal.timeout(15000),
      },
    );
    if (!response.ok)
      return json({ error: "GitHub registration exchange failed" }, 502);
    const credentials: any = await response.json();
    if (
      credentials.slug !== "tauceti-live-ci" ||
      credentials.owner?.login !== "TauCetiProject" ||
      Object.values(credentials.permissions || {}).some((v) => v !== "read")
    )
      return json(
        { error: "Unexpected GitHub App identity or permissions" },
        400,
      );
    await env.ARCHIVE.put(
      "private/registration.json",
      JSON.stringify(credentials),
    );
    return new Response(
      '<html lang="en"><meta charset="utf-8"><title>Install CI App</title><h1>App registered</h1><p>Install the App on the TauCetiProject repositories. Credentials have been saved privately.</p><a href="https://github.com/apps/tauceti-live-ci/installations/new">Install the App</a></html>',
      {
        headers: {
          "Content-Type": "text/html;charset=utf-8",
          "Cache-Control": "no-store",
          "Referrer-Policy": "no-referrer",
        },
      },
    );
  }
  if (path.startsWith("/v1/ci/") && ["GET", "HEAD"].includes(request.method)) {
    if (
      path !== "/v1/ci/manifest.json" &&
      !/^\/v1\/ci\/assets\/[a-f0-9]+\/[a-z0-9-]+\.(svg|json)$/.test(path)
    )
      return json({ error: "Not found" }, 404);
    const cache = await caches.open("ci-reports-v1"),
      cacheKey = new Request(url.origin + path);
    const cached = await cache.match(cacheKey);
    if (cached) {
      if (request.headers.get("if-none-match") === cached.headers.get("etag"))
        return new Response(null, { status: 304, headers: cached.headers });
      return request.method === "HEAD"
        ? new Response(null, { headers: cached.headers })
        : cached;
    }
    const object = await env.ARCHIVE.get("public" + path);
    if (!object)
      return new Response(
        JSON.stringify({ error: "Reports are not published yet" }),
        {
          status: 503,
          headers: {
            ...cors,
            "Content-Type": "application/json",
            "Retry-After": "60",
          },
        },
      );
    const etag = object.httpEtag;
    const headers = {
      ...cors,
      ETag: etag,
      "Cache-Control": path.endsWith("/manifest.json")
        ? "public,max-age=30,stale-if-error=86400"
        : "public,max-age=31536000,immutable",
      "Content-Type":
        object.httpMetadata?.contentType ||
        (path.endsWith(".svg") ? "image/svg+xml" : "application/json"),
      "X-Content-Type-Options": "nosniff",
    };
    const response = new Response(object.body, { headers });
    ctx.waitUntil(cache.put(cacheKey, response.clone()));
    if (request.headers.get("if-none-match") === etag)
      return new Response(null, { status: 304, headers });
    return request.method === "HEAD"
      ? new Response(null, { headers })
      : response;
  }
  if (path === "/webhooks/github" && request.method === "POST") {
    const raw = await body(request, 25_000_000);
    if (
      !(await verify(
        env.WEBHOOK_SECRET,
        raw,
        request.headers.get("x-hub-signature-256") || "",
      ))
    )
      return json({ error: "Invalid signature" }, 401);
    const event = request.headers.get("x-github-event"),
      delivery = request.headers.get("x-github-delivery");
    if (event === "ping") return json({ ok: true });
    const value: any = JSON.parse(raw),
      repo = value.repository?.full_name;
    if (!allowed(env).includes(repo))
      return json({ error: "Repository not allowed" }, 403);
    if (!delivery || !/^[-a-zA-Z0-9]{1,100}$/.test(delivery))
      return json({ error: "Invalid delivery" }, 400);
    if (event === "workflow_run")
      await archiveEvent(env, { repo, delivery, run: value.workflow_run });
    else if (event === "workflow_job")
      await archiveEvent(env, { repo, delivery, jobs: [value.workflow_job] });
    else if (
      event === "push" &&
      value.ref === "refs/heads/main" &&
      repo === "TauCetiProject/TauCeti"
    ) {
      // Arrival is observed at push delivery, an upper bound on arrival on main.
      for (const commit of value.commits || []) {
        const pr = /\(#(\d+)\)\s*$/.exec((commit.message || "").split("\n")[0]);
        if (pr)
          await archiveEvent(env, {
            repo,
            delivery: `${delivery}/${pr[1]}`,
            landing: { pr: Number(pr[1]), at: now() },
          });
      }
    }
    return json({ accepted: true }, 202);
  }
  const token =
    request.headers.get("authorization")?.replace(/^Bearer /, "") || "";
  if (path === "/telemetry/lease" && request.method === "POST") {
    let claims;
    try {
      claims = await verifyPublisher(token, env);
    } catch {
      return json({ error: "Publisher authentication failed" }, 401);
    }
    const b = JSON.parse(await body(request));
    if (
      !/^\d+$/.test(String(b.run_id)) ||
      !Number.isInteger(b.attempt) ||
      b.attempt < 1
    )
      return json({ error: "Invalid attempt" }, 400);
    return json(
      await call(env.CONTROL, "control", "/lease", {
        publisher: `${claims.run_id}/${claims.run_attempt}`,
        run_id: String(b.run_id),
        attempt: b.attempt,
      }),
    );
  }
  if (path === "/telemetry/quota" && request.method === "POST") {
    let claims;
    try {
      claims = await verifyPublisher(token, env);
    } catch {
      return json({ error: "Publisher authentication failed" }, 401);
    }
    const b = JSON.parse(await body(request));
    if (![403, 429].includes(b.status))
      return json({ error: "Invalid quota event" }, 400);
    return json(
      await call(env.CONTROL, "control", "/publisher-quota", {
        ...b,
        publisher: `${claims.run_id}/${claims.run_attempt}`,
      }),
    );
  }
  if (path === "/telemetry" && request.method === "POST") {
    let claims;
    try {
      claims = await verifyPublisher(token, env);
    } catch {
      return json({ error: "Publisher authentication failed" }, 401);
    }
    const b = JSON.parse(await body(request));
    b.repo = "TauCetiProject/TauCeti";
    b.delivery = `summary/${claims.run_id}/${claims.run_attempt}`;
    validateEvent(b, env);
    const lease = await call(env.CONTROL, "control", "/check-lease", {
      publisher: `${claims.run_id}/${claims.run_attempt}`,
      run_id: String(b.run?.id),
      attempt: b.run?.run_attempt,
    });
    if (!lease.valid)
      return json(
        { error: "Summary lease expired or does not match attempt" },
        403,
      );
    await archiveEvent(env, b);
    return json({ accepted: true }, 202);
  }
  if (path === "/ingest" && request.method === "POST") {
    if (!(await equal(token, env.INGEST_TOKEN)) || !env.INGEST_TOKEN)
      return json({ error: "Unauthorized" }, 401);
    const b = JSON.parse(await body(request));
    await archiveEvent(env, b);
    return json({ accepted: true }, 202);
  }
  if (path.startsWith("/admin/")) {
    if (!(await equal(token, env.ADMIN_TOKEN)) || !env.ADMIN_TOKEN)
      return json({ error: "Unauthorized" }, 401);
    const b =
      request.method === "POST"
        ? JSON.parse((await body(request)) || "{}")
        : {};
    if (path === "/admin/import" && request.method === "POST") {
      const events = Array.isArray(b) ? b : [b];
      if (events.length > 100)
        return json({ error: "At most 100 events per import" }, 400);
      for (const event of events) validateEvent(event, env);
      // Keep one-time import bounded while overlapping cross-object I/O.
      for (let i = 0; i < events.length; i += 4)
        await Promise.all(
          events
            .slice(i, i + 4)
            .map((event: any) => call(env.CONTROL, "control", "/event", event)),
        );
      return json({ imported: events.length });
    }
    if (path === "/admin/replay" && request.method === "POST") {
      if (!/^(deliveries|failed|inbox)\/[a-zA-Z0-9/.-]+$/.test(b.key))
        return json({ error: "Invalid delivery key" }, 400);
      await env.EVENTS.send({ key: b.key });
      return json({ accepted: true }, 202);
    }
    if (path === "/admin/replay-failed" && request.method === "POST") {
      const page = await env.ARCHIVE.list({
        prefix: "failed/",
        limit: 100,
        cursor: b.cursor || undefined,
      });
      if (page.objects.length)
        await env.EVENTS.sendBatch(
          page.objects.map(({ key }) => ({ body: { key } })),
        );
      return json(
        {
          enqueued: page.objects.length,
          cursor: page.truncated ? page.cursor : null,
        },
        202,
      );
    }
    if (
      ["/admin/tick", "/admin/publish"].includes(path) &&
      request.method === "POST"
    ) {
      await call(env.CONTROL, "control", path.slice("/admin".length));
      return json({ ok: true });
    }
    if (path === "/admin/health")
      return json(await call(env.CONTROL, "control", "/health"));
    if (path === "/admin/repairs")
      return json(
        await call(env.CONTROL, "control", "/repairs", {
          kind: url.searchParams.get("kind"),
        }),
      );
    if (path === "/admin/prune-repairs" && request.method === "POST")
      return json(await call(env.CONTROL, "control", "/prune-repairs", b));
    if (path === "/admin/repair-members" && request.method === "POST")
      return json(await call(env.CONTROL, "control", "/repair-members"));
    if (path === "/admin/refresh-day" && request.method === "POST") {
      if (typeof b.day !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(b.day) ||
          !Number.isFinite(Date.parse(b.day)))
        return json({ error: "Invalid report day" }, 400);
      return json(await call(env.CONTROL, "control", "/refresh-day", { day: b.day }));
    }
    if (path === "/admin/recompute" && request.method === "POST") {
      if (!allowed(env).includes(b.repo) ||
          typeof b.day !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(b.day) ||
          !Number.isFinite(Date.parse(b.day)) ||
          (b.cursor !== undefined && b.cursor !== null &&
            (typeof b.cursor !== "string" || !/^\d{0,30}$/.test(b.cursor))))
        return json({ error: "Invalid recompute partition or cursor" }, 400);
      return json(await call(env.FACTS, `${b.repo}/${b.day}`, "/recompute", b));
    }
    if (path === "/admin/compare" && request.method === "POST")
      return json(await call(env.CONTROL, "control", "/comparison", b));
    if (path === "/admin/activation-check")
      return json(await call(env.CONTROL, "control", "/activation-check"));
    if (path === "/admin/sample" && request.method === "POST") {
      if (
        !allowed(env).includes(b.repo) ||
        !/^\d+$/.test(String(b.id)) ||
        !Number.isInteger(b.attempt)
      )
        return json({ error: "Invalid sample" }, 400);
      const { directory } = await import("./types");
      const location = await call(
        env.DIRECTORIES,
        directory(b.repo, b.id),
        "/get",
        { id: b.id },
      );
      if (!location.partition) return json({ available: false });
      return json({
        available: true,
        ...(await call(env.FACTS, location.partition, "/status", b)),
      });
    }
    if (path === "/admin/registration") {
      const object = await env.ARCHIVE.get("private/registration.json");
      return object
        ? new Response(object.body, {
            headers: {
              "Content-Type": "application/json",
              "Cache-Control": "no-store",
            },
          })
        : json({ error: "Registration not completed" }, 404);
    }
  }
  return json({ error: "Not found" }, 404);
}
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    try {
      return await handle(request, env, ctx);
    } catch (error) {
      console.error("live-ci request failed", String(error));
      return json({ error: "Request failed" }, 500);
    }
  },
  async queue(batch: MessageBatch<{ key: string }>, env: Env) {
    const consume = async (message: Message<{ key: string }>) => {
      try {
        const object =
          (await env.ARCHIVE.get(message.body.key)) ||
          (await env.ARCHIVE.get(
            message.body.key.replace(/^(inbox|failed)\//, "deliveries/"),
          ));
        if (!object) throw new Error("Archived delivery missing");
        const event = await object.json();
        await call(env.CONTROL, "control", "/event", event);
        if (/^(inbox|failed)\//.test(message.body.key)) {
          await env.ARCHIVE.put(
            message.body.key.replace(/^(inbox|failed)\//, "deliveries/"),
            JSON.stringify(event),
          );
          await env.ARCHIVE.delete(message.body.key);
        }
        message.ack();
      } catch (error) {
        console.error(
          "live-ci delivery deferred",
          message.body.key,
          String(error),
        );
        if (message.attempts >= 20 && message.body.key.startsWith("inbox/")) {
          const object = await env.ARCHIVE.get(message.body.key);
          if (object) {
            await env.ARCHIVE.put(
              message.body.key.replace(/^inbox\//, "failed/"),
              await object.arrayBuffer(),
            );
            await env.ARCHIVE.delete(message.body.key);
          }
        }
        message.retry({ delaySeconds: 60 });
      }
    };
    for (let i = 0; i < batch.messages.length; i += 4)
      await Promise.all(batch.messages.slice(i, i + 4).map(consume));
  },
  async scheduled(_event: ScheduledEvent, env: Env, ctx: ExecutionContext) {
    // Publication has its own single-flight operation: slow reconciliation
    // must not prevent a fresh snapshot of concurrently arriving webhooks.
    ctx.waitUntil(call(env.CONTROL, "control", "/publish"));
    ctx.waitUntil(call(env.CONTROL, "control", "/tick"));
    ctx.waitUntil(
      (async () => {
        // Recover the R2-write/Queue-send gap independently of GitHub redelivery.
        const position = await call(env.CONTROL, "control", "/inbox-position");
        const pending = await env.ARCHIVE.list({
          prefix: "inbox/",
          limit: 100,
          include: ["customMetadata"],
          cursor: position.cursor || undefined,
        });
        for (const item of pending.objects) {
          if (
            now() -
              Number(
                item.customMetadata?.queued_at ||
                  item.uploaded.getTime() / 1000,
              ) <
            120
          )
            continue;
          const object = await env.ARCHIVE.get(item.key);
          if (!object) continue;
          const dispatches = Number(object.customMetadata?.dispatches || 1),
            data = await object.arrayBuffer();
          if (dispatches >= 20) {
            await env.ARCHIVE.put(
              item.key.replace(/^inbox\//, "failed/"),
              data,
            );
            await env.ARCHIVE.delete(item.key);
            continue;
          }
          const updated = await env.ARCHIVE.put(item.key, data, {
            onlyIf: { etagMatches: object.etag },
            customMetadata: {
              queued_at: String(now()),
              dispatches: String(dispatches + 1),
            },
          });
          if (updated) await env.EVENTS.send({ key: item.key });
        }
        await call(env.CONTROL, "control", "/inbox-position", {
          cursor: pending.truncated ? pending.cursor : null,
          save: true,
        });
      })(),
    );
  },
};
