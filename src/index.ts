// R2 download speedtest — Worker + Container Durable Object.
//
// Architecture
// ------------
// Worker serves the static frontend from ./public (assets binding) and
// exposes /api/{config,start,status,reset}. On /api/start it shards a
// static list of 3000 R2 object keys across N containers and fans out to
// each container instance's HTTP agent via the DO getContainer() helper.
// Each container instance runs the Python agent (see container/agent.py)
// which supervises an rclone process that downloads its shard from R2
// into rclone's memory,discard sink.
//
// Timing is authoritative from the containers: elapsed is computed as
//   max(finishedAt) - min(startedAt)
// across every successfully-started shard, so a partial-scheduling failure
// can never masquerade as a completed 1.5 TB run.
//
// Auto-start subtlety
// -------------------
// The Container class's default fetch() auto-starts the container. That's
// wrong for /api/status: opening the frontend would start all 200 containers
// just from polling. We solve this by exposing three "/__*" control-plane
// endpoints on the DO that inspect/manage state without triggering start.

import { Container, getContainer } from "@cloudflare/containers";

// Fixed shape of the seeded dataset. If you change seed.mjs, change here.
const TOTAL_FILES = 3000;
const FILE_SIZE_BYTES = 500_000_000; // exactly 500 MB
const POOL_BYTES = TOTAL_FILES * FILE_SIZE_BYTES; // 1.5 TB pool
const KEY_PREFIX = "bench/file-";
const KEY_SUFFIX = ".bin";

// Every container downloads this many files, sampled randomly from the pool
// (without replacement within a container). With random sampling, different
// containers may pick the same key -- that is intentional: it decouples the
// per-container workload size from the fleet size, so runs can scale past
// TOTAL_FILES / FILES_PER_CONTAINER (3000 / 20 = 150) without reseeding.
const FILES_PER_CONTAINER = 20;

// UI-selectable container counts. Must all be <= wrangler max_instances.
const CONTAINER_COUNTS = [1, 10, 25, 50, 100, 150, 200] as const;
const MAX_CONTAINERS = CONTAINER_COUNTS[CONTAINER_COUNTS.length - 1];

// Warmup pacing: dispatch this many /__ensureStart calls per second.
// Firing all N cold-starts at once overwhelms the containers platform at
// high N (schedule failures, image pull contention). Spacing dispatches
// keeps the burst under a rate the API tolerates while still finishing
// warmup in seconds -- at 200 / 15 ≈ 13 seconds of dispatches, then the
// container cold-start latencies overlap in flight.
const WARMUP_RATE_PER_SEC = 15;

function buildKeyList(): string[] {
  const keys: string[] = [];
  for (let i = 0; i < TOTAL_FILES; i++) {
    keys.push(`${KEY_PREFIX}${i.toString().padStart(4, "0")}${KEY_SUFFIX}`);
  }
  return keys;
}

interface Env {
  BENCH: DurableObjectNamespace;
  ASSETS: Fetcher;
  R2_ACCOUNT_ID: string;
  R2_BUCKET: string;
  R2_ACCESS_KEY_ID: string;
  R2_SECRET_ACCESS_KEY: string;
}

// ---------------------------------------------------------------------------
// Container Durable Object
// ---------------------------------------------------------------------------

export class BenchContainer extends Container<Env> {
  defaultPort = 8080;
  sleepAfter = "3m";

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Env vars injected into the container process. The agent reads these
    // to build the rclone remote config -- no rclone config file needed,
    // no credentials in request bodies, no credentials in wrangler.jsonc.
    this.envVars = {
      R2_ACCOUNT_ID: env.R2_ACCOUNT_ID,
      R2_BUCKET: env.R2_BUCKET,
      R2_ACCESS_KEY_ID: env.R2_ACCESS_KEY_ID,
      R2_SECRET_ACCESS_KEY: env.R2_SECRET_ACCESS_KEY,
    };
  }

  // Override the DO fetch to intercept control-plane paths before the
  // superclass's auto-start behavior kicks in. Any other path falls
  // through to the default forwarding, which does auto-start.
  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const p = url.pathname;

    if (p === "/__state") {
      const state = await this.getState();
      // State shape from @cloudflare/containers is `{ status, lastChange, exitCode? }`.
      // Flatten `status` up to `state` for the Worker's convenience.
      return Response.json({
        state: state.status,
        lastChange: state.lastChange,
        exitCode: "exitCode" in state ? state.exitCode : undefined,
      });
    }

    if (p === "/__ensureStart") {
      try {
        await this.startAndWaitForPorts();
        return Response.json({ started: true });
      } catch (e) {
        return Response.json({ started: false, error: String(e) }, { status: 500 });
      }
    }

    if (p === "/__destroy") {
      try {
        await this.destroy();
        return Response.json({ destroyed: true });
      } catch (e) {
        return Response.json({ destroyed: false, error: String(e) }, { status: 500 });
      }
    }

    return super.fetch(request);
  }
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

interface CpuSample {
  coresUsed: number | null;
  pctOfQuota: number | null;
  vcpus: number | null;
  effectiveVcpus: number | null;
  hostCores: number;
  affinityCores: number | null;
  cgroupThreads: number | null;
  // Cumulative cgroup v2 counters (since container start).
  cumUsageUs: number | null;
  cumNrPeriods: number | null;
  cumNrThrottled: number | null;
  cumThrottledUs: number | null;
  // Deltas over the last poll window.
  dNrPeriods: number | null;
  dNrThrottled: number | null;
  dThrottledUs: number | null;
  // Derived from deltas.
  throttledPctOfWindow: number | null; // wall-clock % of window spent throttled
  throttledPeriodRatio: number | null; // % of enforcement periods throttled
}

interface AgentStatus {
  state: "idle" | "running" | "done" | "error";
  colo: string;
  startedAt: number | null;
  finishedAt: number | null;
  bytes: number;
  elapsed: number;
  speed: number;
  errors: number;
  totalBytes: number;
  totalTransfers: number;
  transfersDone: number;
  lastMsg: string;
  exitCode: number | null;
  rcloneArgs: string[];
  cpu: CpuSample;
}

function containerName(index: number): string {
  return `bench-${index}`;
}

async function agentFetch(
  env: Env,
  index: number,
  path: string,
  init?: RequestInit,
): Promise<Response> {
  const stub = getContainer(env.BENCH, containerName(index));
  return stub.fetch(`http://c/${path.replace(/^\/+/, "")}`, init);
}

// Control-plane calls go through the DO stub directly (not getContainer),
// hitting the /__* endpoints on the BenchContainer override.
async function doControl(
  env: Env,
  index: number,
  path: `/__${string}`,
): Promise<Response> {
  const stub = env.BENCH.get(env.BENCH.idFromName(containerName(index)));
  return stub.fetch(`http://c${path}`);
}

// Bring a container up and wait for port 8080 to be ready, then wipe any
// stale agent state so the container is clean for the upcoming run.
//
// Why the reset is inside warmup, not just in /start:
//   sleepAfter is 3m. A container reused within that window keeps its
//   Python-level STATE from the previous run -- bytes, elapsed, "done"
//   status. /api/status aggregates s.bytes unconditionally, so during
//   the ~n/rate warmup window the dashboard would sum previous-run bytes
//   across every reused container. The /start endpoint DOES reset STATE
//   (agent.py) but that fires only in phase 2 -- too late to keep phase 1
//   clean. Resetting during warmup closes that window.
//
// Failure of either step is a legitimate signal (colo capacity, image
// pull error, agent crash) that we surface verbatim rather than masking
// as a downstream connection error.
async function warmupShard(
  env: Env,
  index: number,
): Promise<{ index: number; ok: boolean; error?: string }> {
  try {
    const ensure = await doControl(env, index, "/__ensureStart");
    if (!ensure.ok) {
      return { index, ok: false, error: `ensureStart ${ensure.status}: ${(await ensure.text()).slice(0, 200)}` };
    }
    // POST /reset to the agent: kills any residual rclone process AND
    // zeroes STATE (bytes, elapsed, startedAt, etc.). Idempotent -- safe
    // to call on a freshly-booted agent as well.
    const reset = await agentFetch(env, index, "reset", { method: "POST" });
    if (!reset.ok) {
      return { index, ok: false, error: `agent reset ${reset.status}: ${(await reset.text()).slice(0, 200)}` };
    }
    return { index, ok: true };
  } catch (e) {
    return { index, ok: false, error: String(e) };
  }
}

// Reset agent state on every container that is ALREADY up. Runs in
// parallel over the whole fleet -- safe because /__state does not
// auto-start containers, and we only issue /reset to containers whose
// state check confirms they're up (so agentFetch's auto-start behavior
// can't kick in here).
//
// The point: /api/status polls during the rate-limited warmup phase
// below can otherwise observe stale STATE.bytes from a previous run
// in any container reused within sleepAfter. Zeroing those before the
// warmup dispatch starts eliminates the phantom "downloaded" figure
// that showed up in the dashboard during warmup.
async function preResetUpContainers(env: Env, n: number): Promise<number> {
  let reset = 0;
  await Promise.all(
    Array.from({ length: n }, async (_, i) => {
      try {
        const st = await doControl(env, i, "/__state");
        if (!st.ok) return;
        const s = (await st.json()) as { state?: string };
        if (s.state !== "running" && s.state !== "healthy") return;
        // Container is up; safe to hit the agent without triggering
        // an auto-start via getContainer's default fetch behavior.
        const r = await agentFetch(env, i, "reset", { method: "POST" });
        if (r.ok) reset++;
      } catch {
        // Ignore; the rate-limited warmup phase surfaces real failures.
      }
    }),
  );
  return reset;
}

// Warm every container in the fleet before starting the benchmark, pacing
// dispatches at ratePerSec per second. Each /__ensureStart is fired without
// awaiting completion, but their promises are collected and awaited at the
// end so the caller knows every container is reachable (or has failed) by
// the time this returns.
//
// Pacing shape: we dispatch a batch of ratePerSec calls, sleep 1 second,
// dispatch the next batch, etc. Individual cold starts run in parallel;
// only the *rate of new dispatches* is limited.
async function warmupFleet(
  env: Env,
  n: number,
  ratePerSec: number,
): Promise<{
  warmed: number;
  failed: Array<{ index: number; error: string }>;
  elapsedMs: number;
  preResetCount: number;
}> {
  const startedAt = Date.now();

  // Phase 0: clear stale STATE on any container already up. This closes
  // the window where /api/status polling could otherwise sum leftover
  // bytes from previous runs during the ~n/ratePerSec dispatch window.
  const preResetCount = await preResetUpContainers(env, n);

  const pending: Promise<{ index: number; ok: boolean; error?: string }>[] = [];

  for (let i = 0; i < n; i++) {
    pending.push(warmupShard(env, i));
    // After each batch of ratePerSec dispatches, wait 1 s before the next.
    // Skip the pause after the very last dispatch.
    if ((i + 1) % ratePerSec === 0 && i < n - 1) {
      await new Promise((r) => setTimeout(r, 1000));
    }
  }

  const results = await Promise.all(pending);
  const warmed = results.filter((r) => r.ok).length;
  const failed = results
    .filter((r) => !r.ok)
    .map((r) => ({ index: r.index, error: r.error ?? "unknown" }));
  return { warmed, failed, elapsedMs: Date.now() - startedAt, preResetCount };
}

// Kick off the rclone workload on a warm container. Assumes warmupShard
// has already succeeded for this index -- the agent HTTP server must be
// listening. Failures here indicate a runtime problem, not a scheduling one.
async function startAgent(
  env: Env,
  index: number,
  files: string[],
  transfers: number,
  disableHttp2: boolean,
): Promise<{ index: number; ok: boolean; error?: string }> {
  try {
    const res = await agentFetch(env, index, "start", {
      method: "POST",
      body: JSON.stringify({ files, transfers, disableHttp2 }),
      headers: { "content-type": "application/json" },
    });
    if (res.status !== 202) {
      return { index, ok: false, error: `agent start ${res.status}: ${(await res.text()).slice(0, 200)}` };
    }
    return { index, ok: true };
  } catch (e) {
    return { index, ok: false, error: String(e) };
  }
}

async function statusShard(
  env: Env,
  index: number,
): Promise<
  | { index: number; reachable: false; containerState: string; error?: string }
  | { index: number; reachable: true; status: AgentStatus }
> {
  try {
    const st = await doControl(env, index, "/__state");
    let containerState = "unknown";
    if (st.ok) containerState = ((await st.json()) as { state: string }).state;
    // Only reach into the agent if the container is up. This avoids the
    // Container class auto-starting instances just from status polling.
    if (containerState !== "running" && containerState !== "healthy") {
      return { index, reachable: false, containerState };
    }
    const res = await agentFetch(env, index, "status");
    if (!res.ok) {
      return { index, reachable: false, containerState, error: `agent status ${res.status}` };
    }
    return { index, reachable: true, status: (await res.json()) as AgentStatus };
  } catch (e) {
    return { index, reachable: false, containerState: "error", error: String(e) };
  }
}

async function resetShard(
  env: Env,
  index: number,
  hard: boolean,
): Promise<{ index: number; ok: boolean; error?: string }> {
  try {
    if (hard) {
      const r = await doControl(env, index, "/__destroy");
      return { index, ok: r.ok, error: r.ok ? undefined : `destroy ${r.status}` };
    }
    // Only reset the agent if the container is currently up.
    const st = await doControl(env, index, "/__state");
    const containerState = st.ok ? ((await st.json()) as { state: string }).state : "unknown";
    if (containerState !== "running" && containerState !== "healthy") {
      return { index, ok: true }; // nothing to reset
    }
    const res = await agentFetch(env, index, "reset", { method: "POST" });
    return { index, ok: res.ok, error: res.ok ? undefined : `agent reset ${res.status}` };
  } catch (e) {
    return { index, ok: false, error: String(e) };
  }
}

function randomShard(files: string[], k: number): string[] {
  // Sample k keys uniformly at random without replacement (per container).
  // Different containers sample independently, so at high N the same key
  // will be picked by more than one container -- accepted tradeoff to
  // decouple per-container workload size from fleet size.
  //
  // Truncated Fisher-Yates: O(files.length) copy, O(k) work. For our
  // sizes (files.length=3000, k=20) this is trivially cheap.
  const arr = files.slice();
  const kk = Math.min(k, arr.length);
  for (let i = 0; i < kk; i++) {
    const j = i + Math.floor(Math.random() * (arr.length - i));
    const tmp = arr[i];
    arr[i] = arr[j];
    arr[j] = tmp;
  }
  return arr.slice(0, kk);
}

// ---------------------------------------------------------------------------

export default {
  async fetch(request: Request, env: Env, _ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    // Frontend is served from ./public via the assets binding.
    if (!url.pathname.startsWith("/api/")) {
      return env.ASSETS.fetch(request);
    }

    if (url.pathname === "/api/config" && request.method === "GET") {
      return Response.json({
        totalFiles: TOTAL_FILES,
        fileSizeBytes: FILE_SIZE_BYTES,
        poolBytes: POOL_BYTES,
        filesPerContainer: FILES_PER_CONTAINER,
        bytesPerContainer: FILES_PER_CONTAINER * FILE_SIZE_BYTES,
        containerCounts: CONTAINER_COUNTS,
        transfersOptions: [8, 16, 20, 32, 64, 128],
        warmupRatePerSec: WARMUP_RATE_PER_SEC,
        // Purely display; must be kept in sync with wrangler.jsonc constraints.
        placement: "ENAM region",
      });
    }

    // Warm the fleet: bring every container up, wait for HTTP-ready,
    // and reset agent state so bytes/elapsed counters begin at zero.
    // This is now a separate step from starting the benchmark so the
    // user can (1) see progress and total warmup time explicitly and
    // (2) run multiple back-to-back benchmarks without re-warming.
    if (url.pathname === "/api/warmup" && request.method === "POST") {
      const body = (await request.json().catch(() => ({}))) as {
        containerCount?: number;
      };
      const requested = Number(body.containerCount ?? MAX_CONTAINERS);
      const n = Number.isFinite(requested)
        ? Math.max(1, Math.min(MAX_CONTAINERS, Math.floor(requested)))
        : MAX_CONTAINERS;

      const warmup = await warmupFleet(env, n, WARMUP_RATE_PER_SEC);

      return Response.json({
        containerCount: n,
        warmed: warmup.warmed,
        failed: warmup.failed,
        elapsedMs: warmup.elapsedMs,
        ratePerSec: WARMUP_RATE_PER_SEC,
      });
    }

    if (url.pathname === "/api/start" && request.method === "POST") {
      const body = (await request.json().catch(() => ({}))) as {
        containerCount?: number;
        transfers?: number;
        disableHttp2?: boolean;
      };
      // Accept any container count in [1, MAX_CONTAINERS] -- CONTAINER_COUNTS is
      // only the UI preset list, not a hard whitelist. Silently falling back to
      // the default on an unfamiliar value was a real footgun (a stray value
      // of 2 launched 50).
      const requested = Number(body.containerCount ?? MAX_CONTAINERS);
      const n = Number.isFinite(requested)
        ? Math.max(1, Math.min(MAX_CONTAINERS, Math.floor(requested)))
        : MAX_CONTAINERS;
      const transfers = Math.max(1, Math.min(256, body.transfers ?? 20));
      const disableHttp2 = body.disableHttp2 ?? true;

      const files = buildKeyList();
      const runId = crypto.randomUUID();

      // Assumes containers are already warm (via /api/warmup). Each container
      // gets an independently-sampled random shard of FILES_PER_CONTAINER
      // keys from the pool. If an agent isn't reachable, startAgent surfaces
      // that as a per-index failure rather than warming inline -- the caller
      // can decide whether to re-warm or proceed.
      const starts = await Promise.all(
        Array.from({ length: n }, (_, i) =>
          startAgent(env, i, randomShard(files, FILES_PER_CONTAINER), transfers, disableHttp2),
        ),
      );

      return Response.json({
        runId,
        containerCount: n,
        filesPerContainer: FILES_PER_CONTAINER,
        bytesPerContainer: FILES_PER_CONTAINER * FILE_SIZE_BYTES,
        runBytes: n * FILES_PER_CONTAINER * FILE_SIZE_BYTES,
        transfers,
        disableHttp2,
        started: starts.filter((r) => r.ok).length,
        failed: starts.filter((r) => !r.ok),
      });
    }

    if (url.pathname === "/api/status" && request.method === "GET") {
      const rawN = Number(url.searchParams.get("n") ?? MAX_CONTAINERS);
      const n = Number.isFinite(rawN)
        ? Math.max(1, Math.min(MAX_CONTAINERS, Math.floor(rawN)))
        : MAX_CONTAINERS;
      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => statusShard(env, i)),
      );

      let totalBytes = 0;
      let liveErrors = 0;
      let running = 0;
      let done = 0;
      let error = 0;
      let idle = 0;
      let unreachable = 0;
      let minStart: number | null = null;
      let maxEnd: number | null = null;
      let anyRunning = false;
      let anyStarted = false;

      // CPU aggregation — the key signal for "is this CPU-bound?".
      const cpuPcts: number[] = [];
      let cpuSum = 0;
      let cpuMax = 0;
      // Throttle aggregation — the tiebreaker when CPU is high but not pinned.
      const throttlePcts: number[] = [];
      let throttleSum = 0;
      let throttleMax = 0;
      let anyThrottled = false;

      const shards = results.map((r) => {
        if (!r.reachable) {
          unreachable++;
          return {
            index: r.index,
            reachable: false,
            containerState: r.containerState,
            error: r.error,
          };
        }
        const s = r.status;
        anyStarted = anyStarted || s.startedAt !== null;
        // Only accumulate bytes when a run has been kicked off (agent
        // reports startedAt). Freshly-reset containers have startedAt=null
        // and bytes=0, but this guard also defends against reading a
        // partially-reset snapshot where bytes hasn't been zeroed yet --
        // no run has started for this session, so bytes cannot yet be
        // meaningful. Prevents phantom "downloaded" figures during the
        // warmup phase for containers reused from previous runs.
        if (s.startedAt !== null) {
          totalBytes += s.bytes;
          liveErrors += s.errors;
        }
        if (s.state === "running") { running++; anyRunning = true; }
        else if (s.state === "done") done++;
        else if (s.state === "error") error++;
        else idle++;
        if (s.startedAt) minStart = minStart === null ? s.startedAt : Math.min(minStart, s.startedAt);
        if (s.finishedAt) maxEnd = maxEnd === null ? s.finishedAt : Math.max(maxEnd, s.finishedAt);
        if (s.cpu && typeof s.cpu.pctOfQuota === "number") {
          cpuPcts.push(s.cpu.pctOfQuota);
          cpuSum += s.cpu.pctOfQuota;
          if (s.cpu.pctOfQuota > cpuMax) cpuMax = s.cpu.pctOfQuota;
        }
        if (s.cpu && typeof s.cpu.throttledPctOfWindow === "number") {
          throttlePcts.push(s.cpu.throttledPctOfWindow);
          throttleSum += s.cpu.throttledPctOfWindow;
          if (s.cpu.throttledPctOfWindow > throttleMax) throttleMax = s.cpu.throttledPctOfWindow;
          if ((s.cpu.dNrThrottled ?? 0) > 0) anyThrottled = true;
        }
        return { index: r.index, reachable: true, ...s };
      });

      // p95 CPU across shards. On 1 vCPU, p95 >= ~85% means CPU-bound.
      let cpuMean: number | null = null;
      let cpuP95: number | null = null;
      if (cpuPcts.length > 0) {
        cpuMean = cpuSum / cpuPcts.length;
        const sorted = [...cpuPcts].sort((a, b) => a - b);
        const idx = Math.min(sorted.length - 1, Math.floor(0.95 * sorted.length));
        cpuP95 = sorted[idx];
      }
      // Throttle p95: % of the sample window we spent CFS-throttled.
      // If this is materially > 0 the container is CPU-capped even if
      // pctOfQuota looks moderate. The single most decision-relevant number.
      let throttleMean: number | null = null;
      let throttleP95: number | null = null;
      if (throttlePcts.length > 0) {
        throttleMean = throttleSum / throttlePcts.length;
        const sorted = [...throttlePcts].sort((a, b) => a - b);
        const idx = Math.min(sorted.length - 1, Math.floor(0.95 * sorted.length));
        throttleP95 = sorted[idx];
      }

      // Timing: while any shard is still running, use wallclock elapsed for
      // a live ETA. Once all have a finishedAt, switch to the authoritative
      // max(finishedAt) - min(startedAt).
      const now = Date.now() / 1000;
      const allFinished = anyStarted && !anyRunning && running === 0 && done + error > 0;
      let elapsed = 0;
      if (minStart !== null) {
        elapsed = allFinished && maxEnd !== null ? maxEnd - minStart : now - minStart;
      }
      const aggBps = elapsed > 0 ? totalBytes / elapsed : 0;

      return Response.json({
        containerCount: n,
        counts: { running, done, error, idle, unreachable },
        totalBytes,
        totalBytesTarget: n * FILES_PER_CONTAINER * FILE_SIZE_BYTES,
        elapsed,
        aggregateBytesPerSec: aggBps,
        aggregateGbps: (aggBps * 8) / 1e9,
        errors: liveErrors,
        cpu: {
          samples: cpuPcts.length,
          meanPct: cpuMean,
          p95Pct: cpuP95,
          maxPct: cpuPcts.length > 0 ? cpuMax : null,
        },
        throttle: {
          samples: throttlePcts.length,
          meanPctOfWindow: throttleMean,
          p95PctOfWindow: throttleP95,
          maxPctOfWindow: throttlePcts.length > 0 ? throttleMax : null,
          anyPeriodsThrottled: anyThrottled,
        },
        allFinished,
        shards,
      });
    }

    if (url.pathname === "/api/reset" && request.method === "POST") {
      const body = (await request.json().catch(() => ({}))) as {
        containerCount?: number;
        hard?: boolean;
      };
      const n = Math.max(1, Math.min(MAX_CONTAINERS, body.containerCount ?? MAX_CONTAINERS));
      const hard = !!body.hard;
      const results = await Promise.all(
        Array.from({ length: n }, (_, i) => resetShard(env, i, hard)),
      );
      return Response.json({
        containerCount: n,
        hard,
        ok: results.filter((r) => r.ok).length,
        failed: results.filter((r) => !r.ok),
      });
    }

    return new Response("not found", { status: 404 });
  },
};
