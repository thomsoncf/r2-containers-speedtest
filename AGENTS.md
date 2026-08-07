# AGENTS.md

Repo-level notes for agents working in `~/Projects/r2-containers-test`.
This repo is intentionally **standalone** — it lives under `~/Projects/`
but is **not** part of the pnpm/Turbo workspace in `~/Projects/apps/*`.
The parent `~/Projects/AGENTS.md` (workspace conventions, Biome, Turbo,
pnpm) does **not** apply here.

## What this is

A benchmark harness that measures Cloudflare R2 download throughput from
up to 200 Cloudflare Containers placed across the ENAM region. Each
container runs `rclone copy r2:BUCKET :memory,discard:` over a shard of
20 pre-seeded 500 MB objects (10 GB per container). A Worker fans out
the shards, aggregates per-shard stats, and serves the dashboard.

The R2 bucket is seeded with **3000 × 500 MB objects = exactly 1.5 TB**
(all sharing the same underlying content via server-side CopyObject).
Different containers randomly sample the pool so the fleet can scale
past 150 without reseeding.

## Toolchain

- **npm** (not pnpm) — this repo has its own `package.json` and
  `package-lock.json`. `wrangler` and `@cloudflare/workers-types` are
  local devDeps, not root-shared.
- **wrangler** invoked via `npx wrangler …` or `npm run …`.
- **License:** Unlicense.
- **TypeScript:** loose. Two pre-existing generic-parameter errors at
  `src/index.ts:79` and `src/index.ts:185` (`@cloudflare/containers` vs.
  `workers-types` drift) — do **not** block `wrangler deploy` on these.
- **No Biome, no Turbo, no Vitest.** If you need to lint or test, add
  the tool explicitly; don't assume workspace defaults.

## Layout

```
r2-containers-test/
├── src/index.ts         Worker + BenchContainer DO class. All routing,
│                        warmup pacing, shard fan-out, status aggregation.
├── public/index.html    Single-file dashboard (HTML + inline JS + CSS).
├── container/
│   ├── Dockerfile       python:3.12-alpine + rclone binary from
│   │                    rclone/rclone:latest. linux/amd64 (wrangler default).
│   └── agent.py         Python stdlib HTTP agent. Supervises rclone,
│                        parses --use-json-log stats, exposes /status,
│                        /start, /reset, /health. Reads R2 creds from env.
├── scripts/seed.mjs     Seeder: 1 PutObject + 2999 CopyObject. Idempotent.
├── wrangler.jsonc       Container + DO config. Custom instance_type object.
├── package.json         npm scripts: seed, dev, deploy, logs.
├── .dev.vars.example    Template. Real .dev.vars is gitignored.
├── README.md            User-facing setup + teardown.
└── LICENSE              Unlicense (public domain).
```

## Architecture

```
Browser --------> Worker (src/index.ts) --------> BenchContainer DO x N
   |               |    - serves ./public assets       - Python agent :8080
   |               |    - /api/warmup, /api/start,     - supervises rclone
   |               |      /api/status, /api/reset      - reads STATE snapshot
   |               |    - shards keys, fans out
   +-- dashboard <-+                                   R2 bucket (ENAM)
       (index.html)                                    3000 x 500 MB objects
```

Per-request flow, benchmarking:

1. Frontend `GET /api/config` → static config for tile options.
2. **Warm phase.** User clicks *Warm containers* → `POST /api/warmup`.
   Worker calls `warmupFleet(env, n, WARMUP_RATE_PER_SEC=15)`:
   - **Phase 0 (`preResetUpContainers`):** for every container index
     `<n`, check `/__state` first. Only if `running`/`healthy`, POST
     `/reset` to the agent to zero `STATE.bytes`. This closes the
     phantom-bytes window where a container reused within `sleepAfter`
     still reports bytes from its previous run.
   - **Phase 1:** dispatch `/__ensureStart` + agent `/reset` per index at
     15/sec (batch, sleep 1s, repeat). Cold-start latencies overlap
     in flight. Returns `{warmed, failed, elapsedMs, preResetCount}`.
3. **Start phase.** User clicks *Start benchmark* → `POST /api/start`.
   No warmup — assumes containers already warm. `startAgent()` POSTs
   `/start` to each agent with `{files, transfers, disableHttp2}`.
   Agent runs `STATE.reset()` then `spawn_rclone()` in its own process
   group. Returns 202 immediately.
4. **Status polling.** Frontend polls `GET /api/status?n=…` every 1s.
   Worker calls `statusShard` per index — `/__state` first to check
   container is up, then `GET /status` on the agent for the snapshot.
   Aggregates bytes (only when `startedAt !== null`, another leak
   defense), CPU %, throttle %, run wallclock.
5. **Reset.** `POST /api/reset` calls agent `/reset` (soft, kills
   rclone + zeros state) or `/__destroy` (hard, tears down the DO).

## Runtime constants

Two independent sources of truth exist for the dataset shape. Keep
them in sync:

- `TOTAL_FILES = 3000`, `FILE_SIZE_BYTES = 500_000_000` in
  `src/index.ts:28-29`.
- `TOTAL_FILES = 3000`, `FILE_SIZE = 500_000_000` in
  `scripts/seed.mjs:76-77`.

Other important knobs, all in `src/index.ts`:

- `FILES_PER_CONTAINER = 20` — 10 GB per container.
- `CONTAINER_COUNTS = [1, 10, 25, 50, 100, 150, 200]` — the UI tiles.
  Not a hard whitelist; `/api/start` accepts any value in
  `[1, MAX_CONTAINERS]`.
- `MAX_CONTAINERS = 200` — must match `wrangler.jsonc` `max_instances`.
- `WARMUP_RATE_PER_SEC = 15` — the platform tolerates roughly this
  dispatch cadence; going faster produces schedule failures.

Client-side (`public/index.html`):

- `STALE_WARM_MS = 150_000` — 2m30s, buffer under `sleepAfter = 3m` so
  Start is blocked before containers can sleep.
- `resetThroughputHistory()` runs on warm and start; instantaneous Gbps
  is computed client-side as `Δbytes / Δt` between polls (not
  server-side, because the server's aggregate is a run-average that
  decays at the tail once fast containers hit their per-shard target).

## Container config gotcha

`wrangler.jsonc` uses a **fully-specified custom `instance_type` object**,
not the `"standard-4"` named alias:

```jsonc
"instance_type": { "vcpu": 4, "memory_mib": 12288, "disk_mb": 20000 }
```

Reason: switching from a custom object to the alias caused wrangler to
retain the previous `vcpu`/`memory_mib` fields in its API PATCH; the
Containers API prioritized those over the alias and containers came up
with 1 vCPU. The custom object avoids the ambiguity.

Related: if you change the instance_type shape, the containers app is
sometimes recreated (new DO id). The `BENCH` DO namespace is bound by
class name in `wrangler.jsonc`, so it survives.

## Auto-start subtlety

`Container.fetch()` from `@cloudflare/containers` **auto-starts** the
container. That's wrong for polling: hitting `/api/status` on a fleet
of 200 would spin every one up just from a dashboard refresh.

Fix in `BenchContainer.fetch` override (`src/index.ts:94`): three
control-plane paths short-circuit before the superclass's auto-start.

- `/__state` — read status without starting.
- `/__ensureStart` — explicit start + wait for port 8080.
- `/__destroy` — tear down.

The Worker uses these via `doControl()` (raw stub) for anything that
must not auto-start. `agentFetch()` uses `getContainer()` (which does
auto-start) and is only called after `/__state` confirms `running`.

## Agent design (agent.py)

- Single-file, Python stdlib only. `ThreadingHTTPServer` so `/status`
  doesn't block the rclone-output reader thread.
- rclone runs in its own **process group** (`preexec_fn=os.setsid`) so
  `/reset` can `killpg(SIGKILL)` the whole tree cleanly.
- Stats parsed from rclone's NDJSON (`--use-json-log --stats 1s`); the
  last `stats` object is the source of truth for `/status`.
- CPU sampler reads cgroup v2 (`cpu.max`, `cpu.stat`) to compute
  per-poll `pctOfQuota` and `throttledPctOfWindow`. This is the primary
  signal for CPU-boundness diagnosis.
- R2 creds come **only** from env (`R2_ACCOUNT_ID`, `R2_BUCKET`,
  `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`) injected by the DO's
  `this.envVars`. No config file, no request-body credentials.
- rclone args include `--buffer-size 64Mi` (do **not** set to `0`; that
  degrades to a 32 KiB staging buffer and ~32× the syscalls per byte).
- `--s3-disable-http2` forces HTTP/1.1, so N transfers = N TCP conns
  (better parallelism than one multiplexed H2 conn in this workload).
- `:memory,discard:` sink has no `OpenChunkWriter` / `OpenWriterAt`, so
  multi-thread ranged GETs are silently ignored by rclone. Don't
  bother with `--multi-thread-*` flags here.

## Commands

Run from the repo root.

| Command | Purpose |
|---|---|
| `npm install` | Install deps. |
| `npm run seed` | Seed 1.5 TB (1 PutObject + 2999 CopyObject). Idempotent. |
| `npm run seed:verify` | Verify seed without writes. |
| `npm run deploy` | `wrangler deploy` — builds container image + deploys. |
| `npm run deploy:dry` | `--dry-run`, no upload. |
| `npm run dev` | `wrangler dev`. Container support is limited locally. |
| `npm run logs` | `wrangler tail`. |

Wrangler needs `CLOUDFLARE_ACCOUNT_ID=<id>` in env if not set globally.
Container image builds require **Docker Desktop running**.

## Secrets

Local dev reads `.dev.vars` (gitignored). Deployed Worker needs the
same four vars set via `wrangler secret put`:

```
R2_ACCOUNT_ID
R2_BUCKET
R2_ACCESS_KEY_ID
R2_SECRET_ACCESS_KEY
```

`.dev.vars.example` has empty placeholder values; keep it committed.
Never commit real values. There's no `.env` fallback.

## What NOT to change without thinking

- **`TOTAL_FILES` / `FILE_SIZE_BYTES`.** Changing either in one file
  and not the other silently produces short shards or 404s. Reseeding
  1.5 TB is free bandwidth-wise (server-side copy) but wastes Class A
  ops and time.
- **The `preResetUpContainers` phase 0** in `warmupFleet`. Without it,
  status polls during the ~n/15 s warmup window sum stale STATE.bytes
  from reused containers → phantom "downloaded" numbers.
- **The `startedAt !== null` guard** in `/api/status` aggregation. Same
  problem, second line of defense.
- **The custom `instance_type` object.** See "Container config gotcha".
- **`sleepAfter = "3m"`.** Client `STALE_WARM_MS` (150 s) is tied to
  it. If you change one, change the other.

## Local dev caveats

- `wrangler dev` cannot fully emulate the Containers runtime. Warmup +
  status routes work against real DOs when deployed; locally they may
  return unexpected shapes. Prefer deploying to `workers.dev` for
  end-to-end tests.
- Docker must be running for any build that touches the container.
- Seeding runs against real R2; no local emulation. Use a scratch
  bucket if experimenting with the seed script.
