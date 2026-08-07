# r2-containers-test

Benchmark Cloudflare R2 download throughput from **up to 200 concurrent
Cloudflare Containers placed across the ENAM region**, backed by rclone
into an in-memory discard sink. Frontend served by the Worker; click
*Start* and watch the aggregate line.

The bucket is seeded with **3000 × 500 MB objects = exactly 1.5 TB**. Each
container downloads **20 files (10 GB) sampled at random from the pool**,
so different containers may pick the same key — this decouples the
per-container workload from the pool size and lets the fleet scale past
150 without reseeding. The Worker aggregates per-shard stats into a live
Gbps figure.

---

## Setup

Prereqs: Node 20+, Docker Desktop, an R2 bucket with a set of R2 API
credentials, and an account on the Workers Paid plan (Containers requires
it).

```bash
git clone <this repo> r2-containers-test
cd r2-containers-test
npm install

cp .dev.vars.example .dev.vars
# fill in R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
```

Push the same values as Worker secrets:

```bash
for k in R2_ACCOUNT_ID R2_BUCKET R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY; do
  npx wrangler secret put "$k"
done
```

Seed 1.5 TB into R2 (server-side, ~3001 Class A ops, no local bandwidth):

```bash
npm run seed
```

Deploy the Worker + Container. First deploy uploads the container image,
which takes a few minutes:

```bash
npm run deploy
```

Open the printed `https://r2-containers-test.<subdomain>.workers.dev/`
and click **Start benchmark**.

---

## What the deploy actually creates

- **1 Worker** (`r2-containers-test`) serving the frontend and `/api/*`
- **1 Container application** (`r2-speedtest-bench`), custom instance type
  `{vcpu:4, memory_mib:12288, disk_mb:20000}`, `max_instances: 200`,
  `constraints.regions: ["enam"]`
- **1 Durable Object class** (`BenchContainer`) with SQLite storage

Each container instance runs a small Python HTTP agent (see
`container/agent.py`) which supervises `rclone copy r2:BUCKET :memory,discard:`
and reports per-second stats parsed from rclone's NDJSON log.

---

## Note

Per-instance throughput scales with vCPU count

## Teardown

There is no teardown script — this is deliberate, to prevent an errant
click from wiping 1.5 TB mid-run. To reclaim storage:

```bash
# Delete all seeded objects (DeleteObject is free).
npx wrangler r2 object delete "$R2_BUCKET/seed/base.bin"
for i in $(seq 0 2999); do
  npx wrangler r2 object delete "$R2_BUCKET/bench/file-$(printf '%04d' $i).bin"
done
```

Or use `rclone delete r2:$R2_BUCKET/bench/` if you have rclone locally.

To remove the deployed Worker + Container:

```bash
npx wrangler delete
```
