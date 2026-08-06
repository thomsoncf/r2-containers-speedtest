#!/usr/bin/env node
/**
 * Seed the R2 bucket with 3000 objects of exactly 500,000,000 bytes each
 * (total: exactly 1,500,000,000,000 bytes = 1.5 TB).
 *
 * Strategy: exactly one PutObject (upload from the Jellyfin test video,
 * truncated to 500 MB) as `seed/base.bin`, then 2999 server-side CopyObject
 * calls to produce `bench/file-0000.bin` … `bench/file-2999.bin`.
 *
 * Rationale: CopyObject is a Class A op that runs entirely server-side, so
 * no bandwidth from the local machine and the whole thing runs in seconds
 * to minutes rather than hours. 500 MB < 5 GiB single-part copy limit.
 *
 * The script is idempotent and resumable — reruns only fill in what's
 * missing (HEAD before every upload/copy).
 *
 * Usage:
 *   node scripts/seed.mjs           # seed to target state
 *   node scripts/seed.mjs --verify  # verify only, no writes
 *
 * Env:
 *   Reads .dev.vars automatically (R2_ACCOUNT_ID, R2_BUCKET,
 *   R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY).
 */

import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import {
  S3Client,
  HeadObjectCommand,
  CopyObjectCommand,
  PutObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..");

// Minimal .dev.vars loader: KEY="value" or KEY=value.
function loadDevVars() {
  const p = join(repoRoot, ".dev.vars");
  if (!existsSync(p)) return;
  for (const raw of readFileSync(p, "utf8").split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const eq = line.indexOf("=");
    if (eq < 0) continue;
    const k = line.slice(0, eq).trim();
    let v = line.slice(eq + 1).trim();
    if ((v.startsWith('"') && v.endsWith('"')) || (v.startsWith("'") && v.endsWith("'"))) {
      v = v.slice(1, -1);
    }
    if (!(k in process.env)) process.env[k] = v;
  }
}

loadDevVars();

const {
  R2_ACCOUNT_ID,
  R2_BUCKET,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
} = process.env;

for (const [k, v] of Object.entries({ R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY })) {
  if (!v) {
    console.error(`missing env var ${k} (set in .dev.vars or shell)`);
    process.exit(2);
  }
}

const VERIFY_ONLY = process.argv.includes("--verify");

const TOTAL_FILES = 3000;
const FILE_SIZE = 500_000_000; // exactly 500 MB
const SEED_KEY = "seed/base.bin";
const PREFIX = "bench/";
const KEY = (i) => `${PREFIX}file-${String(i).padStart(4, "0")}.bin`;

const SRC_URL = "https://syd1.mirror.jellyfin.org/test-videos/HDR/HDR10/HEVC/Test%20Jellyfin%208K%20HEVC%20HDR10%20150M.mp4";

const s3 = new S3Client({
  region: "auto",
  endpoint: `https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: { accessKeyId: R2_ACCESS_KEY_ID, secretAccessKey: R2_SECRET_ACCESS_KEY },
  forcePathStyle: false,
});

async function headSize(key) {
  try {
    const r = await s3.send(new HeadObjectCommand({ Bucket: R2_BUCKET, Key: key }));
    return r.ContentLength ?? 0;
  } catch (e) {
    if (e.$metadata?.httpStatusCode === 404 || e.name === "NotFound" || e.name === "NoSuchKey") return null;
    throw e;
  }
}

async function uploadSeed() {
  const existing = await headSize(SEED_KEY);
  if (existing === FILE_SIZE) {
    console.log(`[seed] ok: ${SEED_KEY} already ${FILE_SIZE} B`);
    return;
  }
  console.log(`[seed] fetching first ${FILE_SIZE} B from Jellyfin…`);
  const resp = await fetch(SRC_URL, { headers: { Range: `bytes=0-${FILE_SIZE - 1}` } });
  if (!resp.ok && resp.status !== 206) {
    throw new Error(`Jellyfin fetch failed: ${resp.status}`);
  }
  const buf = Buffer.from(await resp.arrayBuffer());
  if (buf.length !== FILE_SIZE) throw new Error(`expected ${FILE_SIZE} bytes, got ${buf.length}`);
  console.log(`[seed] putting ${SEED_KEY} (${buf.length} B)…`);
  await s3.send(new PutObjectCommand({
    Bucket: R2_BUCKET, Key: SEED_KEY, Body: buf,
    ContentType: "application/octet-stream",
  }));
  console.log(`[seed] done.`);
}

async function copyOne(i) {
  const key = KEY(i);
  const size = await headSize(key);
  if (size === FILE_SIZE) return { i, skipped: true };
  await s3.send(new CopyObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    CopySource: `/${R2_BUCKET}/${SEED_KEY}`,
    MetadataDirective: "REPLACE",
    ContentType: "application/octet-stream",
  }));
  return { i, skipped: false };
}

async function seedAll() {
  console.log(`[copy] target: ${TOTAL_FILES} × ${FILE_SIZE} B objects at ${PREFIX}file-XXXX.bin`);
  const CONCURRENCY = 32;
  let done = 0;
  let copied = 0;
  let skipped = 0;
  let errors = 0;
  let nextIndex = 0;

  const workers = Array.from({ length: CONCURRENCY }, async () => {
    while (true) {
      const i = nextIndex++;
      if (i >= TOTAL_FILES) return;
      try {
        const r = await copyOne(i);
        if (r.skipped) skipped++; else copied++;
      } catch (e) {
        errors++;
        console.error(`[copy] ${KEY(i)} failed:`, e.name, e.message);
      }
      done++;
      if (done % 25 === 0 || done === TOTAL_FILES) {
        process.stdout.write(`\r[copy] ${done}/${TOTAL_FILES} (copied=${copied} skipped=${skipped} errors=${errors})`);
      }
    }
  });
  await Promise.all(workers);
  process.stdout.write("\n");
  console.log(`[copy] finished: copied=${copied} skipped=${skipped} errors=${errors}`);
  if (errors > 0) process.exit(1);
}

async function verify() {
  console.log(`[verify] listing ${PREFIX}…`);
  let count = 0;
  let bytes = 0n;
  let cont;
  do {
    const r = await s3.send(new ListObjectsV2Command({
      Bucket: R2_BUCKET, Prefix: PREFIX, ContinuationToken: cont,
    }));
    for (const o of r.Contents ?? []) {
      count++;
      bytes += BigInt(o.Size ?? 0);
    }
    cont = r.IsTruncated ? r.NextContinuationToken : undefined;
  } while (cont);
  const expectedBytes = BigInt(TOTAL_FILES) * BigInt(FILE_SIZE);
  console.log(`[verify] objects=${count} bytes=${bytes} (expected ${TOTAL_FILES} / ${expectedBytes})`);
  if (count !== TOTAL_FILES) console.warn(`[verify] WRONG COUNT`);
  if (bytes !== expectedBytes) console.warn(`[verify] WRONG BYTES`);
  return { count, bytes };
}

const t0 = Date.now();
if (!VERIFY_ONLY) {
  await uploadSeed();
  await seedAll();
}
await verify();
console.log(`[done] ${((Date.now() - t0) / 1000).toFixed(1)}s`);
