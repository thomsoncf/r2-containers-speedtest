#!/usr/bin/env python3
"""
Bench agent: supervises `rclone copy r2:BUCKET :memory,discard:` and exposes
a tiny HTTP API used by the Worker.

Design notes
------------
- Single-file stdlib only. No pip installs, no framework, no async.
- ThreadingHTTPServer so `/status` polls don't block the rclone reader thread.
- rclone runs as a subprocess in its own process group so we can kill the
  whole tree cleanly on /reset.
- Stats are parsed from rclone's NDJSON (`--use-json-log --stats 1s`). We keep
  the last `stats` object as the source of truth for `/status`.
- We deliberately do NOT block the /start request on the download completing.
  It spawns the worker and returns 202 immediately; the caller polls /status.
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ----- cgroup v2 CPU monitor ------------------------------------------------
#
# Question we're answering: is rclone CPU-bound on the 1 vCPU quota, or is it
# bandwidth/concurrency-bound? To decide, we sample the cgroup's cumulative
# CPU-time counter each time /status is polled and compute the delta.


class CpuMonitor:
    """Sample cgroup v2 CPU usage and expose per-poll deltas.

    Cloudflare Containers use cgroup v2. Files of interest:
      /sys/fs/cgroup/cpu.max      "quota period" microseconds (or "max period")
      /sys/fs/cgroup/cpu.stat     `usage_usec <N>` cumulative CPU time
      /sys/fs/cgroup/pids.current process count in the cgroup

    All reads are best-effort; missing files return None and the stats are
    simply omitted from /status output.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.last_ts: float | None = None
        self.last_usage_us: int | None = None
        # Delta counters for throttle stats -- the whole point of this class.
        self.last_nr_periods: int | None = None
        self.last_nr_throttled: int | None = None
        self.last_throttled_us: int | None = None
        self.vcpus: float | None = self._read_vcpus()
        self.host_cores: int = os.cpu_count() or 1
        # If Tokio-style oversubscription becomes a factor with obstore later,
        # we'll want to see the OS's view of parallelism too.
        try:
            import ctypes
            libc = ctypes.CDLL(None)
            self.affinity_cores = self._read_affinity(libc)
        except Exception:
            self.affinity_cores = None

    def _read_affinity(self, libc) -> int | None:
        """sched_getaffinity() count -- what the process sees as available parallelism."""
        try:
            return len(os.sched_getaffinity(0))  # type: ignore[attr-defined]
        except Exception:
            return None

    def _read_vcpus(self) -> float | None:
        try:
            with open("/sys/fs/cgroup/cpu.max") as f:
                parts = f.read().strip().split()
            if len(parts) == 2 and parts[0] != "max":
                return int(parts[0]) / int(parts[1])
        except Exception:
            pass
        return None

    def _read_cpu_stat(self) -> dict[str, int]:
        """Return all key=value pairs from /sys/fs/cgroup/cpu.stat.

        Fields we care about (cgroup v2):
          usage_usec       cumulative CPU time
          nr_periods       CFS bandwidth enforcement periods
          nr_throttled     periods where the cgroup was throttled
          throttled_usec   total time spent throttled

        If throttled_usec / (delta * effective_vcpus) is > a few percent, or
        nr_throttled is climbing, CPU is being capped and looks like network.
        """
        out: dict[str, int] = {}
        try:
            with open("/sys/fs/cgroup/cpu.stat") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) == 2:
                        try:
                            out[parts[0]] = int(parts[1])
                        except ValueError:
                            pass
        except Exception:
            pass
        return out

    def _read_usage_us(self) -> int | None:
        stat = self._read_cpu_stat()
        return stat.get("usage_usec")

    def _read_thread_count(self) -> int | None:
        # cgroup v2 pids.current is the total task count (procs + threads).
        try:
            with open("/sys/fs/cgroup/pids.current") as f:
                return int(f.read().strip())
        except Exception:
            pass
        # Fallback: agent process only.
        try:
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("Threads:"):
                        return int(line.split()[1])
        except Exception:
            pass
        return None

    def sample(self) -> dict:
        """Return current CPU utilization + throttle stats since the previous sample."""
        now = time.monotonic()
        stat = self._read_cpu_stat()
        usage = stat.get("usage_usec")
        nr_periods = stat.get("nr_periods")
        nr_throttled = stat.get("nr_throttled")
        throttled_us = stat.get("throttled_usec")
        threads = self._read_thread_count()

        cores_used: float | None = None
        pct_of_quota: float | None = None
        # Throttle deltas over the poll window.
        d_nr_periods: int | None = None
        d_nr_throttled: int | None = None
        d_throttled_us: int | None = None
        throttled_pct_of_window: float | None = None
        throttled_ratio: float | None = None

        # Effective vCPU denominator, in preferred order:
        #   1. cpu.max quota (if present as "N period")
        #   2. sched_getaffinity() count (what the process actually sees)
        #   3. os.cpu_count() (host, worst case)
        effective_vcpus: float | None = (
            self.vcpus
            if self.vcpus is not None
            else (float(self.affinity_cores) if self.affinity_cores else float(self.host_cores))
        )

        with self.lock:
            last_ts = self.last_ts
            last_usage = self.last_usage_us
            last_nr_periods = self.last_nr_periods
            last_nr_throttled = self.last_nr_throttled
            last_throttled_us = self.last_throttled_us

            if usage is not None and last_usage is not None and last_ts is not None:
                dt = now - last_ts
                du = usage - last_usage
                if dt > 0 and du >= 0:
                    cores_used = (du / 1_000_000.0) / dt
                    if effective_vcpus and effective_vcpus > 0:
                        pct_of_quota = 100.0 * cores_used / effective_vcpus

                if nr_periods is not None and last_nr_periods is not None:
                    d_nr_periods = nr_periods - last_nr_periods
                if nr_throttled is not None and last_nr_throttled is not None:
                    d_nr_throttled = nr_throttled - last_nr_throttled
                if throttled_us is not None and last_throttled_us is not None:
                    d_throttled_us = throttled_us - last_throttled_us
                    if dt > 0:
                        # Wall-clock share of the sample window we spent throttled.
                        throttled_pct_of_window = 100.0 * (d_throttled_us / 1_000_000.0) / dt
                    if d_nr_periods is not None and d_nr_periods > 0 and d_nr_throttled is not None:
                        throttled_ratio = 100.0 * d_nr_throttled / d_nr_periods

            self.last_ts = now
            self.last_usage_us = usage
            self.last_nr_periods = nr_periods
            self.last_nr_throttled = nr_throttled
            self.last_throttled_us = throttled_us

        return {
            "coresUsed": cores_used,
            "pctOfQuota": pct_of_quota,
            "vcpus": self.vcpus,
            "effectiveVcpus": effective_vcpus,
            "hostCores": self.host_cores,
            "affinityCores": self.affinity_cores,
            "cgroupThreads": threads,
            # Cumulative counters -- useful for a full-run diff.
            "cumUsageUs": usage,
            "cumNrPeriods": nr_periods,
            "cumNrThrottled": nr_throttled,
            "cumThrottledUs": throttled_us,
            # Deltas over the last sample window.
            "dNrPeriods": d_nr_periods,
            "dNrThrottled": d_nr_throttled,
            "dThrottledUs": d_throttled_us,
            # The two most decision-relevant derived numbers.
            "throttledPctOfWindow": throttled_pct_of_window,
            "throttledPeriodRatio": throttled_ratio,
        }


CPU = CpuMonitor()

# ----- config from env ------------------------------------------------------

BUCKET = os.environ["R2_BUCKET"]  # required
LISTEN_PORT = int(os.environ.get("PORT", "8080"))

# Cached once at boot. Best-effort proxy for the container's egress colo,
# used only as a hint in the UI to confirm placement.
_COLO = "unknown"


def probe_colo() -> str:
    try:
        with urllib.request.urlopen(
            "https://cloudflare.com/cdn-cgi/trace", timeout=3
        ) as r:
            for line in r.read().decode().splitlines():
                if line.startswith("colo="):
                    return line.split("=", 1)[1].strip()
    except Exception:
        pass
    return "unknown"


# ----- shared state ---------------------------------------------------------


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.state: str = "idle"  # idle | running | done | error
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.bytes: int = 0
        self.elapsed: float = 0.0
        self.speed: float = 0.0
        self.errors: int = 0
        self.total_bytes: int = 0
        self.total_transfers: int = 0
        self.transfers_done: int = 0
        self.last_msg: str = ""
        self.exit_code: int | None = None
        self.rclone_args: list[str] = []

    def snapshot(self) -> dict:
        with self.lock:
            snap = {
                "state": self.state,
                "colo": _COLO,
                "startedAt": self.started_at,
                "finishedAt": self.finished_at,
                "bytes": self.bytes,
                "elapsed": self.elapsed,
                "speed": self.speed,
                "errors": self.errors,
                "totalBytes": self.total_bytes,
                "totalTransfers": self.total_transfers,
                "transfersDone": self.transfers_done,
                "lastMsg": self.last_msg,
                "exitCode": self.exit_code,
                "rcloneArgs": self.rclone_args,
            }
        # CPU sample is computed outside the state lock so it never blocks
        # on rclone stdout parsing. The CpuMonitor has its own lock.
        snap["cpu"] = CPU.sample()
        return snap


STATE = State()
PROC_LOCK = threading.Lock()
PROC: subprocess.Popen | None = None
READER_THREAD: threading.Thread | None = None


# ----- rclone supervisor ----------------------------------------------------


def rclone_env() -> dict:
    """Build the env for rclone: parent env + R2 remote config."""
    e = os.environ.copy()
    # RCLONE_CONFIG_<REMOTE>_<OPT> — no config file needed.
    e["RCLONE_CONFIG_R2_TYPE"] = "s3"
    e["RCLONE_CONFIG_R2_PROVIDER"] = "Cloudflare"
    e["RCLONE_CONFIG_R2_REGION"] = "auto"
    e["RCLONE_CONFIG_R2_ENDPOINT"] = (
        f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
    )
    e["RCLONE_CONFIG_R2_ACCESS_KEY_ID"] = os.environ["R2_ACCESS_KEY_ID"]
    e["RCLONE_CONFIG_R2_SECRET_ACCESS_KEY"] = os.environ["R2_SECRET_ACCESS_KEY"]
    # Suppress the "config file not found" info line.
    e["RCLONE_CONFIG"] = "/dev/null"
    return e


def spawn_rclone(files: list[str], transfers: int, disable_http2: bool) -> None:
    """Write --files-from list, spawn rclone, start reader thread."""
    global PROC, READER_THREAD

    files_from = "/tmp/files.txt"
    with open(files_from, "w") as f:
        f.write("\n".join(files))
        f.write("\n")

    args = [
        "/usr/local/bin/rclone",
        "copy",
        f"r2:{BUCKET}",
        ":memory,discard:",
        "--files-from", files_from,
        "--transfers", str(transfers),
        "--checkers", "32",
        # 1 MiB unit x 64 = 64 MiB of async read buffer per transfer.
        # NOT --buffer-size 0: that degrades to a 32 KiB io.Copy staging buffer
        # off the TLS socket, ~32x more syscalls per byte.
        "--buffer-size", "64Mi",
        # Discard sink computes MD5; --ignore-checksum skips the *comparison*
        # only, not the hashing. Kept because R2 multipart ETags aren't MD5
        # and would otherwise produce spurious errors.
        "--ignore-checksum",
        "--no-check-dest",
        "--no-traverse",
        "--retries", "1",
        "--low-level-retries", "3",
        "--use-json-log",
        "--stats", "1s",
        "--stats-log-level", "NOTICE",
    ]
    if disable_http2:
        # Forces HTTP/1.1: one TCP connection per in-flight GET, instead of
        # all N transfers multiplexed onto a single connection.
        args.append("--s3-disable-http2")

    with PROC_LOCK:
        with STATE.lock:
            STATE.rclone_args = args[3:]  # skip binary + `copy` + src for brevity
            STATE.started_at = time.time()
            STATE.finished_at = None
            STATE.state = "running"
            STATE.exit_code = None

        PROC = subprocess.Popen(
            args,
            env=rclone_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            preexec_fn=os.setsid,  # own process group -> clean kill on reset
        )

    READER_THREAD = threading.Thread(target=read_rclone_output, daemon=True)
    READER_THREAD.start()


def read_rclone_output() -> None:
    """Parse rclone's NDJSON stream line by line, update shared state."""
    assert PROC is not None
    assert PROC.stdout is not None
    for line in PROC.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            with STATE.lock:
                STATE.last_msg = line[:400]
            continue
        stats = obj.get("stats")
        if isinstance(stats, dict):
            with STATE.lock:
                STATE.bytes = int(stats.get("bytes", 0))
                STATE.elapsed = float(stats.get("elapsedTime", 0.0))
                STATE.speed = float(stats.get("speed", 0.0))
                STATE.errors = int(stats.get("errors", 0))
                STATE.total_bytes = int(stats.get("totalBytes", 0))
                STATE.total_transfers = int(stats.get("totalTransfers", 0))
                STATE.transfers_done = int(stats.get("transfers", 0))
        else:
            msg = obj.get("msg", "")
            if msg:
                with STATE.lock:
                    STATE.last_msg = msg[:400]

    rc = PROC.wait()
    with STATE.lock:
        STATE.exit_code = rc
        STATE.finished_at = time.time()
        STATE.state = "done" if rc == 0 and STATE.errors == 0 else "error"


def kill_rclone() -> None:
    """Kill the rclone process group. Idempotent."""
    global PROC
    with PROC_LOCK:
        if PROC is not None and PROC.poll() is None:
            try:
                os.killpg(os.getpgid(PROC.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                PROC.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        PROC = None


# ----- HTTP handler ---------------------------------------------------------


def _json(handler: BaseHTTPRequestHandler, code: int, body: dict) -> None:
    payload = json.dumps(body).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(payload)))
    handler.end_headers()
    handler.wfile.write(payload)


class Handler(BaseHTTPRequestHandler):
    # Suppress the default "GET /status 200 -" access log spam.
    def log_message(self, *_args) -> None:  # noqa: D401
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            _json(self, 200, {"ok": True, "colo": _COLO})
            return
        if self.path == "/status":
            _json(self, 200, STATE.snapshot())
            return
        _json(self, 404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw.decode()) if raw else {}
        except json.JSONDecodeError:
            _json(self, 400, {"error": "bad json"})
            return

        if self.path == "/start":
            files = body.get("files") or []
            transfers = int(body.get("transfers", 20))
            disable_http2 = bool(body.get("disableHttp2", True))
            if not files:
                _json(self, 400, {"error": "no files"})
                return
            with PROC_LOCK:
                if PROC is not None and PROC.poll() is None:
                    _json(self, 409, {"error": "already running"})
                    return
            # Fresh run counters, but keep colo.
            STATE.reset()
            try:
                spawn_rclone(files, transfers, disable_http2)
            except Exception as e:
                _json(self, 500, {"error": str(e)})
                return
            _json(self, 202, {"accepted": True, "files": len(files), "transfers": transfers})
            return

        if self.path == "/reset":
            kill_rclone()
            STATE.reset()
            _json(self, 200, {"ok": True})
            return

        _json(self, 404, {"error": "not found"})


def main() -> None:
    global _COLO
    _COLO = probe_colo()
    # Prime CPU monitor so the first /status has a valid delta window.
    CPU.sample()
    boot_cpu = CPU.sample()
    print(
        f"agent: colo={_COLO} bucket={BUCKET} port={LISTEN_PORT} "
        f"vcpus={boot_cpu['vcpus']} hostCores={boot_cpu['hostCores']} "
        f"affinity={boot_cpu['affinityCores']} threads={boot_cpu['cgroupThreads']}",
        flush=True,
    )
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        kill_rclone()


if __name__ == "__main__":
    main()
