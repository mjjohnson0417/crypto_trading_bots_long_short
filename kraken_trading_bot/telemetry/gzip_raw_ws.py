"""Nightly compression of yesterday's raw WebSocket JSONL files.

Walks the raw_ws/ tree and gzips any `*.jsonl` whose filename date is
older than today's UTC date. Today's files are skipped so the live
logger doesn't race against us. After a successful gzip, the .jsonl is
deleted. If a .jsonl.gz already exists, the .jsonl is left alone (the
caller can re-run safely).

After compression, checks the disk usage of the raw_ws/ filesystem. If
it is at or above `telemetry.max_disk_usage_pct` in config.json, deletes
the oldest files (across both `.jsonl` and `.jsonl.gz`) one at a time
until usage drops below the threshold.

Typical crontab (runs at 03:15 UTC every day):
  15 3 * * * cd /home/jason/crypto_trading_bots/kraken_trading_bot \
    && venv/bin/python telemetry/gzip_raw_ws.py \
    >> logs/gzip_raw_ws.log 2>&1
"""
from __future__ import annotations

import argparse
import gzip
import json
import re
import shutil
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

DEFAULT_ROOT = Path("/mnt/externalssd/raw_ws")
FILENAME_RE = re.compile(r".+_(\d{8})\.jsonl$")
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"


def parse_date_from_filename(p: Path):
    m = FILENAME_RE.match(p.name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def gzip_file(src: Path) -> tuple[int, int]:
    """Stream-gzip src → src.gz. Returns (src_bytes, gz_bytes). Deletes src on success."""
    dst = src.with_suffix(src.suffix + ".gz")
    if dst.exists():
        # Don't overwrite an existing compressed file; leave the plain one for inspection.
        return (src.stat().st_size, dst.stat().st_size)

    src_bytes = src.stat().st_size
    tmp = dst.with_suffix(".tmp")
    try:
        with open(src, "rb") as fin, gzip.open(tmp, "wb", compresslevel=6) as fout:
            shutil.copyfileobj(fin, fout, length=1024 * 1024)
        tmp.rename(dst)
        gz_bytes = dst.stat().st_size
        src.unlink()
        return (src_bytes, gz_bytes)
    except Exception:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
        raise


def evict_oldest_until_under_threshold(root: Path, threshold_pct: float) -> None:
    """If disk usage at `root` is at/above `threshold_pct`, delete the
    oldest files (across `.jsonl` and `.jsonl.gz`) one at a time until
    usage is below threshold."""
    usage = shutil.disk_usage(root)
    used_pct = usage.used / usage.total * 100
    if used_pct < threshold_pct:
        print(f"[gzip_raw_ws] disk usage {used_pct:.1f}% < {threshold_pct}% — no eviction needed")
        return

    print(f"[gzip_raw_ws] disk usage {used_pct:.1f}% >= {threshold_pct}% — evicting oldest files")

    candidates = []
    for pattern in ("*.jsonl", "*.jsonl.gz"):
        for f in root.rglob(pattern):
            try:
                candidates.append((f.stat().st_mtime, f))
            except Exception:
                continue
    candidates.sort()  # oldest mtime first

    deleted = 0
    freed = 0
    for _mtime, f in candidates:
        usage = shutil.disk_usage(root)
        used_pct = usage.used / usage.total * 100
        if used_pct < threshold_pct:
            break
        try:
            size = f.stat().st_size
            f.unlink()
            deleted += 1
            freed += size
            print(f"[gzip_raw_ws] deleted {f.relative_to(root)}  ({size / 1024 / 1024:.1f} MB)")
        except Exception as e:
            print(f"[gzip_raw_ws] failed to delete {f}: {e}")

    print(f"[gzip_raw_ws] eviction done: deleted={deleted}  freed={freed / 1024 / 1024 / 1024:.2f} GB  usage now {used_pct:.1f}%")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(DEFAULT_ROOT),
                        help="Root containing channel subfolders (book/, trade/, ticker/)")
    parser.add_argument("--keep-today", action="store_true", default=True,
                        help="Skip files whose filename date is today UTC (always on).")
    args = parser.parse_args()

    root = Path(args.root)
    if not root.exists():
        print(f"[gzip_raw_ws] root does not exist: {root}")
        return 1

    today = datetime.now(timezone.utc).date()
    started = time.time()

    total_files = 0
    total_in = 0
    total_out = 0
    skipped_today = 0
    skipped_existing_gz = 0
    errors = 0

    for channel_dir in sorted(root.iterdir()):
        if not channel_dir.is_dir():
            continue
        for f in sorted(channel_dir.glob("*.jsonl")):
            file_date = parse_date_from_filename(f)
            if file_date is None:
                print(f"[gzip_raw_ws] skip (no date in name): {f.name}")
                continue
            if file_date >= today:
                skipped_today += 1
                continue
            gz = f.with_suffix(f.suffix + ".gz")
            if gz.exists():
                # Shouldn't happen if previous run cleaned up; treat as already done.
                skipped_existing_gz += 1
                continue
            try:
                src_bytes, gz_bytes = gzip_file(f)
                total_files += 1
                total_in += src_bytes
                total_out += gz_bytes
                ratio = (1 - gz_bytes / src_bytes) * 100 if src_bytes else 0
                print(f"[gzip_raw_ws] {f.relative_to(root)}: "
                      f"{src_bytes / 1024 / 1024:.1f} MB -> "
                      f"{gz_bytes / 1024 / 1024:.1f} MB  "
                      f"({ratio:.1f}% saved)")
            except Exception as e:
                errors += 1
                print(f"[gzip_raw_ws] ERROR {f.name}: {e}")

    elapsed = time.time() - started
    total_saved = total_in - total_out
    print(f"\n[gzip_raw_ws] done in {elapsed:.1f}s  "
          f"compressed={total_files}  "
          f"skipped_today={skipped_today}  "
          f"skipped_existing_gz={skipped_existing_gz}  "
          f"errors={errors}")
    if total_files:
        print(f"[gzip_raw_ws] total: {total_in / 1024 / 1024:.1f} MB -> "
              f"{total_out / 1024 / 1024:.1f} MB "
              f"(saved {total_saved / 1024 / 1024:.1f} MB, "
              f"{(total_saved / total_in) * 100:.1f}%)")

    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    threshold_pct = float(cfg["telemetry"]["max_disk_usage_pct"])
    evict_oldest_until_under_threshold(root, threshold_pct)

    return 0 if errors == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
