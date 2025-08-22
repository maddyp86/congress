#!/usr/bin/env python3
# scripts/latest-bill-text.py
# Build a distilled latest_billtext/ tree: one data.json (most-recent) per bill.

import json
import pathlib
import shutil
from datetime import datetime, timezone

base = pathlib.Path("data")
out = pathlib.Path("latest_billtext")

# Remove old output and recreate
if out.exists():
    shutil.rmtree(out)
out.mkdir(parents=True, exist_ok=True)

def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            return datetime.fromisoformat(s2)
        return datetime.fromisoformat(s)
    except Exception:
        try:
            # fallback: parse date-only forms
            return datetime.fromisoformat(s.split("T")[0])
        except Exception:
            return None

def mtime_dt(p: pathlib.Path):
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)

# find all candidate data.json files under any text-versions folder
candidates = list(base.rglob("**/text-versions/*/data.json"))

# Organize by (congress, bill_id)
groups = {}  # key -> list of pathlib.Path objects
for p in candidates:
    parts = p.parts
    # find where 'text-versions' occurs
    if "text-versions" not in parts:
        continue
    ti = parts.index("text-versions")
    # bill_id is two parts before text-versions (e.g., .../<type>/<bill_id>/text-versions/...)
    if ti >= 2:
        congress = parts[1] if parts[0] == "data" else parts[0]
        bill_id = parts[ti - 2]
        key = f"{congress}/{bill_id}"
        groups.setdefault(key, []).append(p)
    else:
        # unexpected layout: skip
        continue

# For each bill, pick the best data.json
picked = 0
for key, plist in sorted(groups.items()):
    best = None  # tuple (dt_primary, dt_tie, path)
    for p in plist:
        dt_primary = None
        try:
            with p.open("r", encoding="utf-8") as fh:
                obj = json.load(fh)
            dt_primary = (obj.get("issued_on") or obj.get("issued") or obj.get("date"))
            dt_primary = parse_date(dt_primary)
        except Exception:
            dt_primary = None

        if dt_primary is None:
            dt_primary = mtime_dt(p)

        tie = mtime_dt(p)
        cand = (dt_primary, tie, p)
        if best is None or (cand[0] > best[0]) or (cand[0] == best[0] and cand[1] > best[1]):
            best = cand

    if best:
        dt, tie, best_path = best
        # prepare destination: latest_billtext/<congress>/bills/<bill_id>/data.json
        congress, bill_id = key.split("/", 1)
        dest = out / congress / "bills" / bill_id
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(best_path, dest / "data.json")
        picked += 1
        print(f"picked {best_path} -> {dest/'data.json'}")
    else:
        print(f"no text versions for {key}")

print(f"done: picked {picked} bills")
