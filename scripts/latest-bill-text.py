#!/usr/bin/env python3
# scripts/latest-bill-text.py
# Build one most-recent text-version data.json per bill into latest_billtext/

import json
from pathlib import Path
from datetime import datetime, timezone
import shutil

base = Path("data")
out = Path("latest_billtext")  # <- matches workflow upload path
if out.exists():
    shutil.rmtree(out)
out.mkdir(parents=True, exist_ok=True)

def parse_date(s: str):
    """Parse ISO-ish dates from bill text JSON ('issued_on'/'issued'/'date').
    Returns a timezone-aware UTC datetime, or None."""
    if not s or not isinstance(s, str):
        return None
    try:
        # Handle trailing Z by converting to +00:00 so fromisoformat accepts it
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    except Exception:
        # Fall back to YYYY-MM-DD if present
        try:
            dt = datetime.fromisoformat(s.split("T")[0])
        except Exception:
            return None
    # Normalize to aware UTC
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

def mtime_dt(p: Path):
    """File mtime as aware UTC datetime (stable tiebreaker)."""
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)

for congress_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
    bills_root = congress_dir / "bills"
    if not bills_root.exists():
        continue
    for bill_dir in sorted([p for p in bills_root.iterdir() if p.is_dir()]):
        tv_root = bill_dir / "text-versions"
        if not tv_root.exists():
            print(f"no text versions for {congress_dir.name}/{bill_dir.name}")
            continue

        best = None  # (issued_dt, tie_dt, path)
        for ver in sorted([p for p in tv_root.iterdir() if p.is_dir()]):
            dataf = ver / "data.json"
            if not dataf.is_file():
                continue
            issued_dt = None
            try:
                with open(dataf, "r") as fh:
                    obj = json.load(fh)
                issued_dt = parse_date(obj.get("issued_on") or obj.get("issued") or obj.get("date"))
            except Exception:
                issued_dt = None
            if issued_dt is None:
                issued_dt = mtime_dt(dataf)
            tie_dt = mtime_dt(dataf)
            cand = (issued_dt, tie_dt, dataf)
            if best is None or cand > best:
                best = cand

        if best:
            dest = out / congress_dir.name / "bills" / bill_dir.name
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(best[2], dest / "data.json")
            print(f"picked {best[2]} -> {dest/'data.json'}")
