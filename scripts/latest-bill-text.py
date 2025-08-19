# scripts/build_latest.py
import json, pathlib, shutil
from datetime import datetime, timezone

base = pathlib.Path("data")
out = pathlib.Path("latest_data")
if out.exists():
    shutil.rmtree(out)
out.mkdir(parents=True, exist_ok=True)

def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.strptime(s.split("T")[0], "%Y-%m-%d")
        except Exception:
            return None

def mtime_dt(p):
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)

for congress_dir in sorted(base.iterdir()):
    if not congress_dir.is_dir():
        continue
    bills_root = congress_dir / "bills"
    if not bills_root.exists():
        continue
    for bill_dir in sorted(bills_root.iterdir()):
        if not bill_dir.is_dir():
            continue
        tv_root = bill_dir / "text-versions"
        if not tv_root.exists():
            continue
        best = None
        for ver in sorted(tv_root.iterdir()):
            if not ver.is_dir():
                continue
            dataf = ver / "data.json"
            if not dataf.is_file():
                continue
            dt = None
            try:
                with open(dataf, "r") as fh:
                    obj = json.load(fh)
                dt = parse_date(obj.get("issued_on") or obj.get("issued") or obj.get("date"))
            except Exception:
                pass
            if dt is None:
                dt = mtime_dt(dataf)
            tie = mtime_dt(dataf)
            cand = (dt, tie, dataf)
            if best is None or (cand[0] > best[0]) or (cand[0] == best[0] and cand[1] > best[1]):
                best = cand
        if best:
            _, _, best_path = best
            dest = out / congress_dir.name / "bills" / bill_dir.name
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(best_path, dest / "data.json")
            print(f"picked {best_path} -> {dest/'data.json'}")
        else:
            print(f"no text versions for {congress_dir.name}/{bill_dir.name}")
