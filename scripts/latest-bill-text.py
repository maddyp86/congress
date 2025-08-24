import json, pathlib, shutil, sys, tempfile
from datetime import datetime, timezone

base = pathlib.Path("data")
out = pathlib.Path("latest_billtext")

def parse_date(s):
    if not s or not isinstance(s, str):
        return None
    try:
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1] + "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        try:
            return datetime.fromisoformat(s.split("T")[0])
        except Exception:
            return None

def mtime_dt(p):
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)

def key_from_path(p: pathlib.Path):
    parts = list(p.parts)
    if "bills" in parts:
        bi = parts.index("bills")
        if bi >= 1 and len(parts) > bi+2:
            congress = parts[bi-1]
            bill_type = parts[bi+1]
            bill_id = parts[bi+2]
            return f"{congress}/{bill_type}/{bill_id}"
    return None

# gather text-version files
text_files = sorted(p for p in base.rglob("*/bills/*/*/text-versions/*/data.json") if p.is_file())
print(f"DEBUG: found {len(text_files)} text-version data.json files")
if text_files:
    print("DEBUG sample:", [str(p) for p in text_files[:10]])

# group by bill key
groups = {}
skipped = 0
for p in text_files:
    k = key_from_path(p)
    if not k:
        print("Skipping unrecognized path:", p)
        skipped += 1
        continue
    groups.setdefault(k, []).append(p)

print(f"DEBUG: grouped into {len(groups)} unique bills (skipped {skipped})")

# pick best per group
picked = 0
tmpdir = pathlib.Path(tempfile.mkdtemp(prefix="latest_billtext_tmp_"))
try:
    for key in sorted(groups.keys()):
        best = None
        for p in groups[key]:
            dt_primary = None
            try:
                with p.open("r", encoding="utf-8") as fh:
                    obj = json.load(fh)
                candidate_date_str = obj.get("issued_on") or obj.get("issued") or obj.get("date")
                dt_primary = parse_date(candidate_date_str)
            except Exception:
                dt_primary = None
            if dt_primary is None:
                dt_primary = mtime_dt(p)
            tie = mtime_dt(p)
            cand = (dt_primary, tie, p)
            if best is None or (cand[0] > best[0]) or (cand[0] == best[0] and cand[1] > best[1]):
                best = cand

        if best:
            _, _, best_path = best
            congress, bill_type, bill_id = key.split("/", 2)
            dest = tmpdir / congress / "bills" / bill_type / bill_id
            dest.mkdir(parents=True, exist_ok=True)
            shutil.copy2(best_path, dest / "data.json")
            picked += 1
            print(f"picked {best_path} -> {dest/'data.json'}")
        else:
            print(f"no valid candidate for {key}")

    print(f"done: picked {picked} bills (skipped {skipped} files)")
    if picked == 0:
        print("ERROR: picked 0 bills — not updating latest_billtext. Exiting with code 2.", flush=True)
        sys.exit(2)

    # atomic swap
    if out.exists():
        backup = out.with_name(out.name + ".backup")
        if backup.exists():
            shutil.rmtree(backup)
        shutil.move(str(out), str(backup))
    shutil.move(str(tmpdir), str(out))
    print(f"latest_billtext updated: {picked} files written under {out}/")
    if 'backup' in locals() and backup.exists():
        shutil.rmtree(backup)
except Exception as e:
    try:
        if tmpdir.exists():
            shutil.rmtree(tmpdir)
    except Exception:
        pass
    print("ERROR during processing:", e, flush=True)
    raise
