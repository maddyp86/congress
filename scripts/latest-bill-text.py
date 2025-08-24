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
        # handle trailing Z (UTC)
        if s.endswith("Z"):
            s2 = s[:-1] + "+00:00"
            return datetime.fromisoformat(s2)
        return datetime.fromisoformat(s)
    except Exception:
        try:
            # fallback: date-only like "YYYY-MM-DD"
            return datetime.fromisoformat(s.split("T")[0])
        except Exception:
            return None


def mtime_dt(p: pathlib.Path):
    try:
        return datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
    except Exception:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def discover_candidates(base_path: pathlib.Path):
    """Return sorted unique candidate paths for data.json (metadata or text-versions)."""
    candidates = set()
    # metadata-level data.json: data/<congress>/bills/<type>/<id>/data.json
    for p in base_path.rglob("**/bills/*/*/data.json"):
        candidates.add(p)
    # text-version data.json: data/<congress>/bills/<type>/<id>/text-versions/*/data.json
    for p in base_path.rglob("**/bills/*/*/text-versions/*/data.json"):
        candidates.add(p)
    # normalize to list sorted for deterministic behavior
    return sorted(candidates)


def extract_congress_type_id(p: pathlib.Path):
    """
    Given a candidate path, try to extract (congress, bill_type, bill_id).
    Expected layouts:
      data/<congress>/bills/<type>/<bill_id>/data.json
      data/<congress>/bills/<type>/<bill_id>/text-versions/<ver>/data.json
    Return (congress, bill_type, bill_id) or (None, None, None) if not recognized.
    """
    parts = list(p.parts)
    # find 'bills' in path
    if "bills" in parts:
        bi = parts.index("bills")
        # congress is usually one element before 'bills' when path starts with 'data'
        if bi >= 1:
            # If path begins with "data", congress is parts[1] (parts[0] == 'data'), else parts[bi-1] maybe the congress
            if parts[0] == "data":
                # data/118/bills/...
                if len(parts) > 2:
                    congress = parts[1]
                else:
                    return (None, None, None)
            else:
                # fallback: element before 'bills'
                congress = parts[bi - 1]
            # bill_type and bill_id must be present after 'bills'
            if len(parts) > bi + 2:
                bill_type = parts[bi + 1]
                bill_id = parts[bi + 2]
                return (congress, bill_type, bill_id)
            else:
                return (None, None, None)
    # not recognized
    return (None, None, None)


# Discover candidates
candidates = discover_candidates(base)

if not candidates:
    print("No candidate data.json files found under data/ (checked metadata and text-versions).")
else:
    print(f"Discovered {len(candidates)} candidate data.json files (metadata + text-versions).")

# Group by (congress, bill_type, bill_id)
groups = {}
skipped = 0
for p in candidates:
    congress, bill_type, bill_id = extract_congress_type_id(p)
    if not (congress and bill_type and bill_id):
        print(f"Skipping (unrecognized layout): {p}")
        skipped += 1
        continue
    key = f"{congress}/{bill_type}/{bill_id}"
    groups.setdefault(key, []).append(p)

print(f"Grouped into {len(groups)} unique bills (skipped {skipped}).")

# Choose best per group
picked = 0
for key in sorted(groups.keys()):
    plist = groups[key]
    best = None  # tuple (primary_dt, tie_dt, path)
    for p in plist:
        dt_primary = None
        try:
            with p.open("r", encoding="utf-8") as fh:
                obj = json.load(fh)
            # data.json might have issued_on / issued / date fields
            candidate_date_str = obj.get("issued_on") or obj.get("issued") or obj.get("date")
            dt_primary = parse_date(candidate_date_str)
        except Exception:
            dt_primary = None

        # fallback to file mtime
        if dt_primary is None:
            dt_primary = mtime_dt(p)

        tie = mtime_dt(p)
        cand = (dt_primary, tie, p)
        if best is None or (cand[0] > best[0]) or (cand[0] == best[0] and cand[1] > best[1]):
            best = cand

    if best:
        dt, tie, best_path = best
        congress, bill_type, bill_id = key.split("/", 2)
        dest = out / congress / "bills" / bill_type / bill_id
        dest.mkdir(parents=True, exist_ok=True)
        shutil.copy2(best_path, dest / "data.json")
        picked += 1
        print(f"picked {best_path} -> {dest/'data.json'}")
    else:
        print(f"no valid candidate for {key}")

print(f"done: picked {picked} bills (skipped {skipped} files)")
