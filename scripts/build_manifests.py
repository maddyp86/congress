#!/usr/bin/env python3
# scripts/build_manifests.py
# Robust manifest builder for votes, bills, and billtext.
# - Scans data/ for all data.json files
# - Classifies into votes, bills (metadata), and billtext (text-versions)
# - Prefers populated latest_billtext/ for billtext, else falls back to data text-versions
# - Writes manifests and -gcs.json mappings atomically, with helpful debug output.

import json
import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

base = Path("data")
latest = Path("latest_billtext")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

def atomic_write(path: Path, obj):
    """Write JSON atomically to avoid partial files on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = None
    try:
        with NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tf:
            json.dump(obj, tf, ensure_ascii=False, indent=2)
            tf.flush()
            tmp = tf.name
        os.replace(tmp, str(path))
    except Exception:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass
        raise

def normalize_rel_for_gcs(filepath: str):
    """Strip leading 'data/' or 'latest_billtext/' so the rel path under <prefix>/data/ is consistent."""
    rel = filepath
    if rel.startswith("data/"):
        rel = rel[len("data/"):]
    elif rel.startswith("latest_billtext/"):
        rel = rel[len("latest_billtext/"):]
    return rel.lstrip("/")

# Debug: top-level existence
print(f"DEBUG: base={base} (exists={base.exists()}); latest={latest} (exists={latest.exists()})")

# Gather all data.json files under data/ (explicitly require file name ends with data.json)
if not base.exists():
    print("DEBUG: data/ directory does not exist; no files to scan.")
    all_data = []
else:
    all_data = sorted(str(p).replace("\\", "/") for p in base.rglob("**/data.json") if p.is_file())

print(f"DEBUG: total data.json found under data/: {len(all_data)}")
if all_data:
    print("DEBUG: sample data.json (up to 8):")
    for s in all_data[:8]:
        print("  ", s)

# Classify files
votes = []
bills = []
text_candidates = []

for p in all_data:
    # simple classification based on path substring
    if "/votes/" in p:
        votes.append(p)
    elif "/bills/" in p:
        if "/text-versions/" in p:
            text_candidates.append(p)
        else:
            bills.append(p)
    else:
        # ignore other data.json (but log a sample)
        print(f"DEBUG: ignoring unexpected data.json location: {p}")

print(f"DEBUG: classified votes={len(votes)}, bills={len(bills)}, text-version candidates={len(text_candidates)}")

# Build billtext listing: prefer populated latest_billtext, else fallback to text_candidates
billtext = []
billtext_src = None

if latest.exists():
    latest_files = sorted(str(p).replace("\\", "/") for p in latest.rglob("**/data.json") if p.is_file())
    print(f"DEBUG: latest_billtext exists with {len(latest_files)} data.json files")
    if latest_files:
        billtext = latest_files
        billtext_src = "latest_billtext"
    else:
        billtext = sorted(text_candidates)
        billtext_src = "data (text-versions; fallback from empty latest_billtext)"
        print(f"DEBUG: latest_billtext present but empty; falling back to data text-versions with {len(billtext)} files")
else:
    billtext = sorted(text_candidates)
    billtext_src = "data (text-versions)"
    print(f"DEBUG: latest_billtext not present; using data text-versions with {len(billtext)} files")

print(f"SUMMARY before write: votes={len(votes)}, bills={len(bills)}, billtext={len(billtext)} (source={billtext_src})")

# Fail loudly if nothing found at all
if not votes and not bills and not billtext:
    print("ERROR: No votes, bills, or billtext files found. Aborting manifest build.", flush=True)
    sys.exit(2)

manifests = {
    "votes-manifest.json": votes,
    "bills-manifest.json": bills,
    "billtext-manifest.json": billtext,
}

for name, files in manifests.items():
    try:
        atomic_write(Path(name), {"files": files})
        print(f"WROTE: {name} -> {len(files)} files")
    except Exception as e:
        print(f"ERROR: writing {name}: {e}", flush=True)
        sys.exit(3)

    # Build GCS mapping file
    gcs_urls = []
    for f in files:
        rel = normalize_rel_for_gcs(f)
        if bucket and prefix:
            url = f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}"
        elif bucket:
            url = f"https://storage.googleapis.com/{bucket}/{rel}"
        else:
            url = ""
        gcs_urls.append(url)

    gcs_name = name.replace(".json", "-gcs.json")
    try:
        atomic_write(Path(gcs_name), {"files": gcs_urls})
        print(f"WROTE: {gcs_name} -> {len(gcs_urls)} urls (billtext source={billtext_src})")
    except Exception as e:
        print(f"ERROR: writing {gcs_name}: {e}", flush=True)
        sys.exit(3)

print("DONE: manifests created successfully.")
