#!/usr/bin/env python3
# scripts/build_manifests.py
# Scans data/ for votes & bills, and chooses billtext source based on what the
# workflow actually produced (prefer populated latest_billtext, else fallback).
#
# Produces:
#   - votes-manifest.json
#   - bills-manifest.json
#   - billtext-manifest.json
# and corresponding -gcs.json files that map to storage.googleapis.com URLs
# using the GCS_BUCKET and GCS_PREFIX environment variables when available.

import json
import os
from pathlib import Path
import sys

base = Path("data")
latest = Path("latest_billtext")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

def gather_from(root: Path, pattern: str):
    if not root.exists():
        return []
    # rglob with pattern passed as glob-like (Path.rglob accepts glob patterns)
    results = sorted(str(p).replace("\\", "/") for p in root.rglob(pattern) if p.is_file())
    return results

def normalize_rel_for_gcs(filepath: str):
    """
    Convert a local path to the relative path we want under <prefix>/data/<rel>.
    - if path starts with 'data/', strip 'data/'.
    - if path starts with 'latest_billtext/', strip 'latest_billtext/' (so GCS layout matches data/).
    - otherwise leave the path as-is.
    """
    if filepath.startswith("data/"):
        return filepath[len("data/"):]
    if filepath.startswith("latest_billtext/"):
        return filepath[len("latest_billtext/"):]
    return filepath

# Debug start
print(f"DEBUG: base={base} (exists={base.exists()}); latest={latest} (exists={latest.exists()})")
# gather votes & bills from data/
votes = gather_from(base, "votes/*/*/data.json")
bills = gather_from(base, "bills/*/data.json")
print(f"DEBUG: found votes={len(votes)} files; bills={len(bills)} files")

# billtext: prefer latest_billtext if it exists AND contains files, else fallback to data text-versions
billtext = []
billtext_src = None

if latest.exists():
    billtext_latest = gather_from(latest, "bills/*/data.json")
    print(f"DEBUG: latest_billtext exists: found {len(billtext_latest)} files under latest_billtext/")
    if billtext_latest:
        billtext = billtext_latest
        billtext_src = "latest_billtext"
    else:
        # latest exists but empty → fallback
        billtext_fallback = gather_from(base, "bills/*/text-versions/*/data.json")
        billtext = billtext_fallback
        billtext_src = "data (text-versions; fallback from empty latest_billtext)"
        print(f"DEBUG: fallback to data text-versions: found {len(billtext_fallback)} files")
else:
    billtext_fallback = gather_from(base, "bills/*/text-versions/*/data.json")
    billtext = billtext_fallback
    billtext_src = "data (text-versions)"
    print(f"DEBUG: latest_billtext not present; using data text-versions: found {len(billtext_fallback)} files")

# Summarize before writing
print(f"SUMMARY: votes={len(votes)}, bills={len(bills)}, billtext={len(billtext)} (source={billtext_src})")

# Fail loudly if *all* manifest sources are empty — prevents silent empty manifests & downstream uploads.
if not votes and not bills and not billtext:
    print("ERROR: No votes, bills, or billtext files found. Aborting manifest build.", flush=True)
    sys.exit(2)

manifests = {
    "votes-manifest.json": votes,
    "bills-manifest.json": bills,
    "billtext-manifest.json": billtext,
}

for name, files in manifests.items():
    # write local manifest
    with open(name, "w", encoding="utf-8") as fh:
        json.dump({"files": files}, fh, ensure_ascii=False, indent=2)
    print(f"WROTE: {name} -> {len(files)} files")

    # build corresponding GCS mapping (urls) that normalize paths under <prefix>/data/<rel>
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
    with open(gcs_name, "w", encoding="utf-8") as fh:
        json.dump({"files": gcs_urls}, fh, ensure_ascii=False, indent=2)
    print(f"WROTE: {gcs_name} -> {len(gcs_urls)} urls (billtext source={billtext_src})")
