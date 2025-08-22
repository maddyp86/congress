#!/usr/bin/env python3
# scripts/build_manifests.py
# Simple manifests: scans local data/ tree and emits local + GCS HTTPS manifests.

import json
import os
from pathlib import Path

base = Path("data")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

def gather(pattern):
    # use rglob so recursive globs actually find nested files
    return sorted(str(p).replace("\\", "/") for p in base.rglob(pattern) if p.is_file())

manifests = {
    "votes-manifest.json": gather("**/votes/*/*/data.json"),
    "bills-manifest.json": gather("**/bills/*/data.json"),
    "billtext-manifest.json": gather("**/bills/*/text-versions/*/data.json"),
}

for name, files in manifests.items():
    with open(name, "w") as fh:
        json.dump({"files": files}, fh)
    print(f"{name} built: {len(files)} files")

    # Build GCS HTTPS manifest variant (strip leading 'data/' so we don't end up with data/data/...)
    gcs_files = []
    for f in files:
        rel = f[len("data/"):] if f.startswith("data/") else f
        if bucket and prefix:
            gcs_files.append(f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}")
        elif bucket:
            gcs_files.append(f"https://storage.googleapis.com/{bucket}/{rel}")
        else:
            gcs_files.append("")
    gcs_name = name.replace(".json", "-gcs.json")
    with open(gcs_name, "w") as fh:
        json.dump({"files": gcs_files}, fh)
    print(f"{gcs_name} built: {len(gcs_files)} files")
