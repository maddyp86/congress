#!/usr/bin/env python3
# scripts/build_manifests.py
# Simple manifests: scans data/ for votes & bills, but prefers latest_billtext/ for billtext.

import json, os
from pathlib import Path

base = Path("data")
latest = Path("latest_billtext")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

def gather_from(root: Path, pattern: str):
    if not root.exists():
        return []
    return sorted(str(p).replace("\\", "/") for p in root.rglob(pattern) if p.is_file())

# votes & bills from data/
votes = gather_from(base, "votes/*/*/data.json")
bills = gather_from(base, "bills/*/data.json")

# billtext: prefer latest_billtext/*/bills/*/data.json, else fallback to data/.../text-versions/*/data.json
if latest.exists():
    billtext = gather_from(latest, "bills/*/data.json")
    src = "latest_billtext"
else:
    billtext = gather_from(base, "bills/*/text-versions/*/data.json")
    src = "data (text-versions)"

manifests = {
    "votes-manifest.json": votes,
    "bills-manifest.json": bills,
    "billtext-manifest.json": billtext,
}

for name, files in manifests.items():
    with open(name, "w") as fh:
        json.dump({"files": files}, fh)
    print(f"{name} built: {len(files)} files")

    # build GCS versions that point under <prefix>/data/... (same old format)
    gcs = []
    for f in files:
        rel = f[len("data/"):] if f.startswith("data/") else f
        if bucket and prefix:
            gcs.append(f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}")
        elif bucket:
            gcs.append(f"https://storage.googleapis.com/{bucket}/{rel}")
        else:
            gcs.append("")
    with open(name.replace(".json","-gcs.json"), "w") as fh:
        json.dump({"files": gcs}, fh)
    print(f"{name.replace('.json','-gcs.json')} built: {len(gcs)} files (billtext source={src})")
