#!/usr/bin/env python3
# Builds local manifests and GCS HTTPS manifests.
# - votes/bills come from the canonical ./data tree
# - billtext prefers ./latest_billtext (short publish path). If absent, falls back to ./data

import json
import os
from pathlib import Path

base = Path("data")
latest_billtext = Path("latest_billtext")

bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

def gather(root: Path, pattern: str):
    return sorted(str(p).replace("\\", "/") for p in root.rglob(pattern) if p.is_file())

manifests = {}

# Votes & Bills (metadata) still come from ./data
manifests["votes-manifest.json"] = gather(base, "votes/*/*/data.json")
manifests["bills-manifest.json"] = gather(base, "bills/*/data.json")

# Bill Text: prefer curated latest_billtext export, else fall back to data/text-versions
billtext_source = "latest"
if latest_billtext.exists():
    billtext_files = gather(latest_billtext, "bills/*/data.json")
else:
    billtext_files = gather(base, "bills/*/text-versions/*/data.json")
    billtext_source = "data"

manifests["billtext-manifest.json"] = billtext_files

for name, files in manifests.items():
    with open(name, "w") as fh:
        json.dump({"files": files}, fh)
    print(f"{name} built: {len(files)} files")

    # Build GCS HTTPS variants
    gcs_files = []
    for f in files:
        # Map local path to the correct GCS prefix
        if name == "billtext-manifest.json" and billtext_source == "latest":
            # local: latest_billtext/<congress>/bills/<type>/<num>/data.json
            rel = f[len("latest_billtext/"):] if f.startswith("latest_billtext/") else f
            gcs_path = f"{prefix}/billtext/{rel}" if prefix else f"billtext/{rel}"
        else:
            # local: data/...
            rel = f[len("data/"):] if f.startswith("data/") else f
            gcs_path = f"{prefix}/data/{rel}" if prefix else f"data/{rel}"

        if bucket:
            gcs_files.append(f"https://storage.googleapis.com/{bucket}/{gcs_path}")
        else:
            gcs_files.append("")

    gcs_name = name.replace(".json", "-gcs.json")
    with open(gcs_name, "w") as fh:
        json.dump({"files": gcs_files}, fh)
    print(f"{gcs_name} built: {len(gcs_files)} files (source for billtext: {billtext_source})")
