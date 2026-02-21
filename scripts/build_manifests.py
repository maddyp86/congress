#!/usr/bin/env python3
# scripts/build_manifests.py
# Manifest builder for bills (and optionally votes + billtext).
# Writes both a local-path manifest (bills-manifest.json) and a
# full GCS URL manifest (bills-manifest-gcs.json) for n8n consumption.

import json
import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

base = Path("data")
latest = Path("latest_billtext")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")

# Minimum expected bill count — fail loudly if processing produced fewer.
# Set conservatively: 119th Congress had hundreds of senate bills by early 2025.
# Adjust downward if running early in a new Congress session.
MIN_BILLS_EXPECTED = 50


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


def local_path_to_gcs_url(filepath: str) -> str:
    """
    Convert a local data.json filepath to its public GCS URL.

    Local path structure (confirmed from GitHub Actions logs):
      data/119/bills/s/s123/data.json
      data/119/bills/sres/sres45/data.json

    GCS path structure (rsync strips 'bills/' during upload):
      congress-bill-data/data/119/s/s123/data.json
      congress-bill-data/data/119/sres/sres45/data.json

    So we strip 'data/' prefix and 'bills/' segment, then build the URL.
    """
    if not bucket:
        return ""

    # Normalize separators
    rel = filepath.replace("\\", "/")

    # Strip leading 'data/' or 'latest_billtext/'
    if rel.startswith("data/"):
        rel = rel[len("data/"):]
    elif rel.startswith("latest_billtext/"):
        rel = rel[len("latest_billtext/"):]

    # Strip 'bills/' segment — this matches what rsync does during GCS upload
    # data/119/bills/s/s123/data.json → after strip → 119/bills/s/s123/data.json
    # → after bills/ removal → 119/s/s123/data.json
    if "/bills/" in rel:
        rel = rel.replace("/bills/", "/", 1)

    rel = rel.lstrip("/")

    # Build final URL:
    # https://storage.googleapis.com/congress-legislative-data/congress-bill-data/data/119/s/s123/data.json
    if prefix:
        return f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}"
    else:
        return f"https://storage.googleapis.com/{bucket}/data/{rel}"


# -----------------------------------------------------------------------
# Scan
# -----------------------------------------------------------------------
print(f"DEBUG: base={base} (exists={base.exists()}); latest={latest} (exists={latest.exists()})")
print(f"DEBUG: GCS_BUCKET={bucket!r}  GCS_PREFIX={prefix!r}")

if not base.exists():
    print("ERROR: data/ directory does not exist — nothing to scan.", flush=True)
    sys.exit(2)

all_data = sorted(str(p).replace("\\", "/") for p in base.rglob("**/data.json") if p.is_file())
print(f"DEBUG: total data.json found under data/: {len(all_data)}")
if all_data:
    print("DEBUG: sample data.json (up to 8):")
    for s in all_data[:8]:
        print("  ", s)

# -----------------------------------------------------------------------
# Classify
# -----------------------------------------------------------------------
votes = []
bills = []
text_candidates = []

for p in all_data:
    if "/votes/" in p:
        votes.append(p)
    elif "/bills/" in p:
        if "/text-versions/" in p:
            text_candidates.append(p)
        else:
            bills.append(p)
    else:
        print(f"DEBUG: ignoring unexpected data.json location: {p}")

print(f"DEBUG: classified — votes={len(votes)}, bills={len(bills)}, text-version candidates={len(text_candidates)}")

# Warn (not fail) if votes is empty — this action doesn't collect votes
if not votes:
    print("INFO: No vote data.json files found — expected if this action only processes bills.")

# -----------------------------------------------------------------------
# Validate bill count before writing anything
# -----------------------------------------------------------------------
if not bills:
    print("ERROR: No bill data.json files found — usc-run bills may have failed.", flush=True)
    sys.exit(2)

if len(bills) < MIN_BILLS_EXPECTED:
    print(
        f"ERROR: Only {len(bills)} bill files found — expected at least {MIN_BILLS_EXPECTED}. "
        f"usc-run bills may have partially failed. Aborting to avoid overwriting a valid manifest.",
        flush=True,
    )
    sys.exit(2)

# -----------------------------------------------------------------------
# Build billtext list
# -----------------------------------------------------------------------
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
        billtext_src = "data text-versions (fallback — latest_billtext was empty)"
else:
    billtext = sorted(text_candidates)
    billtext_src = "data text-versions"

print(f"SUMMARY: votes={len(votes)}, bills={len(bills)}, billtext={len(billtext)} (source={billtext_src})")

# -----------------------------------------------------------------------
# Write manifests
# -----------------------------------------------------------------------
manifests = {
    "votes-manifest.json": votes,
    "bills-manifest.json": bills,
    "billtext-manifest.json": billtext,
}

for name, files in manifests.items():
    # Write local-path manifest
    try:
        atomic_write(Path(name), {"files": files})
        print(f"WROTE: {name} ({len(files)} entries)")
    except Exception as e:
        print(f"ERROR: writing {name}: {e}", flush=True)
        sys.exit(3)

    # Write GCS URL manifest
    # bills-manifest-gcs.json contains full public URLs — used by n8n
    # as a reliable fallback to the local-path manifest
    gcs_urls = [local_path_to_gcs_url(f) for f in files]

    # Spot-check a few URLs for sanity
    if gcs_urls and name == "bills-manifest.json":
        print("DEBUG: sample GCS URLs (first 3):")
        for u in gcs_urls[:3]:
            print("  ", u)
        # Verify 'bills/' is not leaking into the URL
        leaked = [u for u in gcs_urls if "/bills/" in u]
        if leaked:
            print(f"WARNING: {len(leaked)} GCS URLs still contain '/bills/' — path stripping may be wrong:")
            for u in leaked[:3]:
                print("  ", u)

    gcs_name = name.replace(".json", "-gcs.json")
    try:
        atomic_write(Path(gcs_name), {"files": gcs_urls})
        print(f"WROTE: {gcs_name} ({len(gcs_urls)} URLs)")
    except Exception as e:
        print(f"ERROR: writing {gcs_name}: {e}", flush=True)
        sys.exit(3)

print("DONE: all manifests written successfully.")
