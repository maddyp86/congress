#!/usr/bin/env python3
# scripts/build_manifests.py
# Manifest builder for bills (and optionally votes + billtext).
# Writes both a local-path manifest (bills-manifest.json) and a
# full GCS URL manifest (bills-manifest-gcs.json) for n8n consumption.
#
# Environment variables:
#   GCS_BUCKET        — GCS bucket name
#   GCS_PREFIX        — GCS path prefix
#   MANIFEST_MODE     — "bills" (default) or "votes"
#                       Controls which validation rules apply and which
#                       manifests are required. Set to "votes" when called
#                       from the collect-votes action.

import json
import os
import sys
from pathlib import Path
from tempfile import NamedTemporaryFile

base = Path("data")
latest = Path("latest_billtext")
bucket = os.environ.get("GCS_BUCKET", "").rstrip("/")
prefix = os.environ.get("GCS_PREFIX", "").strip("/")
mode = os.environ.get("MANIFEST_MODE", "bills").strip().lower()

# Minimum expected counts — fail loudly if processing produced fewer.
MIN_BILLS_EXPECTED = 50
MIN_VOTES_EXPECTED = 50

print(f"DEBUG: MANIFEST_MODE={mode!r}")
print(f"DEBUG: GCS_BUCKET={bucket!r}  GCS_PREFIX={prefix!r}")


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

    Bills — local path structure (confirmed from GitHub Actions logs):
      data/119/bills/s/s123/data.json
      data/119/bills/sres/sres45/data.json

    Bills — GCS path structure (rsync strips 'bills/' during upload):
      congress-bill-data/data/119/s/s123/data.json
      congress-bill-data/data/119/sres/sres45/data.json

    Votes — local path structure:
      data/119/votes/2025/h1/data.json
      data/119/votes/2025/s1/data.json

    Votes — GCS path structure (rsync preserves path as-is):
      congress-vote-data/data/119/votes/2025/h1/data.json
      congress-vote-data/data/119/votes/2025/s1/data.json
    """
    if not bucket:
        return ""

    rel = filepath.replace("\\", "/")

    # Strip leading 'data/' or 'latest_billtext/'
    if rel.startswith("data/"):
        rel = rel[len("data/"):]
    elif rel.startswith("latest_billtext/"):
        rel = rel[len("latest_billtext/"):]

    # Bills only: strip 'bills/' segment — matches what rsync does during upload
    # Votes: no stripping needed, path maps directly to GCS
    if "/bills/" in rel:
        rel = rel.replace("/bills/", "/", 1)

    rel = rel.lstrip("/")

    if prefix:
        return f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}"
    else:
        return f"https://storage.googleapis.com/{bucket}/data/{rel}"


# -----------------------------------------------------------------------
# Scan
# -----------------------------------------------------------------------
print(f"DEBUG: base={base} (exists={base.exists()}); latest={latest} (exists={latest.exists()})")

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

# -----------------------------------------------------------------------
# Validate — rules depend on which action is calling this script
# -----------------------------------------------------------------------
if mode == "votes":
    # Called from collect-votes action — only votes are expected
    if not votes:
        print("ERROR: No vote data.json files found — usc-run votes may have failed.", flush=True)
        sys.exit(2)
    if len(votes) < MIN_VOTES_EXPECTED:
        print(
            f"ERROR: Only {len(votes)} vote files found — expected at least {MIN_VOTES_EXPECTED}. "
            f"Aborting to avoid overwriting a valid manifest in GCS.",
            flush=True,
        )
        sys.exit(2)
    if not bills:
        print("INFO: No bill data.json files found — expected in votes mode.")

else:
    # Called from collect-bills action (default) — only bills are expected
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
    if not votes:
        print("INFO: No vote data.json files found — expected in bills mode.")

# -----------------------------------------------------------------------
# Build billtext list (bills mode only)
# -----------------------------------------------------------------------
billtext = []
billtext_src = "n/a (votes mode)"

if mode != "votes":
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
    try:
        atomic_write(Path(name), {"files": files})
        print(f"WROTE: {name} ({len(files)} entries)")
    except Exception as e:
        print(f"ERROR: writing {name}: {e}", flush=True)
        sys.exit(3)

    gcs_urls = [local_path_to_gcs_url(f) for f in files]

    # Spot-check bills GCS URLs for sanity
    if gcs_urls and name == "bills-manifest.json":
        print("DEBUG: sample bills GCS URLs (first 3):")
        for u in gcs_urls[:3]:
            print("  ", u)
        leaked = [u for u in gcs_urls if "/bills/" in u]
        if leaked:
            print(f"WARNING: {len(leaked)} GCS URLs still contain '/bills/' — path stripping may be wrong:")
            for u in leaked[:3]:
                print("  ", u)

    # Spot-check votes GCS URLs for sanity
    if gcs_urls and name == "votes-manifest.json" and mode == "votes":
        print("DEBUG: sample votes GCS URLs (first 3):")
        for u in gcs_urls[:3]:
            print("  ", u)

    gcs_name = name.replace(".json", "-gcs.json")
    try:
        atomic_write(Path(gcs_name), {"files": gcs_urls})
        print(f"WROTE: {gcs_name} ({len(gcs_urls)} URLs)")
    except Exception as e:
        print(f"ERROR: writing {gcs_name}: {e}", flush=True)
        sys.exit(3)

print("DONE: all manifests written successfully.")
