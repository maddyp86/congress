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
#
# NOTE (manifest vs. fetch decoupling):
#   The manifest must reflect the FULL set of objects already in the GCS
#   bucket — not just what the current CI run fetched locally. The fetch is
#   intentionally scoped to the current congress (e.g. 119) for speed, so a
#   pure local scan would silently drop past congresses (e.g. 118) from the
#   manifest even though their objects still live in the bucket. To prevent
#   that, we UNION the local scan with a listing of the bucket. See
#   list_gcs_data_paths() and the union step after classification.

import json
import os
import subprocess
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


def path_to_gcs_url(filepath: str) -> str:
    """
    Convert a manifest file path to its public GCS URL.

    Bills — manifest path (bills/ already stripped during classification):
      data/119/s/s123/data.json
      data/119/sres/sres45/data.json

    Bills — GCS URL:
      https://storage.googleapis.com/.../congress-bill-data/data/119/s/s123/data.json

    Votes — manifest path:
      data/119/votes/2025/h1/data.json
      data/119/votes/2025/s1/data.json

    Votes — GCS URL:
      https://storage.googleapis.com/.../congress-vote-data/data/119/votes/2025/h1/data.json
    """
    if not bucket:
        return ""

    rel = filepath.replace("\\", "/")

    # Strip leading 'data/' or 'latest_billtext/'
    if rel.startswith("data/"):
        rel = rel[len("data/"):]
    elif rel.startswith("latest_billtext/"):
        rel = rel[len("latest_billtext/"):]

    # No bills/ stripping needed — already stripped during classification.
    # Votes path maps directly to GCS as-is.

    rel = rel.lstrip("/")

    if prefix:
        return f"https://storage.googleapis.com/{bucket}/{prefix}/data/{rel}"
    else:
        return f"https://storage.googleapis.com/{bucket}/data/{rel}"


def list_gcs_data_paths() -> list:
    """
    List data.json objects already present in the GCS bucket, returned as
    manifest-relative paths (i.e. 'data/...').

    Why: the manifest must reflect everything in the bucket, not just what
    THIS run fetched locally. The CI fetch is intentionally scoped to the
    current congress for speed; a pure local scan would silently drop past
    congresses (e.g. 118) from the manifest even though their objects are
    still in the bucket. Unioning the local scan with this listing keeps the
    manifest authoritative over the bucket and makes the drop non-recurring.

    Requires an authenticated gsutil. The workflow authenticates to GCP
    (google-github-actions/auth + setup-gcloud) before invoking this script,
    so gsutil is on PATH and credentialed at manifest-build time.

    Fails soft: on any listing error this returns [] and the caller falls
    back to the local scan (logging a WARNING), so manifest generation never
    hard-crashes on a transient gsutil hiccup.
    """
    if not bucket:
        return []

    base_url = f"gs://{bucket}/{prefix}/data/" if prefix else f"gs://{bucket}/data/"
    try:
        proc = subprocess.run(
            ["gsutil", "ls", f"{base_url}**/data.json"],
            capture_output=True,
            text=True,
            timeout=600,
        )
    except Exception as e:
        print(f"WARNING: gsutil listing failed ({e}); using local scan only.", flush=True)
        return []

    if proc.returncode != 0:
        print(
            f"WARNING: gsutil ls returned {proc.returncode}; using local scan only. "
            f"stderr: {proc.stderr.strip()[:300]}",
            flush=True,
        )
        return []

    strip = f"gs://{bucket}/{prefix}/" if prefix else f"gs://{bucket}/"
    out = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line.endswith("/data.json"):
            continue
        rel = line[len(strip):] if line.startswith(strip) else line
        rel = rel.lstrip("/")
        if rel.startswith("data/"):
            out.append(rel)
    return out


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
            # Strip 'bills/' segment to match GCS path structure.
            # Local:  data/119/bills/s/s100/data.json
            # GCS:    data/119/s/s100/data.json  (rsync strips bills/)
            # Manifest must use GCS-equivalent path so n8n file_path
            # entries match what's actually in the bucket.
            bills.append(p.replace("/bills/", "/", 1))
    else:
        print(f"DEBUG: ignoring unexpected data.json location: {p}")

print(f"DEBUG: classified — votes={len(votes)}, bills={len(bills)}, text-version candidates={len(text_candidates)}")

# -----------------------------------------------------------------------
# Union local scan with what's already in GCS
#
# The local scan only sees the congress this run fetched. Union it with the
# bucket so the manifest reflects the FULL set of objects present in GCS
# (e.g. past congresses we intentionally no longer re-fetch). This is what
# prevents a scoped fetch (e.g. 119-only) from silently dropping 118 from
# the manifest. Filters keep each mode to its own object class:
#   votes  -> paths containing '/votes/'
#   bills  -> paths NOT containing '/votes/' or '/text-versions/'
#            (bills GCS paths have 'bills/' already stripped, matching the
#             local classification above)
# -----------------------------------------------------------------------
if mode == "votes":
    remote = [p for p in list_gcs_data_paths() if "/votes/" in p]
    _local = len(votes)
    votes = sorted(set(votes) | set(remote))
    print(f"DEBUG: votes union — local={_local}, remote(GCS)={len(remote)}, union={len(votes)}")
else:
    remote = [
        p for p in list_gcs_data_paths()
        if "/votes/" not in p and "/text-versions/" not in p
    ]
    _local = len(bills)
    bills = sorted(set(bills) | set(remote))
    print(f"DEBUG: bills union — local={_local}, remote(GCS)={len(remote)}, union={len(bills)}")

# -----------------------------------------------------------------------
# Validate — rules depend on which action is calling this script
# -----------------------------------------------------------------------
if mode == "votes":
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

    gcs_urls = [path_to_gcs_url(f) for f in files]

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
