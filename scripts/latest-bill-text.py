import json
import pathlib
import shutil
import sys
import tempfile
import xml.etree.ElementTree as ET
import re
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
        if bi >= 1 and len(parts) > bi + 2:
            congress = parts[bi - 1]
            bill_type = parts[bi + 1]
            bill_id = parts[bi + 2]
            return f"{congress}/{bill_type}/{bill_id}"
    return None


# -------------------------
# XML helpers
# -------------------------
def strip_ns(tag):
    return tag.split("}", 1)[-1] if "}" in tag else tag


def find_date_in_mods(root):
    # Look for common MODS date fields: dateIssued, dateCreated, date
    for el in root.iter():
        tag = strip_ns(el.tag).lower()
        if tag in ("dateissued", "datecreated", "date"):
            txt = (el.text or "").strip()
            if txt:
                m = re.search(r"\d{4}-\d{2}-\d{2}", txt)
                if m:
                    return m.group(0)
                m = re.search(r"\d{4}", txt)
                if m:
                    return m.group(0)
                return txt
    # fallback: any 4-digit year or YYYY-MM-DD anywhere in text
    text_blob = " ".join((el.text or "") for el in root.iter())
    m = re.search(r"(\d{4}-\d{2}-\d{2}|\d{4})", text_blob)
    return m.group(1) if m else None


def find_identifier_in_mods(root):
    # prefer type='local' or 'bill', else first non-empty identifier
    for el in root.iter():
        if strip_ns(el.tag).lower() == "identifier":
            attrs = {k.lower(): v for k, v in el.attrib.items()}
            txt = (el.text or "").strip()
            if attrs.get("type", "").lower() in ("local", "bill") and txt:
                return txt
    # second pass: any canonical-looking identifier
    human_re = re.compile(
        r'(?i)^(?:hr|s|hres|sres|hjres|sjres|hconres|sconres)[0-9]+-[0-9]{1,4}-[a-z]{1,10}$'
    )
    for el in root.iter():
        if strip_ns(el.tag).lower() == "identifier":
            txt = (el.text or "").strip()
            if txt and human_re.search(txt):
                return txt
    # fallback: first non-empty identifier
    for el in root.iter():
        if strip_ns(el.tag).lower() == "identifier":
            txt = (el.text or "").strip()
            if txt:
                return txt
    return None


def _add_url(urls_map, u):
    if not u:
        return
    u = u.strip()
    # Guess type by common patterns
    if re.search(r"\.pdf($|\?)", u, re.I):
        key = "pdf"
    elif re.search(r"\.xml($|\?)", u, re.I) or "/xml/" in u.lower():
        key = "xml"
    elif re.search(r"\.htm|/html/|/htm($|\?)", u, re.I):
        key = "html"
    else:
        key = "unknown"
        i = 1
        while key in urls_map:
            i += 1
            key = f"unknown_{i}"
    urls_map[key] = u


def extract_urls_from_mods(root):
    urls = {}
    # look for <location><url> and <url> in MODS; also any element named 'url'
    for el in root.iter():
        tag = strip_ns(el.tag).lower()
        if tag == "location":
            # look for nested <url> elements
            for child in el.iter():
                if strip_ns(child.tag).lower() == "url":
                    u = (child.text or "").strip()
                    if u:
                        _add_url(urls, u)
        elif tag == "url":
            u = (el.text or "").strip()
            if u:
                _add_url(urls, u)
    # Also search for obvious link-like text in related elements (fallback)
    if not urls:
        for el in root.iter():
            txt = (el.text or "").strip()
            if txt and re.search(r"https?://", txt):
                _add_url(urls, txt)
    return urls


def parse_mods_file(path: pathlib.Path):
    try:
        root = ET.parse(str(path)).getroot()
        return root
    except Exception:
        return None


# -------------------------
# New: synthesize bill_id from path (always used)
# -------------------------
def synthesize_bill_id_from_path(tv_path: pathlib.Path) -> str:
    """
    Given a text-version dir path like:
      data/118/bills/hr/hr85/text-versions/ih
    produce canonical bill_id: 'hr85-118' -> normalized to 'hr85-118' (lowercase).
    Returns the string or raises ValueError if unable to synthesize.
    """
    parts = list(tv_path.parts)
    if "bills" not in parts:
        raise ValueError(f"Cannot synthesize bill_id: 'bills' segment not in path {tv_path}")
    try:
        bi = parts.index("bills")
        # Expect: .../<congress>/bills/<bill_type>/<bill_dir>/text-versions/<version>
        if bi < 1 or len(parts) <= bi + 2:
            raise ValueError(f"Path too short to synthesize bill_id: {tv_path}")
        congress_raw = parts[bi - 1]
        bill_type = parts[bi + 1].lower()  # e.g., 'hr' or 's'
        bill_dir = parts[bi + 2].lower()   # e.g., 'hr1237' or 'hr85'
    except Exception as e:
        raise ValueError(f"Failed to parse path for bill_id: {tv_path}: {e}")

    # Prefer pattern like 'hr1237' (alpha prefix + digits)
    m = re.match(r'^([a-z]+)(\d+)', bill_dir)
    if m:
        bt = m.group(1)
        num = str(int(m.group(2)))  # strip leading zeros
        cong = str(int(congress_raw))
        return f"{bt}{num}-{cong}".lower()

    # Otherwise, take first run of digits anywhere in bill_dir
    m2 = re.search(r'(\d+)', bill_dir)
    if m2:
        num = str(int(m2.group(1)))
        bt = bill_type or 'hr'
        cong = str(int(congress_raw))
        return f"{bt}{num}-{cong}".lower()

    # Fallback: scan a few following path components for digits
    for extra in parts[bi + 2:bi + 6]:
        m3 = re.search(r'(\d+)', extra)
        if m3:
            num = str(int(m3.group(1)))
            bt = bill_type or 'hr'
            cong = str(int(congress_raw))
            return f"{bt}{num}-{cong}".lower()

    # If all else fails, raise so caller can log and handle
    raise ValueError(f"Unable to synthesize bill_id from path: {tv_path}")


# -------------------------
# Ensure data.json exists in text-version folders
# -------------------------
def ensure_data_jsons():
    created = 0
    for tv in sorted(base.glob("*/bills/*/*/text-versions/*")):
        if not tv.is_dir():
            continue
        target = tv / "data.json"
        if target.exists():
            continue

        mods_root = None
        # direct mods.xml
        direct = tv / "mods.xml"
        if direct.exists():
            mods_root = parse_mods_file(direct)
        # subdirectories (e.g., BILLS-*)
        if mods_root is None:
            for p in tv.iterdir():
                if p.is_dir():
                    cand = p / "mods.xml"
                    if cand.exists():
                        mods_root = parse_mods_file(cand)
                        if mods_root is not None:
                            break
        # recursive search for any mods.xml under tv (workflow already unzipped)
        if mods_root is None:
            for cand in tv.rglob("mods.xml"):
                mods_root = parse_mods_file(cand)
                if mods_root is not None:
                    break

        issued = None
        version_id = None
        urls_map = {}

        if mods_root is not None:
            issued = find_date_in_mods(mods_root)
            version_id = find_identifier_in_mods(mods_root)
            urls_map = extract_urls_from_mods(mods_root)

        if not issued:
            try:
                issued = datetime.fromtimestamp(tv.stat().st_mtime, tz=timezone.utc).date().isoformat()
            except Exception:
                issued = None

        # synthesize bill_id from path (always prefer path-based deterministic id)
        try:
            bill_id = synthesize_bill_id_from_path(tv)
        except Exception as e:
            bill_id = None
            print(f"WARNING: {e}")

        data = {}
        if issued:
            data["issued_on"] = str(issued)
        data["version_code"] = tv.name
        if version_id:
            data["bill_version_id"] = version_id
        if bill_id:
            data["bill_id"] = bill_id
            data["bill_id_source"] = "path"
        if urls_map:
            data["urls"] = urls_map

        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with target.open("w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2, ensure_ascii=False)
            created += 1
            print(f"WROTE {target} -> {data}")
        except Exception as e:
            print(f"FAILED writing {target}: {e}")

    print(f"Done: created {created} data.json files (when missing)")


# Run generator
ensure_data_jsons()

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

    print(f"done: picked {picked} bills (skipped {skipped})")
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
    if "backup" in locals() and backup.exists():
        shutil.rmtree(backup)
except Exception as e:
    try:
        if tmpdir.exists():
            shutil.rmtree(tmpdir)
    except Exception:
        pass
    print("ERROR during processing:", e, flush=True)
    raise
