import os
import re
import json
from pathlib import Path

# Environment variables passed in from workflow
CONGRESS = os.environ.get("CONGRESS", "119")
SESSION = os.environ.get("SESSION", "1")

# Directory where the workflow saved XMLs temporarily
DOWNLOAD_DIR = Path("data/tmp")
# Final target structure
TARGET_DIR = Path("data") / CONGRESS / "bills"

def build_structure():
    if not DOWNLOAD_DIR.exists():
        print(f"ERROR: Download directory not found: {DOWNLOAD_DIR}")
        return 1

    created = 0

    for billtype_dir in DOWNLOAD_DIR.iterdir():
        if not billtype_dir.is_dir():
            continue

        billtype = billtype_dir.name
        xml_files = list(billtype_dir.glob("*.xml"))
        if not xml_files:
            print(f"⚠️  No XML files found for bill type '{billtype}'")
            continue

        for xml_file in xml_files:
            # Expected format: BILLS-119hconres10ih.xml
            match = re.match(r"BILLS-\d+([a-z]+)(\d+)[a-z]*\.xml", xml_file.name)
            if not match:
                print(f"Skipping unrecognized filename: {xml_file.name}")
                continue

            billtype_extracted, billnum = match.groups()
            bill_id = f"{billtype_extracted}{billnum}"
            dest_dir = TARGET_DIR / billtype / bill_id
            dest_dir.mkdir(parents=True, exist_ok=True)

            source_url = (
                f"https://www.govinfo.gov/bulkdata/BILLS/{CONGRESS}/{SESSION}/"
                f"{billtype}/" + xml_file.name
            )

            metadata = {
                "bill_id": bill_id,
                "bill_type": billtype,
                "congress": CONGRESS,
                "session": SESSION,
                "source_url": source_url,
            }

            data_path = dest_dir / "data.json"
            with open(data_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)

            created += 1
            print(f"✅ Created: {data_path}")

    if created == 0:
        print("ERROR: No data.json files were created.")
        return 1

    print(f"Done. Created {created} data.json files under {TARGET_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(build_structure())
