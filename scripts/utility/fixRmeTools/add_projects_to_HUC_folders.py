#!/usr/bin/env python3
import os
import json
from typing import Any, Dict, Optional

# --- Config ---
MATCHES_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2024CONUS_MISSING_BOUNDS_HUC_MATCHES.json"
HUC_BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_fixed"
OUT_FILENAME = "rme_rscontext_match.json"   # name for the written file in each HUC folder
OVERWRITE = True                            # set False to skip if file exists

def get_meta_value(meta_list, key) -> Optional[str]:
    if not isinstance(meta_list, list):
        return None
    for kv in meta_list:
        try:
            if kv.get("key") == key:
                return kv.get("value")
        except AttributeError:
            continue
    return None


def get_huc_from_project(project: Dict[str, Any]) -> Optional[str]:
    if not isinstance(project, dict):
        return None
    meta = project.get("meta") or []
    # Prefer "HUC", fall back to "Hydrologic Unit Code"
    huc = get_meta_value(meta, "HUC") or get_meta_value(meta, "Hydrologic Unit Code")
    # Final fallback: if project has top-level HUC (rare), use it
    if not huc:
        huc = project.get("HUC")
    return str(huc) if huc else None


def main():
    # Load the top-level JSON array
    with open(MATCHES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected a top-level JSON array in the matches file.")

    written = 0
    skipped = 0
    missing_huc = 0

    for idx, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            print(f"[{idx}] Skipping non-dict item.")
            skipped += 1
            continue

        proj = item.get("project_2024")
        huc = get_huc_from_project(proj)
        if not huc:
            print(f"[{idx}] No HUC found in project_2024.meta — skipping.")
            missing_huc += 1
            continue

        huc_dir = os.path.join(HUC_BASE_DIR, huc)
        os.makedirs(huc_dir, exist_ok=True)

        out_path = os.path.join(huc_dir, OUT_FILENAME)

        if os.path.exists(out_path) and not OVERWRITE:
            print(f"[{idx}] Exists, skip: {out_path}")
            skipped += 1
            continue

        # Write the entire item (includes project_2024 and match_2025)
        with open(out_path, "w", encoding="utf-8") as out:
            json.dump(item, out, ensure_ascii=False, indent=2)

        print(f"[{idx}] Wrote: {out_path}")
        written += 1

    print("\n--- Summary ---")
    print(f"Written: {written}")
    print(f"Skipped: {skipped}")
    print(f"Missing HUC: {missing_huc}")


if __name__ == "__main__":
    main()
