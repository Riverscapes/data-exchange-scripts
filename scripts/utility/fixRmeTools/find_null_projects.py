#!/usr/bin/env python3
"""
Scan a JSON array of match results and report 2024 projects that lack a 2025 match.

Inputs:
  - JSON file at MATCHES_PATH containing objects like:
    { "project_2024": {...}, "match_2025": {... or null ...} }

Outputs (written next to the input file by default):
  - <basename>__missing_2025_ids.txt        # one project_2024.id per line
  - <basename>__missing_2025_details.json   # array of detail records
  - <basename>__summary.txt                 # human-readable aggregations
"""

import os
import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Generator, Optional

# ---- Config -----------------------------------------------------------------
MATCHES_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2024CONUS_MISSING_BOUNDS_HUC_MATCHES.json"

# If you prefer to force outputs somewhere else, set this to a directory path.
# By default we write next to the input file, using its basename.
OUTPUT_DIR: Optional[str] = None

# ---- Helpers ----------------------------------------------------------------


def iter_json_array(path: str) -> Generator[Dict[str, Any], None, None]:
    """
    Memory-friendly reader for a top-level JSON array where each element is a JSON object.
    """
    from json import JSONDecoder
    dec = JSONDecoder()
    with open(path, "r", encoding="utf-8") as f:
        s = f.read()
    i, n = 0, len(s)

    # skip whitespace to '['
    while i < n and s[i].isspace():
        i += 1
    if i >= n or s[i] != "[":
        raise ValueError("Expected a top-level JSON array starting with '['")
    i += 1  # past '['

    while True:
        # skip whitespace and possible commas
        while i < n and (s[i].isspace() or s[i] == ","):
            i += 1
        if i >= n:
            break
        if s[i] == "]":
            break
        obj, j = dec.raw_decode(s, i)
        yield obj
        i = j


def get_meta_value(meta_list: Any, key: str, default: Optional[str] = None) -> Optional[str]:
    if not isinstance(meta_list, list):
        return default
    for m in meta_list:
        if isinstance(m, dict) and m.get("key") == key:
            return m.get("value", default)
    return default


def safe_makedirs(path: str) -> None:
    os.makedirs(path, exist_ok=True)

# ---- Main -------------------------------------------------------------------


def main():
    if not os.path.isfile(MATCHES_PATH):
        raise FileNotFoundError(f"Input not found: {MATCHES_PATH}")

    base = os.path.splitext(os.path.basename(MATCHES_PATH))[0]
    out_dir = OUTPUT_DIR or os.path.dirname(MATCHES_PATH)
    safe_makedirs(out_dir)

    out_ids_path = os.path.join(out_dir, f"{base}__missing_2025_ids.txt")
    out_details_path = os.path.join(out_dir, f"{base}__missing_2025_details.json")
    out_summary_path = os.path.join(out_dir, f"{base}__summary.txt")

    total = 0
    missing = 0
    missing_ids = []
    details = []

    # Aggregations
    by_owner = Counter()
    by_project_type = Counter()
    by_huc = Counter()

    for item in iter_json_array(MATCHES_PATH):
        total += 1
        proj2024 = item.get("project_2024") or {}
        match2025 = item.get("match_2025")

        # consider "no match" when match_2025 is None or empty dict
        has_match = bool(match2025)
        if not has_match:
            missing += 1
            pid = proj2024.get("id")
            missing_ids.append(pid if pid else "UNKNOWN_ID")

            owner = (proj2024.get("ownedBy") or {}).get("name") or "UNKNOWN_OWNER"
            ptype = (proj2024.get("projectType") or {}).get("id") or "UNKNOWN_TYPE"
            huc = get_meta_value(proj2024.get("meta"), "HUC", default="UNKNOWN_HUC")

            by_owner[owner] += 1
            by_project_type[ptype] += 1
            by_huc[huc] += 1

            details.append({
                "project_2024_id": pid,
                "project_2024_name": proj2024.get("name"),
                "owner": owner,
                "project_type": ptype,
                "huc": huc,
                "tags": proj2024.get("tags", []),
                "createdOn": proj2024.get("createdOn"),
                "updatedOn": proj2024.get("updatedOn"),
            })

    # Write outputs
    with open(out_ids_path, "w", encoding="utf-8") as f:
        for pid in missing_ids:
            f.write(f"{pid}\n")

    with open(out_details_path, "w", encoding="utf-8") as f:
        json.dump(details, f, indent=2)

    # Human-readable summary with aggregations
    pct = (missing / total * 100.0) if total else 0.0
    now = datetime.now().isoformat(timespec="seconds")
    lines = []
    lines.append(f"Run: {now}")
    lines.append(f"Input: {MATCHES_PATH}")
    lines.append(f"Total records scanned: {total}")
    lines.append(f"Missing 2025 matches: {missing} ({pct:.2f}%)")
    lines.append("")
    lines.append("Breakdown by Owner (top 20):")
    for name, count in by_owner.most_common(20):
        lines.append(f"  {name}: {count}")
    lines.append("")
    lines.append("Breakdown by Project Type (all):")
    for ptype, count in by_project_type.most_common():
        lines.append(f"  {ptype}: {count}")
    lines.append("")
    lines.append("Breakdown by HUC (top 30):")
    for huc, count in by_huc.most_common(30):
        lines.append(f"  {huc}: {count}")
    lines.append("")
    lines.append("Outputs:")
    lines.append(f"  Missing IDs:      {out_ids_path}")
    lines.append(f"  Missing details:  {out_details_path}")

    with open(out_summary_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    # Simple console logs
    print("\n".join(lines))


if __name__ == "__main__":
    main()
