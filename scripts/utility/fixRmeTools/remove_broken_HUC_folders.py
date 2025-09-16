#!/usr/bin/env python3
import os
import re
import shutil
from typing import Set, List

# --- Config ---
BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_fixed_projects_inserted"
BROKEN_DIRNAME = "_broken"
BROKEN_SUBS = ["missing_rme", "missing_rscontext"]
HUC_REGEX = re.compile(r"^\d{10}$")  # 10-digit HUCs
DRY_RUN = False  # <<< set to False to actually delete


def list_huc_dirs(path: str) -> List[str]:
    """Return names of subdirectories that look like 10-digit HUCs."""
    try:
        entries = os.listdir(path)
    except FileNotFoundError:
        return []
    dirs = []
    for name in entries:
        full = os.path.join(path, name)
        if os.path.isdir(full) and HUC_REGEX.match(name):
            dirs.append(name)
    return dirs


def main():
    broken_path = os.path.join(BASE_DIR, BROKEN_DIRNAME)
    if not os.path.isdir(broken_path):
        raise SystemExit(f"Not found: {broken_path}")

    # Collect HUCs from both _broken subfolders
    hucs_to_remove: Set[str] = set()
    for sub in BROKEN_SUBS:
        subpath = os.path.join(broken_path, sub)
        if not os.path.isdir(subpath):
            print(f"Note: missing _broken subfolder (ok): {subpath}")
            continue
        found = list_huc_dirs(subpath)
        hucs_to_remove.update(found)
        print(f"Found {len(found)} HUCs in {subpath}")

    if not hucs_to_remove:
        print("No HUC folders found under _broken subfolders. Nothing to do.")
        return

    print("\nHUCs flagged for removal from base dir:")
    for h in sorted(hucs_to_remove):
        print(f"  - {h}")

    # Remove matching sibling HUC folders in the base directory
    removed = 0
    missing = 0
    for huc in sorted(hucs_to_remove):
        target = os.path.join(BASE_DIR, huc)
        # Ensure we do not touch the _broken tree itself
        if target.startswith(os.path.join(broken_path, "")):
            print(f"Skip (inside _broken): {target}")
            continue

        if os.path.isdir(target):
            if DRY_RUN:
                print(f"[DRY RUN] Would remove: {target}")
            else:
                print(f"Removing: {target}")
                shutil.rmtree(target)
            removed += 1
        else:
            print(f"Not found (skip): {target}")
            missing += 1

    print("\n--- Summary ---")
    print(f"Flagged HUCs       : {len(hucs_to_remove)}")
    print(f"Removed (or planned): {removed}{' (dry-run)' if DRY_RUN else ''}")
    print(f"Missing/Not present: {missing}")


if __name__ == "__main__":
    main()
