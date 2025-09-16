#!/usr/bin/env python3
"""
Copy <ProjectBounds> from rscontext/project.rs.xml into 2023/project.rs.xml
for each item folder under BASE_DIR, overwriting any existing bounds.

Folder layout per item (example):
  <BASE_DIR>/
    0601010104-channelarea-c547c557-fa51-4d4f-90ee-44f152f5fa6a/
      2023/project.rs.xml
      rscontext/project.rs.xml

What this script does:
- For each item folder (skips names starting with "_"):
  * Reads rscontext/project.rs.xml and extracts <ProjectBounds>
  * Backs up 2023/project.rs.xml -> project.rs.xml.bak (if not already)
  * Removes ALL existing <ProjectBounds> in 2023/project.rs.xml (if any)
  * Inserts the new <ProjectBounds> after <MetaData>,
    else after <Warehouse>, else at top.
- Logs and copies problematic folders to:
    _broken/missing_2023/<folder>/*
    _broken/missing_rscontext/<folder>/*
    _broken/no_bounds_in_rscontext/<folder>/*
- Writes text lists into _broken/*.txt

Safe to re-run: uses .bak only if it doesn't exist, and copytree with dirs_exist_ok.
"""

import os
import re
import sys
import shutil
import copy
import traceback
import xml.etree.ElementTree as ET
from typing import Set

# ---- CONFIG -----------------------------------------------------------------

BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/fix_2023_conus"

RSCONTEXT_REL = os.path.join("rscontext", "project.rs.xml")
YEAR2023_REL = os.path.join("2023", "project.rs.xml")

BROKEN_DIR = os.path.join(BASE_DIR, "_broken")
BROKEN_MISSING_2023_DIR = os.path.join(BROKEN_DIR, "missing_2023")
BROKEN_MISSING_CTX_DIR = os.path.join(BROKEN_DIR, "missing_rscontext")
BROKEN_NO_BOUNDS_CTX_DIR = os.path.join(BROKEN_DIR, "no_bounds_in_rscontext")

# ---- UTIL -------------------------------------------------------------------


def pretty_indent(elem: ET.Element) -> None:
    """Indent XML in-place. Uses ET.indent() when available; else minimal fallback."""
    if hasattr(ET, "indent"):  # Python 3.9+
        ET.indent(elem, space="  ")  # type: ignore[attr-defined]
        return

    def _indent(e, level=0):
        i = "\n" + level*"  "
        if len(e):
            if not e.text or not e.text.strip():
                e.text = i + "  "
            for child in e:
                _indent(child, level+1)
            if not e.tail or not e.tail.strip():
                e.tail = i
        else:
            if level and (not e.tail or not e.tail.strip()):
                e.tail = i
    _indent(elem)


def load_xml(path: str) -> ET.ElementTree:
    return ET.parse(path)


def write_xml(tree: ET.ElementTree, path: str) -> None:
    root = tree.getroot()
    pretty_indent(root)
    tree.write(path, encoding="utf-8", xml_declaration=True)


def ensure_backup(path: str) -> None:
    bak = path + ".bak"
    if not os.path.exists(bak):
        shutil.copy2(path, bak)


def find_child_index(parent: ET.Element, child: ET.Element) -> int:
    for i, c in enumerate(list(parent)):
        if c is child:
            return i
    return -1


def ensure_dirs():
    os.makedirs(BROKEN_MISSING_2023_DIR, exist_ok=True)
    os.makedirs(BROKEN_MISSING_CTX_DIR, exist_ok=True)
    os.makedirs(BROKEN_NO_BOUNDS_CTX_DIR, exist_ok=True)


def copy_folder_to(folder_src: str, folder_dst_root: str, folder_name: str):
    dst = os.path.join(folder_dst_root, folder_name)
    shutil.copytree(folder_src, dst, dirs_exist_ok=True)


def write_list(path: str, items: Set[str]):
    with open(path, "w", encoding="utf-8") as f:
        for x in sorted(items):
            f.write(f"{x}\n")

# ---- CORE -------------------------------------------------------------------


def replace_bounds_in_target(rscontext_xml: str, target_xml: str) -> str:
    """
    Load bounds from rscontext_xml and insert/replace into target_xml.
    Returns "inserted", "replaced", or "inserted_no_meta" for stats/logging.
    """
    # Load rscontext and get bounds
    ctx_tree = load_xml(rscontext_xml)
    ctx_root = ctx_tree.getroot()
    bounds = ctx_root.find("ProjectBounds")
    if bounds is None:
        raise RuntimeError("No <ProjectBounds> found in rscontext XML")

    bounds_copy = copy.deepcopy(bounds)

    # Load target (2023) XML
    tgt_tree = load_xml(target_xml)
    tgt_root = tgt_tree.getroot()

    # Remove all existing <ProjectBounds> nodes in target
    removed_any = False
    for existing in list(tgt_root.findall("ProjectBounds")):
        tgt_root.remove(existing)
        removed_any = True

    # Insert after <MetaData>, else after <Warehouse>, else at top
    meta = tgt_root.find("MetaData")
    if meta is None:
        anchor = tgt_root.find("Warehouse")
        if anchor is None:
            tgt_root.insert(0, bounds_copy)
        else:
            idx = find_child_index(tgt_root, anchor)
            tgt_root.insert(idx + 1, bounds_copy)
        status = "inserted_no_meta"
    else:
        idx = find_child_index(tgt_root, meta)
        tgt_root.insert(idx + 1, bounds_copy)
        status = "replaced" if removed_any else "inserted"

    ensure_backup(target_xml)
    write_xml(tgt_tree, target_xml)
    return status


def process_item_folder(folder_path: str, folder_name: str, logs: dict) -> None:
    rsctx_path = os.path.join(folder_path, RSCONTEXT_REL)
    yr2023_path = os.path.join(folder_path, YEAR2023_REL)

    # Check rscontext XML
    if not os.path.isfile(rsctx_path):
        logs["missing_rscontext"] += 1
        logs["missing_rscontext_folders"].add(folder_name)
        print(f"[SKIP] No rscontext XML: {rsctx_path}")
        try:
            copy_folder_to(folder_path, BROKEN_MISSING_CTX_DIR, folder_name)
        except Exception as e:
            logs["errors"] += 1
            print(f"[ERROR] copying missing_rscontext {folder_name}: {e}")
        return

    # Check 2023 XML
    if not os.path.isfile(yr2023_path):
        logs["missing_2023"] += 1
        logs["missing_2023_folders"].add(folder_name)
        print(f"[SKIP] No 2023 XML:       {yr2023_path}")
        try:
            copy_folder_to(folder_path, BROKEN_MISSING_2023_DIR, folder_name)
        except Exception as e:
            logs["errors"] += 1
            print(f"[ERROR] copying missing_2023 {folder_name}: {e}")
        return

    try:
        status = replace_bounds_in_target(rsctx_path, yr2023_path)
        logs[status] += 1
        print(f"[OK] {status}: {yr2023_path}")
    except Exception as e:
        # If bounds missing in rscontext, copy to no_bounds folder
        msg = str(e)
        if "No <ProjectBounds>" in msg:
            logs["no_bounds_in_rscontext"] += 1
            logs["no_bounds_folders"].add(folder_name)
            print(f"[SKIP] No <ProjectBounds> in: {rsctx_path}")
            try:
                copy_folder_to(folder_path, BROKEN_NO_BOUNDS_CTX_DIR, folder_name)
            except Exception as ce:
                logs["errors"] += 1
                print(f"[ERROR] copying no_bounds {folder_name}: {ce}")
        else:
            logs["errors"] += 1
            print(f"[ERROR] {yr2023_path}\n{e}\n{traceback.format_exc()}")


def main() -> int:
    if not os.path.isdir(BASE_DIR):
        print(f"Base directory not found: {BASE_DIR}")
        return 1

    ensure_dirs()

    logs = {
        "total_item_dirs": 0,
        "processed": 0,

        "missing_rscontext": 0,
        "missing_2023": 0,
        "no_bounds_in_rscontext": 0,

        "missing_rscontext_folders": set(),
        "missing_2023_folders": set(),
        "no_bounds_folders": set(),

        "inserted": 0,
        "replaced": 0,
        "inserted_no_meta": 0,

        "errors": 0,
    }

    # Iterate all first-level dirs in BASE_DIR
    for name in sorted(os.listdir(BASE_DIR)):
        # skip internal/broken/hidden helpers
        if name.startswith("_"):
            continue
        path = os.path.join(BASE_DIR, name)
        if not os.path.isdir(path):
            continue

        logs["total_item_dirs"] += 1
        process_item_folder(path, name, logs)
        logs["processed"] += 1

    # Write broken folder lists
    write_list(os.path.join(BROKEN_DIR, "missing_rscontext.txt"), logs["missing_rscontext_folders"])
    write_list(os.path.join(BROKEN_DIR, "missing_2023.txt"), logs["missing_2023_folders"])
    write_list(os.path.join(BROKEN_DIR, "no_bounds_in_rscontext.txt"), logs["no_bounds_folders"])

    # Summary
    print("\n==== Summary ====")
    for k, v in logs.items():
        if isinstance(v, set):
            print(f"{k}: {len(v)}")
        else:
            print(f"{k}: {v}")
    print(f"\nBroken copies at: {BROKEN_DIR}")
    print(f" - Missing rscontext list: {os.path.join(BROKEN_DIR, 'missing_rscontext.txt')}")
    print(f" - Missing 2023 list:      {os.path.join(BROKEN_DIR, 'missing_2023.txt')}")
    print(f" - No-bounds list:         {os.path.join(BROKEN_DIR, 'no_bounds_in_rscontext.txt')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
