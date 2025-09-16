#!/usr/bin/env python3
"""
Copy <ProjectBounds> from rscontext/project.rs.xml into rme/project.rs.xml
for each 10-digit HUC folder under BASE_DIR, inserting it directly after <MetaData>.

NEW:
- Tracks HUCs with missing rme/project.rs.xml and missing rscontext/project.rs.xml
- Writes logs:
    _broken/missing_rme.txt
    _broken/missing_rscontext.txt
    (_broken/no_bounds_in_rscontext.txt for convenience)
- Copies entire HUC folders with issues into:
    _broken/missing_rme/<HUC>/*
    _broken/missing_rscontext/<HUC>/*
    _broken/no_bounds_in_rscontext/<HUC>/*

- Creates a backup of rme/project.rs.xml as project.rs.xml.bak before changes
- Replaces existing <ProjectBounds> in rme if present
"""

import os
import re
import sys
import shutil
import copy
import traceback
import xml.etree.ElementTree as ET

# ---- CONFIG -----------------------------------------------------------------

BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_fixer"
RSCONTEXT_REL = os.path.join("rscontext", "project.rs.xml")
RME_REL = os.path.join("rme", "project.rs.xml")

BROKEN_DIR = os.path.join(BASE_DIR, "_broken")
BROKEN_MISSING_RME_DIR = os.path.join(BROKEN_DIR, "missing_rme")
BROKEN_MISSING_CTX_DIR = os.path.join(BROKEN_DIR, "missing_rscontext")
BROKEN_NO_BOUNDS_CTX_DIR = os.path.join(BROKEN_DIR, "no_bounds_in_rscontext")  # optional/helpful

# ---- UTIL -------------------------------------------------------------------

HUC_DIR_RE = re.compile(r"^\d{10}$")


def pretty_indent(elem: ET.Element) -> None:
    """Indent XML in-place. Uses ET.indent() when available; else minimal fallback."""
    if hasattr(ET, "indent"):
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
    os.makedirs(BROKEN_MISSING_RME_DIR, exist_ok=True)
    os.makedirs(BROKEN_MISSING_CTX_DIR, exist_ok=True)
    os.makedirs(BROKEN_NO_BOUNDS_CTX_DIR, exist_ok=True)


def copy_huc_to(folder_src: str, folder_dst_root: str, huc: str):
    dst = os.path.join(folder_dst_root, huc)
    # Use copytree with dirs_exist_ok to avoid crashes on re-runs
    shutil.copytree(folder_src, dst, dirs_exist_ok=True)


def write_list(path: str, items):
    with open(path, "w", encoding="utf-8") as f:
        for x in sorted(items):
            f.write(f"{x}\n")

# ---- CORE -------------------------------------------------------------------


def process_huc_dir(huc_dir: str, huc: str, logs: dict) -> None:
    rscontext_path = os.path.join(huc_dir, RSCONTEXT_REL)
    rme_path = os.path.join(huc_dir, RME_REL)

    # Missing rscontext?
    if not os.path.isfile(rscontext_path):
        logs["missing_rscontext"] += 1
        logs["missing_rscontext_hucs"].add(huc)
        print(f"[SKIP] No rscontext XML: {rscontext_path}")
        try:
            copy_huc_to(huc_dir, BROKEN_MISSING_CTX_DIR, huc)
        except Exception as e:
            logs["errors"] += 1
            print(f"[ERROR] copying missing_rscontext {huc}: {e}")
        return

    # Missing rme?
    if not os.path.isfile(rme_path):
        logs["missing_rme"] += 1
        logs["missing_rme_hucs"].add(huc)
        print(f"[SKIP] No rme XML:       {rme_path}")
        try:
            copy_huc_to(huc_dir, BROKEN_MISSING_RME_DIR, huc)
        except Exception as e:
            logs["errors"] += 1
            print(f"[ERROR] copying missing_rme {huc}: {e}")
        return

    try:
        # Load rscontext, extract <ProjectBounds>
        ctx_tree = load_xml(rscontext_path)
        ctx_root = ctx_tree.getroot()
        bounds = ctx_root.find("ProjectBounds")
        if bounds is None:
            logs["no_bounds_in_rscontext"] += 1
            logs["no_bounds_hucs"].add(huc)
            print(f"[SKIP] No <ProjectBounds> in: {rscontext_path}")
            try:
                copy_huc_to(huc_dir, BROKEN_NO_BOUNDS_CTX_DIR, huc)
            except Exception as e:
                logs["errors"] += 1
                print(f"[ERROR] copying no_bounds {huc}: {e}")
            return

        bounds_copy = copy.deepcopy(bounds)

        # Load rme XML
        rme_tree = load_xml(rme_path)
        rme_root = rme_tree.getroot()

        # Remove existing <ProjectBounds> (if any)
        existing = rme_root.find("ProjectBounds")
        replaced = False
        if existing is not None:
            rme_root.remove(existing)
            replaced = True

        # Insert after <MetaData>, else after <Warehouse>, else at top
        meta = rme_root.find("MetaData")
        if meta is None:
            anchor = rme_root.find("Warehouse")
            if anchor is None:
                rme_root.insert(0, bounds_copy)
            else:
                idx = find_child_index(rme_root, anchor)
                rme_root.insert(idx + 1, bounds_copy)
            logs["inserted_no_meta"] += 1
            print(f"[INFO] Inserted (no <MetaData> found): {rme_path}")
        else:
            idx = find_child_index(rme_root, meta)
            rme_root.insert(idx + 1, bounds_copy)
            if replaced:
                logs["replaced"] += 1
                print(f"[OK] Replaced bounds: {rme_path}")
            else:
                logs["inserted"] += 1
                print(f"[OK] Inserted bounds: {rme_path}")

        # Backup then write
        ensure_backup(rme_path)
        write_xml(rme_tree, rme_path)

    except Exception as e:
        logs["errors"] += 1
        print(f"[ERROR] {rme_path}\n{e}\n{traceback.format_exc()}")


def main() -> int:
    if not os.path.isdir(BASE_DIR):
        print(f"Base directory not found: {BASE_DIR}")
        return 1

    ensure_dirs()

    logs = {
        "total_huc_dirs": 0,
        "processed": 0,

        "missing_rscontext": 0,
        "missing_rme": 0,
        "no_bounds_in_rscontext": 0,

        "missing_rscontext_hucs": set(),
        "missing_rme_hucs": set(),
        "no_bounds_hucs": set(),

        "inserted": 0,
        "replaced": 0,
        "inserted_no_meta": 0,
        "errors": 0,
    }

    for name in sorted(os.listdir(BASE_DIR)):
        path = os.path.join(BASE_DIR, name)
        if not os.path.isdir(path):
            continue
        if not HUC_DIR_RE.match(name):
            continue

        logs["total_huc_dirs"] += 1
        process_huc_dir(path, name, logs)
        logs["processed"] += 1

    # Write broken HUC lists
    write_list(os.path.join(BROKEN_DIR, "missing_rscontext.txt"), logs["missing_rscontext_hucs"])
    write_list(os.path.join(BROKEN_DIR, "missing_rme.txt"), logs["missing_rme_hucs"])
    write_list(os.path.join(BROKEN_DIR, "no_bounds_in_rscontext.txt"), logs["no_bounds_hucs"])

    # Summary
    print("\n==== Summary ====")
    for k, v in logs.items():
        if isinstance(v, set):
            print(f"{k}: {len(v)}")
        else:
            print(f"{k}: {v}")
    print(f"\nBroken copies at: {BROKEN_DIR}")
    print(f" - Missing rscontext list: {os.path.join(BROKEN_DIR, 'missing_rscontext.txt')}")
    print(f" - Missing rme list:       {os.path.join(BROKEN_DIR, 'missing_rme.txt')}")
    print(f" - No-bounds list:         {os.path.join(BROKEN_DIR, 'no_bounds_in_rscontext.txt')}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
