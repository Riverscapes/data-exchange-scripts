#!/usr/bin/env python3
import os
import json
import re
import shutil
from typing import Optional, Dict, Any, List, Iterable
from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI

# --- Config paths (UPDATED to your new context) ---
MISMATCHES_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_boundsId_mismatches.json"
BASE_OUT = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/fix_2023_conus"

# --- Regex filters ---
# Use PROJECT_XML for the 2023 (source) download:
PROJECT_XML = r".*project\.rs\.xml$"
ONLY_BOUNDS_GEOJSON = r".*project_bounds\.geojson$"

# Keep compatibility label but identical regex:
ONLY_RME_PROJECT_XML = PROJECT_XML

# One-pass download for the rscontext (2025) side:
COMBINED_2025 = [ONLY_BOUNDS_GEOJSON, ONLY_RME_PROJECT_XML]

# --- Streaming reader for a top-level JSON array (memory friendly) ---


def iter_json_array(path: str) -> Iterable[dict]:
    from json import JSONDecoder
    decoder = JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = f.read()
    i, n = 0, len(buf)
    while i < n and buf[i].isspace():
        i += 1
    if i >= n or buf[i] != "[":
        raise ValueError("Expected a top-level JSON array.")
    i += 1
    while True:
        while i < n and buf[i].isspace():
            i += 1
        if i < n and buf[i] == ",":
            i += 1
            while i < n and buf[i].isspace():
                i += 1
        if i < n and buf[i] == "]":
            break
        if i >= n:
            break
        obj, end = decoder.raw_decode(buf, idx=i)
        yield obj
        i = end

# --- Helpers (same behavior as your working script) ---


def ensure_dir(path: str):
    if not os.path.exists(path):
        safe_makedirs(path)


def find_matching_files(root_dir: str, pattern: str) -> List[str]:
    """Find files under root_dir whose relative path matches `pattern` (case-insensitive)."""
    regex = re.compile(pattern, re.IGNORECASE)
    matches = []
    for cur, _dirs, files in os.walk(root_dir):
        for fname in files:
            rel = os.path.relpath(os.path.join(cur, fname), root_dir)
            if regex.match(rel):
                matches.append(os.path.join(cur, fname))
    return matches


def move_files_preserve_subpath(src_root: str, dst_root: str, pattern: str) -> int:
    """
    Move files that match `pattern` from src_root to dst_root, preserving relative subpaths.
    Returns count moved.
    """
    moved = 0
    src_matches = find_matching_files(src_root, pattern)
    for abs_src in src_matches:
        rel = os.path.relpath(abs_src, src_root)
        abs_dst = os.path.join(dst_root, rel)
        ensure_dir(os.path.dirname(abs_dst))
        shutil.move(abs_src, abs_dst)
        moved += 1
    return moved


def slug_component(s: str) -> str:
    """
    Make a filesystem-friendly component: keep letters, digits, dash/underscore.
    (Your IDs already have dashes; this preserves them.)
    """
    s = (s or "").strip()
    if not s:
        return "unknown"
    return re.sub(r"[^A-Za-z0-9\-_]", "_", s)


def build_item_dirname(huc: str, src_type: Optional[str], src_id: Optional[str]) -> str:
    """
    Folder name: HUC-projectType-id (based on the *source* object).
    Example: 1202000302-taudem-d91d4c7f-cebb-4288-a433-926c2fab05b1
    """
    return f"{slug_component(huc)}-{slug_component((src_type or 'unknown').lower())}-{slug_component(src_id or 'noid')}"

# --- Main workflow that adheres to your new requirements ---


def process_mismatches(api: RiverscapesAPI, mismatches_path: str = MISMATCHES_PATH, base_out: str = BASE_OUT):
    log = Logger("Fix-2023-CONUS-Downloads")
    log.title("Download 2023 (source) XML and rscontext (combined) → move bounds to 2023 (unique per item)")

    if not os.path.exists(mismatches_path):
        log.error(f"Input file not found: {mismatches_path}")
        return

    # Ensure the base folder exists (no-op if already there)
    ensure_dir(base_out)

    total = 0
    metrics = {
        "items_processed": 0,
        "created_item_dirs": 0,  # now counts per unique item (HUC-type-id)
        "source_2023_xml_downloaded": 0,
        "rscontext_combined_downloads": 0,
        "bounds_geojson_moved_to_2023": 0,
        "warnings_no_huc": 0,
        "warnings_missing_ids": 0,
        "errors_per_item": 0,
    }

    for item in iter_json_array(mismatches_path):
        total += 1

        # Expected schema:
        # {"huc": "...", "source": {"id": ..., "projectType": ...}, "match": {"id": ...}}
        huc = str(item.get("huc") or "").strip()
        src = item.get("source") or {}
        mch = item.get("match") or {}

        if not huc:
            metrics["warnings_no_huc"] += 1
            log.warning(f"[{total}] Missing HUC; skipping.")
            continue

        src_id = src.get("id")
        mch_id = mch.get("id")
        src_type = src.get("projectType")  # <- you said you added this to source

        if not src_id or not mch_id:
            metrics["warnings_missing_ids"] += 1
            log.warning(f"[{total}] HUC {huc}: missing source/match project id; skipping.")
            continue

        # Build a unique per-item folder name: HUC-projectType-id (based on source)
        item_dir_name = build_item_dirname(huc, src_type, src_id)
        item_dir = os.path.join(base_out, item_dir_name)

        # Subfolders
        dir_2023 = os.path.join(item_dir, "2023")
        dir_rsctx = os.path.join(item_dir, "rscontext")

        # Create folders (don't recreate if present)
        created_now = False
        if not os.path.exists(item_dir):
            ensure_dir(item_dir)
            created_now = True
        ensure_dir(dir_2023)
        ensure_dir(dir_rsctx)
        if created_now:
            metrics["created_item_dirs"] += 1

        log.info(f"\n=== {item_dir_name} ===")

        try:
            # 1) SOURCE (2023): download only project.rs.xml into 2023/
            api.download_files(
                project_id=src_id,
                download_dir=dir_2023,
                re_filter=[PROJECT_XML],   # only project.rs.xml from the 2023/source project
                force=True,
            )
            metrics["source_2023_xml_downloaded"] += 1
            log.info(f"[{total}] 2023 project.rs.xml → {dir_2023}")

            # 2) RSCONTEXT: one pass for bounds + xml into rscontext/
            api.download_files(
                project_id=mch_id,
                download_dir=dir_rsctx,
                re_filter=COMBINED_2025,   # project_bounds.geojson + project.rs.xml
                force=True,
            )
            metrics["rscontext_combined_downloads"] += 1
            log.info(f"[{total}] rscontext combined download → {dir_rsctx}")

            # 3) Move ONLY project_bounds.geojson into 2023/ (leave xml in rscontext/)
            moved_bounds = move_files_preserve_subpath(
                src_root=dir_rsctx,
                dst_root=dir_2023,
                pattern=ONLY_BOUNDS_GEOJSON,
            )
            metrics["bounds_geojson_moved_to_2023"] += moved_bounds
            if moved_bounds > 0:
                log.info(f"[{total}] Moved {moved_bounds} bounds file(s) → {dir_2023}")
            else:
                log.warning(f"[{total}] No project_bounds.geojson found to move for {item_dir_name}")

            metrics["items_processed"] += 1

        except Exception as e:
            metrics["errors_per_item"] += 1
            log.error(f"[{total}] Error processing {item_dir_name}: {e}")

    # --- Summary ---
    log.title("Fix 2023 CONUS download summary")
    log.info(f"Items scanned: {total}")
    for k, v in metrics.items():
        log.info(f"{k}: {v}")


def main():
    print("Starting Fix 2023 CONUS downloader (unique per item)...")
    with RiverscapesAPI() as api:
        process_mismatches(api, MISMATCHES_PATH, BASE_OUT)


if __name__ == "__main__":
    main()
