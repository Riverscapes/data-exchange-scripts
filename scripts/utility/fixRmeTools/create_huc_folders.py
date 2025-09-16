#!/usr/bin/env python3
import os
import json
import re
import shutil
from typing import Optional, Dict, Any, List, Iterable
from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI

# --- Config paths ---
MATCHES_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2024CONUS_MISSING_BOUNDS_HUC_MATCHES.json"
BASE_OUT = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_fixer"

# Regex filters used by the API helper (which uses re.match on localPath)
ONLY_RME_PROJECT_XML = r".*project\.rs\.xml$"
ONLY_BOUNDS_GEOJSON = r".*project_bounds\.geojson$"
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

# --- Helpers ---


def get_meta_value(meta_list: list, key_name: str) -> Optional[str]:
    if not isinstance(meta_list, list):
        return None
    for kv in meta_list:
        if isinstance(kv, dict) and kv.get("key") == key_name:
            val = kv.get("value")
            return str(val) if val is not None else None
    return None


def extract_huc_from_project(project: Dict[str, Any]) -> Optional[str]:
    """
    Prefer 'HUC', fallback 'Hydrologic Unit Code'. Normalize to digits only.
    """
    meta = project.get("meta", [])
    huc = get_meta_value(meta, "HUC") or get_meta_value(meta, "Hydrologic Unit Code")
    if not huc and "HUC" in project:
        huc = project.get("HUC")
    if not huc:
        return None
    digits = "".join(ch for ch in str(huc) if ch.isdigit())
    return digits or None


def ensure_dir(path: str):
    if not os.path.exists(path):
        safe_makedirs(path)


def find_matching_files(root_dir: str, pattern: str) -> List[str]:
    """Find files under root_dir whose relative path matches `pattern`."""
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

# --- Download workflow ---


def process_matches(api: RiverscapesAPI, matches_path: str = MATCHES_PATH, base_out: str = BASE_OUT):
    log = Logger("RME-Bounds-Fixer-Downloader")
    log.title("Download selected files for 2024→2025 HUC matches (single-call 2025)")

    if not os.path.exists(matches_path):
        log.error(f"Input file not found: {matches_path}")
        return

    total = 0
    downloaded_counts = {
        "2024_rme_project_xml": 0,
        "2025_combined_download": 0,
        "2025_moved_bounds_to_rme": 0,
        "2025_rscontext_project_xml_present": 0,
        "skipped_no_huc_2024": 0,
        "skipped_no_match_2025": 0,
        "errors_per_item": 0,
    }

    for item in iter_json_array(matches_path):
        total += 1
        proj_2024 = item.get("project_2024")
        match_2025 = item.get("match_2025")

        try:
            # --- Step 1: 2024: project.rs.xml -> HUC/rme
            if not isinstance(proj_2024, dict):
                log.warning(f"[{total}] Missing/invalid project_2024; skipping item.")
                downloaded_counts["errors_per_item"] += 1
                continue

            huc_2024 = extract_huc_from_project(proj_2024)
            if not huc_2024:
                downloaded_counts["skipped_no_huc_2024"] += 1
                log.warning(f"[{total}] project_2024 has no HUC; skipping 2024 download.")
            else:
                rme_dir_24 = os.path.join(base_out, huc_2024, "rme")
                ensure_dir(rme_dir_24)
                api.download_files(
                    project_id=proj_2024.get("id"),
                    download_dir=rme_dir_24,
                    re_filter=[ONLY_RME_PROJECT_XML],
                    force=True,
                )
                downloaded_counts["2024_rme_project_xml"] += 1
                log.info(f"[{total}] 2024 project.rs.xml → {rme_dir_24}")

            # --- Step 2: 2025 (single call): download both files into rscontext dir, then move bounds to rme
            if not isinstance(match_2025, dict):
                downloaded_counts["skipped_no_match_2025"] += 1
                log.warning(f"[{total}] No match_2025; skipping 2025 downloads.")
                continue

            huc_2025 = match_2025.get("HUC") or huc_2024
            if not huc_2025:
                log.warning(f"[{total}] No HUC available for 2025; skipping 2025 downloads.")
                continue

            rme_dir_25 = os.path.join(base_out, huc_2025, "rme")
            rsctx_dir_25 = os.path.join(base_out, huc_2025, "rscontext")
            ensure_dir(rme_dir_25)
            ensure_dir(rsctx_dir_25)

            # Single network call for both files, downloaded initially to rscontext
            api.download_files(
                project_id=match_2025.get("id"),
                download_dir=rsctx_dir_25,
                re_filter=COMBINED_2025,   # both bounds + project.rs.xml
                force=True,
            )
            downloaded_counts["2025_combined_download"] += 1

            # Move the bounds to rme dir (preserve subpaths)
            moved = move_files_preserve_subpath(
                src_root=rsctx_dir_25,
                dst_root=rme_dir_25,
                pattern=ONLY_BOUNDS_GEOJSON,
            )
            downloaded_counts["2025_moved_bounds_to_rme"] += moved
            if moved > 0:
                log.info(f"[{total}] Moved {moved} bounds file(s) → {rme_dir_25}")

            # Confirm the project.rs.xml remains in rscontext
            rsxml_left = find_matching_files(rsctx_dir_25, ONLY_RME_PROJECT_XML)
            if rsxml_left:
                downloaded_counts["2025_rscontext_project_xml_present"] += 1
                log.info(f"[{total}] 2025 project.rs.xml present in rscontext → {rsctx_dir_25}")

        except Exception as e:
            downloaded_counts["errors_per_item"] += 1
            log.error(f"[{total}] Error processing item: {e}")

    # --- Summary ---
    log.title("Download summary")
    log.info(f"Items processed: {total}")
    for k, v in downloaded_counts.items():
        log.info(f"{k}: {v}")


def main():
    print("Starting HUC-bound file downloader (single-call 2025)...")
    with RiverscapesAPI() as api:
        process_matches(api, MATCHES_PATH, BASE_OUT)


if __name__ == "__main__":
    main()
