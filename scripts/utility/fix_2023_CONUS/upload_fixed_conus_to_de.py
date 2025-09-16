#!/usr/bin/env python3
"""
Resumable uploader for fix_2023_conus folders.

Folder name format: HUC-projectType-id
Example: 0601010104-channelarea-c547c557-fa51-4d4f-90ee-44f152f5fa6a

For each folder:
  - Project ID is parsed by splitting name on '-' and joining from the 3rd element onward.
  - Upload ONLY:
      2023/project.rs.xml
      2023/project_bounds.geojson
    (Never upload project.rs.xml.bak)
  - After a successful finalize, write upload_state.json so re-runs skip it.

Defaults to DRY_RUN (no network changes). Set DRY_RUN = False to perform real uploads.
"""

import os
import re
import json
import time
import requests
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import tempfile

from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI

# ============================
# Config
# ============================
BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/fix_2023_conus"
SUB_2023 = "2023"
FILES_TO_UPLOAD = ["project.rs.xml", "project_bounds.geojson"]  # order only affects logs
STATE_FILE = "upload_state.json"  # written after successful finalize

# Safety: dry run by default
DRY_RUN = False

# Optional CSV summary
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_PATH = os.path.join(BASE_DIR, "_upload_summary.csv")

# Polling (only if DRY_RUN is False & you want it)
POLL_UPLOAD_STATUS = False
POLL_INTERVAL_SEC = 5

# ============================
# Helpers
# ============================


def atomic_write_json(path: str, obj: Any) -> None:
    """Atomic write to avoid partial/corrupt JSON on failures."""
    dir_ = os.path.dirname(path)
    safe_makedirs(dir_)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_, encoding="utf-8") as tmp:
        json.dump(obj, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)


def list_item_dirs(root: str) -> List[str]:
    """Return immediate subdirectories that aren't helper dirs (skip those starting with '_')."""
    try:
        names = sorted(os.listdir(root))
    except FileNotFoundError:
        return []
    out = []
    for name in names:
        if name.startswith("_"):
            continue
        full = os.path.join(root, name)
        if os.path.isdir(full):
            out.append(full)
    return out


def parse_project_id_from_folder_name(name: str) -> Optional[str]:
    """
    Folder format: HUC-projectType-id
    Extract project id by joining parts from the 3rd element onward.
    Example:
      "0601010104-channelarea-c547c557-fa51-4d4f-90ee-44f152f5fa6a"
      -> "c547c557-fa51-4d4f-90ee-44f152f5fa6a"
    """
    parts = name.split("-")
    if len(parts) < 3:
        return None
    return "-".join(parts[2:]).strip() or None


def collect_upload_files(item_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Only take the two exact filenames from the 2023 subfolder:
      - project.rs.xml (exclude .bak)
      - project_bounds.geojson
    """
    d2023 = os.path.join(item_dir, SUB_2023)
    xml = os.path.join(d2023, "project.rs.xml")
    bounds = os.path.join(d2023, "project_bounds.geojson")
    xml_ok = os.path.isfile(xml) and not xml.endswith(".bak")
    bounds_ok = os.path.isfile(bounds)
    return (xml if xml_ok else None, bounds if bounds_ok else None)


def human_size(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def plan_line(folder: str, project_id: str, files_map: Dict[str, str]) -> str:
    sizes = []
    for rel, abs_path in files_map.items():
        try:
            sizes.append(f"{rel} ({human_size(os.path.getsize(abs_path))})")
        except Exception:
            sizes.append(f"{rel} (size n/a)")
    return f"{os.path.basename(folder)} → Project {project_id} | Files: {', '.join(sizes)}"


def read_state(path: str) -> Dict[str, Any]:
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def mark_uploaded(state_path: str, project_id: str, files_rel: List[str], note: str = "finalized"):
    state = {
        "uploaded": True,
        "uploadedAt": datetime.now(timezone.utc).isoformat(),
        "projectId": project_id,
        "files": files_rel,
        "note": note,
    }
    atomic_write_json(state_path, state)


def already_uploaded(state_path: str) -> bool:
    s = read_state(state_path)
    return bool(s.get("uploaded") is True)

# ============================
# Upload logic (real path)
# ============================


def do_real_upload(api: RiverscapesAPI, project_id: str, files_abs_by_rel: Dict[str, str], log: Logger,
                   finalize: bool = True) -> None:
    """
    Request upload → get presigned URLs → PUT → finalize.
    Mirrors your working approach.
    """
    # Fetch project for owner/visibility/tags
    existing = api.get_project_full(project_id)

    rels = list(files_abs_by_rel.keys())
    sizes = [os.path.getsize(files_abs_by_rel[r]) for r in rels]
    etags = ["X" * 24 for _ in rels]  # placeholder etags

    upload_params = {
        "projectId": project_id,
        "files": rels,
        "etags": etags,
        "sizes": sizes,
        "noDelete": True,
        "owner": {
            "id": existing.json["ownedBy"]["id"],
            "type": existing.json["ownedBy"]["__typename"].upper(),
        },
        "visibility": existing.json["visibility"],
        "tags": existing.json.get("tags", []),
    }

    # request upload
    q_request = api.load_query("requestUploadProject")
    up = api.run_query(q_request, upload_params)
    token = up["data"]["requestUploadProject"]["token"]
    update_list = up["data"]["requestUploadProject"]["update"]
    create_list = up["data"]["requestUploadProject"]["create"]

    # request file URLs
    q_urls = api.load_query("requestUploadProjectFilesUrl")
    url_resp = api.run_query(q_urls, {"files": update_list + create_list, "token": token})
    entries = url_resp["data"]["requestUploadProjectFilesUrl"]
    log.info(f"Received {len(entries)} presigned URLs")

    # PUT files
    for info in entries:
        relp = info["relPath"]
        url = info["urls"][0]
        abs_path = files_abs_by_rel[relp]
        log.info(f"Uploading {relp} → {url.split('?')[0]}")
        with open(abs_path, "rb") as f:
            resp = requests.put(url, data=f, timeout=180)
        if resp.status_code != 200:
            raise RuntimeError(f"Failed upload {relp}: {resp.status_code} {resp.text}")
        log.info(f"OK: {relp}")

    if not finalize:
        return

    # finalize
    q_finalize = api.load_mutation("finalizeProjectUpload")
    api.run_query(q_finalize, {"token": token})
    log.info("Finalize requested")

    # optional polling
    if POLL_UPLOAD_STATUS:
        q_check = api.load_query("checkUpload")
        while True:
            st = api.run_query(q_check, {"token": token})
            s = st["data"]["checkUpload"]
            if s["status"] == "SUCCESS":
                log.info("Upload complete")
                break
            if s["status"] == "FAILED":
                raise RuntimeError(f"Upload failed: {json.dumps(s, indent=2)}")
            log.info(f"...Upload status: {s['status']} (sleep {POLL_INTERVAL_SEC}s)")
            time.sleep(POLL_INTERVAL_SEC)

# ============================
# Main
# ============================


def main():
    log = Logger("Fix-2023-CONUS Uploader")
    log.title("Uploading updated 2023 XML + bounds (resumable)")

    item_dirs = list_item_dirs(BASE_DIR)
    if not item_dirs:
        print(f"No item folders found in {BASE_DIR}")
        return

    rows: List[List[str]] = [["folder", "project_id", "xml", "bounds", "status", "note"]]

    api_ctx = (RiverscapesAPI(stage="production") if not DRY_RUN else None)
    try:
        if api_ctx and hasattr(api_ctx, "__enter__"):
            api_ctx = api_ctx.__enter__()

        for item_dir in item_dirs:
            name = os.path.basename(item_dir)

            # parse project id from folder name
            project_id = parse_project_id_from_folder_name(name)
            if not project_id:
                note = "could not parse project id from folder name"
                log.warning(f"{name}: {note}")
                rows.append([name, "", "", "", "skip", note])
                continue

            # resumability check
            state_path = os.path.join(item_dir, STATE_FILE)
            if already_uploaded(state_path):
                note = "already uploaded"
                log.info(f"{name}: {note}")
                rows.append([name, project_id, "", "", "skip", note])
                continue

            # collect files (ONLY these two; never .bak)
            xml_abs, bounds_abs = collect_upload_files(item_dir)
            missing = []
            if not xml_abs:
                missing.append("2023/project.rs.xml")
            if not bounds_abs:
                missing.append("2023/project_bounds.geojson")
            if missing:
                note = f"missing: {', '.join(missing)}"
                log.warning(f"{name}: {note}")
                rows.append([name, project_id, xml_abs or "", bounds_abs or "", "skip", note])
                continue

            files_abs_by_rel = {
                "project.rs.xml": xml_abs,
                "project_bounds.geojson": bounds_abs,
            }

            # show plan
            log.info(plan_line(item_dir, project_id, files_abs_by_rel))

            if DRY_RUN:
                rows.append([name, project_id, xml_abs, bounds_abs, "dry-run", "would upload"])
                continue

            # real upload
            try:
                finalize = True
                do_real_upload(api_ctx, project_id, files_abs_by_rel, log, finalize=finalize)
                mark_uploaded(state_path, project_id, list(files_abs_by_rel.keys()), note="finalized")
                rows.append([name, project_id, xml_abs, bounds_abs, "uploaded", "finalized"])
            except Exception as e:
                note = f"upload error: {e}"
                log.error(f"{name}: {note}")
                rows.append([name, project_id, xml_abs, bounds_abs, "error", note])

    finally:
        if api_ctx and hasattr(api_ctx, "__exit__"):
            api_ctx.__exit__(None, None, None)

    if WRITE_SUMMARY_CSV:
        try:
            with open(SUMMARY_CSV_PATH, "w", encoding="utf-8") as f:
                for r in rows:
                    f.write(",".join([str(x) for x in r]) + "\n")
            print(f"\nWrote summary: {SUMMARY_CSV_PATH}")
        except Exception as e:
            print(f"Failed to write summary CSV: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
