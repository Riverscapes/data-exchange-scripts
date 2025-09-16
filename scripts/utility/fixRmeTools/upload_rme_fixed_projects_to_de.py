#!/usr/bin/env python3
import os
import json
import time
import re
import requests
from typing import Dict, Any, List, Optional, Tuple

from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI
from datetime import datetime, timezone
import tempfile

# ============================
# Config
# ============================
BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_upload_ready"
MATCH_FILENAME = "rme_rscontext_match.json"
RME_SUBDIR = "rme"
FILES_TO_UPLOAD = ["project.rs.xml", "project_bounds.geojson"]  # Upload order for readability
HUC_REGEX = re.compile(r"^\d{10}$")

# Safety: stay dry-run unless you intentionally flip this to False
DRY_RUN = False

# If you want to log a summary CSV
WRITE_SUMMARY_CSV = True
SUMMARY_CSV_PATH = os.path.join(BASE_DIR, "_upload_summary.csv")

# Polling (only if DRY_RUN is False)
POLL_UPLOAD_STATUS = True
POLL_INTERVAL_SEC = 5

# If you temporarily comment out the finalize step, you *may* choose to mark uploaded=True
# immediately after successful PUTs. Safer to leave this False in production.
MARK_AFTER_PUTS_IF_NO_FINALIZE = False


def read_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def atomic_write_json(path: str, obj: Any) -> None:
    """Write JSON atomically to avoid corrupting files on partial writes."""
    dir_ = os.path.dirname(path)
    safe_makedirs(dir_)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=dir_, encoding="utf-8") as tmp:
        json.dump(obj, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, path)


def find_huc_dirs(root: str) -> List[str]:
    try:
        entries = os.listdir(root)
    except FileNotFoundError:
        return []
    out = []
    for name in sorted(entries):
        full = os.path.join(root, name)
        if os.path.isdir(full) and HUC_REGEX.match(name):
            out.append(full)
    return out


def extract_project_id(match_obj: Dict[str, Any]) -> Optional[str]:
    """Get project_2024.id"""
    try:
        pid = match_obj.get("project_2024", {}).get("id")
        return str(pid) if pid else None
    except Exception:
        return None


def collect_upload_files(huc_dir: str) -> Tuple[Optional[str], Optional[str]]:
    """Return absolute paths to (project.rs.xml, project_bounds.geojson) inside the RME subfolder if they exist."""
    rme_dir = os.path.join(huc_dir, RME_SUBDIR)
    xml_path = os.path.join(rme_dir, "project.rs.xml")
    bounds_path = os.path.join(rme_dir, "project_bounds.geojson")
    return (xml_path if os.path.isfile(xml_path) else None,
            bounds_path if os.path.isfile(bounds_path) else None)


def human_size(num_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def plan_line(huc: str, project_id: str, files: Dict[str, str]) -> str:
    sizes = []
    for rel, abs_path in files.items():
        try:
            sizes.append(f"{rel} ({human_size(os.path.getsize(abs_path))})")
        except Exception:
            sizes.append(f"{rel} (size: n/a)")
    return f"HUC {huc} → Project {project_id} | Files: {', '.join(sizes)}"


def do_real_upload(api: RiverscapesAPI, project_id: str, files_abs_by_rel: Dict[str, str], log: Logger,
                   finalize: bool = True) -> None:
    """
    The real upload path (only used when DRY_RUN=False). Closely follows your provided example.
    If finalize=False, we stop after PUTs (useful for step-by-step testing).
    """
    # Step 0: fetch existing project (for owner/visibility/tags)
    existing_project = api.get_project_full(project_id)

    # Step 1: Request an upload
    rels = list(files_abs_by_rel.keys())
    sizes = [os.path.getsize(files_abs_by_rel[r]) for r in rels]
    # Fake etags per pattern
    etags = ["X" * 24 for _ in rels]

    upload_params = {
        "projectId": project_id,
        "files": rels,
        "etags": etags,
        "sizes": sizes,
        "noDelete": True,
        "owner": {
            "id": existing_project.json["ownedBy"]["id"],
            "type": existing_project.json["ownedBy"]["__typename"].upper(),
        },
        "visibility": existing_project.json["visibility"],
        "tags": existing_project.json.get("tags", []),
    }

    project_upload_qry = api.load_query("requestUploadProject")
    project_upload = api.run_query(project_upload_qry, upload_params)
    token = project_upload["data"]["requestUploadProject"]["token"]
    # TODO combine update array with create array
    # Step 2: request file upload URLs
    print(project_upload["data"]["requestUploadProject"]["update"] + project_upload["data"]["requestUploadProject"]["create"])
    upload_urls_qry = api.load_query("requestUploadProjectFilesUrl")
    upload_urls = api.run_query(upload_urls_qry, {
        "files": project_upload["data"]["requestUploadProject"]["update"] + project_upload["data"]["requestUploadProject"]["create"],
        "token": token
    })
    # Step 3: upload each file
    entries = upload_urls["data"]["requestUploadProjectFilesUrl"]
    log.info(f"Received {len(entries)} upload URLs")
    for info in entries:
        rel_path = info["relPath"]
        url = info["urls"][0]
        file_path = files_abs_by_rel[rel_path]
        log.info(f"Uploading {rel_path} → {url.split('?')[0]}")
        with open(file_path, "rb") as f:
            resp = requests.put(url, data=f, timeout=120)
        if resp.status_code == 200:
            log.info(f"OK: {rel_path}")
        else:
            raise RuntimeError(f"Failed upload {rel_path}: {resp.status_code} {resp.text}")
    log.info("All files uploaded successfully")
    if not finalize:
        # Early return for step-by-step testing (no finalize/poll)
        return

    # Step 4: finalize upload
    finalize_upload_qry = api.load_mutation("finalizeProjectUpload")
    api.run_query(finalize_upload_qry, {"token": token})
    log.info(f"Finalized. Project URL: https://{'staging.' if api.stage == 'staging' else ''}data.riverscapes.net/p/{project_id}")

    # Step 5: (optional) poll until done
    # if POLL_UPLOAD_STATUS:
    #     status_qry = api.load_query("checkUpload")
    #     while True:
    #         status = api.run_query(status_qry, {"token": token})
    #         s = status["data"]["checkUpload"]
    #         if s["status"] == "SUCCESS":
    #             log.info("Upload complete")
    #             break
    #         if s["status"] == "FAILED":
    #             raise RuntimeError(f"Upload failed: {json.dumps(s, indent=2)}")
    #         log.info(f"...Upload status: {s['status']} (retry in {POLL_INTERVAL_SEC}s)")
    #         time.sleep(POLL_INTERVAL_SEC)


def mark_uploaded(match_path: str, match_obj: Dict[str, Any], note: str = "") -> None:
    """Set uploaded flag (and timestamp/note) in the match JSON (atomic)."""
    match_obj["uploaded"] = True
    match_obj["uploadedAt"] = datetime.now(timezone.utc).isoformat()
    if note:
        match_obj["uploadNote"] = note
    atomic_write_json(match_path, match_obj)


def already_uploaded(match_obj: Dict[str, Any]) -> bool:
    return bool(match_obj.get("uploaded") is True)


def main():
    log = Logger("RME → Upload Plan")
    log.title("RME → Upload Plan (Resumable)")

    huc_dirs = find_huc_dirs(BASE_DIR)
    if not huc_dirs:
        print(f"No HUC folders found in {BASE_DIR}")
        return

    # Optional CSV summary
    rows: List[List[str]] = [["huc", "project_id", "project_rs_xml", "project_bounds_geojson", "status", "note"]]

    # RiverscapesAPI context ONLY if we’re actually uploading
    api_ctx = (RiverscapesAPI(stage="production") if not DRY_RUN else None)

    try:
        if api_ctx and hasattr(api_ctx, "__enter__"):
            api_ctx = api_ctx.__enter__()

        for huc_dir in huc_dirs:
            huc = os.path.basename(huc_dir)

            # Load match file
            match_path = os.path.join(huc_dir, MATCH_FILENAME)
            if not os.path.isfile(match_path):
                msg = f"{huc}: Missing {MATCH_FILENAME} — skipping"
                log.debug(msg)
                rows.append([huc, "", "", "", "skip", msg])
                continue

            try:
                match_obj = read_json(match_path)
            except Exception as e:
                msg = f"{huc}: Failed to parse {MATCH_FILENAME}: {e}"
                log.error(msg)
                rows.append([huc, "", "", "", "error", msg])
                continue

            # === RESUME CHECK ===
            if already_uploaded(match_obj):
                msg = f"{huc}: already uploaded — skipping"
                log.info(msg)
                rows.append([huc, match_obj.get('project_2024', {}).get('id', ''), "", "", "skip", msg])
                continue

            project_id = extract_project_id(match_obj)
            if not project_id:
                msg = f"{huc}: project_2024.id not found — skipping"
                log.debug(msg)
                rows.append([huc, "", "", "", "skip", msg])
                continue

            # Collect files
            xml_abs, bounds_abs = collect_upload_files(huc_dir)
            if not xml_abs or not bounds_abs:
                missing = []
                if not xml_abs:
                    missing.append("project.rs.xml")
                if not bounds_abs:
                    missing.append("project_bounds.geojson")
                msg = f"{huc}: Missing required files: {', '.join(missing)} — skipping"
                log.debug(msg)
                rows.append([huc, project_id, xml_abs or "", bounds_abs or "", "skip", msg])
                continue

            # Prepare rel→abs mapping (server expects these relative names)
            files_abs_by_rel = {
                "project.rs.xml": xml_abs,
                "project_bounds.geojson": bounds_abs,
            }

            # Print a plan line (always)
            log.info(plan_line(huc, project_id, files_abs_by_rel))

            if DRY_RUN:
                rows.append([huc, project_id, xml_abs, bounds_abs, "dry-run", "would upload"])
                continue

            # === Real upload path ===
            try:
                # If you're testing step-by-step, pass finalize=False.
                # When you’re ready, set finalize=True so we only mark uploaded after finalization.
                finalize = True  # <- set to False for pre-finalize testing
                do_real_upload(api_ctx, project_id, files_abs_by_rel, log, finalize=finalize)

                # Mark uploaded if finalized; or optionally after PUTs if finalize is disabled and you allow it.
                if finalize:
                    mark_uploaded(match_path, match_obj, note="finalized")
                    rows.append([huc, project_id, xml_abs, bounds_abs, "uploaded", "finalized"])
                elif MARK_AFTER_PUTS_IF_NO_FINALIZE:
                    mark_uploaded(match_path, match_obj, note="marked after PUTs (no finalize) by config")
                    rows.append([huc, project_id, xml_abs, bounds_abs, "uploaded", "no finalize (per config)"])
                else:
                    rows.append([huc, project_id, xml_abs, bounds_abs, "ok", "PUTs complete; not finalized; not marked"])

            except Exception as e:
                msg = f"{huc}: Upload error: {e}"
                log.error(msg)
                rows.append([huc, project_id, xml_abs, bounds_abs, "error", msg])

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
