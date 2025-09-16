#!/usr/bin/env python3
import os
import json
import re
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from statistics import mean

# ============================
# Config
# ============================
BASE_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/data/rme_bounds_upload_ready"
MATCH_FILENAME = "rme_rscontext_match.json"
RME_SUBDIR = "rme"
HUC_REGEX = re.compile(r"^\d{10}$")

WRITE_CSV = True
CSV_PATH = os.path.join(BASE_DIR, "_huc_upload_status.csv")

WRITE_JSON_SUMMARY = True
JSON_SUMMARY_PATH = os.path.join(BASE_DIR, "_huc_upload_summary.json")


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


def read_match(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def human_size(n: Optional[int]) -> str:
    if n is None:
        return "n/a"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


def file_size(path: str) -> Optional[int]:
    try:
        return os.path.getsize(path)
    except Exception:
        return None


def collect_status(huc_dir: str) -> Dict[str, Any]:
    huc = os.path.basename(huc_dir)
    match_path = os.path.join(huc_dir, MATCH_FILENAME)
    rme_dir = os.path.join(huc_dir, RME_SUBDIR)
    xml_path = os.path.join(rme_dir, "project.rs.xml")
    bounds_path = os.path.join(rme_dir, "project_bounds.geojson")

    match = read_match(match_path)
    if match is None:
        return {
            "huc": huc,
            "has_match": False,
            "uploaded": False,
            "uploadedAt": None,
            "project_id": None,
            "has_xml": os.path.isfile(xml_path),
            "has_bounds": os.path.isfile(bounds_path),
            "xml_size": file_size(xml_path),
            "bounds_size": file_size(bounds_path),
            "note": "missing rme_rscontext_match.json",
        }

    uploaded = bool(match.get("uploaded") is True)
    uploaded_at = match.get("uploadedAt")
    pid = (match.get("project_2024") or {}).get("id")

    return {
        "huc": huc,
        "has_match": True,
        "uploaded": uploaded,
        "uploadedAt": uploaded_at,
        "project_id": pid,
        "has_xml": os.path.isfile(xml_path),
        "has_bounds": os.path.isfile(bounds_path),
        "xml_size": file_size(xml_path),
        "bounds_size": file_size(bounds_path),
        "note": "",
    }


def main():
    huc_dirs = find_huc_dirs(BASE_DIR)
    if not huc_dirs:
        print(f"No HUC folders found in {BASE_DIR}")
        return

    rows: List[Dict[str, Any]] = []
    for d in huc_dirs:
        rows.append(collect_status(d))

    total = len(rows)
    uploaded = sum(1 for r in rows if r["uploaded"])
    not_uploaded = total - uploaded

    have_match = sum(1 for r in rows if r["has_match"])
    missing_match = total - have_match

    # Not uploaded subsets
    not_up_rows = [r for r in rows if not r["uploaded"]]
    ready_to_upload = sum(1 for r in not_up_rows if r["has_xml"] and r["has_bounds"])
    miss_xml = sum(1 for r in not_up_rows if not r["has_xml"] and r["has_bounds"])
    miss_bounds = sum(1 for r in not_up_rows if r["has_xml"] and not r["has_bounds"])
    miss_both = sum(1 for r in not_up_rows if not r["has_xml"] and not r["has_bounds"])

    # Size stats (for “ready” set)
    ready_rows = [r for r in not_up_rows if r["has_xml"] and r["has_bounds"]]
    xml_sizes = [r["xml_size"] for r in ready_rows if r["xml_size"] is not None]
    bounds_sizes = [r["bounds_size"] for r in ready_rows if r["bounds_size"] is not None]

    # Uploaded timestamps (if present)
    uploaded_times = []
    for r in rows:
        ts = r.get("uploadedAt")
        if ts:
            try:
                uploaded_times.append(datetime.fromisoformat(ts.replace("Z", "+00:00")))
            except Exception:
                pass
    earliest = min(uploaded_times).isoformat() if uploaded_times else None
    latest = max(uploaded_times).isoformat() if uploaded_times else None

    pct = (uploaded / total * 100.0) if total else 0.0

    print("\n=== RME Upload Status Summary ===")
    print(f"Base dir                 : {BASE_DIR}")
    print(f"Total HUC folders        : {total}")
    print(f"With match.json          : {have_match}  (missing: {missing_match})")
    print(f"Uploaded                 : {uploaded} / {total}  ({pct:.1f}%)")
    print(f"Not uploaded             : {not_uploaded}")
    print(f" ├─ Ready to upload      : {ready_to_upload}")
    print(f" ├─ Missing XML          : {miss_xml}")
    print(f" ├─ Missing bounds       : {miss_bounds}")
    print(f" └─ Missing both         : {miss_both}")

    if xml_sizes and bounds_sizes:
        print("\nEstimated payload (ready set):")
        print(f" - XML total / avg        : {human_size(sum(xml_sizes))} / {human_size(mean(xml_sizes))}")
        print(f" - Bounds total / avg     : {human_size(sum(bounds_sizes))} / {human_size(mean(bounds_sizes))}")

    if earliest or latest:
        print("\nUploaded timestamps:")
        print(f" - Earliest               : {earliest or 'n/a'}")
        print(f" - Latest                 : {latest or 'n/a'}")

    # Optional CSV of per-HUC details
    if WRITE_CSV:
        import csv
        with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "huc", "project_id", "uploaded", "uploadedAt",
                "has_match", "has_xml", "has_bounds",
                "xml_size_bytes", "bounds_size_bytes", "note"
            ])
            for r in rows:
                w.writerow([
                    r["huc"], r.get("project_id", ""),
                    r["uploaded"], r.get("uploadedAt") or "",
                    r["has_match"], r["has_xml"], r["has_bounds"],
                    r["xml_size"] if r["xml_size"] is not None else "",
                    r["bounds_size"] if r["bounds_size"] is not None else "",
                    r.get("note", "")
                ])
        print(f"\nWrote CSV: {CSV_PATH}")

    # Optional compact JSON summary (topline numbers)
    if WRITE_JSON_SUMMARY:
        summary = {
            "baseDir": BASE_DIR,
            "totals": {
                "hucs": total,
                "withMatch": have_match,
                "missingMatch": missing_match,
                "uploaded": uploaded,
                "notUploaded": not_uploaded,
            },
            "notUploadedBreakdown": {
                "readyToUpload": ready_to_upload,
                "missingXML": miss_xml,
                "missingBounds": miss_bounds,
                "missingBoth": miss_both,
            },
            "sizesReadySet": {
                "xmlTotalBytes": int(sum(xml_sizes)) if xml_sizes else 0,
                "xmlAvgBytes": int(mean(xml_sizes)) if xml_sizes else 0,
                "boundsTotalBytes": int(sum(bounds_sizes)) if bounds_sizes else 0,
                "boundsAvgBytes": int(mean(bounds_sizes)) if bounds_sizes else 0,
            },
            "uploadedTimestamps": {
                "earliest": earliest,
                "latest": latest,
            },
        }
        with open(JSON_SUMMARY_PATH, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"Wrote JSON summary: {JSON_SUMMARY_PATH}")

    print("\nDone.")


if __name__ == "__main__":
    main()
