#!/usr/bin/env python3
import os
import json
from json import JSONDecoder
from typing import Dict, Any, Iterable, List, Optional
from datetime import datetime

# --- INPUTS (as given) ---
SRC_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS.json"
BIG_PATH = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2025CONUS__rscontext_only.json"

# --- OUTPUTS ---
OUT_DIR = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs"
OUT_JSON = os.path.join(OUT_DIR, "fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS.json")
OUT_CSV = os.path.join(OUT_DIR, "fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS_summary.csv")
OUT_STATS = os.path.join(OUT_DIR, "fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS_stats.json")

HUC_KEYS = {"HUC", "Hydrologic Unit Code", "HUV"}  # include typo key just in case


def safe_makedirs(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def get_meta_value(meta: List[Dict[str, Any]], keys: Iterable[str]) -> Optional[str]:
    """Return the first meta value where key matches any in keys (case-sensitive)."""
    if not meta:
        return None
    targets = set(keys)
    for m in meta:
        k = m.get("key")
        if k in targets:
            return m.get("value")
    return None


def iter_json_array_stream(path: str, chunk_size: int = 1_048_576) -> Iterable[Dict[str, Any]]:
    """
    True streaming JSON array reader:
    Yields each object from a top-level JSON array without loading the entire file.
    """
    dec = JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = ""
        # Seek to '['
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                raise ValueError("Unexpected EOF before '[' in JSON.")
            buf += chunk
            buf = buf.lstrip()
            if not buf:
                continue
            if buf[0] != "[":
                # still before the array start; keep reading
                continue
            buf = buf[1:]  # skip '['
            break

        ended = False
        while not ended:
            # Skip whitespace/commas until next value or closing ']'
            while True:
                buf = buf.lstrip()
                if buf:
                    if buf[0] == ",":
                        buf = buf[1:]
                        continue
                    if buf and buf[0] == "]":
                        ended = True
                    break
                # need more data
                chunk = f.read(chunk_size)
                if not chunk:
                    # Allow empty array edge-case; otherwise this is malformed
                    break
                buf += chunk

            if ended:
                break

            # Decode one JSON value; if incomplete, read more
            while True:
                try:
                    obj, idx = dec.raw_decode(buf)
                    yield obj
                    buf = buf[idx:]
                    break
                except ValueError:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        raise ValueError("Malformed JSON or unexpected EOF while decoding a value.")
                    buf += chunk


def minimal_project_view(p: Dict[str, Any]) -> Dict[str, Any]:
    """Trim to stable, helpful fields for outputs."""
    return {
        "id": p.get("id"),
        "name": p.get("name"),
        "projectType": (p.get("projectType") or {}).get("id"),
        "createdOn": p.get("createdOn"),
        "updatedOn": p.get("updatedOn"),
        "ownedBy": (p.get("ownedBy") or {}).get("name"),
        "totalSize": p.get("totalSize"),
        "boundsId": (p.get("bounds") or {}).get("id"),
        # Keep a couple of meta pointers that are often helpful
        "meta": [
            m for m in (p.get("meta") or [])
            if m.get("key") in {"HUC", "Hydrologic Unit Code", "Watershed", "Model Version", "Date Created"}
        ]
    }


def main():
    safe_makedirs(OUT_JSON)

    # 1) Read the 2023CONUS file (assumed smaller) and collect projects + HUCs
    original_projects: List[Dict[str, Any]] = []
    needed_hucs: set[str] = set()

    print("Scanning source (2023CONUS) file...")
    for proj in iter_json_array_stream(SRC_PATH):
        huc = get_meta_value(proj.get("meta") or [], HUC_KEYS)
        if not huc:
            # Skip if it has no HUC-like value
            continue
        original_projects.append({
            "huc": huc,
            "project": minimal_project_view(proj)
        })
        needed_hucs.add(huc)

    print(f"Collected {len(original_projects)} projects with HUC from 2023CONUS; unique HUCs: {len(needed_hucs)}")

    # 2) Stream the large 2025CONUS file once; collect matches by HUC
    matches_by_huc: Dict[str, List[Dict[str, Any]]] = {}
    total_scanned = 0
    total_matched = 0

    print("Streaming large (2025CONUS) file for matches...")
    for proj in iter_json_array_stream(BIG_PATH):
        total_scanned += 1
        huc = get_meta_value(proj.get("meta") or [], HUC_KEYS)
        if not huc or huc not in needed_hucs:
            continue
        matches_by_huc.setdefault(huc, []).append(minimal_project_view(proj))
        total_matched += 1

    print(f"Scanned {total_scanned} projects in 2025CONUS; matched {total_matched} to needed HUCs.")

    # 3) Assemble output list: original project -> all matches
    out: List[Dict[str, Any]] = []
    match_count_histogram: Dict[int, int] = {}
    for entry in original_projects:
        huc = entry["huc"]
        matches = matches_by_huc.get(huc, [])
        out.append({
            "huc": huc,
            "sourceProject": entry["project"],
            "matchesIn2025CONUS": matches
        })
        c = len(matches)
        match_count_histogram[c] = match_count_histogram.get(c, 0) + 1

    # 4) Write outputs
    print("Writing outputs...")
    with open(OUT_JSON, "w", encoding="utf8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # CSV summary: huc, sourceProjectId, sourceProjectName, matchCount
    with open(OUT_CSV, "w", encoding="utf8") as f:
        f.write("huc,sourceProjectId,sourceProjectName,matchCount\n")
        for row in out:
            huc = row["huc"]
            sp = row["sourceProject"]
            f.write(f"{huc},{sp.get('id')},{(sp.get('name') or '').replace(',', ' ')},{len(row['matchesIn2025CONUS'])}\n")

    # Stats
    stats = {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "sourceFile": SRC_PATH,
        "targetFile": BIG_PATH,
        "outputJson": OUT_JSON,
        "outputCsv": OUT_CSV,
        "sourceProjectsWithHUC": len(original_projects),
        "uniqueHUCs": len(needed_hucs),
        "targetProjectsScanned": total_scanned,
        "totalMatchesFound": total_matched,
        "matchCountHistogram": match_count_histogram
    }
    with open(OUT_STATS, "w", encoding="utf8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("Done.")
    print(f"- JSON: {OUT_JSON}")
    print(f"- CSV : {OUT_CSV}")
    print(f"- Stats: {OUT_STATS}")


if __name__ == "__main__":
    main()
