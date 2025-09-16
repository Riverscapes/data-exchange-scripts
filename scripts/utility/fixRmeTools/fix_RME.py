from typing import Dict, Any, Optional
import os
import json
from datetime import datetime
from typing import List, Tuple
from rsxml import Logger
from rsxml.util import safe_makedirs
from pydex import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject
import io
from json import JSONDecoder


def fix_rme(riverscapes_api: RiverscapesAPI, logdir: str = None) -> Tuple[str, str]:
    """
    Find all projects with tags 2024CONUS and 2025CONUS and write each set to a log file.

    Args:
        riverscapes_api: Active RiverscapesAPI context/instance.
        logdir: Directory to write logs into. Defaults to ~/RSTagging.

    Returns:
        Tuple of (path_to_2024_log, path_to_2025_log).
    """
    log = Logger("FixRME")
    log.title("Scan ALL projects by CONUS tags and write logs")

    # Resolve log directory
    if not logdir:
        logdir = os.path.join(os.path.expanduser("~"), "Desktop", "Software", "data-exchange-scripts", "logs")
    safe_makedirs(logdir)

    def make_search_params(tag: str) -> RiverscapesSearchParams:
        """Build search params for a single tag, across all projects."""
        try:
            return RiverscapesSearchParams(
                input_obj={
                    "createdOn": {
                        "from": "2023-01-01T08:00:00Z",
                        "to": "2024-01-01T08:00:00Z"
                    },
                    "ownedBy": {
                        "type": "ORGANIZATION",
                        "id": "b35b8f4f-016d-4c60-bbaa-11c9563fb744"
                    },
                    "tags": [
                        "Cybercastor"
                    ],
                    "excludeArchived": False
                }
            )
        except TypeError:
            sp = RiverscapesSearchParams({})
            setattr(sp, "tags", [tag])
            return sp

    def search_and_write(tag: str) -> str:
        """Run a search for a single tag and write results to a JSON file; return the path."""
        sp = make_search_params(tag)
        projects: List[RiverscapesProject] = []

        total = 0
        log.info(f"Searching ALL projects with tag: {tag}")
        for project, _stats, search_total, _prg in riverscapes_api.search(sp, progress_bar=True):
            total = search_total
            projects.append(project)

        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"fix_2023_CONUS_{riverscapes_api.stage}_{tag}.json"
        fpath = os.path.join(logdir, fname)

        with open(fpath, "w", encoding="utf8") as fobj:
            def _proj_json(p: RiverscapesProject):
                try:
                    return p.json
                except Exception:
                    return {
                        "id": getattr(p, "id", None),
                        "name": getattr(p, "name", None),
                        "tags": getattr(p, "tags", []),
                        "owner": getattr(p, "owner", None),
                    }

            json.dump([_proj_json(p) for p in projects], fobj, indent=2)

        log.info(f"Wrote {len(projects)}/{total} projects for tag '{tag}' to: {fpath}")
        return fpath

    path_2023 = search_and_write("2023CONUS")
    # path_2025 = search_and_write("2025CONUS")

    log.info("Done scanning CONUS tags.")
    return (path_2023)


# ----------------------------
# NEXT SECTION: Filter by bounds (streaming)
# ----------------------------


def _iter_json_array(path: str):
    """
    Stream a top-level JSON array from `path`, yielding one parsed object at a time
    using a JSONDecoder with raw_decode to avoid loading the entire file.
    """
    decoder = JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = f.read()

    i = 0
    n = len(buf)

    # Skip leading whitespace and expect '['
    while i < n and buf[i].isspace():
        i += 1
    if i >= n or buf[i] != "[":
        raise ValueError("Expected a top-level JSON array.")
    i += 1  # skip '['

    # Iterate array elements
    while True:
        # Skip whitespace and optional commas
        while i < n and buf[i].isspace():
            i += 1
        if i < n and buf[i] == ",":
            i += 1
            while i < n and buf[i].isspace():
                i += 1

        # End of array?
        if i < n and buf[i] == "]":
            break
        if i >= n:
            break

        # Decode next JSON value (object)
        obj, end = decoder.raw_decode(buf, idx=i)
        yield obj
        i = end

    # Done


def _bounds_missing(p: dict) -> bool:
    """
    True if bounds are absent/empty:
      - 'bounds' key missing
      - bounds is None
      - bounds is not a dict
      - bounds.id missing or empty string
    """
    b = p.get("bounds", None)
    if not b:
        return True
    if not isinstance(b, dict):
        return True
    bid = b.get("id", None)
    if bid is None:
        return True
    if isinstance(bid, str) and bid.strip() == "":
        return True
    return False


def stream_filter_projects_without_bounds(input_path: str, output_path: str, log: Logger | None = None) -> Tuple[int, int]:
    """
    Stream `input_path` (a JSON array of projects) and write only projects with
    missing/empty `bounds` to `output_path`, also as a JSON array — streaming style.

    Returns:
        (kept_count, total_count)
    """
    if log:
        log.title("Filter projects missing bounds (streaming)")
        log.info(f"Input:  {input_path}")
        log.info(f"Output: {output_path}")

    total = 0
    kept = 0

    # Write a streamed JSON array: we manage commas manually
    with open(output_path, "w", encoding="utf8") as out_f:
        out_f.write("[")
        first = True

        for proj in _iter_json_array(input_path):
            total += 1
            if _bounds_missing(proj):
                if not first:
                    out_f.write(",\n")
                else:
                    first = False
                json.dump(proj, out_f, ensure_ascii=False)
                kept += 1

        out_f.write("]\n")

    if log:
        log.info(f"Kept {kept}/{total} projects with missing bounds.")
    return kept, total

# ---- Invoke the filter for your specific 2024 file ----


def filter_2024conus_missing_bounds():
    log = Logger("FixRME-MissingBounds")
    # Your specific source file:
    input_path = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2024CONUS.json"

    # New output file (overwrite if exists):
    base, ext = os.path.splitext(input_path)
    output_path = f"{base}_MISSING_BOUNDS{ext}"

    # Make sure the log directory exists (should already from above)
    safe_makedirs(os.path.dirname(output_path))

    if not os.path.exists(input_path):
        log.error(f"Input file not found: {input_path}")
        return None

    # Stream + write filtered results
    kept, total = stream_filter_projects_without_bounds(input_path, output_path, log)
    log.info(f"Wrote filtered list to: {output_path}")
    return output_path


def _get_meta_value(meta_list: list, key_name: str) -> Optional[str]:
    """Find meta entry by key (exact match) and return its value as string, else None."""
    if not isinstance(meta_list, list):
        return None
    for kv in meta_list:
        try:
            if kv.get("key") == key_name:
                val = kv.get("value")
                return str(val) if val is not None else None
        except AttributeError:
            continue
    return None

# ----------------------------
# NEXT SECTION: HUC Matching
# ----------------------------


def _parse_iso8601(dt_str: str) -> Optional[datetime]:
    """Parse ISO8601-ish datetime safely, return None if invalid."""
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _pick_most_recent(projects: list) -> Optional[dict]:
    """Given a list of lean project dicts, return the one with the latest updatedOn/createdOn."""
    if not projects:
        return None

    def sort_key(p: dict):
        ts = _parse_iso8601(p.get("updatedOn")) or _parse_iso8601(p.get("createdOn"))
        return ts or datetime.min

    return max(projects, key=sort_key)


def _extract_huc(project: dict) -> Optional[str]:
    """
    Extract a canonical HUC string from a project meta.
    Tries 'HUC' then 'Hydrologic Unit Code'. Returns normalized digits-only string if found.
    """
    meta = project.get("meta", [])
    huc = _get_meta_value(meta, "HUC") or _get_meta_value(meta, "Hydrologic Unit Code")
    if not huc:
        return None
    # Normalize: keep only digits (common HUCs are 10- or 12-digit strings).
    digits = "".join(ch for ch in str(huc) if ch.isdigit())
    return digits or None


def _lean_project_record(p: dict) -> dict:
    """
    Reduce 2025 project to a lean record for output.
    Add/adjust fields as needed (keep this small to avoid memory bloat).
    """
    return {
        "id": p.get("id"),
        "name": p.get("name"),
        "tags": p.get("tags", []),
        "bounds": p.get("bounds", None),
        "projectType": p.get("projectType", None),
        "createdOn": p.get("createdOn", None),
        "updatedOn": p.get("updatedOn", None),
        "ownedBy": p.get("ownedBy", None),
        # Keep HUC so we can eyeball it in the results:
        "HUC": _extract_huc(p),
    }


def _build_2025_huc_index(path_2025: str, log: Logger | None = None) -> Dict[str, list]:
    """
    Stream the 2025CONUS file once and build an index of HUC -> [lean project dict, ...].
    """
    if log:
        log.title("Build 2025 HUC index (streaming)")
        log.info(f"Source: {path_2025}")

    index: Dict[str, list] = {}
    total = 0
    indexed = 0

    for proj in _iter_json_array(path_2025):
        total += 1
        huc = _extract_huc(proj)
        if not huc:
            continue
        index.setdefault(huc, []).append(_lean_project_record(proj))
        indexed += 1

    if log:
        log.info(f"Indexed {indexed}/{total} projects with a valid HUC from 2025CONUS.")
    return index


def match_hucs_2024_missing_to_2025(
    path_2024_missing: str = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2024CONUS_MISSING_BOUNDS.json",
    path_2025_all: str = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_rme_PRODUCTION_2025CONUS.json",
) -> Optional[str]:
    """
    For each 2024 (missing-bounds) project, find the **most recent** 2025 project with the same HUC.
    Writes an array of:
      { "project_2024": <full 2024 object>, "match_2025": <lean 2025 obj or null> }
    Returns the output path, or None if input missing.
    """
    log = Logger("FixRME-HUCMatch")

    if not os.path.exists(path_2024_missing):
        log.error(f"Missing 2024 file: {path_2024_missing}")
        return None
    if not os.path.exists(path_2025_all):
        log.error(f"Missing 2025 file: {path_2025_all}")
        return None

    # Output path next to the 2024 missing-bounds file
    base_24, ext_24 = os.path.splitext(path_2024_missing)
    output_path = f"{base_24}_HUC_MATCHES{ext_24}"
    safe_makedirs(os.path.dirname(output_path))

    # Build index of 2025 projects by HUC
    huc_index_2025 = _build_2025_huc_index(path_2025_all, log)

    log.title("Match 2024 (missing bounds) -> 2025 by HUC (most recent only)")
    log.info(f"2024 source: {path_2024_missing}")
    log.info(f"Output:      {output_path}")

    total_2024 = 0
    matched_2024 = 0

    with open(output_path, "w", encoding="utf8") as out_f:
        out_f.write("[")
        first = True

        for proj_2024 in _iter_json_array(path_2024_missing):
            total_2024 += 1
            huc = _extract_huc(proj_2024)
            match = None
            if huc:
                candidates = huc_index_2025.get(huc, [])
                match = _pick_most_recent(candidates)

            if match:
                if not first:
                    out_f.write(",\n")
                else:
                    first = False

                record = {
                    "project_2024": proj_2024,  # full original object
                    "match_2025": match,        # just the most recent one
                }
                json.dump(record, out_f, ensure_ascii=False)
                matched_2024 += 1

        out_f.write("]\n")

    log.info(f"Matched {matched_2024}/{total_2024} 2024 projects by HUC. Wrote: {output_path}")
    return output_path


if __name__ == "__main__":
    print("Starting fix_RME.py...")
    with RiverscapesAPI() as api:
        fix_rme(api)
    # filter_2024conus_missing_bounds()
    # match_hucs_2024_missing_to_2025()
