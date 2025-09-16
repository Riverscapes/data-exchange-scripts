#!/usr/bin/env python3
"""
Check boundsId mismatches between sourceProject and matchesIn2025CONUS[0].

- Streams a large top-level JSON array (memory friendly).
- For each entry:
    * If len(matchesIn2025CONUS) > 1 -> log a warning.
    * If len(matchesIn2025CONUS) == 0 -> skip (no match to compare).
    * Compare sourceProject.boundsId vs matchesIn2025CONUS[0].boundsId.
      If they differ (including if one is missing), write a compact record to output.

Usage:
  python check_bounds_mismatch.py \
    /path/to/huc_matched_withMatchesOnly.json \
    /path/to/huc_boundsId_mismatches.json \
    /path/to/huc_boundsId_mismatch_warnings.log

If args are omitted, defaults below are used.
"""

import os
import sys
import json
from datetime import datetime
from typing import Iterable, Dict, Any, Optional

# --- Defaults (adjust as needed) ---
DEFAULT_IN = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS__withMatchesOnly.json"
DEFAULT_OUT = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_boundsId_mismatches.json"
DEFAULT_LOG = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_boundsId_mismatch_warnings.log"

# Pretty JSON output? (False keeps output smaller)
PRETTY = False


def iter_json_array_stream(path: str, chunk_size: int = 1_048_576) -> Iterable[Dict[str, Any]]:
    """
    Stream-read a top-level JSON array; yield one element (dict) at a time.
    """
    dec = json.JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = ""

        # Find '['
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                raise ValueError("Expected a top-level JSON array, reached EOF before '['.")
            buf += chunk
            buf = buf.lstrip()
            if not buf:
                continue
            if buf[0] == "[":
                buf = buf[1:]
                break
            else:
                raise ValueError("Top-level JSON value is not an array.")

        ended = False
        while not ended:
            # Skip ws and commas; stop at ']'
            while True:
                buf = buf.lstrip()
                if buf:
                    if buf[0] == ",":
                        buf = buf[1:]
                        continue
                    if buf[0] == "]":
                        ended = True
                    break
                chunk = f.read(chunk_size)
                if not chunk:
                    ended = True
                    break
                buf += chunk

            if ended:
                break

            # Decode exactly one JSON value
            while True:
                try:
                    obj, idx = dec.raw_decode(buf)
                    yield obj
                    buf = buf[idx:]
                    break
                except ValueError:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        raise ValueError("Malformed JSON or unexpected EOF while decoding array element.")
                    buf += chunk


def stream_write_json_array(objs: Iterable[Dict[str, Any]], out_path: str) -> int:
    """
    Stream-write iterable of dicts to a valid JSON array file.
    Returns number of written objects.
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    count = 0
    with open(out_path, "w", encoding="utf8") as out:
        out.write("[\n")
        first = True
        for obj in objs:
            if not first:
                out.write(",\n")
            first = False
            if PRETTY:
                out.write(json.dumps(obj, ensure_ascii=False, indent=2))
            else:
                out.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")))
            count += 1
        out.write("\n]\n")
    return count


def warn(msg: str, log_path: Optional[str]) -> None:
    """
    Print warning to stderr and append to a log file (if provided).
    """
    line = f"[WARN] {datetime.utcnow().isoformat()}Z {msg}"
    print(line, file=sys.stderr)
    if log_path:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf8") as lf:
            lf.write(line + "\n")


def mismatch_records(in_path: str, warn_path: Optional[str]) -> Iterable[Dict[str, Any]]:
    """
    Generator that yields a compact record for each boundsId mismatch.
    """
    scanned = 0
    for entry in iter_json_array_stream(in_path):
        scanned += 1

        huc = entry.get("huc")
        sp = entry.get("sourceProject") or {}
        sp_bounds = sp.get("boundsId")
        sp_id = sp.get("id")
        sp_name = sp.get("name")
        sp_projectType = sp.get("projectType")

        matches = entry.get("matchesIn2025CONUS")
        if not isinstance(matches, list) or len(matches) == 0:
            # No match to compare; ignore (file is expected to be "withMatchesOnly", but be robust)
            continue

        if len(matches) > 1:
            warn(f"HUC {huc}: {len(matches)} matches found (expected 1). Using the first for comparison.", warn_path)

        m0 = matches[0] or {}
        m_bounds = m0.get("boundsId")
        m_id = m0.get("id")
        m_name = m0.get("name")

        # Mismatch if different, including cases where one is missing and the other not.
        if sp_bounds != m_bounds:
            yield {
                "huc": huc,
                "source": {
                    "id": sp_id,
                    "name": sp_name,
                    "boundsId": sp_bounds,
                    "projectType": sp_projectType
                },
                "match": {
                    "id": m_id,
                    "name": m_name,
                    "boundsId": m_bounds
                }
            }


def main():
    in_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_IN
    out_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT
    log_path = sys.argv[3] if len(sys.argv) > 3 else DEFAULT_LOG

    if not os.path.exists(in_path):
        print(f"Error: input file not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    start = datetime.utcnow()
    written = stream_write_json_array(mismatch_records(in_path, log_path), out_path)
    end = datetime.utcnow()

    print("Done.")
    print(f"- Input:  {in_path}")
    print(f"- Output (mismatches only): {out_path}")
    print(f"- Warnings log: {log_path}")
    print(f"- Mismatch count: {written}")
    print(f"- Duration: {(end - start).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
