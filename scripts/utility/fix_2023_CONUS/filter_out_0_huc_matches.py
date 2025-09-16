#!/usr/bin/env python3
"""
Filter HUC-matched entries to only those with non-empty matchesIn2025CONUS.

Usage:
  python filter_huc_matches.py \
    /Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS.json \
    /Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS__withMatchesOnly.json

If you omit args, defaults below are used.
"""

import os
import sys
import json
from datetime import datetime
from typing import Iterable, Dict, Any

# --- Defaults (edit if you like) ---
DEFAULT_IN = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS.json"
DEFAULT_OUT = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2023CONUS_matches_in_2025CONUS__withMatchesOnly.json"

# Pretty JSON output? (False keeps file smaller)
PRETTY = False


def iter_json_array_stream(path: str, chunk_size: int = 1_048_576) -> Iterable[Dict[str, Any]]:
    """
    Stream a top-level JSON array, yielding one object at a time.
    Avoids loading the entire file into memory.
    """
    dec = json.JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = ""

        # Read until '[' (start of array)
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                raise ValueError("Expected a top-level JSON array, but reached EOF before '['.")
            buf += chunk
            buf = buf.lstrip()
            if not buf:
                continue
            if buf[0] == "[":
                buf = buf[1:]  # consume '['
                break
            if buf[0] != "[":
                raise ValueError("Top-level JSON value is not an array.")

        ended = False
        while not ended:
            # Skip whitespace/commas; detect ']' (end of array)
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

            # Decode one JSON value; read more if incomplete
            while True:
                try:
                    obj, idx = dec.raw_decode(buf)
                    yield obj
                    buf = buf[idx:]
                    break
                except ValueError:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        raise ValueError("Malformed JSON or unexpected EOF while decoding an array element.")
                    buf += chunk


def stream_write_json_array(objs: Iterable[Dict[str, Any]], out_path: str) -> int:
    """
    Stream-write an iterable of objects to a valid JSON array file.
    Returns the count written.
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


def has_nonempty_matches(entry: Dict[str, Any]) -> bool:
    """
    True if entry['matchesIn2025CONUS'] is a list with length > 0.
    Treat missing/non-list as empty (i.e., filter out).
    """
    matches = entry.get("matchesIn2025CONUS")
    return isinstance(matches, list) and len(matches) > 0


def main():
    in_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_IN
    out_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUT

    if not os.path.exists(in_path):
        print(f"Error: input file not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    scanned = 0
    kept = 0

    def generator():
        nonlocal scanned, kept
        for entry in iter_json_array_stream(in_path):
            scanned += 1
            if has_nonempty_matches(entry):
                kept += 1
                yield entry

    start = datetime.utcnow()
    written = stream_write_json_array(generator(), out_path)
    end = datetime.utcnow()

    # Sanity check
    assert written == kept

    print("Done.")
    print(f"- Input:  {in_path}")
    print(f"- Output: {out_path}")
    print(f"- Scanned objects: {scanned}")
    print(f"- Kept (non-empty matchesIn2025CONUS): {kept}")
    print(f"- Duration: {(end - start).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
