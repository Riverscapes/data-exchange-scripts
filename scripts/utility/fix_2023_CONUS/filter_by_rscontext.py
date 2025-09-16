#!/usr/bin/env python3
"""
Filter a large top-level JSON array to only objects with projectType.id == 'rscontext'
(using a streaming reader and streaming writer).

Usage:
  python rscontext_filter.py \
    /Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2025CONUS.json \
    /Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2025CONUS__rscontext_only.json

If you omit args, defaults are baked in below.
"""

import os
import sys
import json
from datetime import datetime
from typing import Iterable, Dict, Any

# --- Defaults (update as needed) ---
DEFAULT_IN = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2025CONUS.json"
DEFAULT_OUT = "/Users/jagmeetdhillon/Desktop/Software/data-exchange-scripts/logs/fix_2023_CONUS_PRODUCTION_2025CONUS__rscontext_only.json"

# Pretty print output? (False keeps file smaller)
PRETTY = False


def iter_json_array_stream(path: str, chunk_size: int = 1_048_576) -> Iterable[Dict[str, Any]]:
    """
    Stream a top-level JSON array, yielding one object at a time.
    Avoids loading the entire file into memory.
    """
    dec = json.JSONDecoder()
    with open(path, "r", encoding="utf8") as f:
        buf = ""

        # Read until we encounter '[' (start of array)
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
    Returns the count of written objects.
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


def is_rscontext(project: Dict[str, Any]) -> bool:
    pt = (project.get("projectType") or {}).get("id")
    return isinstance(pt, str) and pt.strip().lower() == "rscontext"


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
        for obj in iter_json_array_stream(in_path):
            scanned += 1
            if is_rscontext(obj):
                kept += 1
                yield obj

    start = datetime.utcnow()
    kept_count = stream_write_json_array(generator(), out_path)
    end = datetime.utcnow()

    assert kept_count == kept
    print("Done.")
    print(f"- Input:  {in_path}")
    print(f"- Output: {out_path}")
    print(f"- Scanned objects: {scanned}")
    print(f"- Kept (rscontext): {kept}")
    print(f"- Duration: {(end - start).total_seconds():.2f}s")


if __name__ == "__main__":
    main()
