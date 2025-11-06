#!/usr/bin/env python3
"""Flatten layer catalog + layer definitions into partitioned Parquet (default) files for Athena.

Output pattern:
    dist/metadata/authority_name=<name>/authority_version=<version>/layer_metadata.parquet

Index manifest:
    dist/metadata/index.json with partition list + row counts.

Defaults:
    - Format: parquet (use --format csv to get CSV instead)
    - Partition columns (authority_name, authority_version) EXCLUDED from file contents unless --include-partition-cols provided.
    - commit_sha always included (current repo HEAD).
    - No per-row timestamp; generation timestamp stored once in index.json.

Usage examples:
    python scripts/metadata/flatten_layer_catalog.py
    python scripts/metadata/flatten_layer_catalog.py --format csv --include-partition-cols

Notes:
    - Scans repository recursively for 'layer_catalog.json'.
    - Resolves each layer's definition via its 'def_path'.
    - Robust to missing optional fields.
    - Designed for Python >= 3.12 (compatible >=3.10).
    - Parquet writing uses pyarrow.
"""
from __future__ import annotations
import argparse
import csv
import json
import subprocess
import datetime as _dt
from pathlib import Path

from jsonschema import Draft202012Validator
import pyarrow as pa
import pyarrow.parquet as pq

CATALOG_FILENAME = "layer_catalog.json"
OUTPUT_COLUMNS = [  # logical full schema (including potential partition columns)
    "authority_name",
    "authority_version",
    "layer_id",
    "layer_name",
    "layer_type",
    "layer_description",
    # column-defining fields with prefixes removed
    "name",
    "friendly_name",
    "data_unit",
    "dtype",
    "description",
    "is_key",
    "is_nullable",
    "commit_sha",
]


def git_commit_sha() -> str | None:
    """Return current git commit SHA or None if not available."""
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
        return sha
    except Exception:
        return None


def find_catalogs(root: Path) -> list[Path]:
    """Find all catalogs in the repository."""
    return [p for p in root.rglob(CATALOG_FILENAME)]


def load_json(path: Path) -> dict:
    """Load JSON from disk."""
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def safe_get(d: dict, key: str, default: str = "") -> str:
    v = d.get(key, default)
    if v is None:
        return default
    return str(v)


def _load_validator(root: Path, schema_rel: str) -> Draft202012Validator | None:
    """Load a JSON Schema validator or return None if the schema file is absent."""
    schema_path = (root / schema_rel).resolve()
    if not schema_path.exists():
        return None
    with schema_path.open("r", encoding="utf-8") as f:
        schema_data = json.load(f)
    return Draft202012Validator(schema_data)


def flatten_catalog(catalog_path: Path, commit_sha: str | None, catalog_validator: Draft202012Validator | None, layer_validator: Draft202012Validator | None, errors: list[dict]) -> list[dict]:
    """Flatten a single catalog into row dicts, validating schemas if validators provided."""
    catalog = load_json(catalog_path)
    if catalog_validator:
        for e in catalog_validator.iter_errors(catalog):
            errors.append({
                "file": str(catalog_path),
                "type": "catalog",
                "message": e.message,
                "path": list(e.path)
            })
    authority_name = catalog.get("authority_name", "")
    authority_version = catalog.get("authority_version", "")
    layers = catalog.get("layers", {})
    rows: list[dict] = []

    for layer_id, layer_info in layers.items():
        def_path_raw = layer_info.get("def_path")
        if not def_path_raw:
            continue  # skip malformed layer entry
        def_path = (catalog_path.parent / def_path_raw).resolve()
        if not def_path.exists():
            # If definition file missing, skip silently (could log)
            continue
        try:
            layer_def = load_json(def_path)
        except Exception as ex:
            errors.append({
                "file": str(def_path),
                "type": "layer",
                "message": f"Failed to load: {ex}",
                "path": []
            })
            continue

        if layer_validator:
            for e in layer_validator.iter_errors(layer_def):
                errors.append({
                    "file": str(def_path),
                    "type": "layer",
                    "message": e.message,
                    "path": list(e.path)
                })

        # layer_type and description are now sourced ONLY from catalog to avoid duplication.
        layer_name = layer_def.get("layer_name", layer_id)
        layer_type = layer_info.get("layer_type", "")
        layer_description = layer_info.get("description", "")
        columns = layer_def.get("columns", [])

        for col in columns:
            # Some truncated JSON may omit fields entirely; ensure robustness.
            cname = col.get("name", "")
            if not cname:
                continue
            rows.append({
                "authority_name": authority_name,
                "authority_version": authority_version,
                "layer_id": layer_id,
                "layer_name": layer_name,
                "layer_type": layer_type,
                "layer_description": layer_description,
                "name": cname,
                "friendly_name": col.get("friendly_name", ""),
                "data_unit": col.get("data_unit", ""),
                "dtype": col.get("dtype", ""),
                "description": col.get("description", ""),
                "is_key": col.get("is_key", False),
                "is_nullable": col.get("is_nullable", True),
                "commit_sha": commit_sha or "",
            })

    return rows


def write_csv(rows: list[dict], output: Path, columns: list[str]) -> None:
    """Write a list of row dicts to CSV with specified columns."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in columns})


def write_parquet(rows: list[dict], output: Path, columns: list[str]) -> None:
    """Write rows to Parquet using pyarrow."""
    output.parent.mkdir(parents=True, exist_ok=True)
    # Build Arrow schema dynamically
    field_types = {
        "layer_id": pa.string(),
        "layer_name": pa.string(),
        "layer_type": pa.string(),
        "layer_description": pa.string(),
        "name": pa.string(),
        "friendly_name": pa.string(),
        "data_unit": pa.string(),
        "dtype": pa.string(),
        "description": pa.string(),
        "is_key": pa.bool_(),
        "is_nullable": pa.bool_(),
        "commit_sha": pa.string(),
        "authority_name": pa.string(),
        "authority_version": pa.string(),
    }
    pa_fields = [pa.field(c, field_types.get(c, pa.string())) for c in columns]
    data_cols = {c: [r.get(c) for r in rows] for c in columns}
    table = pa.Table.from_pydict(data_cols, schema=pa.schema(pa_fields))
    pq.write_table(table, output)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten layer catalogs into partitioned metadata files for Athena.")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[2]), help="Repo root to scan (default: project root).")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet", help="Output file format per partition (default parquet).")
    parser.add_argument("--include-partition-cols", action="store_true", help="Include authority_name and authority_version columns inside each file (default: excluded).")
    parser.add_argument("--output", default="dist/metadata", help="Base output directory.")
    return parser.parse_args()


def group_rows(rows: list[dict]) -> dict[tuple[str, str], list[dict]]:
    groups: dict[tuple[str, str], list[dict]] = {}
    for r in rows:
        key = (r.get("authority_name", ""), r.get("authority_version", ""))
        groups.setdefault(key, []).append(r)
    return groups


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    base_output = Path(args.output)
    if not base_output.is_absolute():
        base_output = root / base_output
    commit_sha = git_commit_sha()

    catalogs = find_catalogs(root)
    # Load validators (tolerate absence)
    catalog_validator = _load_validator(root, "metadata_schemas/layer_catalog.schema.json")
    layer_validator = _load_validator(root, "metadata_schemas/layer.schema.json")
    validation_errors: list[dict] = []
    all_rows: list[dict] = []
    for c in catalogs:
        all_rows.extend(flatten_catalog(c, commit_sha=commit_sha, catalog_validator=catalog_validator, layer_validator=layer_validator, errors=validation_errors))

    # Partitioned mode only (single-file legacy removed)
    groups = group_rows(all_rows)
    index_manifest = []
    for (authority, version), rows_group in groups.items():
        part_dir = base_output / f"authority_name={authority}" / f"authority_version={version}"
        columns = OUTPUT_COLUMNS.copy()
        # commit_sha always included
        if not args.include_partition_cols:
            columns = [c for c in columns if c not in {"authority_name", "authority_version"}]
        out_path = part_dir / ("layer_metadata." + ("parquet" if args.format == "parquet" else "csv"))
        if args.format == "parquet":
            write_parquet(rows_group, out_path, columns)
        else:
            write_csv(rows_group, out_path, columns)
        index_manifest.append({
            "authority_name": authority,
            "authority_version": version,
            "row_count": len(rows_group),
            "path": str(out_path.relative_to(base_output)),
        })

    # Write index.json
    index_path = base_output / "index.json"
    index_path.parent.mkdir(parents=True, exist_ok=True)
    with index_path.open("w", encoding="utf-8") as f:
        json.dump({
            "generated_at": _dt.date.today().isoformat(),
            "commit_sha": commit_sha,
            "partitions": index_manifest,
            "total_rows": len(all_rows),
            "validation_errors": validation_errors,
        }, f, indent=2)
    print(f"Wrote {len(all_rows)} rows across {len(groups)} partitions to {base_output}")


if __name__ == "__main__":
    main()
