# GeoPackage -> Athena / S3 Tables Interactive Upload

An interactive command-line script that reads a layer from a GeoPackage file and uploads it as an Apache Iceberg table to either AWS Athena on S3 (via Glue) or AWS S3 Tables.

## Overview

The script walks through these interactive steps:

1. Credential check via `sts:GetCallerIdentity`.
2. GeoPackage path and layer selection.
3. Source schema preview (field, dtype, sample value).
4. User confirmation that schema is correct.
5. Optional load of column comments and layer description from `layer_definitions.json`.
6. Optional reprojection (default target CRS prompt is `EPSG:4326`).
7. Destination choice:
   - Athena on S3 (Parquet/Iceberg via Glue)
   - S3 Tables (Iceberg REST catalog)
8. Destination schema preview and final confirmation.
9. Chunked Iceberg append with a live progress bar.
10. Automatic rollback of newly created namespace/table on failure.

Key data behavior:

- Geometry is written to a binary `geom_wkb` column (WKB bytes).
- CRS is stored in table property `geo.crs_wkt`.
- Source column names are normalized to lowercase before schema derivation and write.
- If a `fid` field is expected by schema but only exists in index, it is exposed as a column before write.

## Prerequisites

### Python environment

```bash
# From the repo root
uv sync
```

Run with `uv run` (no manual venv activation required).

### AWS credentials

Valid AWS credentials are required before running the script. The startup check prints remediation guidance when credentials are missing or expired.

Supported credential sources (standard AWS chain):

- AWS SSO: `aws sso login [--profile <profile>]`
- Named profile: `AWS_PROFILE=<profile>`
- Static keys: `AWS_ACCESS_KEY_ID=...` and `AWS_SECRET_ACCESS_KEY=...`
- IAM role credentials (EC2/ECS/Lambda)

### Region

- Uses `AWS_REGION` when set.
- Defaults to `us-west-2` when `AWS_REGION` is unset.

## Usage

From repo root:

```bash
uv run python scripts/athena_upload/geopackage_athena_iceberg_upload.py
```

No command-line arguments are required.

## Destination-specific prompts

### Athena on S3 (Glue catalog)

Prompts for:

- Glue database (select existing or enter new)
- Table name (auto-sanitized)
- S3 table location (default: `s3://riverscapes-athena/<db>/<table>/`)

On completion, logs a ready-to-run Athena query:

```sql
SELECT * FROM "<database>"."<table>" LIMIT 10;
```

If metadata was loaded from `layer_definitions.json`, Glue column comments and table description are updated to match.

### S3 Tables (REST catalog)

Prompts for:

- S3 Table Bucket (select from listed buckets when possible; fallback to ARN text input)
- Namespace (select existing or enter new)
- Table name (auto-sanitized)

The script connects via the PyIceberg REST catalog endpoint:

- `https://s3tables.<AWS_REGION>.amazonaws.com/iceberg`
- SigV4 enabled (`rest.signing-name=s3tables`)

## Existing table behavior

If the target table already exists (Athena or S3 Tables), you must choose one:

- Cancel upload (default)
- Append to existing table
- Delete and recreate table

## Run log

For Athena uploads, the script writes/updates a JSON history log next to the GeoPackage:

- `<gpkg_stem>_upload_log.json`

Each run record includes timestamp, input path/layer, destination, table reference, row count, table action, target CRS, and optional `layer_definitions.json` path.

## Querying Athena tables

Example queries:

```sql
-- Preview rows
SELECT * FROM "my_database"."network_intersected" LIMIT 10;

-- Decode WKB geometry
SELECT linkno, slope,
       ST_AsText(ST_GeomFromBinary(geom_wkb)) AS wkt
FROM "my_database"."network_intersected"
WHERE slope > 0.01
LIMIT 20;

-- Bounding-box spatial filter
SELECT COUNT(*) AS cnt
FROM "my_database"."network_intersected"
WHERE ST_Within(
    ST_GeomFromBinary(geom_wkb),
    ST_GeomFromText('POLYGON((-120 44, -119 44, -119 45, -120 45, -120 44))')
);
```

Tip: tables are queryable after the first Iceberg snapshot commit. `MSCK REPAIR TABLE` is not needed.

## Troubleshooting

### Credentials missing or expired

Re-authenticate (`aws sso login`, refresh profile/session, or role credentials), then re-run.

### Could not list Glue databases / S3 buckets / namespaces

The script falls back to manual text prompts. Verify permissions and resource visibility for your AWS principal.

### Invalid table name errors

Table names are sanitized to lowercase letters, digits, and underscores. Leading underscores are stripped.

### PyIceberg schema mismatch on append

An existing table may have a different schema. Choose "Delete and recreate table" or upload to a new table name.

### Geometry reprojection errors

Use canonical CRS strings like `EPSG:4326`. Validate with `pyproj.CRS("<your-crs>")`.

### Large GeoPackages consume too much memory

The layer is read fully before chunked writes. Split very large inputs or lower `CHUNK_SIZE` in the script.

### Missing Python modules

Run `uv sync` from repo root and re-run with `uv run`.
