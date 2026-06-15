# Athena / S3 Tables → GeoPackage Interactive Download

An interactive command-line script that downloads an Apache Iceberg table from either **AWS Athena on S3 (via Glue)** or **AWS S3 Tables** and saves it as a layer in a GeoPackage file ready to open in QGIS or any other GIS tool.

---

## Overview

The script guides you through every step via interactive prompts:

1. **Credential check** — verifies AWS credentials before anything else; bails with actionable instructions if they are missing or expired.
2. Choose a download source: **Athena/Glue** or **S3 Tables**.
3. Select a database/namespace and then a table from live-fetched lists.
4. Preview the **source schema** — Iceberg column names, Iceberg types, and the pandas/GeoPackage types they will become.
5. Set the output GeoPackage path and layer name (defaults to the table name).
6. Optionally supply a GeoJSON file to spatially filter the download to a bounding area.
7. Confirm, then download with live progress bars at every stage.

Geometry is reconstructed from the `geom_wkb` WKB binary column and the CRS is read from the `geo.crs_wkt` table property (stored at upload time). The result is a fully valid GeoPackage layer with correct CRS metadata.

---

## Prerequisites

### Python environment

```bash
# From the repo root — all dependencies are managed by uv
uv sync
source .venv/bin/activate
```

### AWS credentials

Valid AWS credentials must be present **before** running the script. The script performs an automatic `sts:GetCallerIdentity` check at startup and will print clear remediation instructions if credentials are missing or expired.

Supported credential sources (standard AWS chain):
- AWS SSO: `aws sso login [--profile <profile>]`
- Named profile: `export AWS_PROFILE=<profile>`
- Static keys: `export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...`
- IAM instance / task role (EC2, ECS, Lambda)

---

## Usage

```bash
cd scripts/athena_upload
python geopackage_athena_iceberg_download.py
```

No command-line arguments — all configuration is collected interactively.

---

## Schema type mapping

| Iceberg type | pandas / GeoPackage type |
|---|---|
| `IntegerType` | `int32` |
| `LongType` | `int64` |
| `FloatType` | `float32` |
| `DoubleType` | `float64` |
| `BooleanType` | `bool` |
| `StringType` | `object (str)` |
| `DateType` | `date` |
| `TimestampType` | `datetime64[ns]` |
| `TimestamptzType` | `datetime64[ns, UTC]` |
| `BinaryType` (geom_wkb) | `geometry (WKB → Shapely)` |

---

## Troubleshooting

### Credentials missing or expired

The startup check will catch this and print remediation steps. Re-authenticate with `aws sso login` (or equivalent) then re-run.

### `'geom_wkb' not found` error

The table was not uploaded with this script (or has a non-standard geometry column name). Only tables created by `geopackage_athena_iceberg_upload.py` are guaranteed to have a `geom_wkb` column.

### No features after bounds filter

The bounds GeoJSON may be in a different geographic area than the table data, or the reprojection may have failed silently. Check that the GeoJSON CRS is correct and that the bounds actually overlap the table's extent.

### GeoPackage won't open in QGIS

Ensure the download completed without error and that the output path has a `.gpkg` extension. QGIS requires valid CRS metadata — if `geo.crs_wkt` was not set at upload time the script falls back to `EPSG:4326`, which may be incorrect for the data.

### Large tables run out of memory

`scan().to_arrow()` loads the entire table into memory before chunked processing begins. For very large tables, consider filtering by bounds to reduce the download size. The processing chunk size (50 000 rows) can be adjusted by changing `DOWNLOAD_CHUNK_SIZE` at the top of the script.

### `ModuleNotFoundError`

Run `uv sync` from the repo root to ensure all dependencies are installed.
