# GeoPackage → Athena / S3 Tables Interactive Upload

An interactive command-line script that reads a layer from a GeoPackage file and uploads it as an Apache Iceberg table to either **AWS Athena on S3 (via Glue)** or **AWS S3 Tables**.

---

## 1. Overview

The script guides you through every step via interactive prompts:

1. **Credential check** — verifies AWS credentials before anything else; bails with actionable instructions if they are missing or expired.
2. Select a GeoPackage file and layer.
3. Preview the source schema (field names, pandas dtypes, sample values).
4. Optionally reproject geometry to a target CRS (default: WGS 84 / EPSG:4326).
5. Choose an upload destination:
   - **Athena on S3** — Parquet files in a regular S3 bucket registered in the AWS Glue Data Catalog; immediately queryable via Athena SQL.
   - **S3 Tables** — Dedicated S3 Tables bucket accessed via the PyIceberg REST catalog (SigV4-authenticated).
6. Preview the **destination schema** (Iceberg column names, types, field IDs) and confirm before writing.
7. Data is written in chunks as atomic Iceberg snapshots with a live progress bar.
8. On failure, any newly-created table or namespace is automatically rolled back.

Geometry is stored as WKB bytes in a `geom_wkb` binary column. The CRS is persisted as the `geo.crs_wkt` table property so downstream tools can recover the spatial reference.

---

## Prerequisites

### Python environment

```bash
# From the repo root — all dependencies are managed by uv
uv sync
source .venv/bin/activate
```

### AWS credentials

Valid AWS credentials must be present **before** running the script. The script performs an automatic check at startup (see [§5 — Step 0](#step-0-credential-check)) and will print clear remediation instructions if credentials are missing or expired.

Supported credential sources (standard AWS chain):
- AWS SSO: `aws sso login [--profile <profile>]`
- Named profile: `export AWS_PROFILE=<profile>`
- Static keys: `export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...`
- IAM instance / task role (EC2, ECS, Lambda)

---

## Usage

```bash
cd scripts/athena_upload
python geopackage_athena_iceberg_upload.py
```

No command-line arguments — all configuration is collected interactively.

---

## 5. Querying Athena tables

Once uploaded, query the table from the Athena console or AWS CLI:

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

> **Tip:** Tables are visible immediately after the first Iceberg snapshot is committed — no `MSCK REPAIR TABLE` needed.

---

## 8. Troubleshooting

### Credentials missing or expired

The startup check will catch this and print remediation steps. Re-authenticate with `aws sso login` (or equivalent) then re-run.

### `NotFoundException` when creating a namespace

The bucket ARN may be wrong or you may lack `s3tables:CreateNamespace`. Verify with:
```bash
aws s3tables list-table-buckets
```

### `invalid_table_name` from S3 Tables

Table names must be lowercase, alphanumeric, and underscores only. The script sanitizes names automatically, but if you bypass the prompt this error can still surface. Run the script and let it suggest the safe default.

### PyIceberg `SchemaError` on append

The target table already exists with a different schema. Either drop it first or choose a new table name.

### `NamespaceAlreadyExistsError`

Harmless — the script detects and skips creation silently.

### Geometry reprojection fails

Use canonical EPSG codes such as `EPSG:4326` rather than proj4 strings. Verify the source CRS is valid with `pyproj.CRS("<your-crs>")`.

### Large GeoPackages run out of memory

The script reads the entire layer into memory before chunking. For very large layers, consider splitting the GeoPackage first. The chunk size (100 000 rows per Iceberg snapshot) can be adjusted by changing `CHUNK_SIZE` at the top of the script.

### `ModuleNotFoundError`

Run `uv sync` from the repo root to ensure all dependencies are installed.
