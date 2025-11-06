# Data Exchange Scripts (pydex)

**EXPERIMENTAL**: This repository contains the Python module `pydex` for connecting to the Riverscapes Data Exchange API. It also includes a collection of scripts that use `pydex` classes to perform useful tasks.

## Project Overview

This project is designed to simplify interaction with the Riverscapes GraphQL API. It uses modern Python packaging standards, including a `pyproject.toml` file for configuration and dependency management.

## Using UV for Environment Management

This project uses [uv](https://github.com/astral-sh/uv) to manage Python virtual environments and dependencies. `uv` is an alternative to tools like `pipenv` and `poetry`.

### Prerequisites

1. Install `uv` by following the [installation instructions](https://github.com/astral-sh/uv#installation) for your operating system.
2. Ensure you have Python 3.9 or higher installed.

### Spatialite

We have started using [Spatialite](https://www.gaia-gis.it) for some operations. This is a binary that sites on top of SQLite and provides several powerful geospatial operations as `ST_` functions, similar to PostGIS on top of Postgres.

Spatialite is distributed as an extension to SQLite, but unfortunately the core SQLite3 Python package is not compiled to allow extensions to be loaded (presumably for security reasons). Therefore we use a package called [APSW (Another Python SQLite Wrapper)](https://pypi.org/project/apsw/) that does. APSW can be installed with UV and then you have to load the extension with the following code, where `spatialite_path` is the path to the Spatialite binary. MacOS users can install Spatialite using homebrew and then search for the file `mod_spatialite.8.dylib`. Windows users can download Spatialite binaries from the Gaia GIS site. Our Python that uses Spatialite should all allow you to specify this path in the `launch.json` file.

```python
conn = apsw.Connection(rme_gpkg)
conn.enable_load_extension(True)
conn.load_extension(spatialite_path)
curs = conn.cursor()
```



### Setting Up the Project

To set up the project, follow these steps:

```bash
# Clone the repository
git clone https://github.com/Riverscapes/data-exchange-scripts.git
cd data-exchange-scripts

# Sync the environment using uv
uv sync
```

This will create a `.venv` folder in the root of the repository with the correct Python environment and dependencies installed.

### Using the Virtual Environment in VSCode

1. Open the repository in VSCode.
2. If the `.venv` environment is not automatically detected, reload the window or restart VSCode.
3. Select the Python interpreter located at `.venv/bin/python` (on macOS/Linux) or `.venv\Scripts\python.exe` (on Windows).

## Running Scripts

The best way to run a script is going to be using the "Run and Debug" feature in VSCode. This will ensure that the correct virtual environment is activated and that the script runs in the correct context.

Click that button and select the dropdown item that best fits. If you're just trying to run a file without a launch item you can use `🚀 Python: Run/Debug Current File (with .env)`. This will run the script and set you up with a server environment context (production or staging).

Running scripts this way will also allow you to drop breakpoints in your code and debug it.

## Optional Dependencies

This project includes optional dependencies for geospatial functionality. To install these dependencies, run:

```bash
uv sync --extra geo
```

This will install packages like `gdal` and `shapely`. Note that `gdal` may require additional system-level dependencies. On macOS, you can install `gdal` using Homebrew:

```bash
brew install gdal
```

## Codespace Instructions

1. Open the codespace "Riverscapes API Codespace."
2. In VSCode, load the `RiverscapesAPI.code-workspace` workspace.
3. Ensure the appropriate Python version is selected (e.g., `3.12.9 ('.venv')`).

### Codespace GDAL Limitation

> NOTE: The codespace environment does not currently support scripts requiring GDAL (e.g. project merging). Run those locally.

## Best Practices

- **Dependency Management**: Use `uv sync` to ensure your environment is always up-to-date with the dependencies specified in `pyproject.toml`.

## Port Conflicts

This project uses port `4721` to authenticate locally and `4723` when used inside a codespace. This may conflict with other codespaces (such as `riverscapes-tools`).

## Using this Repository from other places

If you want to use this repository as a dependency in another project you can do so by adding it to your `pyproject.toml` file. For example:

```toml
[tool.uv.sources]
pydex = { git = "https://github.com/Riverscapes/data-exchange-scripts.git", branch = "main" }
```

For legacy projects that use `pip` you can install it directly from the repository:

```bash
pip install git+https://github.com/Riverscapes/data-exchange-scripts.git
```


## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a clear description of your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Metadata Catalog Pipeline

The repository includes an automated pipeline for publishing tool/layer column definitions to Amazon Athena as an external table.

### Source Metadata Files

- Each tool now publishes a single unified `layer_definitions.json` containing both descriptive metadata and column definitions (no separate per-layer files or `def_path` indirection).
- These live beside the tool's code (e.g. under `scripts/<tool_name>/`).

### Flattening Script

`scripts/metadata/flatten_layer_catalog.py` scans the repo for every `layer_definitions.json` and produces partitioned Parquet output:

```text
dist/metadata/
  authority_name=<authority>/authority_version=<version>/layer_metadata.parquet
  index.json
```

Default behavior:

- Output format: Parquet (use `--format csv` for CSV).
- Partition columns (`authority_name`, `authority_version`) are NOT inside the Parquet files unless `--include-partition-cols` is passed.
- A `commit_sha` (current HEAD) is written into every row and stored again in `index.json` with a run timestamp.
- Schema validation is enforced; any validation error causes a loud failure (non-zero exit code). An `index.json` with `status: validation_failed` and the collected `validation_errors` is still written for diagnostics, but no partition files are produced.

Run locally:

```bash
python scripts/metadata/flatten_layer_catalog.py
```

Optional flags:

```bash
python scripts/metadata/flatten_layer_catalog.py --format csv             # CSV instead of Parquet
python scripts/metadata/flatten_layer_catalog.py --include-partition-cols # Embed partition columns in each file
```

### Athena External Table

We publish to: `s3://riverscapes-athena/metadata/layer_column_defs/`

Recommended external table DDL (Parquet, partition columns excluded from file content):

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS layer_column_defs (
  layer_id          string COMMENT 'Stable identifier of the layer or table, for example used for project.rs.xml id',
  layer_name        string COMMENT 'Human-readable layer or table name (may match layer_id)',
  layer_type        string COMMENT 'Layer category (table, view, raster, vector)',
  layer_description string COMMENT 'Human-readable summary of the layer',
  name              string COMMENT 'Column (or raster band) identifier',
  friendly_name     string COMMENT 'Display-friendly name for the column',
  theme             string COMMENT 'Grouping theme -- useful for very wide tables (e.g., Beaver, Hydrology)',
  data_unit         string COMMENT 'Pint-compatible unit string (e.g., m, km^2, %)',
  dtype             string COMMENT 'Data type (INTEGER, REAL, TEXT, etc.)',
  description       string COMMENT 'Detailed description of the column',
  is_key            boolean COMMENT 'Participates in a primary/unique key',
  is_required       boolean COMMENT 'True if field cannot be empty. Corresponds to SQL NOT NULL',
  default_value     string COMMENT 'Default value for new records',
  commit_sha        string COMMENT 'git commit at time of harvest from authority json'
)
COMMENT 'Unified Riverscapes layer column definitions (structural + descriptive metadata)'
PARTITIONED BY (
  authority_name    string COMMENT 'Issuing package/tool authority name',
  authority_version string COMMENT 'Schema version (semver)'
)
STORED AS PARQUET
LOCATION 's3://riverscapes-athena/metadata/layer_column_defs/';
```

Add new partitions (after upload):

```sql
MSCK REPAIR TABLE layer_column_defs;  -- auto-discover
-- OR manual:
ALTER TABLE layer_column_defs
ADD IF NOT EXISTS PARTITION (authority_name='rme_to_athena', authority_version='1.0')
LOCATION 's3://riverscapes-athena/metadata/layer_column_defs/authority_name=rme_to_athena/authority_version=1.0/';
```

### Example Queries

List column definitions for a tool version:

```sql
SELECT name, friendly_name, dtype, description
FROM layer_column_defs
WHERE authority_name='rme_to_athena' AND authority_version='1.0'
ORDER BY name;
```

Count columns by dtype across all authorities:

```sql
SELECT dtype, COUNT(*) AS n
FROM layer_column_defs
GROUP BY dtype
ORDER BY n DESC;
```

Compare two versions of a tool:

```sql
SELECT a.name,
       a.dtype AS dtype_v1,
       b.dtype AS dtype_v2
FROM layer_column_defs a
JOIN layer_column_defs b
  ON a.authority_name = b.authority_name
 AND a.name = b.name
WHERE a.authority_name='rme_to_athena'
  AND a.authority_version='1.0'
  AND b.authority_version='1.1';
```

### GitHub Actions Workflow

Workflow file: `.github/workflows/metadata-catalog.yml`

Steps performed:

1. Checkout code.
2. Assume AWS IAM role via OIDC (secret `METADATA_PUBLISH_ROLE_ARN`).
3. Install dependencies (Python 3.12 + `uv sync`).
4. Run flatten script -> partitioned Parquet.
5. Sync `dist/metadata` to S3 bucket prefix.
6. Create external table if missing.
7. Run `MSCK REPAIR TABLE` to load partitions.
8. Perform sample queries (partition listing / row count).


### IAM Role (Least Privilege Summary)

The role must allow:

- S3 List/Get/Put/Delete under `metadata/layer_column_defs/` and query result prefix.
- Athena: StartQueryExecution, GetQueryExecution, GetQueryResults.
- Glue: Get/Create/Update table & partitions for the database/table.

### Future Enhancements

- Validate layer schemas (dtype whitelist, required fields).
- Explicit partition adds instead of MSCK for faster updates.
- Historical snapshots (extra partition like `snapshot_date`).
- Option to emit both Parquet + CSV for human diffing.

## Troubleshooting Metadata

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| Empty Athena table | Partitions not loaded | Run `MSCK REPAIR TABLE` or add partitions manually |
| Wrong data types | Created table before column rename | Drop & recreate external table with new DDL |
| Missing new version | Workflow didn’t run or lacked perms | Check Actions logs & IAM role policies |
| Zero rows for authority | Upload sync failed | Inspect S3 prefix & re-run workflow |

<!-- End of Metadata Section -->