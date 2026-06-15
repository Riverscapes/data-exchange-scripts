"""Interactive GeoPackage → Apache Iceberg upload script.

Reads a layer from a GeoPackage file and writes it to either:

  * **Athena on S3 (Parquet/Iceberg via Glue)** — stores Parquet data files in a
    regular S3 bucket and registers metadata in the AWS Glue Data Catalog so that
    Athena can query the table immediately.

  * **S3 Tables (Iceberg REST catalog)** — writes to a dedicated S3 Tables bucket
    using PyIceberg's REST catalog with SigV4 authentication.

In both cases geometry is stored as WKB in a ``geom_wkb`` binary column, and the
CRS is persisted as the ``geo.crs_wkt`` table property.  The user is guided through
every choice interactively via ``questionary`` prompts.
"""

from __future__ import annotations

import os
import sys

import boto3
import botocore.exceptions
import geopandas as gpd
import pyarrow as pa
import pyiceberg.exceptions
import pyogrio
import pyproj
import questionary
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.table import Table as IcebergTable
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)
from rich.console import Console
from rich.table import Table as RichTable
from rsxml.logging.logger import Logger
from rsxml.logging.progress_bar import ProgressBar

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SSO_HELP = """
AWS credentials are not configured or have expired.  To authenticate:

  • SSO login:        aws sso login [--profile <profile>]
  • Named profile:    export AWS_PROFILE=<profile>
  • Static keys:      export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...

If you use SSO, run  aws sso login  and then re-run this script.
"""

AWS_REGION: str = os.environ.get("AWS_REGION", "us-west-2")
CHUNK_SIZE: int = 100_000

_DEST_ATHENA = "Athena on S3 (Parquet/Iceberg via Glue)"
_DEST_S3TABLES = "S3 Tables (Iceberg REST catalog)"

# ---------------------------------------------------------------------------
# AWS credentials check
# ---------------------------------------------------------------------------


def check_aws_credentials() -> None:
    """Verify that valid AWS credentials are available before doing anything else.

    Calls ``sts:GetCallerIdentity`` — the lightest-weight call that requires no
    specific resource permissions and fails immediately when credentials are
    absent or expired.

    On success, logs the confirmed identity (Account, ARN) so the user can
    verify they are using the intended AWS account.

    Raises ``SystemExit(1)`` with a helpful remediation message on any
    authentication failure.
    """
    log = Logger("check_aws_credentials")
    log.info("Checking AWS credentials …")

    try:
        sts = boto3.client("sts", region_name=AWS_REGION)
        identity = sts.get_caller_identity()
    except botocore.exceptions.NoCredentialsError:
        log.error("No AWS credentials found." + _SSO_HELP)
        sys.exit(1)
    except botocore.exceptions.TokenRetrievalError as exc:
        log.error(f"SSO token retrieval failed ({exc})." + _SSO_HELP)
        sys.exit(1)
    except botocore.exceptions.SSOTokenLoadError as exc:
        log.error(f"SSO token could not be loaded ({exc})." + _SSO_HELP)
        sys.exit(1)
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("ExpiredTokenException", "ExpiredToken", "InvalidClientTokenId", "AuthFailure"):
            log.error(f"AWS credentials have expired or are invalid ({code})." + _SSO_HELP)
            sys.exit(1)
        # Unexpected error — re-raise so it surfaces properly
        raise
    except botocore.exceptions.ProfileNotFound as exc:
        log.error(f"AWS profile not found: {exc}." + _SSO_HELP)
        sys.exit(1)

    account = identity.get("Account", "unknown")
    arn = identity.get("Arn", "unknown")
    log.info(f"  ✔  Account : {account}")
    log.info(f"  ✔  Identity: {arn}")
    log.info(f"  ✔  Region  : {AWS_REGION}")


# ---------------------------------------------------------------------------
# Table name sanitization
# ---------------------------------------------------------------------------


def sanitize_table_name(name: str) -> str:
    """Return a sanitized version of *name* that satisfies S3 Tables and Glue naming rules.

    Rules applied (both S3 Tables and Glue Catalog share these constraints):

    * Lowercase only
    * Replace spaces, hyphens, and dots with underscores
    * Strip any remaining characters that are not alphanumeric or underscores
    * Must start with a letter or digit (leading underscores are stripped)
    * Truncated to 255 characters

    Parameters
    ----------
    name:
        Raw table name string (e.g. from a layer name or user input).

    Returns
    -------
    str
        Sanitized name, guaranteed to be valid for S3 Tables and Glue.
    """
    import re

    sanitized = name.lower()
    sanitized = re.sub(r"[\s\-\.]+", "_", sanitized)   # spaces / hyphens / dots → _
    sanitized = re.sub(r"[^a-z0-9_]", "", sanitized)    # strip everything else
    sanitized = sanitized.lstrip("_")                    # must start with letter/digit
    return sanitized[:255] or "table"


def ask_table_name(default: str, label: str = "Table name:") -> str:
    """Prompt the user for a table name, auto-sanitizing and warning on changes.

    If the sanitized name differs from the user-entered value the change is
    logged so there are no surprises.  The function loops until the user
    provides a non-empty valid name.

    Parameters
    ----------
    default:
        Suggested default (will itself be sanitized before display).
    label:
        Questionary prompt label.

    Returns
    -------
    str
        Sanitized, non-empty table name.
    """
    log = Logger("ask_table_name")
    safe_default = sanitize_table_name(default)
    while True:
        raw = questionary.text(label, default=safe_default).ask()
        if raw is None:
            sys.exit(1)
        sanitized = sanitize_table_name(raw)
        if not sanitized:
            log.error("Table name cannot be empty after sanitization.  Please try again.")
            continue
        if sanitized != raw:
            log.warning(f"Table name sanitized: '{raw}' → '{sanitized}'")
        return sanitized


# ---------------------------------------------------------------------------
# Step 1 - GeoPackage path & layer selection
# ---------------------------------------------------------------------------


def ask_gpkg_path() -> str:
    """Prompt the user for the path to a GeoPackage file.

    Returns
    -------
    str
        Absolute (or as-entered) path to the GeoPackage file.
    """
    path = questionary.path(
        "Path to GeoPackage file:",
        only_directories=False,
    ).ask()
    if not path:
        Logger("ask_gpkg_path").error("No path provided.")
        sys.exit(1)
    if not os.path.isfile(path):
        Logger("ask_gpkg_path").error(f"File not found: {path}")
        sys.exit(1)
    return path


def ask_layer(gpkg_path: str) -> str:
    """List all layers in *gpkg_path* and let the user pick one.

    Uses ``pyogrio.list_layers`` (NOT fiona) so that fiona is not required.

    Parameters
    ----------
    gpkg_path:
        Path to the GeoPackage.

    Returns
    -------
    str
        Name of the selected layer.
    """
    log = Logger("ask_layer")
    layers_info = pyogrio.list_layers(gpkg_path)  # ndarray of [name, geometry_type]
    layer_names = [str(row[0]) for row in layers_info]
    if not layer_names:
        log.error("No layers found in GeoPackage.")
        sys.exit(1)
    log.info(f"Found {len(layer_names)} layer(s) in {os.path.basename(gpkg_path)}")
    chosen = questionary.select("Select layer to upload:", choices=layer_names).ask()
    if not chosen:
        log.error("No layer selected.")
        sys.exit(1)
    return chosen


# ---------------------------------------------------------------------------
# Step 2 - Schema display
# ---------------------------------------------------------------------------


def display_schema_table(gdf: gpd.GeoDataFrame) -> None:
    """Print a rich table summarising the GeoDataFrame schema.

    Shows Field Name, dtype, and a sample (first non-null) value for every
    column, plus a synthetic ``geom_wkb (geometry)`` row at the bottom.

    Parameters
    ----------
    gdf:
        The GeoDataFrame whose schema is to be displayed.
    """
    console = Console()
    rich_tbl = RichTable(title="Layer Schema", show_header=True, header_style="bold cyan")
    rich_tbl.add_column("Field Name", style="bold")
    rich_tbl.add_column("Type")
    rich_tbl.add_column("Sample Value")

    geom_col = gdf.geometry.name

    for col in gdf.columns:
        if col == geom_col:
            continue
        dtype_str = str(gdf[col].dtype)
        # First non-null value as sample
        non_null = gdf[col].dropna()
        sample = str(non_null.iloc[0]) if len(non_null) > 0 else "(null)"
        # Truncate long samples
        if len(sample) > 60:
            sample = sample[:57] + "…"
        rich_tbl.add_row(col, dtype_str, sample)

    # Geometry row
    geom_type = gdf.geometry.geom_type.value_counts().idxmax() if len(gdf) > 0 else "Unknown"
    rich_tbl.add_row("geom_wkb", "geometry (→ WKB binary)", f"[{geom_type}]")

    console.print(rich_tbl)


def display_iceberg_schema(schema: Schema, destination: str) -> None:
    """Print a rich table showing the destination Iceberg column names and types.

    Parameters
    ----------
    schema:
        PyIceberg ``Schema`` that will be used to create / append to the table.
    destination:
        Human-readable destination label shown as the table title
        (e.g. ``"mydb.mytable"`` or ``"namespace.mytable @ S3 Tables"``).
    """
    console = Console()
    rich_tbl = RichTable(
        title=f"Destination Schema → {destination}",
        show_header=True,
        header_style="bold magenta",
    )
    rich_tbl.add_column("#", style="dim", justify="right")
    rich_tbl.add_column("Column Name", style="bold")
    rich_tbl.add_column("Iceberg Type")
    rich_tbl.add_column("Required", justify="center")

    for field in schema.fields:
        rich_tbl.add_row(
            str(field.field_id),
            field.name,
            str(field.field_type),
            "✔" if field.required else "",
        )

    console.print(rich_tbl)


# ---------------------------------------------------------------------------
# Step 3 - CRS detection & optional reprojection
# ---------------------------------------------------------------------------


def ask_reproject(gdf: gpd.GeoDataFrame) -> str:
    """Detect the current CRS and optionally prompt for a target CRS.

    Parameters
    ----------
    gdf:
        GeoDataFrame whose CRS will be inspected.

    Returns
    -------
    str
        The target CRS string (e.g. ``"EPSG:4326"``).  If the user declines
        reprojection, the current CRS authority string is returned unchanged.
    """
    log = Logger("ask_reproject")
    current_crs = gdf.crs
    log.info(f"Detected CRS: {current_crs}")

    reproject = questionary.confirm(f"Reproject from {current_crs} to another CRS?", default=True).ask()
    if reproject is None:
        sys.exit(1)

    if reproject:
        target_crs = questionary.text("Target CRS (e.g. EPSG:4326):", default="EPSG:4326").ask()
        if not target_crs:
            log.error("No target CRS provided.")
            sys.exit(1)
        return target_crs

    # Return a canonical string form of the current CRS
    return current_crs.to_string() if current_crs else "EPSG:4326"


# ---------------------------------------------------------------------------
# Schema derivation
# ---------------------------------------------------------------------------

# Mapping from pandas dtype name fragments → PyIceberg field type factory.
# datetime64 / timestamp handling is done explicitly in _dtype_to_iceberg below
# because timezone-aware dtypes (e.g. "datetime64[ns, UTC]") need special casing.
_DTYPE_MAP: list[tuple[tuple[str, ...], type]] = [
    (("int64",), LongType),
    (("int8", "int16", "int32", "int", "integer"), IntegerType),
    (("float16", "float32", "float64", "float", "double", "real", "numeric"), DoubleType),
    (("bool",), BooleanType),
    (("date",), DateType),
    (("object", "str", "string", "unicode"), StringType),
]


def _dtype_to_iceberg(dtype_str: str):
    """Map a pandas dtype string to a PyIceberg type instance.

    Timezone-aware datetime dtypes (pandas uses strings like
    ``"datetime64[ns, UTC]"`` — note the comma) map to ``TimestamptzType``;
    timezone-naive datetime/timestamp dtypes map to ``TimestampType``.
    All other dtypes are matched against :data:`_DTYPE_MAP`, falling back to
    ``StringType`` for unrecognised ones.

    Parameters
    ----------
    dtype_str:
        The string representation of a pandas dtype (e.g. ``"int64"``, ``"object"``).

    Returns
    -------
    PyIceberg type instance.
    """
    dtype_lower = dtype_str.lower()
    # datetime64[ns, UTC] → TimestamptzType; datetime64[ns] → TimestampType
    if dtype_lower.startswith("datetime64") or "datetime" in dtype_lower or "timestamp" in dtype_lower:
        if "," in dtype_str:
            return TimestamptzType()
        return TimestampType()
    for fragments, factory in _DTYPE_MAP:
        if any(dtype_lower.startswith(f) or f in dtype_lower for f in fragments):
            return factory()
    return StringType()


def derive_iceberg_schema(gdf: gpd.GeoDataFrame) -> Schema:
    """Auto-derive a PyIceberg ``Schema`` from a GeoDataFrame's column dtypes.

    The geometry column is replaced by ``geom_wkb`` (``BinaryType``).
    All other columns are mapped via :func:`_dtype_to_iceberg`.
    All fields are ``required=False`` to tolerate missing source values.
    Field IDs are assigned sequentially starting from 1.

    Parameters
    ----------
    gdf:
        Source GeoDataFrame.

    Returns
    -------
    Schema
        PyIceberg schema ready for table creation or validation.
    """
    geom_col = gdf.geometry.name
    fields: list[NestedField] = []
    field_id = 1

    for col in gdf.columns:
        if col == geom_col:
            continue
        iceberg_type = _dtype_to_iceberg(str(gdf[col].dtype))
        fields.append(NestedField(field_id=field_id, name=col, field_type=iceberg_type, required=False))
        field_id += 1

    # Geometry column always last
    fields.append(NestedField(field_id=field_id, name="geom_wkb", field_type=BinaryType(), required=False))
    return Schema(*fields)


# ---------------------------------------------------------------------------
# Arrow table preparation
# ---------------------------------------------------------------------------


def prepare_arrow_table(gdf: gpd.GeoDataFrame, schema: Schema, target_crs: str) -> pa.Table:
    """Reproject geometry, encode as WKB, and convert to a typed PyArrow table.

    Parameters
    ----------
    gdf:
        Source GeoDataFrame (will not be mutated; a copy is used internally).
    schema:
        PyIceberg schema defining the expected columns and types.
    target_crs:
        CRS string to reproject geometry to before WKB encoding.

    Returns
    -------
    pa.Table
        Arrow table whose schema matches the PyArrow translation of *schema*.
    """
    # Columns are assumed to already be lowercase on entry (normalised in main()).
    log = Logger("prepare_arrow_table")
    gdf = gdf.copy()

    schema_names = {f.name.lower() for f in schema.fields}

    # Expose the GeoPackage FID as a regular column if present in schema
    named_idx = [n for n in gdf.index.names if n and n.lower() in schema_names]
    if named_idx:
        gdf = gdf.reset_index(level=named_idx)
    elif "fid" in schema_names and "fid" not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={"index": "fid"})

    log.info(f"Reprojecting to {target_crs} …")
    gdf = gdf.to_crs(target_crs)

    # Encode geometry as WKB and drop the Shapely geometry column
    geom_col_name = gdf.geometry.name
    gdf["geom_wkb"] = gdf.geometry.to_wkb()
    gdf = gdf.drop(columns=[geom_col_name])

    # Fill any schema columns absent from the source with nulls
    for field in schema.fields:
        if field.name not in gdf.columns:
            gdf[field.name] = None

    arrow_table = pa.Table.from_pandas(
        gdf,
        schema=schema_to_pyarrow(schema),
        preserve_index=False,
    )
    log.info(f"Arrow table: {arrow_table.num_rows:,} rows x {arrow_table.num_columns} cols ({arrow_table.nbytes / 1024 / 1024:.1f} MB)")
    return arrow_table


# ---------------------------------------------------------------------------
# Chunked append
# ---------------------------------------------------------------------------


def append_in_chunks(table: IcebergTable, arrow_table: pa.Table, dest_label: str, chunk_size: int = CHUNK_SIZE) -> None:
    """Append *arrow_table* to *table* in chunks, showing a progress bar.

    Each :py:meth:`~pyiceberg.table.Table.append` call writes one or more Parquet
    files and commits an atomic Iceberg snapshot.

    Parameters
    ----------
    table:
        Open PyIceberg table to append to.
    arrow_table:
        Data to write.
    dest_label:
        Human-readable label shown in the progress bar (e.g. ``"db.tbl"``).
    chunk_size:
        Maximum number of rows per Iceberg snapshot.
    """
    log = Logger("append_in_chunks")
    total = arrow_table.num_rows
    batches = list(arrow_table.to_batches(max_chunksize=chunk_size))
    log.info(f"Writing {total:,} rows to '{dest_label}' in {len(batches)} batch(es) of up to {chunk_size:,} rows …")

    rows_written = 0
    progbar = ProgressBar(total, text=f"Writing → {dest_label}")
    for batch in batches:
        table.append(pa.Table.from_batches([batch], schema=arrow_table.schema))
        rows_written += batch.num_rows
        progbar.update(rows_written)
    progbar.finish()
    log.info(f"Done — {rows_written:,} rows written across {len(batches)} snapshot(s).")


# ---------------------------------------------------------------------------
# Athena / Glue helpers
# ---------------------------------------------------------------------------


def ensure_namespace_glue(catalog, database: str) -> bool:
    """Create the Glue database (Iceberg namespace) if it does not already exist.

    Parameters
    ----------
    catalog:
        An open PyIceberg GlueCatalog instance.
    database:
        Glue database name to ensure.

    Returns
    -------
    bool
        ``True`` if the namespace was created; ``False`` if it already existed.
    """
    log = Logger("ensure_namespace_glue")
    try:
        catalog.create_namespace(database)
        log.info(f"Created Glue database '{database}'.")
        return True
    except pyiceberg.exceptions.NamespaceAlreadyExistsError:
        log.info(f"Glue database '{database}' already exists.")
        return False


def get_or_create_table_glue(catalog, database: str, table_name: str, schema: Schema, s3_location: str, crs_wkt: str) -> tuple[IcebergTable, bool]:
    """Load the Iceberg table from Glue if it exists, or create it.

    Parameters
    ----------
    catalog:
        An open PyIceberg GlueCatalog instance.
    database:
        Glue database that owns (or will own) the table.
    table_name:
        Table name.
    schema:
        PyIceberg schema used when creating a new table.
    s3_location:
        S3 URI (e.g. ``s3://bucket/prefix/``) for Iceberg data and metadata.
    crs_wkt:
        WKT string of the geometry CRS, stored as the ``geo.crs_wkt`` property.

    Returns
    -------
    tuple[IcebergTable, bool]
        The Iceberg table and a flag that is ``True`` if the table was newly created.
    """
    log = Logger("get_or_create_table_glue")
    try:
        tbl = catalog.load_table((database, table_name))
        log.info(f"Loaded existing table '{database}.{table_name}'.")
        return tbl, False
    except pyiceberg.exceptions.NoSuchTableError:
        tbl = catalog.create_table(
            identifier=(database, table_name),
            schema=schema,
            location=s3_location,
            properties={
                "geo.crs_wkt": crs_wkt,
                "write.format.default": "parquet",
            },
        )
        log.info(f"Created table '{database}.{table_name}' at {s3_location}.")
        return tbl, True


def run_athena_workflow(gdf: gpd.GeoDataFrame, schema: Schema, layer_name: str, target_crs: str) -> None:
    """Drive the interactive Athena-on-S3 (Glue) upload workflow.

    Prompts the user for:
      * Glue database (selected from existing or typed manually)
      * Table name
      * S3 location prefix

    Then creates/opens the Iceberg table and appends the data.

    Parameters
    ----------
    gdf:
        Source GeoDataFrame (pre-loaded, not yet reprojected).
    schema:
        PyIceberg schema derived from *gdf*.
    layer_name:
        Original GeoPackage layer name (used as default table name).
    target_crs:
        CRS string to reproject to before upload.
    """
    log = Logger("run_athena_workflow")

    # --- Glue database selection ---
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    try:
        paginator = glue_client.get_paginator("get_databases")
        db_names = [db["Name"] for page in paginator.paginate() for db in page["DatabaseList"]]
    except Exception as exc:
        log.warning(f"Could not list Glue databases: {exc}")
        db_names = []

    if db_names:
        choices = [*db_names, "[Enter new database name]"]
        db_choice = questionary.select("Select Glue database:", choices=choices).ask()
        if db_choice is None:
            sys.exit(1)
        if db_choice == "[Enter new database name]":
            glue_db = questionary.text("New Glue database name:").ask()
        else:
            glue_db = db_choice
    else:
        glue_db = questionary.text("Enter Glue database name:").ask()

    if not glue_db:
        log.error("Glue database name is required.")
        sys.exit(1)

    # --- Table name & S3 location ---
    table_name = ask_table_name(layer_name)

    default_s3 = f"s3://riverscapes-athena/{glue_db}/{table_name}/"
    s3_location = questionary.text("S3 location for table data:", default=default_s3).ask()
    if not s3_location:
        log.error("S3 location is required.")
        sys.exit(1)

    # --- Confirm ---
    log.info(f"Target: {glue_db}.{table_name}  →  {s3_location}")
    display_iceberg_schema(schema, f"{glue_db}.{table_name}")
    if not questionary.confirm(f"Upload '{layer_name}' to Athena table '{glue_db}.{table_name}'?").ask():
        log.info("Upload cancelled.")
        return

    # --- Prepare data ---
    crs_wkt = pyproj.CRS(target_crs).to_wkt()
    arrow_table = prepare_arrow_table(gdf, schema, target_crs)

    # --- Connect to Glue catalog ---
    catalog = load_catalog("glue", **{"type": "glue", "region_name": AWS_REGION})

    namespace_created = False
    table_created = False
    try:
        namespace_created = ensure_namespace_glue(catalog, glue_db)
        iceberg_table, table_created = get_or_create_table_glue(catalog, glue_db, table_name, schema, s3_location, crs_wkt)

        # Write
        append_in_chunks(iceberg_table, arrow_table, f"{glue_db}.{table_name}")
        log.info(f'Query via Athena: SELECT * FROM "{glue_db}"."{table_name}" LIMIT 10;')

    except Exception:
        log.error("Upload failed. Rolling back …")
        if table_created:
            try:
                catalog.drop_table((glue_db, table_name))
                log.info(f"Dropped table '{glue_db}.{table_name}'.")
            except Exception as drop_exc:
                log.warning(f"Could not drop table during rollback: {drop_exc}")
        if namespace_created:
            try:
                catalog.drop_namespace(glue_db)
                log.info(f"Dropped namespace '{glue_db}'.")
            except Exception as drop_exc:
                log.warning(f"Could not drop namespace during rollback: {drop_exc}")
        raise


# ---------------------------------------------------------------------------
# S3 Tables helpers
# ---------------------------------------------------------------------------


def ensure_namespace_s3tables(s3tables_client, bucket_arn: str, namespace: str) -> bool:
    """Create the namespace in the S3 Tables bucket if it does not already exist.

    Parameters
    ----------
    s3tables_client:
        A ``boto3`` S3 Tables client.
    bucket_arn:
        ARN of the S3 Tables bucket.
    namespace:
        Namespace name to ensure.

    Returns
    -------
    bool
        ``True`` if the namespace was created; ``False`` if it already existed.
    """
    log = Logger("ensure_namespace_s3tables")
    try:
        s3tables_client.get_namespace(tableBucketARN=bucket_arn, namespace=namespace)
        log.info(f"Namespace '{namespace}' already exists.")
        return False
    except s3tables_client.exceptions.NotFoundException:
        s3tables_client.create_namespace(tableBucketARN=bucket_arn, namespace=[namespace])
        log.info(f"Created namespace '{namespace}'.")
        return True


def get_or_create_table_s3tables(catalog, namespace: str, table_name: str, schema: Schema, crs_wkt: str) -> tuple[IcebergTable, bool]:
    """Load the Iceberg table from the S3 Tables catalog if it exists, or create it.

    Parameters
    ----------
    catalog:
        An open PyIceberg REST catalog pointing at the S3 Tables endpoint.
    namespace:
        Namespace that owns (or will own) the table.
    table_name:
        Table name.
    schema:
        PyIceberg schema used when creating a new table.
    crs_wkt:
        WKT string of the geometry CRS, stored as the ``geo.crs_wkt`` property.

    Returns
    -------
    tuple[IcebergTable, bool]
        The Iceberg table and a flag that is ``True`` if the table was newly created.
    """
    log = Logger("get_or_create_table_s3tables")
    try:
        tbl = catalog.load_table((namespace, table_name))
        log.info(f"Loaded existing table '{namespace}.{table_name}'.")
        return tbl, False
    except pyiceberg.exceptions.NoSuchTableError:
        tbl = catalog.create_table(
            identifier=(namespace, table_name),
            schema=schema,
            properties={
                "geo.crs_wkt": crs_wkt,
                "write.format.default": "parquet",
            },
        )
        log.info(f"Created table '{namespace}.{table_name}'.")
        return tbl, True


def run_s3tables_workflow(gdf: gpd.GeoDataFrame, schema: Schema, layer_name: str, target_crs: str) -> None:
    """Drive the interactive S3 Tables (REST catalog) upload workflow.

    Prompts the user for:
      * S3 Tables bucket ARN
      * Namespace (selected from existing or typed)
      * Table name

    Then creates/opens the Iceberg table and appends the data.

    Parameters
    ----------
    gdf:
        Source GeoDataFrame (pre-loaded, not yet reprojected).
    schema:
        PyIceberg schema derived from *gdf*.
    layer_name:
        Original GeoPackage layer name (used as default table name).
    target_crs:
        CRS string to reproject to before upload.
    """
    log = Logger("run_s3tables_workflow")

    # --- Bucket ARN ---
    default_arn = os.environ.get("S3_TABLE_BUCKET_ARN", "")
    s3tables_client = boto3.client("s3tables", region_name=AWS_REGION)

    # Try to list available table buckets and offer a chooser
    try:
        buckets: list[dict] = []
        continuation_token: str | None = None
        while True:
            kwargs: dict = {}
            if continuation_token:
                kwargs["continuationToken"] = continuation_token
            resp = s3tables_client.list_table_buckets(**kwargs)
            buckets.extend(resp.get("tableBuckets", []))
            continuation_token = resp.get("continuationToken")
            if not continuation_token:
                break
    except Exception as exc:
        log.warning(f"Could not list S3 Table buckets: {exc}")
        buckets = []

    if buckets:
        # Build display labels:  name  (ARN)
        arn_by_label = {f"{b['name']}  ({b['arn']})": b["arn"] for b in buckets}
        labels = list(arn_by_label.keys())
        # Pre-select the env-var default if it matches one of the listed ARNs
        default_label = next((lbl for lbl, arn in arn_by_label.items() if arn == default_arn), labels[0])
        chosen_label = questionary.select(
            "Select S3 Table Bucket:",
            choices=labels,
            default=default_label,
        ).ask()
        if chosen_label is None:
            sys.exit(1)
        bucket_arn = arn_by_label[chosen_label]
    else:
        # Fall back to free-text entry if listing failed or returned nothing
        bucket_arn = questionary.text("S3 Table Bucket ARN:", default=default_arn).ask()
        if not bucket_arn:
            log.error("S3 Table Bucket ARN is required.")
            sys.exit(1)

    # --- Namespace ---
    try:
        existing: list[str] = []
        continuation_token: str | None = None
        while True:
            kwargs: dict[str, str] = {"tableBucketARN": bucket_arn}
            if continuation_token:
                kwargs["continuationToken"] = continuation_token
            resp = s3tables_client.list_namespaces(**kwargs)
            existing.extend(ns["namespace"][0] for ns in resp.get("namespaces", []))
            continuation_token = resp.get("continuationToken")
            if not continuation_token:
                break
    except Exception as exc:
        log.warning(f"Could not list namespaces: {exc}")
        existing = []

    if existing:
        choices = [*existing, "[Enter new namespace]"]
        ns_choice = questionary.select("Select namespace:", choices=choices).ask()
        if ns_choice is None:
            sys.exit(1)
        if ns_choice == "[Enter new namespace]":
            namespace = questionary.text("New namespace name:").ask()
        else:
            namespace = ns_choice
    else:
        namespace = questionary.text("Namespace name:").ask()

    if not namespace:
        log.error("Namespace is required.")
        sys.exit(1)

    # --- Table name ---
    table_name = ask_table_name(layer_name)

    # --- Confirm ---
    log.info(f"Target: {namespace}.{table_name}  (bucket: {bucket_arn})")
    display_iceberg_schema(schema, f"{namespace}.{table_name} @ S3 Tables")
    if not questionary.confirm(f"Upload '{layer_name}' to S3 Tables '{namespace}.{table_name}'?").ask():
        log.info("Upload cancelled.")
        return

    # --- Prepare data ---
    crs_wkt = pyproj.CRS(target_crs).to_wkt()
    arrow_table = prepare_arrow_table(gdf, schema, target_crs)

    # --- Connect to S3 Tables REST catalog ---
    catalog = load_catalog(
        "s3tables",
        **{
            "type": "rest",
            "uri": f"https://s3tables.{AWS_REGION}.amazonaws.com/iceberg",
            "warehouse": bucket_arn,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": AWS_REGION,
            "rest.signing-name": "s3tables",
        },
    )

    namespace_created = False
    table_created = False
    try:
        namespace_created = ensure_namespace_s3tables(s3tables_client, bucket_arn, namespace)
        iceberg_table, table_created = get_or_create_table_s3tables(catalog, namespace, table_name, schema, crs_wkt)

        # Write
        append_in_chunks(iceberg_table, arrow_table, f"{namespace}.{table_name}")

    except Exception:
        log.error("Upload failed. Rolling back …")
        if table_created:
            try:
                catalog.drop_table((namespace, table_name))
                log.info(f"Dropped table '{namespace}.{table_name}'.")
            except Exception as drop_exc:
                log.warning(f"Could not drop table during rollback: {drop_exc}")
        if namespace_created:
            try:
                s3tables_client.delete_namespace(tableBucketARN=bucket_arn, namespace=namespace)
                log.info(f"Deleted namespace '{namespace}'.")
            except Exception as drop_exc:
                log.warning(f"Could not delete namespace during rollback: {drop_exc}")
        raise


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main interactive entry point.

    Orchestrates the full workflow:
      1. Ask for GeoPackage path and layer.
      2. Read the layer and display its schema.
      3. Detect CRS and ask about reprojection.
      4. Derive the Iceberg schema.
      5. Ask for upload destination (Athena/Glue or S3 Tables).
      6. Delegate to the appropriate workflow function.
    """
    log = Logger("main")
    log.title("GeoPackage → Iceberg Upload")

    try:
        # ── Step 0: AWS credentials check ────────────────────────────────────
        check_aws_credentials()

        # ── Step 1: GeoPackage path & layer ─────────────────────────────────
        gpkg_path = ask_gpkg_path()
        layer_name = ask_layer(gpkg_path)

        # ── Step 2: Read layer & display schema ──────────────────────────────
        log.info(f"Reading layer '{layer_name}' from {os.path.basename(gpkg_path)} …")
        gdf = gpd.read_file(gpkg_path, layer=layer_name)
        log.info(f"  {len(gdf):,} features loaded.")

        # Normalise column names to lowercase early so that schema derivation
        # and Arrow table construction use consistent, Athena-compatible names.
        gdf.columns = [c.lower() for c in gdf.columns]

        display_schema_table(gdf)

        if not questionary.confirm("Is this schema correct? Continue with upload?").ask():
            log.info("Aborted by user.")
            sys.exit(0)

        # ── Step 3: CRS detection & reprojection ─────────────────────────────
        target_crs = ask_reproject(gdf)

        # ── Step 4: Derive Iceberg schema ────────────────────────────────────
        schema = derive_iceberg_schema(gdf)
        log.info(f"Derived Iceberg schema with {len(schema.fields)} field(s).")

        # ── Step 5: Destination selection ────────────────────────────────────
        destination = questionary.select(
            "Upload destination:",
            choices=[_DEST_ATHENA, _DEST_S3TABLES],
        ).ask()
        if not destination:
            log.error("No destination selected.")
            sys.exit(1)

        # ── Step 6: Run workflow ──────────────────────────────────────────────
        if destination == _DEST_ATHENA:
            run_athena_workflow(gdf, schema, layer_name, target_crs)
        else:
            run_s3tables_workflow(gdf, schema, layer_name, target_crs)

    except KeyboardInterrupt:
        Logger("main").info("\nInterrupted by user.")
        sys.exit(0)
    except Exception as exc:
        log.error(f"Unhandled error: {exc}")
        raise


if __name__ == "__main__":
    main()
