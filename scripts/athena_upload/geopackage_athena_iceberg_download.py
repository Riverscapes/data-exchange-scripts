"""Interactive Apache Iceberg → GeoPackage download script.

Reads a layer from either:

  * **Athena on S3 (Parquet/Iceberg via Glue)** — data stored in a regular S3
    bucket registered in the AWS Glue Data Catalog.

  * **S3 Tables (Iceberg REST catalog)** — a dedicated S3 Tables bucket queried
    via PyIceberg's REST catalog with SigV4 authentication.

In both cases geometry is reconstructed from the ``geom_wkb`` binary column and
the CRS is read from the ``geo.crs_wkt`` table property.  The user is guided
through every choice interactively via ``questionary`` prompts.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import botocore.exceptions
import geopandas as gpd
import pandas as pd
import pyproj
import questionary
from pyiceberg.catalog import load_catalog
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)
from rich.console import Console
from rich.table import Table as RichTable
from rsxml.logging.logger import Logger
from rsxml.logging.progress_bar import ProgressBar
from shapely.ops import transform

if TYPE_CHECKING:
    from pyiceberg.table import Table as IcebergTable
    from shapely.geometry.base import BaseGeometry

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
DOWNLOAD_CHUNK_SIZE: int = 50_000

_SRC_ATHENA = "Athena on S3 (Parquet/Iceberg via Glue)"
_SRC_S3TABLES = "S3 Tables (Iceberg REST catalog)"

# ---------------------------------------------------------------------------
# Iceberg type → pandas/GeoPackage type mapping (used in schema display)
# ---------------------------------------------------------------------------

_ICEBERG_TYPE_MAP: list[tuple[type, str]] = [
    (LongType, "int64"),
    (IntegerType, "int32"),
    (DoubleType, "float64"),
    (FloatType, "float32"),
    (BooleanType, "bool"),
    (StringType, "object (str)"),
    (DateType, "date"),
    (TimestampType, "datetime64[ns]"),
    (TimestamptzType, "datetime64[ns, UTC]"),
    (BinaryType, "bytes"),
]


def _normalize_user_path(raw_path: str) -> Path:
    """Return a normalized Path from user input.

    Handles Windows "Copy as path" values that include surrounding quotes.
    """
    cleaned = raw_path.strip()
    if len(cleaned) >= 2 and cleaned[0] == cleaned[-1] and cleaned[0] in {"\"", "'"}:
        cleaned = cleaned[1:-1]
    return Path(cleaned).expanduser()


def _iceberg_type_to_pandas(iceberg_type, col_name: str) -> str:
    """Return a human-readable pandas/GeoPackage type label for *iceberg_type*.

    The ``geom_wkb`` column is always labelled as geometry regardless of its
    underlying Iceberg type.

    Parameters
    ----------
    iceberg_type:
        A PyIceberg type instance.
    col_name:
        Column name — ``geom_wkb`` receives special treatment.

    Returns
    -------
    str
        Human-readable type label.
    """
    if col_name == "geom_wkb":
        return "geometry (WKB → Shapely)"
    for iceberg_cls, pandas_label in _ICEBERG_TYPE_MAP:
        if isinstance(iceberg_type, iceberg_cls):
            return pandas_label
    return "object"


# ---------------------------------------------------------------------------
# AWS credentials check (copied verbatim from upload script)
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
# Schema display
# ---------------------------------------------------------------------------


def display_download_schema(iceberg_table: IcebergTable) -> None:
    """Print a rich table summarising the Iceberg schema that will be downloaded.

    Shows Iceberg column name, Iceberg type, the pandas/GeoPackage type it will
    become, and marks the ``geom_wkb`` row specially.  Also displays the
    ``geo.crs_wkt`` property when present.

    Parameters
    ----------
    iceberg_table:
        Open PyIceberg table whose schema and properties are to be displayed.
    """
    log = Logger("display_download_schema")
    console = Console()

    crs_wkt = iceberg_table.properties.get("geo.crs_wkt")
    if crs_wkt:
        log.info(f"Table CRS (geo.crs_wkt): {crs_wkt[:120]}{'…' if len(crs_wkt) > 120 else ''}")

    rich_tbl = RichTable(
        title=f"Download Schema ← {iceberg_table.name()}",
        show_header=True,
        header_style="bold cyan",
    )
    rich_tbl.add_column("#", style="dim", justify="right")
    rich_tbl.add_column("Column Name", style="bold")
    rich_tbl.add_column("Iceberg Type")
    rich_tbl.add_column("→ pandas / GeoPackage Type")

    for field in iceberg_table.schema().fields:
        pandas_type = _iceberg_type_to_pandas(field.field_type, field.name)
        style = "bold green" if field.name == "geom_wkb" else ""
        rich_tbl.add_row(
            str(field.field_id),
            field.name,
            str(field.field_type),
            pandas_type,
            style=style,
        )

    console.print(rich_tbl)


# ---------------------------------------------------------------------------
# Bounds helper
# ---------------------------------------------------------------------------


def ask_bounds() -> tuple[BaseGeometry | None, str | None]:
    """Optionally prompt for a GeoJSON file to use as a geographic bounds filter.

    If the user opts in, loads the file with geopandas and returns the union
    geometry together with its WKT CRS string.

    Returns
    -------
    tuple[BaseGeometry | None, str | None]
        ``(bounds_geom, crs_wkt)`` where both are ``None`` when the user declines
        the bounds filter.
    """
    log = Logger("ask_bounds")

    want_bounds = questionary.confirm("Do you want to filter by geographic bounds?", default=False).ask()
    if want_bounds is None:
        sys.exit(1)
    if not want_bounds:
        return None, None

    geojson_input = questionary.path("Path to GeoJSON bounds file:").ask()
    if not geojson_input:
        log.error("No GeoJSON path provided.")
        sys.exit(1)
    geojson_path = _normalize_user_path(geojson_input)
    if not geojson_path.is_file():
        log.error(f"File not found: {geojson_path}")
        sys.exit(1)

    bounds_gdf = gpd.read_file(geojson_path)
    if bounds_gdf.empty:
        log.warning("GeoJSON file contains no features — skipping bounds filter.")
        return None, None

    bounds_geom = bounds_gdf.geometry.union_all()
    crs = bounds_gdf.crs
    crs_wkt = crs.to_wkt() if crs else "EPSG:4326"
    log.info(f"Bounds loaded: {bounds_geom.geom_type}, CRS={crs_wkt[:80]}")
    return bounds_geom, crs_wkt


def _pick_layer_definition(layer_defs: dict[str, Any], selected_layer: str) -> dict[str, Any] | None:
    """Select the layer definition record that should map to the selected layer name."""
    layers = layer_defs.get("layers", [])
    if not isinstance(layers, list) or not layers:
        return None

    selected_norm = selected_layer.strip().lower()

    exact_id = [layer for layer in layers if isinstance(layer, dict) and str(layer.get("layer_id", "")).strip().lower() == selected_norm]
    if len(exact_id) == 1:
        return exact_id[0]

    exact_name = [layer for layer in layers if isinstance(layer, dict) and str(layer.get("layer_name", "")).strip().lower() == selected_norm]
    if len(exact_name) == 1:
        return exact_name[0]

    if len(layers) == 1 and isinstance(layers[0], dict):
        return layers[0]

    choices: list[str] = []
    by_label: dict[str, dict[str, Any]] = {}
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        layer_id = str(layer.get("layer_id", "")).strip() or "(no layer_id)"
        layer_name = str(layer.get("layer_name", "")).strip() or "(no layer_name)"
        label = f"{layer_name} [{layer_id}]"
        choices.append(label)
        by_label[label] = layer

    if not choices:
        return None

    chosen = questionary.select(
        "Select layer from layer_definitions.json:",
        choices=choices,
    ).ask()
    if not chosen:
        return None
    return by_label.get(chosen)


def ask_layer_metadata(layer_name: str, default_dir: Path) -> tuple[dict[str, str], dict[str, str], dict[str, str], str]:
    """Optionally load column descriptions, aliases, and dtype intent from layer_definitions.json.

    Returns:
        (column_comments, column_aliases, column_dtypes, layer_description)
    """
    log = Logger("ask_layer_metadata")

    use_defs = questionary.confirm(
        "Load column metadata/dtypes from layer_definitions.json?",
        default=True,
    ).ask()
    if not use_defs:
        return {}, {}, {}, ""

    default_path = default_dir / "layer_definitions.json"
    raw_path = questionary.path(
        "Path to layer_definitions.json:",
        default=str(default_path),
        only_directories=False,
    ).ask()
    if not raw_path:
        log.warning("No layer_definitions path provided. Continuing without metadata overrides.")
        return {}, {}, {}, ""

    defs_path = _normalize_user_path(raw_path)
    if not defs_path.is_file():
        log.warning(f"layer_definitions.json not found: {defs_path}. Continuing without metadata overrides.")
        return {}, {}, {}, ""

    try:
        with defs_path.open("r", encoding="utf-8") as f:
            layer_defs = json.load(f)
    except Exception as exc:
        log.warning(f"Could not parse {defs_path}: {exc}. Continuing without metadata overrides.")
        return {}, {}, {}, ""

    selected = _pick_layer_definition(layer_defs, layer_name)
    if not selected:
        log.warning("No matching layer found in layer_definitions.json. Continuing without metadata overrides.")
        return {}, {}, {}, ""

    comments: dict[str, str] = {}
    aliases: dict[str, str] = {}
    dtypes: dict[str, str] = {}
    for col in selected.get("columns", []):
        if not isinstance(col, dict):
            continue
        name = str(col.get("name", "")).strip().lower()
        friendly_name = str(col.get("friendly_name", "")).strip()
        description = str(col.get("description", "")).strip()
        dtype = str(col.get("dtype", "")).strip().upper()
        if name and friendly_name:
            aliases[name] = friendly_name
        if name and description:
            comments[name] = description
        if name and dtype:
            dtypes[name] = dtype

    layer_description = str(selected.get("description", "")).strip()
    layer_id = str(selected.get("layer_id", "")).strip() or "(unknown layer_id)"
    log.info(f"Loaded {len(comments)} column comments, {len(aliases)} aliases, and {len(dtypes)} dtype definitions for layer '{layer_id}'.")
    return comments, aliases, dtypes, layer_description


def _coerce_chunk_to_layer_dtypes(
    gdf_chunk: gpd.GeoDataFrame,
    column_dtypes: dict[str, str],
) -> gpd.GeoDataFrame:
    """Coerce chunk columns to layer_definitions semantic dtypes where possible."""
    if not column_dtypes:
        return gdf_chunk

    log = Logger("coerce_chunk_dtypes")
    out = gdf_chunk.copy()
    geom_col = out.geometry.name
    dtype_lookup = {k.lower(): v for k, v in column_dtypes.items()}

    for col in out.columns:
        if col == geom_col:
            continue
        intended = dtype_lookup.get(col.lower())
        if not intended:
            continue

        try:
            if intended == "INTEGER":
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("Int64")
            elif intended == "FLOAT":
                out[col] = pd.to_numeric(out[col], errors="coerce").astype("float64")
            elif intended == "BOOLEAN":
                out[col] = out[col].astype("boolean")
            elif intended == "DATETIME":
                parsed = pd.to_datetime(out[col], errors="coerce", utc=True)
                out[col] = pd.Series(parsed.dt.tz_localize(None), index=out.index)
            elif intended == "DATE":
                out[col] = pd.to_datetime(out[col], errors="coerce").dt.date
            elif intended == "STRING":
                out[col] = out[col].astype("string")
        except Exception as exc:
            log.warning(f"Could not coerce column '{col}' to '{intended}' ({exc}); keeping source dtype '{out[col].dtype}'.")

    return out


def _write_gpkg_metadata(
    gpkg_path: Path,
    layer_name: str,
    column_comments: dict[str, str],
    column_aliases: dict[str, str],
    layer_description: str,
) -> None:
    """Write layer/column descriptions into GeoPackage metadata tables."""
    if not column_comments and not column_aliases and not layer_description:
        return

    log = Logger("write_gpkg_metadata")
    try:
        with sqlite3.connect(gpkg_path) as conn:
            cur = conn.cursor()

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_data_columns (
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    name TEXT,
                    title TEXT,
                    description TEXT,
                    mime_type TEXT,
                    constraint_name TEXT,
                    CONSTRAINT pk_gdc PRIMARY KEY (table_name, column_name),
                    CONSTRAINT fk_gdc_tn FOREIGN KEY (table_name)
                        REFERENCES gpkg_contents(table_name)
                )
                """
            )

            if layer_description:
                cur.execute(
                    "UPDATE gpkg_contents SET description = ? WHERE table_name = ?",
                    (layer_description, layer_name),
                )

            # Upsert rows when either alias/title or description is present.
            all_cols = sorted(set(column_comments.keys()) | set(column_aliases.keys()))
            for col_name in all_cols:
                description = column_comments.get(col_name, "")
                alias = column_aliases.get(col_name, "")
                if not description and not alias:
                    continue
                cur.execute(
                    """
                    INSERT INTO gpkg_data_columns (
                        table_name, column_name, name, title, description, mime_type, constraint_name
                    )
                    VALUES (?, ?, ?, ?, ?, NULL, NULL)
                    ON CONFLICT(table_name, column_name) DO UPDATE SET
                        name = excluded.name,
                        title = excluded.title,
                        description = excluded.description
                    """,
                    (
                        layer_name,
                        col_name,
                        alias or col_name,
                        alias or col_name,
                        description,
                    ),
                )

            conn.commit()
        log.info(f"Wrote GeoPackage metadata for layer '{layer_name}' ({len(column_comments)} description(s), {len(column_aliases)} alias(es)).")
    except Exception as exc:
        log.warning(f"Could not write GeoPackage metadata: {exc}")


# ---------------------------------------------------------------------------
# Core download function
# ---------------------------------------------------------------------------


def download_and_save(
    iceberg_table: IcebergTable,
    gpkg_path: Path,
    layer_name: str,
    bounds_geom: BaseGeometry | None,
    bounds_crs_wkt: str | None,
    column_comments: dict[str, str] | None = None,
    column_aliases: dict[str, str] | None = None,
    column_dtypes: dict[str, str] | None = None,
    layer_description: str = "",
    chunk_size: int = DOWNLOAD_CHUNK_SIZE,
) -> None:
    """Download an Iceberg table and save it as a GeoPackage layer.

    Steps performed:

    1. **Plan the scan** — calls ``plan_files()`` to count Parquet files and
       estimate the total download size before any data is transferred.
    2. **Download** — ``scan().to_arrow()`` fetches all Parquet files from S3
       into an in-memory Arrow table.
    3. **Process in chunks** — the Arrow table is sliced into batches of
       *chunk_size* rows; each batch is decoded (WKB → Shapely), optionally
       filtered against *bounds_geom*, and appended to the GeoPackage layer.
       A single progress bar tracks all three sub-steps together.

    Parameters
    ----------
    iceberg_table:
        Open PyIceberg table to download.
    gpkg_path:
        Output GeoPackage file path.
    layer_name:
        Layer name to write inside the GeoPackage.
    bounds_geom:
        Optional Shapely geometry used as a spatial filter.  Pass ``None`` to
        download all rows.
    bounds_crs_wkt:
        WKT CRS string matching the CRS of *bounds_geom*.  Required when
        *bounds_geom* is not ``None``.
    chunk_size:
        Number of rows to decode and write per iteration.
    """
    log = Logger("download_and_save")

    # ── 1. Plan scan ─────────────────────────────────────────────────────────
    log.info("Planning scan …")
    tasks = list(iceberg_table.scan().plan_files())
    total_files = len(tasks)
    total_s3_mb = sum(t.file.file_size_in_bytes or 0 for t in tasks) / 1024 / 1024
    if total_files == 0:
        log.warning("Table has no data files — nothing to download.")
        return
    log.info(f"  {total_files} Parquet file(s), ~{total_s3_mb:.1f} MB on S3")

    # ── 2. Download ──────────────────────────────────────────────────────────
    log.info(f"Downloading {total_files} Parquet file(s) …")
    arrow_table = iceberg_table.scan().to_arrow()
    total_rows = arrow_table.num_rows
    log.info(f"Downloaded: {total_rows:,} rows x {arrow_table.num_columns} cols ({arrow_table.nbytes / 1024 / 1024:.1f} MB in memory)")

    if total_rows == 0:
        log.warning("Table is empty — nothing to write.")
        return

    # ── 3. Recover CRS ───────────────────────────────────────────────────────
    crs_wkt = iceberg_table.properties.get("geo.crs_wkt", "EPSG:4326")
    table_crs = pyproj.CRS(crs_wkt)
    log.info(f"CRS: {table_crs.to_string()}")

    if "geom_wkb" not in arrow_table.schema.names:
        log.error("Column 'geom_wkb' not found in downloaded table — cannot reconstruct geometry.")
        sys.exit(1)

    # ── 4. Pre-reproject bounds once, before the chunk loop ──────────────────
    bounds_geom_reprojected = None
    if bounds_geom is not None and bounds_crs_wkt is not None:
        project = pyproj.Transformer.from_crs(
            pyproj.CRS(bounds_crs_wkt),
            table_crs,
            always_xy=True,
        ).transform
        bounds_geom_reprojected = transform(project, bounds_geom)
        log.info("Bounds reprojected to table CRS.")

    # ── 5. Process in chunks: decode WKB → filter → write ────────────────────
    # First chunk uses "w" for a brand-new file, or "a" to append to an
    # existing one.  Every subsequent chunk always appends so we don't
    # overwrite earlier chunks.
    initial_mode = "a" if gpkg_path.exists() else "w"
    rows_written = 0
    comments = {k.lower(): v for k, v in (column_comments or {}).items()}
    aliases = {k.lower(): v for k, v in (column_aliases or {}).items()}
    dtypes = {k.lower(): v for k, v in (column_dtypes or {}).items()}

    progbar = ProgressBar(total_rows, text=f"Processing & writing → {layer_name}")
    for chunk_idx, offset in enumerate(range(0, total_rows, chunk_size)):
        length = min(chunk_size, total_rows - offset)
        slice_ = arrow_table.slice(offset, length)
        df = slice_.to_pandas()

        geometry = gpd.GeoSeries.from_wkb(df["geom_wkb"])
        df = df.drop(columns=["geom_wkb"])
        gdf_chunk = gpd.GeoDataFrame(df, geometry=geometry, crs=table_crs)
        gdf_chunk = _coerce_chunk_to_layer_dtypes(gdf_chunk, dtypes)

        if bounds_geom_reprojected is not None:
            gdf_chunk = gdf_chunk[gdf_chunk.geometry.intersects(bounds_geom_reprojected)].copy()

        if not gdf_chunk.empty:
            write_mode = initial_mode if chunk_idx == 0 else "a"
            gdf_chunk.to_file(
                gpkg_path,
                layer=layer_name,
                driver="GPKG",
                mode=write_mode,
                engine="pyogrio",
            )
            rows_written += len(gdf_chunk)

        progbar.update(min(offset + length, total_rows))

    progbar.finish()

    if rows_written == 0:
        log.warning("No features remain after filtering — GeoPackage layer not written.")
        return

    file_size_mb = gpkg_path.stat().st_size / 1024 / 1024
    log.info(f"Saved {rows_written:,} features to layer '{layer_name}' in {gpkg_path} ({file_size_mb:.1f} MB).")
    _write_gpkg_metadata(gpkg_path, layer_name, comments, aliases, layer_description)


# ---------------------------------------------------------------------------
# Athena / Glue helpers
# ---------------------------------------------------------------------------


def list_tables_glue(catalog, database: str) -> list[str]:
    """List all table names in *database* using the PyIceberg GlueCatalog.

    Parameters
    ----------
    catalog:
        An open PyIceberg GlueCatalog instance.
    database:
        Glue database (namespace) to inspect.

    Returns
    -------
    list[str]
        Sorted list of table names.
    """
    log = Logger("list_tables_glue")
    try:
        identifiers = catalog.list_tables(database)
        # Each identifier is a tuple: (namespace, table_name)
        table_names = sorted(ident[-1] for ident in identifiers)
        log.info(f"Found {len(table_names)} table(s) in database '{database}'.")
        return table_names
    except Exception as exc:
        log.warning(f"Could not list tables in database '{database}': {exc}")
        return []


def run_athena_workflow() -> None:
    """Drive the interactive Athena-on-S3 (Glue) download workflow.

    Prompts the user for:
      * Glue database (selected from existing databases)
      * Table name (selected from tables in the chosen database)
      * Output GeoPackage path and layer name
      * Optional geographic bounds filter

    Then downloads the Iceberg table and saves it to the GeoPackage.
    """
    log = Logger("run_athena_workflow")

    # ── Glue database selection ───────────────────────────────────────────────
    glue_client = boto3.client("glue", region_name=AWS_REGION)
    try:
        paginator = glue_client.get_paginator("get_databases")
        db_names = sorted(db["Name"] for page in paginator.paginate() for db in page["DatabaseList"])
    except Exception as exc:
        log.warning(f"Could not list Glue databases: {exc}")
        db_names = []

    if db_names:
        glue_db = questionary.select("Select Glue database:", choices=db_names).ask()
    else:
        glue_db = questionary.text("Enter Glue database name:").ask()

    if not glue_db:
        log.error("Glue database name is required.")
        sys.exit(1)

    # ── Table selection ───────────────────────────────────────────────────────
    catalog = load_catalog("glue", **{"type": "glue", "region_name": AWS_REGION})
    table_names = list_tables_glue(catalog, glue_db)

    if not table_names:
        log.error(f"No Iceberg tables found in database '{glue_db}'.")
        sys.exit(1)

    table_name = questionary.select("Select table to download:", choices=table_names).ask()
    if not table_name:
        log.error("No table selected.")
        sys.exit(1)

    # ── Load table & display schema ───────────────────────────────────────────
    log.info(f"Loading table '{glue_db}.{table_name}' …")
    iceberg_table = catalog.load_table((glue_db, table_name))
    display_download_schema(iceberg_table)

    # ── Output path & layer name ──────────────────────────────────────────────
    gpkg_input = questionary.path("Output GeoPackage file path:", only_directories=False).ask()
    if not gpkg_input:
        log.error("Output path is required.")
        sys.exit(1)
    gpkg_path = _normalize_user_path(gpkg_input)

    layer_name = questionary.text("Layer name in GeoPackage:", default=table_name).ask()
    if not layer_name:
        log.error("Layer name is required.")
        sys.exit(1)

    # ── Optional layer metadata + dtype intent ───────────────────────────────
    column_comments, column_aliases, column_dtypes, layer_description = ask_layer_metadata(table_name, gpkg_path.parent)

    # ── Optional bounds filter ────────────────────────────────────────────────
    bounds_geom, bounds_crs_wkt = ask_bounds()

    # ── Confirm ───────────────────────────────────────────────────────────────
    log.info(f"Source: {glue_db}.{table_name}  →  {gpkg_path} [{layer_name}]")
    if not questionary.confirm(f"Download '{glue_db}.{table_name}' → '{gpkg_path}' (layer '{layer_name}')?").ask():
        log.info("Download cancelled.")
        return

    # ── Download & save ───────────────────────────────────────────────────────
    download_and_save(
        iceberg_table,
        gpkg_path,
        layer_name,
        bounds_geom,
        bounds_crs_wkt,
        column_comments=column_comments,
        column_aliases=column_aliases,
        column_dtypes=column_dtypes,
        layer_description=layer_description,
    )


# ---------------------------------------------------------------------------
# S3 Tables helpers
# ---------------------------------------------------------------------------


def list_tables_s3tables(s3tables_client, bucket_arn: str, namespace: str) -> list[str]:
    """List all table names in *namespace* using the S3 Tables API.

    Uses a ``continuationToken`` loop because there is no boto3 paginator for
    ``list_tables`` on the S3 Tables service.

    Parameters
    ----------
    s3tables_client:
        A ``boto3`` S3 Tables client.
    bucket_arn:
        ARN of the S3 Tables bucket.
    namespace:
        Namespace to inspect.

    Returns
    -------
    list[str]
        Sorted list of table names.
    """
    log = Logger("list_tables_s3tables")
    table_names: list[str] = []
    continuation_token: str | None = None
    try:
        while True:
            kwargs: dict[str, str] = {"tableBucketARN": bucket_arn, "namespace": namespace}
            if continuation_token:
                kwargs["continuationToken"] = continuation_token
            resp = s3tables_client.list_tables(**kwargs)
            table_names.extend(t["name"] for t in resp.get("tables", []))
            continuation_token = resp.get("continuationToken")
            if not continuation_token:
                break
    except Exception as exc:
        log.warning(f"Could not list tables in namespace '{namespace}': {exc}")
        return []
    log.info(f"Found {len(table_names)} table(s) in namespace '{namespace}'.")
    return sorted(table_names)


def run_s3tables_workflow() -> None:
    """Drive the interactive S3 Tables (REST catalog) download workflow.

    Prompts the user for:
      * S3 Tables bucket (selected from available buckets)
      * Namespace (selected from namespaces in the chosen bucket)
      * Table name (selected from tables in the chosen namespace)
      * Output GeoPackage path and layer name
      * Optional geographic bounds filter

    Then downloads the Iceberg table and saves it to the GeoPackage.
    """
    log = Logger("run_s3tables_workflow")

    s3tables_client = boto3.client("s3tables", region_name=AWS_REGION)
    default_arn = os.environ.get("S3_TABLE_BUCKET_ARN", "")

    # ── Bucket selection ──────────────────────────────────────────────────────
    try:
        buckets: list[dict] = []
        continuation_token: str | None = None
        while True:
            kwargs: dict[str, str] = {}
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
        arn_by_label = {f"{b['name']}  ({b['arn']})": b["arn"] for b in buckets}
        labels = list(arn_by_label.keys())
        default_label = next((lbl for lbl, arn in arn_by_label.items() if arn == default_arn), labels[0])
        chosen_label = questionary.select("Select S3 Table Bucket:", choices=labels, default=default_label).ask()
        if chosen_label is None:
            sys.exit(1)
        bucket_arn = arn_by_label[chosen_label]
    else:
        bucket_arn = questionary.text("S3 Table Bucket ARN:", default=default_arn).ask()
        if not bucket_arn:
            log.error("S3 Table Bucket ARN is required.")
            sys.exit(1)

    # ── Namespace selection ───────────────────────────────────────────────────
    try:
        namespaces: list[str] = []
        continuation_token = None
        while True:
            kwargs = {"tableBucketARN": bucket_arn}
            if continuation_token:
                kwargs["continuationToken"] = continuation_token
            resp = s3tables_client.list_namespaces(**kwargs)
            namespaces.extend(ns["namespace"][0] for ns in resp.get("namespaces", []))
            continuation_token = resp.get("continuationToken")
            if not continuation_token:
                break
    except Exception as exc:
        log.warning(f"Could not list namespaces: {exc}")
        namespaces = []

    if not namespaces:
        log.error("No namespaces found in the selected bucket.")
        sys.exit(1)

    namespace = questionary.select("Select namespace:", choices=sorted(namespaces)).ask()
    if not namespace:
        log.error("No namespace selected.")
        sys.exit(1)

    # ── Table selection ───────────────────────────────────────────────────────
    table_names = list_tables_s3tables(s3tables_client, bucket_arn, namespace)
    if not table_names:
        log.error(f"No tables found in namespace '{namespace}'.")
        sys.exit(1)

    table_name = questionary.select("Select table to download:", choices=table_names).ask()
    if not table_name:
        log.error("No table selected.")
        sys.exit(1)

    # ── Load table via REST catalog & display schema ───────────────────────────
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
    log.info(f"Loading table '{namespace}.{table_name}' …")
    iceberg_table = catalog.load_table((namespace, table_name))
    display_download_schema(iceberg_table)

    # ── Output path & layer name ──────────────────────────────────────────────
    gpkg_input = questionary.path("Output GeoPackage file path:", only_directories=False).ask()
    if not gpkg_input:
        log.error("Output path is required.")
        sys.exit(1)
    gpkg_path = _normalize_user_path(gpkg_input)

    layer_name = questionary.text("Layer name in GeoPackage:", default=table_name).ask()
    if not layer_name:
        log.error("Layer name is required.")
        sys.exit(1)

    # ── Optional layer metadata + dtype intent ───────────────────────────────
    column_comments, column_aliases, column_dtypes, layer_description = ask_layer_metadata(table_name, gpkg_path.parent)

    # ── Optional bounds filter ────────────────────────────────────────────────
    bounds_geom, bounds_crs_wkt = ask_bounds()

    # ── Confirm ───────────────────────────────────────────────────────────────
    log.info(f"Source: {namespace}.{table_name} @ S3 Tables  →  {gpkg_path} [{layer_name}]")
    if not questionary.confirm(f"Download '{namespace}.{table_name}' → '{gpkg_path}' (layer '{layer_name}')?").ask():
        log.info("Download cancelled.")
        return

    # ── Download & save ───────────────────────────────────────────────────────
    download_and_save(
        iceberg_table,
        gpkg_path,
        layer_name,
        bounds_geom,
        bounds_crs_wkt,
        column_comments=column_comments,
        column_aliases=column_aliases,
        column_dtypes=column_dtypes,
        layer_description=layer_description,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Main interactive entry point.

    Orchestrates the full download workflow:
      1. Verify AWS credentials.
      2. Ask for source type (Athena/Glue or S3 Tables).
      3. Delegate to the appropriate workflow function.
    """
    log = Logger("main")
    log.title("Iceberg → GeoPackage Download")

    try:
        # ── Step 0: AWS credentials check ────────────────────────────────────
        check_aws_credentials()

        # ── Step 1: Source selection ──────────────────────────────────────────
        source = questionary.select(
            "Download from:",
            choices=[_SRC_ATHENA, _SRC_S3TABLES],
        ).ask()
        if not source:
            log.error("No source selected.")
            sys.exit(1)

        # ── Step 2-7: Run workflow ────────────────────────────────────────────
        if source == _SRC_ATHENA:
            run_athena_workflow()
        else:
            run_s3tables_workflow()

    except KeyboardInterrupt:
        Logger("main").info("\nInterrupted by user.")
        sys.exit(0)
    except Exception as exc:
        log.error(f"Unhandled error: {exc}")
        raise


if __name__ == "__main__":
    main()
