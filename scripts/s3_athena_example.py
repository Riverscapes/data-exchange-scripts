"""
Write a GeoPackage layer to an Apache Iceberg table in a conventional S3 bucket
via the AWS Glue Data Catalog.

This script is a companion to ``s3_table_example.py``, which targets the S3 Tables
REST catalog (a specialised per-table-bucket service).  Here, PyIceberg connects
directly to the standard Glue Data Catalog and writes Parquet data files to a
regular S3 bucket.  Because PyIceberg writes data files directly to S3 — bypassing
Athena's SQL API entirely — there is no 262 144-character query-string limit and no
ceiling on geometry size.  The resulting table is immediately queryable via Athena
because Athena reads from the same Glue catalog.

Workflow
--------
1.  Connect to the Glue Data Catalog via PyIceberg's GlueCatalog.
2.  Ensure the Glue database (Iceberg namespace) exists.
3.  Load the table if it already exists in Glue, or create it at the configured
    S3 location.  PyIceberg validates any incoming data against the stored schema,
    so accidental schema mismatches are caught before a single byte is written.
4.  Read the GeoPackage, reproject geometry to WGS 84, encode as WKB.
5.  Append rows in batches.  Each ``table.append()`` call writes one or more
    Parquet data files directly to S3, then commits an atomic Iceberg snapshot.
    Athena sees the new rows immediately after the snapshot is committed.

Prerequisites
-------------
    pip install "pyiceberg[pyarrow,glue]>=0.7.0" geopandas pyarrow pyproj boto3

AWS credentials / IAM permissions required
-------------------------------------------
    glue:CreateDatabase / GetDatabase
    glue:CreateTable / GetTable / UpdateTable
    s3:GetObject / PutObject / DeleteObject  (table location bucket)
"""

import os

import geopandas as gpd
import pyarrow as pa
import pyiceberg.exceptions
import pyproj
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import BinaryType, DoubleType, IntegerType, LongType, NestedField
from rsxml.logging.logger import Logger
from rsxml.logging.progress_bar import ProgressBar

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "sagemaker_sample_db")
TABLE_NAME = os.environ.get("TABLE_NAME", "s3_athena_upload")
TABLE_S3_LOCATION = os.environ.get(
    "TABLE_S3_LOCATION",
    "s3://riverscapes-athena/deleteme/s3_athena_upload/",
)

# GeoPackage contains two layers:
#   network_intersected  — 6 720 line features, 18 attribute columns (EPSG:32611)
#   subwatersheds        — 13 451 polygon features, 1 attribute column  (EPSG:32611)
GPKG_PATH = os.environ.get(
    "GPKG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "TMP", "hydro_derivatives.gpkg"),
)
LAYER_NAME = os.environ.get("LAYER_NAME", "network_intersected")
TARGET_CRS = "EPSG:4326"

# Rows per PyIceberg append call.  Each call is one atomic Iceberg snapshot.
# Unlike INSERT VALUES there is no query-string size limit — tune purely for
# memory comfort.  100 000 rows is a safe starting point for wide geometry tables.
CHUNK_SIZE = 100_000

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# Derived from: ogrinfo TMP/hydro_derivatives.gpkg network_intersected -al -so
# Column names are lowercased for Athena compatibility.
# All fields are nullable (required=False) to tolerate missing source values.
# Geometry is stored as WKB bytes (BinaryType); the CRS travels as a table
# property rather than being embedded per-row.

ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="fid", field_type=LongType(), required=False),
    NestedField(field_id=2, name="linkno", field_type=IntegerType(), required=False),
    NestedField(field_id=3, name="dslinkno", field_type=IntegerType(), required=False),
    NestedField(field_id=4, name="uslinkno1", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="uslinkno2", field_type=IntegerType(), required=False),
    NestedField(field_id=6, name="dsnodeid", field_type=LongType(), required=False),
    NestedField(field_id=7, name="strmorder", field_type=IntegerType(), required=False),
    NestedField(field_id=8, name="length", field_type=DoubleType(), required=False),
    NestedField(field_id=9, name="magnitude", field_type=IntegerType(), required=False),
    NestedField(field_id=10, name="dscontarea", field_type=DoubleType(), required=False),
    NestedField(field_id=11, name="strmdrop", field_type=DoubleType(), required=False),
    NestedField(field_id=12, name="slope", field_type=DoubleType(), required=False),
    NestedField(field_id=13, name="straightl", field_type=DoubleType(), required=False),
    NestedField(field_id=14, name="uscontarea", field_type=DoubleType(), required=False),
    NestedField(field_id=15, name="wsno", field_type=IntegerType(), required=False),
    NestedField(field_id=16, name="doutend", field_type=DoubleType(), required=False),
    NestedField(field_id=17, name="doutstart", field_type=DoubleType(), required=False),
    NestedField(field_id=18, name="doutmid", field_type=DoubleType(), required=False),
    NestedField(field_id=19, name="level_path", field_type=DoubleType(), required=False),
    NestedField(field_id=20, name="geom_wkb", field_type=BinaryType(), required=False),
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_and_prepare(gpkg_path: str, layer_name: str, schema: Schema) -> pa.Table:
    """Read a GeoPackage layer, reproject to WGS 84, encode geometry as WKB,
    and return a PyArrow table typed to match *schema*.

    Parameters
    ----------
    gpkg_path:
        File-system path to the GeoPackage.
    layer_name:
        Name of the layer to read from the GeoPackage.
    schema:
        PyIceberg schema defining expected columns and types.

    Returns
    -------
    pa.Table
        Arrow table whose schema matches the PyArrow translation of *schema*.
    """
    log = Logger("load_and_prepare")

    log.info(f"Reading '{layer_name}' from {gpkg_path} …")
    gdf = gpd.read_file(gpkg_path, layer=layer_name)
    log.info(f"  {len(gdf):,} features | CRS: {gdf.crs}")

    # Normalise column names to lowercase for Athena / Iceberg compatibility.
    gdf.columns = [c.lower() for c in gdf.columns]

    # GeoPandas may expose the GeoPackage FID as a named index ('fid') or as
    # an unnamed RangeIndex.  Either way, land it as a regular column.
    schema_names = {f.name for f in schema.fields}
    named_idx = [n for n in gdf.index.names if n in schema_names]
    if named_idx:
        gdf = gdf.reset_index(level=named_idx)
    elif "fid" in schema_names and "fid" not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={"index": "fid"})

    log.info(f"Reprojecting to {TARGET_CRS} …")
    gdf = gdf.to_crs(TARGET_CRS)

    # Encode geometry as WKB and drop the Shapely column (Arrow can't hold it).
    gdf["geom_wkb"] = gdf.geometry.to_wkb()
    gdf = gdf.drop(columns=[gdf.geometry.name])

    # Fill any schema columns absent from the source with nulls.
    for field in schema.fields:
        if field.name not in gdf.columns:
            gdf[field.name] = None

    arrow_table = pa.Table.from_pandas(
        gdf,
        schema=schema_to_pyarrow(schema),
        preserve_index=False,
    )
    log.info(f"  Arrow table: {arrow_table.num_rows:,} rows × {arrow_table.num_columns} cols ({arrow_table.nbytes / 1024 / 1024:.1f} MB)")
    return arrow_table


def ensure_namespace(catalog, database: str) -> None:
    """Create the Glue database (Iceberg namespace) if it does not already exist.

    Parameters
    ----------
    catalog:
        An open PyIceberg GlueCatalog instance.
    database:
        Glue database name to ensure.
    """
    log = Logger("ensure_namespace")
    try:
        catalog.create_namespace(database)
        log.info(f"Created Glue database '{database}'.")
    except pyiceberg.exceptions.NamespaceAlreadyExistsError:
        log.info(f"Glue database '{database}' already exists.")


def get_or_create_table(catalog, database: str, table_name: str, schema: Schema, s3_location: str, crs_wkt: str):
    """Load the Iceberg table from Glue if it exists, otherwise create it.

    When the table already exists PyIceberg reads its schema from the stored
    Iceberg metadata.  Any subsequent ``table.append()`` call will validate the
    incoming Arrow table against that schema before writing a single byte.

    Parameters
    ----------
    catalog:
        An open PyIceberg GlueCatalog instance.
    database:
        Glue database that owns (or will own) the table.
    table_name:
        Table name.
    schema:
        PyIceberg schema used when creating a new table.  Ignored if the table
        already exists — the stored schema takes precedence.
    s3_location:
        S3 URI (``s3://bucket/prefix/``) for Iceberg data and metadata files.
        Only used when creating a new table.
    crs_wkt:
        WKT string of the geometry CRS, stored as a table property.

    Returns
    -------
    pyiceberg.table.Table
    """
    log = Logger("get_or_create_table")
    try:
        table = catalog.load_table((database, table_name))
        log.info(f"Loaded existing table '{database}.{table_name}'.")
    except pyiceberg.exceptions.NoSuchTableError:
        table = catalog.create_table(
            identifier=(database, table_name),
            schema=schema,
            location=s3_location,
            properties={
                "geo.crs_wkt": crs_wkt,
                "write.format.default": "parquet",
            },
        )
        log.info(f"Created table '{database}.{table_name}' at {s3_location}.")
    return table


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    log = Logger("main")
    log.title("Glue / Iceberg write")

    log.info(f"Target table  : {GLUE_DATABASE}.{TABLE_NAME}")
    log.info(f"Table location: {TABLE_S3_LOCATION}")
    log.info(f"GeoPackage    : {GPKG_PATH}  layer={LAYER_NAME}  (alt layer: subwatersheds)")
    log.info(f"AWS region    : {AWS_REGION}")

    # Step 1 — load and prepare data from the GeoPackage.
    arrow_table = load_and_prepare(GPKG_PATH, LAYER_NAME, ICEBERG_SCHEMA)
    crs_wkt = pyproj.CRS(TARGET_CRS).to_wkt()

    # Step 2 — connect to the Glue Data Catalog via PyIceberg.
    # PyIceberg picks up AWS credentials automatically from the environment,
    # ~/.aws/credentials, or an attached IAM role — no explicit key handling needed.
    # Tables written here are immediately queryable via Athena because Athena reads
    # from this same Glue catalog.
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "region_name": AWS_REGION,
        },
    )

    # Step 3 — ensure the database exists and get (or create) the table.
    ensure_namespace(catalog, GLUE_DATABASE)
    table = get_or_create_table(catalog, GLUE_DATABASE, TABLE_NAME, ICEBERG_SCHEMA, TABLE_S3_LOCATION, crs_wkt)

    # Step 4 — append data in chunks.
    # Each table.append() call:
    #   a) validates the Arrow batch schema against the stored Iceberg schema
    #   b) writes one or more Parquet data files directly to S3
    #   c) commits an atomic snapshot — Athena sees the rows immediately after
    # There is no Athena query-string size limit here; geometry can be arbitrarily large.
    total = arrow_table.num_rows
    batches = list(arrow_table.to_batches(max_chunksize=CHUNK_SIZE))
    log.info(f"Writing {total:,} rows in {len(batches)} batch(es) of up to {CHUNK_SIZE:,} rows …")

    rows_written = 0
    progbar = ProgressBar(total, text=f"Writing → {GLUE_DATABASE}.{TABLE_NAME}")
    for batch in batches:
        table.append(pa.Table.from_batches([batch], schema=arrow_table.schema))
        rows_written += batch.num_rows
        progbar.update(rows_written)
    progbar.finish()

    log.info(f"Done — {rows_written:,} rows written across {len(batches)} snapshot(s).")
    log.info(f'Query via Athena: SELECT * FROM "{GLUE_DATABASE}"."{TABLE_NAME}" LIMIT 10;')


if __name__ == "__main__":
    main()
