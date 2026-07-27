"""
Write a shapefile to an Apache Iceberg table in a conventional S3 bucket
via the AWS Glue Data Catalog.

Geometry is stored as WKB in a BINARY column following the GeoParquet
convention (https://geoparquet.org), with the CRS saved as a table property.

Prerequisites:
    pip install "pyiceberg[pyarrow,glue]>=0.7.0" geopandas pyarrow pyproj boto3

AWS credentials must have:
    glue:CreateDatabase / GetDatabase
    glue:CreateTable / GetTable / UpdateTable
    s3:GetObject / PutObject / DeleteObject  (table location bucket)

NOTE: This script is *not* idempotent. Running it multiple times will append
data and create duplicate rows. Use ``table.overwrite()`` instead of
``table.append()`` if you need replace semantics.
"""

import os

import geopandas as gpd
import pyarrow as pa
import pyiceberg.exceptions
import pyproj
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import BinaryType, LongType, NestedField, StringType
from rsxml.logging.logger import Logger
from rsxml.logging.progress_bar import ProgressBar

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AWS_REGION = "us-west-2"

GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "demo")
TABLE_NAME = os.environ.get("TABLE_NAME", "ownership")
TABLE_S3_LOCATION = os.environ.get(
    "TABLE_S3_LOCATION",
    "s3://riverscapes-athena/demo/ownership/",
)

SHP_PATH = os.environ.get(
    "SHP_PATH",
    r"/Volumes/SAMSUNG2TB/Riverscapes/cybercastor/NationalDatasets/ownership/surface_management_agency.shp",
    # r"F:\nardata\rslocal\nationaldatasets\national_datasets_20250609\ownership\surface_management_agency.shp",
)

TARGET_CRS = "EPSG:4326"

# Rows per PyIceberg append call.  Each call is one atomic Iceberg snapshot.
# No query-string size limit applies — tune purely for memory comfort.
CHUNK_SIZE = 4_000

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# All fields are nullable (required=False) to tolerate missing source values.
# Geometry is stored as WKB bytes (BinaryType); the CRS travels with the table
# as a property rather than being embedded per-row.
# Column names are lowercased for Athena compatibility.

ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="fid", field_type=LongType(), required=True),
    NestedField(field_id=2, name="admin_agen", field_type=StringType(), required=False),
    NestedField(field_id=3, name="geom_wkb", field_type=BinaryType(), required=True),
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_and_prepare(shp_path: str, schema: Schema) -> pa.Table:
    """Read a shapefile, reproject to WGS 84, encode geometry as WKB,
    and return a PyArrow table typed to match *schema*.

    Parameters
    ----------
    shp_path:
        File-system path to the shapefile (or any format readable by GeoPandas).
    schema:
        PyIceberg schema defining expected columns and types.

    Returns
    -------
    pa.Table
        Arrow table whose schema matches the PyArrow translation of *schema*.
    """
    log = Logger("load_and_prepare")

    log.info(f"Reading {shp_path} …")
    gdf = gpd.read_file(shp_path)
    log.info(f"  {len(gdf):,} features | CRS: {gdf.crs}")

    # Normalise column names to lowercase for Athena / Iceberg compatibility.
    gdf.columns = [c.lower() for c in gdf.columns]

    # GeoPandas may expose the FID as a named index or an unnamed RangeIndex.
    # Either way, land it as a regular column.
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
    log.info(f"  Arrow table: {arrow_table.num_rows:,} rows x {arrow_table.num_columns} cols ({arrow_table.nbytes / 1024 / 1024:.1f} MB)")
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
    Iceberg metadata.  Any subsequent ``table.append()`` call validates the
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
    log.title("Glue / Iceberg write — ownership")

    log.info(f"Target table  : {GLUE_DATABASE}.{TABLE_NAME}")
    log.info(f"Table location: {TABLE_S3_LOCATION}")
    log.info(f"Source        : {SHP_PATH}")
    log.info(f"AWS region    : {AWS_REGION}")

    # Step 1 — load and prepare data from the shapefile.
    arrow_table = load_and_prepare(SHP_PATH, ICEBERG_SCHEMA)
    crs_wkt = pyproj.CRS(TARGET_CRS).to_wkt()

    # Step 2 — connect to the Glue Data Catalog via PyIceberg.
    # PyIceberg picks up AWS credentials automatically from the environment,
    # ~/.aws/credentials, or an attached IAM role.
    # Tables written here are immediately queryable via Athena because Athena
    # reads from this same Glue catalog.
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
    #   b) writes Parquet data files directly to S3
    #   c) commits an atomic snapshot — Athena sees the rows immediately after
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
