"""
Write a GeoPackage layer to an AWS S3 Tables (Apache Iceberg) table.

Geometry is stored as WKB in a BINARY column following the GeoParquet
convention (https://geoparquet.org), with the CRS saved as a table property.

Prerequisites:
    pip install "pyiceberg[pyarrow]>=0.7.0" geopandas pyarrow boto3

AWS credentials must have:
    s3tables:CreateNamespace / GetNamespace
    s3tables:CreateTable / GetTable
    s3tables:GetTableBucketMaintenanceConfiguration
    s3:GetObject / PutObject / DeleteObject

NOTE THE SCRIPT IS *NOT* IDEMPOTENT. Running it multiple times (or partial runs) will append the data creating duplicates.
"""

import os

import boto3
import geopandas as gpd
import pyarrow as pa
import pyiceberg.exceptions
import pyproj
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    LongType,
    NestedField,
    StringType,
)
from rsxml.logging.logger import Logger
from rsxml.logging.progress_bar import ProgressBar

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUCKET_ARN = os.environ.get("S3_TABLES_BUCKET_ARN", "")
if not BUCKET_ARN:
    raise OSError("S3_TABLES_BUCKET_ARN environment variable is not set.\nExample: export S3_TABLES_BUCKET_ARN=arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket")

AWS_REGION = "us-west-2"

GPKG_PATH = os.environ.get(
    "GPKG_PATH",
    os.path.join(os.path.dirname(__file__), "..", "TMP", "hydro_derivatives.gpkg"),
)

SHP_PATH = r"F:\nardata\rslocal\nationaldatasets\national_datasets_20250609\ownership\surface_management_agency.shp"

LAYER_NAME = "network_intersected"
NAMESPACE = "demo"
TABLE_NAME = "ownership"
TARGET_CRS = "EPSG:4326"

# Rows per Iceberg append call.  Each batch becomes one atomic snapshot.
# Smaller values help on high-latency / satellite connections where large
# multipart-upload parts time out mid-transfer.
CHUNK_SIZE = 4_000

# ---------------------------------------------------------------------------
# Iceberg schema
# ---------------------------------------------------------------------------
# All fields are nullable (required=False) to tolerate missing source values.
# Geometry is stored as WKB bytes (BinaryType); the CRS travels with the table
# as a property rather than being embedded per-row.

ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1, name="fid", field_type=LongType(), required=True),
    NestedField(field_id=2, name="ADMIN_AGEN", field_type=StringType(), required=False),
    NestedField(field_id=3, name="geom_wkb", field_type=BinaryType(), required=True),
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_gpkg(gpkg_path: str, layer_name: str) -> gpd.GeoDataFrame:
    log = Logger("load_gpkg")
    log.info(f"Reading '{layer_name}' from {gpkg_path} …")
    gdf = gpd.read_file(gpkg_path, layer=layer_name)
    return gdf


def load_shp(shp_path: str) -> gpd.GeoDataFrame:
    log = Logger("load_gpkg")
    log.info(f"Reading from {shp_path} …")
    gdf = gpd.read_file(shp_path)
    return gdf


def load_and_prepare(gpkg_path: str, layer_name: str, schema: Schema) -> pa.Table:
    """Read a GeoPackage layer, reproject to WGS 84, encode geometry as WKB,
    and return a PyArrow table typed to match `schema`."""
    log = Logger("load_and_prepare")

    # gdf = load_gpkg(gpkg_path, layer_name)
    gdf = load_shp(gpkg_path)
    log.info(f"  {len(gdf):,} features | CRS: {gdf.crs}")
    # GeoPandas may expose the GeoPackage FID as a named index ('fid') or as
    # an unnamed RangeIndex depending on the read engine.  Either way, make
    # sure it lands as a regular column before we build the Arrow table.
    schema_names = {f.name for f in schema.fields}
    named_idx = [n for n in gdf.index.names if n in schema_names]
    if named_idx:
        gdf = gdf.reset_index(level=named_idx)
    elif "fid" in schema_names and "fid" not in gdf.columns:
        gdf = gdf.reset_index().rename(columns={"index": "fid"})

    log.info(f"Reprojecting to {TARGET_CRS} …")
    gdf = gdf.to_crs(TARGET_CRS)

    # Encode geometry as WKB and drop the shapely column (Arrow can't hold it).
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


def ensure_namespace(s3tables_client, bucket_arn: str, namespace: str) -> None:
    """Create the namespace inside the S3 Tables bucket if it doesn't exist."""
    log = Logger("ensure_namespace")
    try:
        s3tables_client.get_namespace(tableBucketARN=bucket_arn, namespace=namespace)
        log.info(f"Namespace '{namespace}' already exists.")
    except s3tables_client.exceptions.NotFoundException:
        s3tables_client.create_namespace(tableBucketARN=bucket_arn, namespace=[namespace])
        log.info(f"Created namespace '{namespace}'.")


def get_or_create_table(catalog, namespace: str, table_name: str, schema: Schema, crs_wkt: str):
    """Load the Iceberg table if it exists, otherwise create it."""
    log = Logger("get_or_create_table")
    try:
        table = catalog.load_table((namespace, table_name))
        log.info(f"Loaded existing table '{namespace}.{table_name}'.")
    except pyiceberg.exceptions.NoSuchTableError:
        table = catalog.create_table(
            identifier=(namespace, table_name),
            schema=schema,
            properties={
                "geo.crs_wkt": crs_wkt,
                "write.format.default": "parquet",
            },
        )
        log.info(f"Created table '{namespace}.{table_name}'.")
    return table


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    log = Logger("main")
    log.title("S3 Tables / Iceberg write")

    # Prepare data
    arrow_table = load_and_prepare(SHP_PATH, LAYER_NAME, ICEBERG_SCHEMA)
    crs_wkt = pyproj.CRS(TARGET_CRS).to_wkt()

    # Connect to the S3 Tables REST catalog (SigV4-signed)
    catalog = load_catalog(
        "s3tables",
        **{
            "type": "rest",
            "uri": f"https://s3tables.{AWS_REGION}.amazonaws.com/iceberg",
            "warehouse": BUCKET_ARN,
            "rest.sigv4-enabled": "true",
            "rest.signing-region": AWS_REGION,
            "rest.signing-name": "s3tables",
            # Satellite-friendly: longer timeouts and more retries for S3 writes
            "s3.connect-timeout": "120",
            "s3.socket-timeout": "300",
            "s3.retry.num-retries": "10",
        },
    )

    s3tables = boto3.client("s3tables", region_name=AWS_REGION)
    ensure_namespace(s3tables, BUCKET_ARN, NAMESPACE)
    table = get_or_create_table(catalog, NAMESPACE, TABLE_NAME, ICEBERG_SCHEMA, crs_wkt)

    # Write in chunks — each table.append() call is one atomic Iceberg snapshot.
    total = arrow_table.num_rows
    batches = list(arrow_table.to_batches(max_chunksize=CHUNK_SIZE))
    log.info(f"Writing {total:,} rows to '{NAMESPACE}.{TABLE_NAME}' in {len(batches)} batch(es) …")
    rows_written = 0
    progbar = ProgressBar(total, text="Writing → S3 Tables")
    for batch in batches:
        table.append(pa.Table.from_batches([batch], schema=arrow_table.schema))
        rows_written += batch.num_rows
        progbar.update(rows_written)
    progbar.finish()
    log.info(f"Done — {total:,} rows written.")


if __name__ == "__main__":
    main()
