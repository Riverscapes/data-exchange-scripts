"""Add simplified geometry column to existing geo-parquet file
1. download file from s3
2. process it, generating new file
3. upload that to a different prefix in s3
"""
from pathlib import Path
import geopandas as gpd
import boto3

from rsxml import Logger
from rme_to_athena.rme_to_athena_parquet import upload_to_s3

DEFAULT_DATA_BUCKET = "riverscapes-athena"


def download_s3_file(s3_bucket: str, s3_key: str, local_file_path: Path):
    """Download a file from S3 to a local path."""
    s3 = boto3.client('s3')
    local_file_path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(s3_bucket, s3_key, str(local_file_path))


def list_s3_files(bucket, prefix):
    """List all S3 object keys in a bucket with the given prefix."""
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            files.append(obj['Key'])
    return files


def process_multiple(filepattern: str):
    """process all files starting with filepattern (empty means all files)"""
    s3_prefix = 'data_exchange/riverscape_metrics/'
    s3_prefix_new = 'data_exchange/rs_metric_engine2'
    local_folder_source = Path(r"F:\nardata\work\rme_extraction\from-s3-rsathena-data_exchange_rsmetrics")
    local_folder_dest = Path(r"F:\nardata\work\rme_extraction\to-s3-rsathena-data_exchange_rsmetrics2")

    files = list_s3_files(DEFAULT_DATA_BUCKET, s3_prefix + filepattern)
    print(f'Found {len(files)} files matching pattern {filepattern}')
    for filename in files:
        print(f'Processing {filename}')
        s3key = s3_prefix + filename
        local_file_path_from_s3 = local_folder_source / filename
        local_file_path_newto_s3 = local_folder_dest / filename
        s3key_new = s3_prefix_new + 'filename'
        download_s3_file(DEFAULT_DATA_BUCKET, s3key, local_file_path_from_s3)
        process_pq1_to_pq2(local_file_path_from_s3, local_file_path_newto_s3)
        upload_to_s3(local_file_path_newto_s3, DEFAULT_DATA_BUCKET, s3key_new)

    return


def process_pq1_to_pq2(inputpqpath: Path, outputpqpath: Path):
    """take a geo-parquet file, add a simplified geometry column, save back to new geo-parquet file"""
    gdf = gpd.read_parquet(inputpqpath)

    # Reproject to EPSG:5070 for simplification
    gdf_proj = gdf.to_crs(epsg=5070)

    # Use simplify_coverage for topology-preserving simplification
    gdf["geometry_simplified"] = gdf_proj.geometry.simplify_coverage(tolerance=tolerance)
    # Reproject simplified geometry back to EPSG:4326
    gdf["geometry_simplified"] = gpd.GeoSeries(gdf["geometry_simplified"], crs=5070).to_crs(epsg=4326)
    gdf = gdf.set_crs(epsg=4326)
    gdf = gdf.reset_index(drop=True)
    gdf.to_parquet(outputpqpath)


def main():
    """Main entry point"""
    process_multiple('rme_16020204')


if __name__ == '__main__':
    main()
