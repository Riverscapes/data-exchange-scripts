"""Add simplified geometry column to existing geo-parquet file
1. download file from s3
2. process it, generating new file
3. upload that to a different prefix in s3
"""
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import geopandas as gpd
import boto3
from tqdm import tqdm  # trying this instead of ProgressBar, I've heard good things

from rsxml import Logger
from rme_to_athena_parquet import upload_to_s3

DEFAULT_DATA_BUCKET = "riverscapes-athena"
DATA_ROOT = Path(r"F:\nardata\work\rme_extraction")


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


def process_one_file(filekey: str, local_folder_downloaded: Path, local_folder_processed: Path, s3_prefix_new: str):
    """download, process to new file, then upload"""
    log = Logger('Process One')
    tqdm.write(f'Processing {filekey}')
    filename = Path(filekey).name
    local_file_path_downloaded = local_folder_downloaded / filename
    local_file_path_processed = local_folder_processed / filename
    s3key_new = s3_prefix_new + filename
    try:
        log.debug(f'Downloading to {local_file_path_downloaded}')
        download_s3_file(DEFAULT_DATA_BUCKET, filekey, local_file_path_downloaded)
        log.debug(f'Processing to {local_file_path_processed}')
        process_pq1_to_pq2(local_file_path_downloaded, local_file_path_processed)
        log.debug(f'Uploading to {s3key_new}')
        upload_to_s3(local_file_path_processed, DEFAULT_DATA_BUCKET, s3key_new)
    except Exception as e:
        log.error(f"Failed to process {filename}: {e}")


def process_multiple(filepattern: str):
    """process all files starting with filepattern (empty means all files)"""
    log = Logger("Process multiple")
    s3_prefix = 'data_exchange/riverscape_metrics/'
    s3_prefix_new = 'data_exchange/rs_metric_engine2/'
    local_folder_downloaded = DATA_ROOT / "from-s3-rsathena-data_exchange_rsmetrics"
    local_folder_processed = DATA_ROOT / "to-s3-rsathena-data_exchange_rsmetrics2"

    files = list_s3_files(DEFAULT_DATA_BUCKET, s3_prefix + filepattern)
    log.info(f'Found {len(files)} files matching pattern {filepattern}')
    with ThreadPoolExecutor(max_workers=12) as executor:  # ADJUST as needed
        futures = [executor.submit(process_one_file, filekey, local_folder_downloaded, local_folder_processed, s3_prefix_new) for filekey in files]
        for _ in tqdm(as_completed(futures), total=len(futures)):
            pass  # Optionally handle results or exceptions here


def process_pq1_to_pq2(inputpqpath: Path, outputpqpath: Path, tolerance: float = 11):
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
    log = Logger('Setup')
    log.setup(log_path=str(DATA_ROOT / 'add_simplified_geom.log'), log_level=logging.INFO)
    log.title('Add simplified geometry')
    process_multiple('rme')
    log.title('Completed.')


if __name__ == '__main__':
    main()
