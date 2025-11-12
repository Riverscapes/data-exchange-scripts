"""
Scrape HUC10 information and load it to Athena.

Philip Bailey
5 Oct 2025

THis is going to help us build the Hypsometric Curve report for the WSCA
https://alden-report.s3.us-east-1.amazonaws.com/1005001203/wsca_report.html

S3 path for JSON Files s3://riverscapes-athena/data_exchange/rs-context/

Searches for projects in the Data Exchange, downloads specific files, uses geo to bin rasters, 
 and uploads the results to an S3 bucket.
"""
from __future__ import annotations
import sys
import traceback
import argparse
import logging
import os
import re
import json
import shutil
from pathlib import PurePosixPath

import boto3

from rsxml import dotenv, Logger, ProgressBar
from rsxml.util import safe_makedirs

from pydex.lib.raster import Raster
from pydex.classes.riverscapes_helpers import RiverscapesProject
from pydex import RiverscapesAPI, RiverscapesSearchParams

# RegEx for finding DEM files
REGEXES = {
    "DEM_REGEX": r'.*\/dem\.tif$',
    "METRICS_REGEX": r'.*rscontext_metrics\.json$',
    "VEG_REGEX": r'.*\/existing_veg\.tif$'
}
S3_BUCKET = 'riverscapes-athena'
S3_BASE_PATH = 'data_exchange/rs-context'

# Number of decimal places to truncate floats
FLOAT_DEC_PLACES = 4

MAJOR = 1000000
MINOR = 1000


def join_s3_key(*parts: str) -> str:
    """Build an S3 key with forward slashes regardless of OS."""
    return str(PurePosixPath(*parts))

def scrape_rsprojects(rs_api: RiverscapesAPI, search_params: RiverscapesSearchParams, download_dir: str, delete_downloads: bool, skip_overwrite: bool) -> None:
    """
    Loop over all the projects, download the RME output GeoPackage, and scrape the geometries and metrics.
    """

    log = Logger('HUC10 Scrape')
    s3 = boto3.client('s3')

    count = 0
    for project, _stats, _searchtotal, prg in rs_api.search(search_params, progress_bar=True, page_size=100):
        project: RiverscapesProject
        prg: ProgressBar

        # Upload just metrics['rs_context'] flattened to one line to s3
        s3_key = join_s3_key(S3_BASE_PATH, f'{project.huc}.json')

        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue

        if project.model_version is None:
            log.warning(f'Project {project.id} does not have a model version. Skipping.')
            continue

        try:
            if skip_overwrite is True:
                try:
                    # head is the cheapest way to check if a file exists on S3
                    s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                    log.info(f'File s3://{S3_BUCKET}/{s3_key} already exists. Skipping project {project.id}.')
                    count += 1
                    prg.update(count)
                    continue
                except s3.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        pass
                    else:
                        raise e

            huc_dir = os.path.join(download_dir, project.huc)
            safe_makedirs(huc_dir)

            # Download all the files we might need then load the paths and make sure they exist
            retry = 0
            complete = False
            while retry < 3 and complete is False:
                try:
                    rs_api.download_files(project_id=project.id, download_dir=huc_dir, re_filter=list(REGEXES.values()))
                    complete = True
                    break
                except Exception as e:
                    log.error(f'Error downloading files for project {project.id}: {e}')
                    traceback.print_exc(file=sys.stdout)
                    retry += 1
                continue
            dem_tif = os.path.join(huc_dir, 'topography', 'dem.tif')
            if not os.path.isfile(dem_tif):
                raise FileNotFoundError(f'Could not find DEM file for project {project.id}')
            veg_tif = os.path.join(huc_dir, 'vegetation', 'existing_veg.tif')
            if not os.path.isfile(veg_tif):
                raise FileNotFoundError(f'Could not find vegetation file for project {project.id}')
            metrics_json = os.path.join(huc_dir, 'rscontext_metrics.json')

            try:
                metrics = json.loads(open(metrics_json, 'r', encoding='utf-8').read())
            except Exception as e:
                log.warning(f'Could not find or read metrics JSON for project {project.id}: {e}')
                metrics = {}

            huc10_json = os.path.join(huc_dir, f'huc10_{project.huc}.json')

            dem_raster = Raster(dem_tif)
            dem_bins = dem_raster.bin_raster(100)
            veg_raster = Raster(veg_tif)
            veg_bins = veg_raster.bin_raster_categorical()

            if 'rs_context' not in metrics:
                metrics['rs_context'] = {}
            metrics['rs_context']['dem_bins'] = dem_bins
            metrics['rs_context']['existing_veg_bins'] = veg_bins
            log.info(f'Writing HUC10 metrics to {huc10_json}')

            # Add the project ID to the metrics so we can trace this back to its source
            metrics['rs_context']['project_id'] = project.id
            metrics['rs_context']['model_version'] = str(project.model_version)

            # Write the JSON back to `huc10code.json` (just for debugging purposes really)
            with open(huc10_json, 'w', encoding='utf-8') as f:
                json.dump(metrics, f, indent=2)

            # Now use boto3 to upload the file to S3
            log.info(f'Uploading {huc10_json} to s3://{S3_BUCKET}/{s3_key}')

            s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(metrics['rs_context']))

            count += 1
            prg.update(count)

        except Exception as e:
            log.error(f'Error scraping HUC {project.huc}: {e}')
            traceback.print_exc(file=sys.stdout)

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')


def get_matching_file(parent_dir: str, regex: str) -> str:
    """
    Get the path to the first file in the parent directory that matches the regex.
    Returns None if no file is found.
    This is used to check if the output GeoPackage has already been downloaded and
    to avoid downloading it again.
    """

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def main():
    """
    Search the Data Exchange for RME projects that have the RME scrape and then
    merge the contents into a single output database.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('--tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('--collection', help='Collection GUID', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--skip-overwrite', help='Whether or not to skip overwriting existing S3 files',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter SQL prefix ("17%")', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')

    safe_makedirs(working_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(working_folder, 'huc10-athena.log'), log_level=logging.DEBUG)

    # Data Exchange Search Params
    search_params = RiverscapesSearchParams({
        'projectTypeId': 'rscontext',
    })

    if args.collection != '.':
        search_params.collection = args.collection

    if args.tags is not None and args.tags != '.':
        search_params.tags = args.tags.split(',')

    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta = {'HUC':  args.huc_filter}

    try:
        with RiverscapesAPI(stage=args.stage) as api:
            scrape_rsprojects(api, search_params, download_folder, args.delete, args.skip_overwrite)
    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    log.info('Process complete')


if __name__ == '__main__':
    main()
