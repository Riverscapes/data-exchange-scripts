"""Scrape rs_context projects (HUC10) from Data Exchange and load the data S3 for Athena
This version queries the Athena index of Data Exchange projects instead of using graphql API

Downloads specific files and uses geo to bin rasters.
Requires geo extras
`uv sync --extra geo`

Lorin 2026-March-23

"""

import argparse
import json
import logging
import shutil
import sys
import traceback
from pathlib import Path, PurePosixPath

import boto3
from rsxml import Logger, ProgressBar, dotenv
from rsxml.util import safe_makedirs

from pydex import RiverscapesAPI, RiverscapesProject
from pydex.lib.athena import query_to_dataframe
from pydex.lib.raster import Raster

# RegEx for finding DEM, Vegetation and Metrics files
REGEXES = {"DEM_REGEX": r'.*\/dem\.tif$', "METRICS_REGEX": r'.*rscontext_metrics\.json$', "VEG_REGEX": r'.*\/existing_veg\.tif$'}
S3_BUCKET = 'riverscapes-athena'
S3_BASE_PATH = 'data_exchange/rs-context'

# Number of decimal places to truncate floats
FLOAT_DEC_PLACES = 4

MAJOR = 1000000
MINOR = 1000

missing_projects_query = """
-- rscontext projects that we have not scraped into Athena (missing or newer versions of HUCs previously loaded)
select p.project_id AS project_id,
       p.huc,
       p.created_on,
       s.huc AS scraped_huc,
       s.project_id AS scraped_project_id,
       sp.created_on AS scraped_project_created_on
from conus_projects p
         left join default.rs_context_huc10 s on p.huc = s.huc
         left join conus_projects sp on s.project_id = sp.project_id
where p.project_type_id = 'rscontext'
  and (s.huc is null or sp.created_on < p.created_on)
"""


def join_s3_key(*parts: str) -> str:
    """Build an S3 key with forward slashes regardless of OS."""
    return str(PurePosixPath(*parts))


def scrape_rscontext_project(s3, rs_api: RiverscapesAPI, project: RiverscapesProject, download_dir: Path, skip_overwrite: bool):
    """Scrape (download, transform, upload) a single project"""
    DOWNLOAD_RETRIES = 3
    log = Logger("Scrape RSContext project")
    # S3 key for upload
    s3_key = join_s3_key(S3_BASE_PATH, f'{project.huc}.json')
    if project.huc is None or project.huc == '':
        log.warning(f'Project {project.id} does not have a HUC. Skipping.')
        return

    if project.model_version is None:
        log.warning(f'Project {project.id} does not have a model version. Skipping.')
        return

    try:
        if skip_overwrite is True:
            try:
                # head is the cheapest way to check if a file exists on S3
                s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                log.info(f'File s3://{S3_BUCKET}/{s3_key} already exists. Skipping project {project.id}.')
                return
            except s3.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    pass
                else:
                    raise e

        # download the files we need
        huc_dir = download_dir / str(project.huc)
        safe_makedirs(str(huc_dir))

        retry = 0
        complete = False
        while retry < DOWNLOAD_RETRIES and complete is False:
            try:
                rs_api.download_files(project_id=project.id, download_dir=str(huc_dir), re_filter=list(REGEXES.values()))
                complete = True
                break
            except Exception as e:
                log.error(f'Error downloading files for project {project.id}: {e}')
                traceback.print_exc(file=sys.stdout)
                retry += 1
            continue

        dem_tif_path = huc_dir / 'topography' / 'dem.tif'
        if not dem_tif_path.exists():
            raise FileNotFoundError(f'Could not find DEM file for project {project.id}')
        veg_tif_path = huc_dir / 'vegetation' / 'existing_veg.tif'
        if not veg_tif_path.exists():
            raise FileNotFoundError(f'Could not find vegetation file for project {project.id}')
        metrics_json_path = huc_dir / 'rscontext_metrics.json'
        try:
            metrics = json.loads(open(metrics_json_path, 'r', encoding='utf-8').read())
        except Exception as e:
            log.warning(f'Could not find or read metrics JSON for project {project.id}: {e}')
            metrics = {}

        huc10_json_path = huc_dir / f'huc10_{project.huc}.json'
        dem_raster = Raster(str(dem_tif_path))
        dem_bins = dem_raster.bin_raster(100)
        veg_raster = Raster(str(veg_tif_path))
        veg_bins = veg_raster.bin_raster_categorical()
        if 'rs_context' not in metrics:
            metrics['rs_context'] = {}
        metrics['rs_context']['dem_bins'] = dem_bins
        metrics['rs_context']['existing_veg_bins'] = veg_bins

        # Add the project ID to the metrics so we can trace this back to its source
        metrics['rs_context']['project_id'] = project.id
        metrics['rs_context']['model_version'] = str(project.model_version)

        log.info(f'Writing HUC10 metrics to {huc10_json_path}')
        # Write the JSON back to `huc10_{huc}.json` (just for debugging purposes really)
        with open(huc10_json_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)

        # Now use boto3 to upload the file to S3
        # log.info(f'Uploading metrics to s3://{S3_BUCKET}/{s3_key}')

        # s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(metrics['rs_context']))

    except Exception as e:
        log.error(f'Error scraping HUC {project.huc}: {e}')
        traceback.print_exc(file=sys.stdout)


def scrape_rsprojects(rs_api: RiverscapesAPI, download_dir: Path, delete_downloads: bool, skip_overwrite: bool):
    """Scrape all projects matching criteria"""
    log = Logger('Scrape RSContext')
    projects_to_add_df = query_to_dataframe(missing_projects_query, 'identify new projects')
    if projects_to_add_df.empty:
        log.info("Query to identify projects to scrape returned no results.")
        return
    count = 0
    prg = ProgressBar(projects_to_add_df.shape[0], text="Scrape Progress")
    s3 = boto3.client('s3')
    for project_id in projects_to_add_df['project_id']:
        project = rs_api.get_project_full(project_id)
        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue
        scrape_rscontext_project(s3, rs_api, project, download_dir, skip_overwrite)
        count += 1
        prg.update(count)

    if delete_downloads is True and download_dir.is_dir():
        try:
            log.info(f'Deleting download directory {download_dir}')
            shutil.rmtree(download_dir)
        except Exception as e:
            log.error(f'Error deleting download directory {download_dir}: {e}')


def main():
    """
    Parse arguments and call function to run the scrape
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('--delete', help='Delete downloaded files after processing', action='store_true', default=False)
    parser.add_argument('--skip-overwrite', help='Whether or not to skip overwriting existing S3 files', action='store_true', default=False)

    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = Path(args.working_folder)
    download_folder = working_folder / 'downloads'
    safe_makedirs(str(working_folder))

    log = Logger('Setup')
    log.setup(log_path=working_folder / 'rscontext_to_athena.log', log_level=logging.DEBUG)
    try:
        with RiverscapesAPI(stage=args.stage) as rs_api:
            scrape_rsprojects(rs_api, download_folder, args.delete, args.skip_overwrite)

    except Exception as e:
        log.error(e)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    log.info('Process complete')


if __name__ == '__main__':
    main()
