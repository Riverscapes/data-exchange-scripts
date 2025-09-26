"""
Searches Data Exchange for RME projects matching criteria
Generates parquet files from metrics in the GeoPackages
Uploads to s3

Lorin Gaertner
Sept 2025
Enhances Philip's June 2025 rme_to_athena.py
"""
import argparse
import logging
import os
import re
import shutil
import warnings

import apsw
import boto3
import geopandas as gpd
import pandas as pd
from shapely import wkb
from semver import Version

from rsxml.util import safe_makedirs
from rsxml import dotenv, Logger

from pydex import RiverscapesAPI, RiverscapesSearchParams, RiverscapesProject
from pydex.lib.athena import athena_query_get_parsed


def semver_to_int(version: Version) -> int:
    """convert to integer for easier comparisons

    Args:
        version (Version): semver Version e.g. 3.1.5

    Returns:
        int: integerpresentation e.g. 3001005
    """
    MAJOR = 1000000
    MINOR = 1000
    return version.major * MAJOR + version.minor * MINOR + version.patch


def get_athena_rme_projects(s3_bucket: str) -> dict[str, int]:
    """
    Query Athena for existing RME projects  
    return: lookup dict consisting of watershedID (ie huc10,str) and timestamp (integer)
    FUTURE ENHANCEMENT: if we're only interested in updating a subset, no need to return everything in rme
    """
    # FUTURE ENHANCEMENT - unless watershed_id a global id across countries we need something better
    existing_rme = athena_query_get_parsed(s3_bucket, 'SELECT DISTINCT watershed_id, rme_date_created_ts FROM raw_rme')
    # this should look like:
    # [{'rme_date_created_ts': '1752810123000', 'watershed_id': '1704020402'},
    #  {'rme_date_created_ts': '1756512492000', 'watershed_id': '1030010112'},
    # ...
    # ]
    if not existing_rme:
        print("got nothing back! failure?!")
        raise NotImplementedError

    # Convert list of dicts to a dict keyed by watershed_id. assumes no null values
    return {
        row['watershed_id']: int(row['rme_date_created_ts'])
        for row in existing_rme
    }


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: str) -> str:
    """
    Download files from a project on Data Exchange that match the regex string
    Return the path to the downloaded file
    """
    # check if it has previously been downloaded
    log = Logger('download RS DEX file')
    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        log.debug(f'file for {project_id} previously downloaded')
        return gpkg_path

    rs_api.download_files(project_id, download_dir, [regex])

    gpkg_path = get_matching_file(download_dir, regex)
    log.debug(f'file for {project_id} downloaded to {gpkg_path}')

    # Cannot proceed with this HUC if the output GeoPackage is missing
    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


def get_matching_file(parent_dir: str, regex_str: str) -> str | None:
    """
    Get the path to the *first* file in the parent directory that matches the regex.
    Returns None if no file is found.
    This is used to check if the output GeoPackage has already been downloaded and
    to avoid downloading it again.
    """

    regex = re.compile(regex_str)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def download_rme_geopackage(
    rs_api: RiverscapesAPI,
    project: RiverscapesProject,
    huc_dir: str
) -> str:
    """
    Download the RME GeoPackage for a project and return its file path.
    """
    # RegEx string for finding RME output GeoPackages
    RME_SCRAPE_GPKG_REGEX = r'.*riverscapes_metrics.gpkg'
    rme_gpkg = download_file(rs_api, project.id, huc_dir, RME_SCRAPE_GPKG_REGEX)  # pyright: ignore[reportArgumentType]
    return rme_gpkg


def extract_metrics_to_geodataframe(gpkg_path: str, spatialite_path: str) -> gpd.GeoDataFrame:
    """
    Connect to the GeoPackage, run the SQL, and return a GeoDataFrame.
    """
    conn = apsw.Connection(gpkg_path)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)

    sql = '''
        SELECT
            dgos.level_path,
            dgos.seg_distance,
            dgos.centerline_length,
            dgos.segment_area,
            dgos.FCode as fcode,
            ST_X(castautomagic(igos.geom)) longitude,
            ST_Y(castautomagic(igos.geom)) latitude,
            dgo_desc.*,
            dgo_geomorph.*,
            dgo_veg.*,
            dgo_hydro.*,
            dgo_impacts.*,
            dgo_beaver.*,
            ST_AsBinary(dgo_geom) dgo_geom
        FROM dgo_desc
            INNER JOIN dgo_geomorph ON dgo_desc.dgoid = dgo_geomorph.dgoid
            INNER JOIN dgo_veg ON dgo_desc.dgoid = dgo_veg.dgoid
            INNER JOIN dgo_hydro ON dgo_desc.dgoid = dgo_hydro.dgoid
            INNER JOIN dgo_impacts ON dgo_desc.dgoid = dgo_impacts.dgoid
            INNER JOIN dgo_beaver ON dgo_desc.dgoid = dgo_beaver.dgoid
            INNER JOIN
            (
                    SELECT
                    dgoid,
                    st_union(CastAutomagic(dgos.geom)) dgo_geom,
                    level_path,
                    seg_distance,
                    centerline_length,
                    segment_area,
                    FCode
                FROM dgos
                GROUP BY level_path, seg_distance
                HAVING GeometryType(dgo_geom) = 'POLYGON'
            ) dgos ON dgo_desc.dgoid = dgos.dgoid
            INNER JOIN igos ON dgos.level_path = igos.level_path AND dgos.seg_distance = igos.seg_distance
    '''
    # we need apsw / spatialite . this seems to work despite pandas not supporting it
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")
        df = pd.read_sql_query(sql, conn)

    # Remove all columns named 'dgoid' (case-insensitive, even if duplicated)
    df = df.loc[:, [col for col in df.columns if col.lower() != 'dgoid']]
    # convert wkb geometry to shapely objects
    df['dgo_geom'] = df['dgo_geom'].apply(wkb.loads)  # pyright: ignore[reportCallIssue, reportArgumentType]
    gdf = gpd.GeoDataFrame(df, geometry='dgo_geom', crs='EPSG:4326')
    return gdf


def delete_folder(dirpath: str) -> None:
    """delete a local folder and its contents"""
    log = Logger('delete downloads')
    if os.path.isdir(dirpath):
        try:
            log.info(f'Deleting directory {dirpath}')
            shutil.rmtree(dirpath)
            log.debug(f'removed {dirpath}')
        except Exception as e:
            log.error(f'Error deleting download directory {dirpath}: {e}')


def upload_to_s3(
        file_path: str,
        s3_bucket: str,
        s3_key: str
) -> None:
    """upload a file to s3

    Args:
        file_path (str): local file path
        s3_bucket (str): s3 bucket name
        s3_key (str): s3_key (including 'folders'?)
    """
    log = Logger('upload to s3')
    s3 = boto3.client('s3')
    s3.upload_file(file_path, s3_bucket, s3_key)
    log.debug(f'file uploaded to s3 {s3_bucket} {s3_key}')


def scrape_rme(rs_api: RiverscapesAPI, spatialite_path: str, search_params: RiverscapesSearchParams,
               download_dir: str, s3_bucket: str, delete_downloads_when_done: bool) -> None:
    """
    Orchestrate the scraping, processing, and uploading of RME projects.
    """
    # 1. Get list of projects to process
    # 2. For each project:
    #    - create a folder
    #    - Download and validate
    #    - Extract metrics as GeoDataFrame
    #    - Write GeoParquet
    #    - Upload to S3
    #    - Optionally clean up

    log = Logger('Scrape RME')

    rme_in_athena = get_athena_rme_projects(s3_bucket)
    log.debug(f'{len(rme_in_athena)} existing rme projects found in athena')
    # loop through data exchange projects
    count = 0
    for project, _stats, _searchtotal, prg in rs_api.search(search_params, progress_bar=True, page_size=100):
        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue

        # check whether the project is already in Athena with the same or newer date
        project_created_date_ts = int(project.created_date.timestamp()) * 1000  # pyright: ignore[reportOptionalMemberAccess] Projects always have a created_date
        if project.huc in rme_in_athena and rme_in_athena[project.huc] <= project_created_date_ts:
            log.info(f'NORMALLY WOULD BE Skipping project {project.id} as it is already in Athena with the same or newer date. DEX ts = {project_created_date_ts}; Athena ts={rme_in_athena[project.huc]}')
            # TODO: uncomment after first run
            # continue

        if project.model_version is None:
            log.warning(f'Project {project.id} does not have a model version. Skipping.')
            continue

        model_version_int = semver_to_int(project.model_version)

        try:
            huc_dir = os.path.join(download_dir, project.huc)
            safe_makedirs(huc_dir)
            gpkg_path = download_rme_geopackage(rs_api, project, huc_dir)
            data_gdf = extract_metrics_to_geodataframe(gpkg_path, spatialite_path)
            # add common project-level columns
            data_gdf['rme_project_id'] = project.id
            data_gdf['rme_date_created_ts'] = project_created_date_ts
            data_gdf['rme_version'] = str(project.model_version)
            data_gdf['rme_version_int'] = model_version_int

            log.debug(f"Dataframe prepared with shape {data_gdf.shape}")
            rme_pq_filepath = os.path.join(huc_dir, f'rme_{project.huc}.parquet')
            data_gdf.to_parquet(rme_pq_filepath)
            # don't use os.path.join because this is aws os, not system os
            s3_key = f'rme/raw-pq/{os.path.basename(rme_pq_filepath)}'
            upload_to_s3(rme_pq_filepath, s3_bucket, s3_key)

            if delete_downloads_when_done:
                delete_folder(download_dir)
            count += 1
            prg.update(count)
        except Exception as e:
            log.error(f'Error scraping HUC {project.huc}: {e}')
            raise


def main():
    """Process arguments, set up logs and orchestrate call to other functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('s3_bucket', help='s3 bucket RME files will be placed', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('--tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('--collection', help='Collection GUID', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter SQL prefix ("17%")', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')

    safe_makedirs(working_folder)
    log = Logger('Setup')
    log.setup(log_path=os.path.join(working_folder, 'rme-athena.log'), log_level=logging.DEBUG)

    log.title("rme scrape to parquet to athena")

    # Data Exchange Search Params
    search_params = RiverscapesSearchParams({
        'projectTypeId': 'rs_metric_engine',
    })

    if args.collection != '.':
        search_params.collection = args.collection

    if args.tags is not None and args.tags != '.':
        search_params.tags = args.tags.split(',')

    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta = {'HUC':  args.huc_filter}

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, args.spatialite_path, search_params, download_folder, args.s3_bucket, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
