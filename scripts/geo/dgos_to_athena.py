"""
Searches for RME projects in the Data Exchange, downloads the RME output GeoPackages,
scrapes the DGO metrics from the GeoPackages, and uploads the results to an S3 bucket.

Philip Bailey
June 2025
"""
import shutil
import csv
import re
import os
import logging
import argparse
import apsw
import boto3
import geopandas as gpd
from rsxml.util import safe_makedirs
from rsxml import dotenv, Logger, ProgressBar
from pydex import RiverscapesAPI, RiverscapesSearchParams
from pydex.classes.riverscapes_helpers import RiverscapesProject
from pydex.lib.athena import athena_query

# RegEx for finding RME output GeoPackages
RME_SCRAPE_GPKG_REGEX = r'.*riverscapes_metrics.gpkg'

# Number of decimal places to truncate floats
FLOAT_DEC_PLACES = 4

# Float columns that should keep their full decimal places.
FULL_FLOAT_COLS = ['latitude', 'longitude', 'prim_channel_gradient']

MAJOR = 1000000
MINOR = 1000


def scrape_rme(rs_api: RiverscapesAPI, spatialite_path: str, search_params: RiverscapesSearchParams, download_dir: str, s3_bucket: str, delete_downloads: bool, tolerance: float) -> None:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    log = Logger('Merge RME Scrapes')
    s3 = boto3.client('s3')

    # Build a list of existing RME runs that are stored in Athena.
    # results = athena_query(s3_bucket, 'SELECT DISTINCT watershed_id, rme_date_created_ts FROM raw_rme')
    # existing_rme = {row['Data'][0]['VarCharValue']: int(row['Data'][1]['VarCharValue']) for row in results[1:]}

    # Create a timedelta object with a difference of 1 day
    count = 0
    for project, _stats, _searchtotal, prg in rs_api.search(search_params, progress_bar=True, page_size=100):
        project: RiverscapesProject
        prg: ProgressBar

        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue

        # check whether the project is already in Athena with the same or newer date
        # project_created_date_ts = int(project.created_date.timestamp()) * 1000
        # if project.huc in existing_rme and existing_rme[project.huc] <= project_created_date_ts:
        #     log.info(f'Skipping project {project.id} as it is already in Athena with the same or newer date.')
        #     continue

        if project.model_version is None:
            log.warning(f'Project {project.id} does not have a model version. Skipping.')
            continue

        model_version_int = project.model_version.major * MAJOR + project.model_version.minor * MINOR + project.model_version.patch

        try:
            huc_dir = os.path.join(download_dir, project.huc)
            safe_makedirs(huc_dir)
            rme_gpkg = download_file(rs_api, project.id, huc_dir, RME_SCRAPE_GPKG_REGEX)
            # rme_tsv = os.path.join(huc_dir, f'rme_{project.huc}.tsv')
            # s3_key = os.path.join('rme', 'raw', os.path.basename(rme_tsv))

            # Simplify the DGO geometries in the GeoPackage
            simplified_gpkg = os.path.join(huc_dir, f'simplified_dgos_{project.huc}.gpkg')
            simplify_dgo_geometries(rme_gpkg, simplified_gpkg, tolerance)

            conn = apsw.Connection(simplified_gpkg)
            conn.enable_load_extension(True)
            conn.load_extension(spatialite_path)

            conn.execute(f"ATTACH DATABASE '{rme_gpkg}' AS rme")

            # Get a list of the distinct HUC12 in this GeoPackage
            curs = conn.cursor()
            curs.execute('SELECT DISTINCT huc12 FROM rme.dgo_desc')
            huc12s = [row[0] for row in curs.fetchall()]

            for huc12 in huc12s:
                huc12_tsv = os.path.join(huc_dir, f'rme_{huc12}.tsv')
                s3_key = os.path.join('rme', 'huc12-geom-cartography', os.path.basename(huc12_tsv))

                curs.execute('''SELECT d.level_path, d.seg_distance, st_astext(CastAutomagic(d.geom)) geom
                             FROM simplified_dgos d 
                             inner join rme.dgos rmed on d.level_path = rmed.level_path and d.seg_distance = rmed.seg_distance
                             inner join rme.dgo_desc dd on rmed.dgoid = dd.dgoid
                             WHERE dd.huc12 = ?''', [huc12])
                with open(huc12_tsv, "w", newline='', encoding="utf-8") as f:
                    writer = csv.writer(f, delimiter="\t")
                    writer.writerow(['level_path', 'seg_distance', 'geom'])
                    for row in curs.fetchall():
                        writer.writerow(row)

                s3.upload_file(huc12_tsv, s3_bucket, s3_key)
            count += 1
            prg.update(count)

        except Exception as e:
            log.error(f'Error scraping HUC {project.huc}: {e}')

        if delete_downloads is True and os.path.isdir(huc_dir):
            try:
                log.info(f'Deleting download directory {huc_dir}')
                shutil.rmtree(huc_dir)
            except Exception as e:
                log.error(f'Error deleting download directory {huc_dir}: {e}')


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: str) -> str:
    """
    Download files from a project on Data Exchange that match the regex string
    Return the path to the downloaded file
    """

    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        return gpkg_path

    rs_api.download_files(project_id, download_dir, [regex])

    gpkg_path = get_matching_file(download_dir, regex)

    # Cannot proceed with this HUC if the output GeoPackage is missing
    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


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


def simplify_dgo_geometries(gpkg_path: str, output_path: str, tolerance: int) -> None:
    """
    Simplify the DGO geometries in the GeoPackage using a specified tolerance.
    The simplified geometries are saved to the output path.
    """

    gdf = gpd.read_file(gpkg_path, layer='dgos')
    gdf = gdf.to_crs(epsg=5070)  # Reproject to EPSG:5070 so we can use linear tolerance
    gdf["geometry"] = gdf.geometry.simplify_coverage(tolerance=tolerance)
    gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(output_path, driver="GPKG", layer='simplified_dgos')


def main():
    """
    Search the Data Exchange for RME projects that have the RME scrape and then
    merge the contents into a single output database.
    """

    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('s3_bucket', help='s3 bucket RME files will be placed', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('tolerance', help='Simplification tolerance', type=int, default=11)
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
        scrape_rme(api, args.spatialite_path, search_params, download_folder, args.s3_bucket, args.delete, args.tolerance)

    log.info('Process complete')


if __name__ == '__main__':
    main()
