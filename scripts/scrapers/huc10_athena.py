"""
Scrape HUC10 information and load it to Athena.

Philip Bailey
5 Oct 2025
"""

"""
Searches for RME projects in the Data Exchange, downloads the RME output GeoPackages,
scrapes the DGO metrics from the GeoPackages, and uploads the results to an S3 bucket.

Philip Bailey
June 2025
"""
import shutil
import csv
import json
import re
import os
import logging
import argparse
import apsw
import boto3
from rsxml.util import safe_makedirs
from rsxml import dotenv, Logger, ProgressBar
from pydex import RiverscapesAPI, RiverscapesSearchParams
from pydex.classes.riverscapes_helpers import RiverscapesProject
from pydex.lib.athena import athena_query

# RegEx for finding DEM files
DEM_REGEX = r'.*dem\.tif$'

# RSContext metrics JSON
METRICS_REGEX = r'.*metrics\.json$'

# Number of decimal places to truncate floats
FLOAT_DEC_PLACES = 4

MAJOR = 1000000
MINOR = 1000


def scrape_rme(rs_api: RiverscapesAPI, spatialite_path: str, search_params: RiverscapesSearchParams, download_dir: str, s3_bucket: str, delete_downloads: bool) -> None:
    """
    Loop over all the projects, download the RME output GeoPackage, and scrape the geometries and metrics.
    """

    log = Logger('HUC10 Scrape')
    s3 = boto3.client('s3')

    count = 0
    for project, _stats, _searchtotal, prg in rs_api.search(search_params, progress_bar=True, page_size=100):
        project: RiverscapesProject
        prg: ProgressBar

        if project.huc is None or project.huc == '':
            log.warning(f'Project {project.id} does not have a HUC. Skipping.')
            continue

        if project.model_version is None:
            log.warning(f'Project {project.id} does not have a model version. Skipping.')
            continue

        model_version_int = project.model_version.major * MAJOR + project.model_version.minor * MINOR + project.model_version.patch

        try:
            huc_dir = os.path.join(download_dir, project.huc)
            safe_makedirs(huc_dir)
            dem_tif = download_file(rs_api, project.id, huc_dir, DEM_REGEX)
            metrics_json = download_file(rs_api, project.id, huc_dir, METRICS_REGEX)
            metrics = json.loads(open(metrics_json, 'r', encoding='utf-8').read())

            huc10_json = os.path.join(huc_dir, f'huc10_{project.huc}.json')
            s3_key = os.path.join('huc10', 'metrics', os.path.basename(huc10_json))


            def dict_row_factory(cursor, row):
                return {description[0]: value for description, value in zip(cursor.getdescription(), row)}

            curs = conn.cursor()
            curs.setrowtrace(dict_row_factory)

            curs.execute('''
                SELECT
                    ? as rme_version,
                    ? as rme_version_int,
                    ? as rme_date_created_ts,
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
                    ST_AsText(dgo_geom) dgo_geom
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
            ''', [str(project.model_version), model_version_int, project_created_date_ts])

            with open(rme_tsv, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                cols = [description[0] for description in curs.description]
                # remove any columns called DGOID
                cols = [col for col in cols if col.lower() != 'dgoid']
                writer.writerow(cols)
                for row in curs.fetchall():
                    values = []
                    for col in cols:
                        value = row[col]
                        if isinstance(value, float):
                            # Truncate floats to FLOAT_DEC_PLACES decimal places, except for FULL_FLOAT_COLS
                            if col in FULL_FLOAT_COLS:
                                values.append(str(value))
                            else:
                                values.append(f'{value:.{FLOAT_DEC_PLACES}f}')
                        elif isinstance(value, str):
                            values.append(value.replace(',', '|'))
                        elif value is None:
                            values.append('')
                        else:
                            values.append(str(value))
                    writer.writerow(values)

            s3.upload_file(rme_tsv, s3_bucket, s3_key)
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

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, args.spatialite_path, search_params, download_folder, args.s3_bucket, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
