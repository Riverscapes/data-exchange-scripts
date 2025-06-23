"""
Scrapes RME and RCAT output GeoPackages from Data Exchange and extracts statistics for each HUC.
Produced for the BLM 2024 September analysis of 2024 CONUS RME projects.

This script assumes that the `scrape_huc_statistics.py` script has been run on each RME project.
The scrape_huc_statistics.py script extracts statistics from the RME and RCAT output GeoPackages
and generates a new 'rme_scrape.sqlite' file in the project. This is then uploaded into the 
project on the Data Exchange. 
"""
import shutil
import csv
import re
import os
import sqlite3
import logging
import argparse
import apsw
import boto3
from pydex import RiverscapesAPI, RiverscapesSearchParams
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs

# RegEx for finding RME and RCAT output GeoPackages
RME_SCRAPE_GPKG_REGEX = r'.*riverscapes_metrics.gpkg'


def scrape_rme(rs_api: RiverscapesAPI, spatialite_path: str, search_params: RiverscapesSearchParams, download_dir: str, s3_bucket: str, delete_downloads: bool) -> None:
    """
    Loop over all the projects, download the RME and RCAT output GeoPackages, and scrape the statistics
    """

    log = Logger('Merge RME Scrapes')
    s3 = boto3.client('s3')

    # Create a timedelta object with a difference of 1 day
    for project, _stats, _searchtotal, _prg in rs_api.search(search_params, progress_bar=True, page_size=100):

        # Attempt to retrieve the huc10 from the project metadata if it exists
        huc10 = None
        try:
            for key in ['HUC10', 'huc10', 'HUC', 'huc']:
                if key in project.project_meta:
                    value = project.project_meta[key]
                    huc10 = value if len(value) == 10 else None
                    break

            huc_dir = os.path.join(download_dir, huc10)
            safe_makedirs(huc_dir)
            rme_gpkg = download_file(rs_api, project.id, huc_dir, RME_SCRAPE_GPKG_REGEX)
            rme_tsv = os.path.join(huc_dir, f'rme_{huc10}.tsv')
            s3_key = os.path.join('raw_huc10_rme', os.path.basename(rme_tsv))

            conn = apsw.Connection(rme_gpkg)
            conn.enable_load_extension(True)
            conn.load_extension(spatialite_path)
            curs = conn.cursor()

            curs.execute('''
                SELECT
                    ?,
                    dgos.level_path,
                    dgos.seg_distance,
                    dgos.centerline_length,
                    dgos.segment_area,
                    dgos.FCode,
                    ST_AsText(dgo_geom) dgo_geom,
                    ST_AsText(castAutomagic(igos.geom)) igo_geom,
                    ST_AsText(castautomagic(igos.geom)) longitude,
                    ST_AsText(castautomagic(igos.geom)) latitude,
                    dgo_desc.*,
                    dgo_geomorph.*,
                    dgo_veg.*,
                    dgo_hydro.*,
                    dgo_impacts.*,
                    dgo_beaver.*
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
            ''', [huc10])

            with open(rme_tsv, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f, delimiter="\t")
                # writer.writerow([description[0] for description in cursor.description])
                writer.writerows(curs.fetchall())

            s3.upload_file(rme_tsv, s3_bucket, s3_key)

        except Exception as e:
            log.error(f'Error scraping HUC {huc10}: {e}')

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


def copy_table_between_cursors(src_cursor, dest_cursor, table_name, create_table: bool):
    """
    Copy a table structure and data from the source cursor to destination cursor
    """

    if create_table is True:
        # Get table schema from the source database
        src_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        create_table_sql = src_cursor.fetchone()[0]
        dest_cursor.execute(create_table_sql)

    # Get all data from the source table
    src_cursor.execute(f"SELECT * FROM {table_name}")
    rows = src_cursor.fetchall()

    # Get the column names from the source table
    src_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [info[1] for info in src_cursor.fetchall()]  # info[1] gives the column names
    columns_str = ', '.join(columns)

    # Insert data into the destination table
    placeholders = ', '.join(['?' for _ in columns])  # Create placeholders for SQL insert
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    dest_cursor.executemany(insert_sql, rows)


def create_output_db(output_db: str, delete: bool) -> None:
    """ 
    Build the output SQLite database by running the schema file.
    """
    log = Logger('Create Output DB')

    # As a precaution, do not overwrite or delete the output database.
    # Force the user to delete it manually if they want to rebuild it.
    if os.path.isfile(output_db):
        if delete is True:
            log.info(f'Deleting existing output database {output_db}')
            os.remove(output_db)
        else:
            log.info('Output database already exists. Skipping creation.')
            return

    schema_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'packages', 'rme', 'rme', 'database')
    if not os.path.isdir(schema_dir):
        raise FileNotFoundError(f'Could not find database schema directory {schema_dir}')

    safe_makedirs(os.path.dirname(output_db))

    with sqlite3.connect(output_db) as conn:
        curs = conn.cursor()
        log.info('Creating output database schema')
        with open(os.path.join(schema_dir, 'rme_scrape_huc_statistics.sql'), encoding='utf-8') as sqlfile:
            sql_commands = sqlfile.read()
            curs.executescript(sql_commands)
            conn.commit()

    log.info(f'Output database at {output_db}')


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
    parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
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
        'tags': args.tags.split(','),
        'projectTypeId': 'rs_metric_engine',
        "meta": {"ModelVersion": "3.0.1"}
    })

    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta['HUC'] = args.huc_filter

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(api, args.spatialite_path, search_params, download_folder, args.s3_bucket, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
