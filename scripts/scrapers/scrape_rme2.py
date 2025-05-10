"""
RME scrape.

NEW VERSION FOR JORDAN'S 2025 reformatted RME output GeoPackage.

This script copies across all the IGOs and their side tables. For the DGOs it drops the geometry
column and copies across the side tables. Then it recreates all the DGO views to use the IGO geometry
instead of the DGO geometry.


1) Searches Data Exchange for RME projects with the specified tags (and optional HUC filter)
2) Downloads the RME output GeoPackages, project files and bounds GeoJSON files
3) Scrapes the metrics from the RME output GeoPackages into a single output GeoPackage
4) Optionally deletes the downloaded GeoPackages
"""
from typing import List, Tuple
from datetime import datetime
import subprocess
import shutil
import re
import os
import json
import sqlite3
import logging
import argparse
import semver
import apsw
from rsxml import dotenv, Logger
from rsxml.util import safe_makedirs
from rsxml.project_xml import (
    Project,
    MetaData,
    Meta,
    ProjectBounds,
    Coords,
    BoundingBox,
    Realization,
    Geopackage,
    GeopackageLayer,
    GeoPackageDatasetTypes,
    Dataset
)
from pydex import RiverscapesAPI, RiverscapesSearchParams

# RegEx for finding the RME output GeoPackages
RME_OUTPUT_GPKG_REGEX = r'.*riverscapes_metrics\.gpkg$'
RME_BOUNDS_REGEX = r'.*project_bounds\.geojson$'
RME_PROJECT_REGEX = r'.*project\.rs\.xml$'

MINIMUM_RME_VERSION = '3.0.1'


def scrape_rme(rs_stage: str, rs_api: RiverscapesAPI, log_path: str, spatialite_path: str, search_params: RiverscapesSearchParams, download_dir: str, project_dir: str, project_name: str, delete_downloads: bool) -> None:
    """
    Download RME output GeoPackages from Data Exchange and scrape the metrics into a single GeoPackage
    """

    log = Logger('Scrape RME')

    projects = {}
    bounds_gpkg = os.path.join(os.path.dirname(os.path.dirname(project_dir)), 'project_bounds.gpkg')
    output_gpkg = os.path.join(project_dir, 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(os.path.dirname(output_gpkg))
    for project, _stats, _searchtotal, _prg in rs_api.search(search_params, progress_bar=True, page_size=100):
        try:
            # Attempt to retrieve the huc10 from the project metadata if it exists
            huc10 = get_project_meta_value(project, ['HUC10', 'huc10', 'HUC', 'huc'], 10)
            version = get_project_meta_value(project, ['ModelVersion', 'Model Version', 'model_version', 'ModelVersion', 'model_version'])

            sem_version = semver.VersionInfo.parse(version) if version else None
            if sem_version is None or sem_version < semver.VersionInfo.parse(MINIMUM_RME_VERSION):
                log.warning(f'Skipping project {project.id} with version {version} (less than {MINIMUM_RME_VERSION})')
                continue

            # While this allows for stopping and restarting the script, the output project file will only
            # reflect the latest run of projects.
            if continue_with_huc(huc10, output_gpkg) is not True:
                continue

            log.info(f'Scraping RME metrics for HUC {huc10}')
            log.info(f'https://{"staging." if rs_stage == "STAGING" else ""}data.riverscapes.net/p/{project.id}')
            huc_dir = os.path.join(download_dir, huc10)
            safe_makedirs(huc_dir)
            rs_api.download_files(project.id, huc_dir, [RME_OUTPUT_GPKG_REGEX, RME_BOUNDS_REGEX, RME_PROJECT_REGEX])
            projects[huc10] = huc_dir

            # append the project bounds to a temporary GeoPackage
            bounds_path = get_matching_file(huc_dir, RME_BOUNDS_REGEX)
            if os.path.isfile(bounds_path):
                cmd = f'ogr2ogr -makevalid -append -nln project_bounds "{bounds_gpkg}" "{bounds_path}"'
                log.debug(f'EXECUTING: {cmd}')
                subprocess.call([cmd], shell=True, cwd=os.path.dirname(output_gpkg))
            else:
                log.warning(f'Could not find bounds file for project {project.id} at {bounds_path}')

        except Exception as e:
            log.error(f'Error scraping HUC {huc10}: {e}')
            continue

    if len(projects) == 0:
        log.error('No projects found to scrape. Exiting...')
        return

    log.info(f'Found {len(projects)} projects to scrape')
    for huc10, huc_dir in projects.items():
        rme_gpkg = get_matching_file(huc_dir, RME_OUTPUT_GPKG_REGEX)
        if not os.path.isfile(output_gpkg):
            create_gpkg(huc10, rme_gpkg, output_gpkg, spatialite_path)
        else:
            scrape_huc(spatialite_path, huc10, rme_gpkg, output_gpkg)

        curs = apsw.Connection(output_gpkg).cursor()
        curs.execute('INSERT INTO hucs (huc, rme_project_id) VALUES (?, ?)', [huc10, None])

    # Done scraping... clean up
    clean_up_gpkg(output_gpkg, spatialite_path)

    # Build the bounds for the new RME scrape project
    bounds, centroid, bounding_rect = get_bounds(bounds_gpkg, spatialite_path)
    output_bounds_path = os.path.join(project_dir, 'project_bounds.geojson')
    with open(output_bounds_path, "w", encoding='utf8') as f:
        json.dump(bounds, f, indent=2)

    if delete_downloads is True and os.path.isdir(huc_dir):
        try:
            log.info(f'Deleting download directory {huc_dir}')
            shutil.rmtree(huc_dir)
        except Exception as e:
            log.error(f'Error deleting download directory {huc_dir}: {e}')

    rs_project = Project(
        project_name,
        project_type='igos',
        description=f"""This project was generated by scraping metrics from {len(projects)}
                         Riverscapes Metric Engine projects together, using the scrape_rme2.py script.
                         The project bounds are the union of the bounds of the individual projects.""",
        meta_data=MetaData([
            Meta('Date Created',  str(datetime.now().isoformat()), type='isodate', ext=None),
            Meta('ModelVersion', version),
        ]),
        bounds=ProjectBounds(
            Coords(centroid[0], centroid[1]),
            BoundingBox(bounding_rect[0], bounding_rect[1], bounding_rect[2], bounding_rect[3]),
            os.path.basename(output_bounds_path)
        ),
        realizations=[Realization(
            name='Realization1',
            xml_id='REALIZATION1',
            date_created=datetime.now(),
            product_version='1.0.0',
            datasets=[
                Geopackage(
                    name='Riverscapes Metrics',
                    xml_id='RME',
                    path=os.path.relpath(output_gpkg, project_dir),
                    layers=get_datasets(output_gpkg)
                ),
                Dataset(
                    xml_id='LOG',
                    ds_type='LogFile',
                    name='Lof File',
                    description='Processing log file',
                    path=os.path.relpath(log_path, project_dir),
                ),
            ]
        )]
    )

    merged_project_xml = os.path.join(project_dir, 'project.rs.xml')
    rs_project.write(merged_project_xml)
    log.info(f'Project XML file written to {merged_project_xml}')


def get_project_meta_value(project: Project, keys: List[str], required_length: int = None) -> str:
    """
    Get the value of a metadata item from a project.
    """
    for key in keys:
        if key in project.project_meta:
            value = project.project_meta[key]
            if required_length is None or len(value) == required_length:
                return value

    return None


def get_datasets(output_gpkg: str) -> List[GeopackageLayer]:
    """
    Returns a list of the datasets from the output GeoPackage.
    These are the spatial views that are created from the igos and dgos tables.
    """

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    curs = conn.cursor()

    # Get the names of all the tables in the database
    curs.execute("SELECT table_name FROM gpkg_contents WHERE data_type='features'")
    datasets = [GeopackageLayer(
        lyr_name=row[0],
        ds_type=GeoPackageDatasetTypes.VECTOR,
        name=row[0]
    ) for row in curs.fetchall()]
    return datasets


def get_bounds(bounds_gpkg: str, spatialite_path: str) -> Tuple[str, str, str]:
    """
    Union all the polygons in the bounds GeoPackage and return the centroid, bounding box and GeoJSON
    """

    conn = apsw.Connection(bounds_gpkg)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    curs = conn.cursor()
    curs.execute('''
        SELECT AsGeoJSON(union_geom) AS geojson,
            ST_X(ST_Centroid(union_geom)),
            ST_Y(ST_Centroid(union_geom)),
            ST_MinX(union_geom),
            ST_MinY(union_geom),
            ST_MaxX(union_geom),
            ST_MaxY(union_geom) FROM (
                SELECT ST_Simplify(ST_Buffer(ST_Union(ST_Buffer(CastAutomagic(geom), 0.001)), -0.001), 0.01) union_geom FROM project_bounds
            )''')

    bounds_row = curs.fetchone()
    geojson_geom = json.loads(bounds_row[0])
    centroid = (bounds_row[1], bounds_row[2])
    bounding_box = [
        bounds_row[3],
        bounds_row[4],
        bounds_row[5],
        bounds_row[6]
    ]

    geojson_output = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": geojson_geom,
            "properties": {}
        }]
    }

    return geojson_output, centroid, bounding_box


def scrape_huc(spatialite_path: str, huc10: str, rme_gpkg: str, output_gpkg: str) -> None:
    """
    Process a single RME GeoPackage and copy the IGOs and DGOs to the output GeoPackage
    """

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    curs = conn.cursor()
    curs.execute('ATTACH DATABASE ? as rme', [rme_gpkg])

    try:
        scrape_igos(curs, huc10)
        scrape_dgos(curs, huc10)

        # Be sure to detatch the rme database, just in case the connection is still open
        curs.execute('DETACH DATABASE rme')

    except Exception as e:
        print(f'Error inserting into igos table: {e}')
        raise


def scrape_igos(curs: apsw.Cursor, huc10: str) -> None:
    """ Copy IGOs and the side table metrics to the output GeoPackage
    Virtually the same code is used for DGOs, but the subtle difference because we don't
    need DGO geometries means that DGO code is in separate function"""

    # Get the schema of the igos table
    curs.execute('PRAGMA table_info(igos)')
    igo_cols = [col[1] for col in curs.fetchall() if col[1] != 'igoid' and col[1] != 'huc10' and col[1] != 'rme_fid']

    # Copy the IGOs, with geometries and also include the huc and rme_fid columns
    curs.execute(f'INSERT INTO main.igos ({",".join(igo_cols)}, huc10, rme_fid) SELECT {",".join(igo_cols)}, ?, igoid FROM rme.igos', [huc10])

    # For safety, clear the RME IDs for this HUC
    curs.execute('UPDATE igos SET rme_fid = NULL WHERE huc10 = ?', [huc10])


def scrape_dgos(curs: apsw.Cursor, huc10: str) -> None:
    """ Copy DGOs and the side table metrics to the output GeoPackage
    Virtually the same code is used for IGOs, but the subtle difference because we don't
    need DGO geometries means that DGO code is in separate function"""

    # Get the schema of the igos table
    curs.execute('PRAGMA table_info(dgos)')
    dgo_cols = [col[1] for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'huc10' and col[1] != 'rme_fid' and col[1] != 'geom']

    # Copy the DGOs, WITHOUT geometries and also include the huc and rme_fid columns
    curs.execute(f'INSERT INTO dgos ({",".join(dgo_cols)}, huc10, rme_fid) SELECT {",".join(dgo_cols)}, ?, dgoid FROM rme.dgos', [huc10])

    # Get the names of all the tables in the database that start with "igo_"
    curs.execute("SELECT name FROM sqlite_master WHERE type='table' and name like 'dgo_%' and name != 'dgos' and name != 'DGOVegetation'")
    dgo_tables = [row[0] for row in curs.fetchall()]

    for dgo_table in dgo_tables:
        # Get the columns of the igo side table
        curs.execute(f'PRAGMA table_info({dgo_table})')
        table_cols = [col[1] for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']

        # Copy across the IGO table records, being sure to lookup the new igoid
        # from the main.igos table by referencing the rme_fid
        curs.execute(f'''
                INSERT INTO main.{dgo_table} (dgoid, {",".join(table_cols)})
                SELECT m.dgoid, {",".join(table_cols)}
                FROM rme.{dgo_table} r INNER JOIN main.dgos m ON r.dgoid = m.rme_fid
                WHERE m.huc10 = ?
            ''', [huc10])

    # For safety, clear the RME IDs for this HUC
    curs.execute('UPDATE dgos SET rme_fid = NULL WHERE huc10 = ?', [huc10])


def download_file(rs_api: RiverscapesAPI, project_id: str, download_dir: str, regex: List[str]) -> str:
    '''
    Download files from a project on Data Exchange
    '''

    gpkg_path = get_matching_file(download_dir, regex)
    if gpkg_path is not None and os.path.isfile(gpkg_path):
        return gpkg_path

    rs_api.download_files(project_id, download_dir, regex)

    gpkg_path = get_matching_file(download_dir, regex)

    if gpkg_path is None or not os.path.isfile(gpkg_path):
        raise FileNotFoundError(f'Could not find output GeoPackage in {download_dir}')

    return gpkg_path


def get_matching_file(parent_dir: str, regex: str) -> str:
    '''
    Get the path to a file that matches the regex
    '''

    regex = re.compile(regex)
    for root, __dirs, files in os.walk(parent_dir):
        for file_name in files:
            # Check if the file name matches the regex
            if regex.match(file_name):
                return os.path.join(root, file_name)

    return None


def continue_with_huc(huc10: str, output_gpkg: str) -> bool:
    '''
    Check if the HUC already exists in the output GeoPackage
    '''

    if not os.path.isfile(output_gpkg):
        return True

    with sqlite3.connect(output_gpkg) as conn:
        curs = conn.cursor()

        # The hucs table only exists if at least one HUC has been scraped
        curs.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'hucs'")
        if curs.fetchone() is None:
            return True

        curs.execute('SELECT huc FROM hucs WHERE huc = ? LIMIT 1', [huc10])
        if curs.fetchone() is None:
            return True
        else:
            log = Logger('Scrape RME')
            log.info(f'HUC {huc10} already scraped. Skipping...')

    return False


def create_gpkg(huc10: str, rme_gpkg: str, output_gpkg: str, spatialite_path: str) -> None:
    '''
    Creates the output GeoPackage
    '''

    log = Logger('Creating GPKG')

    # Make a literal file copy of the RME output GeoPackage
    safe_makedirs(os.path.dirname(output_gpkg))
    shutil.copy(rme_gpkg, output_gpkg)

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    curs = conn.cursor()

    # Add the HUC10 column to the IGOs table
    curs.execute('ALTER TABLE igos ADD COLUMN huc10 TEXT')
    curs.execute('UPDATE igos SET huc10 = ?', [huc10])
    curs.execute('CREATE INDEX igos_huc10 ON igos (huc10, level_path, seg_distance)')

    # Add an ID column to keep track of the IGO FIDs from the old RME GeoPackages
    curs.execute('ALTER TABLE igos ADD COLUMN rme_fid INTEGER')
    curs.execute('CREATE INDEX igos_rme_fid ON igos (rme_fid)')

    # Get the names of all the columns in  the dgos table and remove the geom column
    curs.execute('PRAGMA table_info(dgos)')
    dgos_cols = [col[1] for col in curs.fetchall() if col[1] != 'geom']

    # Drop the views that depend on the dgos table
    curs.execute("select name, sql FROM sqlite_master where type = 'view' and ((name like 'vw_dgo_%') OR (name like 'vw_igo_%') )")
    dgo_views = {row[0]: row[1] for row in curs.fetchall()}
    for view_name, _sql in dgo_views.items():
        log.info(f'Dropping view {view_name}')
        curs.execute(f'DROP VIEW {view_name}')
        curs.execute('DELETE FROM gpkg_geometry_columns WHERE table_name = ?', [view_name])

    # Drop the IGO side tables
    curs.execute("SELECT name FROM sqlite_master WHERE type='table' and (name like 'igo_%') and name != 'igos'")
    igo_tables = [row[0] for row in curs.fetchall()]
    for igo_table in igo_tables:
        log.info(f'Dropping table {igo_table}')
        curs.execute(f'DROP TABLE {igo_table}')

    # Make a copy of the dgos table called dgos_temp with the same schema and data but without the geom column
    curs.execute(f'CREATE TABLE dgos_temp AS SELECT {",".join(dgos_cols)}, dgoid as rme_fid, ? huc10 FROM dgos', [huc10])
    curs.execute('DROP TABLE dgos')

    curs.execute('''
        CREATE TABLE dgos (
            dgoid INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            level_path TEXT,
            seg_distance REAL,
            centerline_length REAL,
            segment_area REAL,
            FCode INT,
            rme_fid INTEGER,
            huc10 TEXT)
    ''')

    # Copy the data from dgos_temp to dgos
    curs.execute(f'INSERT INTO dgos ({",".join(dgos_cols)}) SELECT {",".join(dgos_cols)} FROM dgos_temp')
    curs.execute('DROP TABLE dgos_temp')

    # curs.execute('ALTER TABLE dgos_temp RENAME TO dgos')
    # curs.execute('ALTER TABLE dgos ADD COLUMN rme_fid INTEGER')
    curs.execute('CREATE INDEX dgos_rme_fid ON dgos (rme_fid)')

    # curs.execute('ALTER TABLE dgos ADD COLUMN huc10 TEXT')
    curs.execute('UPDATE dgos SET huc10 = ?', [huc10])
    curs.execute('CREATE INDEX dgos_huc10 ON dgos (huc10, level_path, seg_distance)')
    curs.execute('CREATE INDEX dgos_fcode ON dgos (FCode)')

    # Change the dgos table from feature class to attributes table
    curs.execute("UPDATE gpkg_contents SET data_type = 'attributes' WHERE table_name = 'dgos'")
    curs.execute("DELETE FROM gpkg_contents WHERE table_name Like 'vw_igo_%'")
    curs.execute("DELETE FROM gpkg_contents WHERE table_name Like 'vw_dgo_%'")
    curs.execute("DELETE FROM gpkg_geometry_columns WHERE table_name = 'dgos'")
    curs.execute("DELETE FROM gpkg_ogr_contents WHERE table_name = 'dgos'")

    # Create the hucs table to keep track of progress
    curs.execute('''
        CREATE TABLE hucs(
            huc TEXT PRIMARY KEY NOT NULL,
            rme_project_id TEXT,
            scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # log.info('Creating hucs table to track progress')
    # with sqlite3.connect(output_gpkg) as conn:
    #     curs = conn.cursor()
    #     curs.execute('''
    #         CREATE TABLE hucs(
    #             huc TEXT PRIMARY KEY NOT NULL,
    #             rme_project_id TEXT,
    #             scraped_on DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    #         )
    #     ''')


def clean_up_gpkg(output_gpkg: str, spatialite_path: str) -> None:
    """ Final cleanup of the output GeoPackage
    Remove the temporary rme_fid columns on igos and dgos tables
    Create the spatial views using the IGO geometry and DGO metrics
    """

    conn = apsw.Connection(output_gpkg)
    conn.enable_load_extension(True)
    conn.load_extension(spatialite_path)
    curs = conn.cursor()

    # Drop indexes on the igos and dgos tables
    curs.execute("SELECT name, sql FROM sqlite_master WHERE type = 'index' AND sql LIKE '%rme_fid%';")
    for row in curs.fetchall():
        curs.execute(f'DROP INDEX {row[0]}')

    # Drop the temporary column for keeping track of the RME FIDs
    curs.execute('ALTER TABLE igos DROP COLUMN rme_fid')
    curs.execute('ALTER TABLE dgos DROP COLUMN rme_fid')

    # Create the views using the IGO geometry and DGO metrics
    curs.execute("SELECT name FROM sqlite_master WHERE (type = 'table') AND (name LIKE 'dgo_%') and (name != 'DGOVegetation');")
    dgo_tables = [row[0] for row in curs.fetchall()]

    # Get the columns from the dgos table
    curs.execute('PRAGMA table_info(dgos)')
    dgo_cols = ['dgos.' + col[1] for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']

    for dgo_table in dgo_tables:
        # Get the columns of the dgo side table
        curs.execute(f'PRAGMA table_info({dgo_table})')
        dgo_table_cols = ['t.' + col[1] for col in curs.fetchall() if col[1] != 'dgoid' and col[1] != 'DGOID']

        # Create the view using the IGO geometry and DGO metrics
        view_name = f'vw_{dgo_table}_metrics'
        curs.execute(f'''
            CREATE VIEW {view_name} AS
            SELECT igos.igoid,
            igos.geom,
            {",".join(dgo_table_cols)},
            {",".join(dgo_cols)}
            FROM {dgo_table} t INNER JOIN dgos ON t.dgoid=dgos.dgoid INNER JOIN igos ON dgos.huc10=igos.huc10 AND dgos.level_path=igos.level_path AND dgos.seg_distance=igos.seg_distance
        ''')

        curs.execute('''
            INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y)
            SELECT ?, 'features', ?, min_x, min_y, max_x, max_y FROM gpkg_contents WHERE table_name = 'igos'
        ''', [view_name, view_name])

        curs.execute('''
            INSERT INTO gpkg_geometry_columns (table_name, column_name, geometry_type_name, srs_id, z, m)
            SELECT ?, 'geom', 'POINT', 4326, 0, 0 FROM gpkg_geometry_columns WHERE table_name = 'igos'
        ''', [view_name])

    curs.execute('VACUUM')


def main():
    '''
    Scrape RME projects. Combine IGOs with their geometries. Include DGO metrics only.
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', help='Environment: staging or production', type=str)
    parser.add_argument('spatialite_path', help='Path to the mod_spatialite library', type=str)
    parser.add_argument('working_folder', help='top level folder for downloads and output', type=str)
    parser.add_argument('tags', help='Data Exchange tags to search for projects', type=str)
    parser.add_argument('project_name', help='Name for the output project', type=str)
    parser.add_argument('--delete', help='Whether or not to delete downloaded GeoPackages',  action='store_true', default=False)
    parser.add_argument('--huc_filter', help='HUC filter begins with (e.g. 14)', type=str, default='')
    args = dotenv.parse_args_env(parser)

    # Set up some reasonable folders to store things
    working_folder = args.working_folder
    download_folder = os.path.join(working_folder, 'downloads')
    project_dir = os.path.join(working_folder, 'project')  # , 'outputs', 'riverscapes_metrics.gpkg')
    safe_makedirs(project_dir)

    log = Logger('Setup')
    log_path = os.path.join(project_dir, 'rme-scrape.log')
    log.setup(log_path=log_path, log_level=logging.DEBUG)

    # Data Exchange Search Params
    search_params = RiverscapesSearchParams({
        'tags': args.tags.split(','),
        'projectTypeId': 'rs_metric_engine',
    })

    # Optional HUC filter
    if args.huc_filter != '' and args.huc_filter != '.':
        search_params.meta = {
            "HUC": args.huc_filter
        }

    with RiverscapesAPI(stage=args.stage) as api:
        scrape_rme(args.stage, api, log_path, args.spatialite_path, search_params, download_folder, project_dir, args.project_name, args.delete)

    log.info('Process complete')


if __name__ == '__main__':
    main()
