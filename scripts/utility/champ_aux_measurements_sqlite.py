import argparse
import os
import json
import psycopg2
import psycopg2.extras
import sqlite3
import shutil
import datetime
from rsxml.util import safe_makedirs
from rsxml import ProgressBar, dotenv, Logger
from psycopg2.extensions import cursor as Cursor
from new_project_upload import upload_project
from pydex import RiverscapesAPI
from rsxml.project_xml import Project, Dataset, Meta, MetaData

# from scripts.utility.champ_aux_measurements_postgres import COLUMNS_TO_SKIP

TABLES_TO_SKIP = [
    'sqlite_master',
    'Visits',
    'Livestock',
    'ElectroPassTransectFish',
    'SlopeAndBearing',
    'SlopeAndBearingSetup'
]


MEASUREMENT_NAMES = [
    'Air Temp Logger Output',
    'Air Temperature Logger',
    'Air Temperature Result Measurement',
    'Artificially Placed Instream Structure Photo',
    'Artificially Placed Instream Structures',
    'Bankfull Width',
    'Benchmark',
    'Channel Constraint Measurements',
    'Channel Constraints',
    'Channel Segment',
    'Channel Unit',
    'Channel Unit Supplement',
    'Control Point',
    'Crew',
    'Cross Section',
    'Daily Solar Access Meas',
    'Daily Solar Access Trans',
    'Discharge',
    'Drift Invertebrate Sample',
    'Drift Invertebrate Sample Result',
    'Electro Pass',
    'Electro Setup',
    'Fish Cover',
    'Jam Has Channel Unit',
    'Large Woody Debris',
    'Large Wood Piece',
    'Mid Channel Bottom Of Site',
    'Monthly SolarPathfinder Result Measurement',
    'Monument',
    'Pebble',
    'Pebble Cross Section',
    'Pool Tail Fines',
    'Riparian Structure',
    'Sample Biomass',
    'Side Channel',
    'Site Marker',
    'Snorkel Fish',
    'Snorkel Fish Count Binned',
    'Snorkel Fish Count Steelhead Binned',
    'Snorkel Fish Species Present',
    'Snorkel Fish Young of the Year Salmon',
    'Snorkel Import',
    'Snorkel Lane',
    'Snorkel Relative Abundance',
    'Snorkel Setup',
    'Solar Input Result',
    'Solar Pathfinder',
    'Stream Temperature Logger',
    'Stream Temperature Logger Maintenance',
    'Stream Temperature Result File',
    'Stream Temperature Result Measurement',
    'Stream Temperature Result Message',
    'Streambank',
    'Substrate Cover',
    'Supplementary Photo',
    'Targeted Riffle Sample',
    'Targeted Riffle Sample Replicate',
    'Taxon By Size Class Count',
    'Topo Tool Log Entries',
    'Topo Tool Messages',
    'Topographic Info Corrected',
    'Topographic Info Original',
    'Transect',
    'Transect Photo',
    'Undercut Bank',
    'Visit Information',
    'Water Chemistry',
    'Woody Debris Jam'
]


def process_champ_visits(api: RiverscapesAPI, sqlite_curs: sqlite3.Cursor, pg_curs: psycopg2.extensions.cursor, download_dir: str, delete_files: bool, project_owner: str, visit_id: int = None, watershed: str = None, year: int = None) -> None:

    log = Logger('CHaMP_Aux_Measurements')

    # Get a list of all the tables in the SQLite database and remove the ones we want to skip
    sqlite_curs.execute("SELECT name as table_name FROM sqlite_master WHERE type='table';")
    tables = sqlite_curs.fetchall()
    table_names = {table['table_name']: {'multirow': None, 'columns': []} for table in tables if table['table_name'] not in TABLES_TO_SKIP}
    log.info(f'Found {len(table_names)} measurement tables to process.')

    for table_name in table_names.keys():
        # Get the columns for each table
        sqlite_curs.execute(f"PRAGMA table_info({table_name})")
        table_names[table_name]['columns'] = [column['name'] for column in sqlite_curs.fetchall()]

        try:
            # Get the maximum number of rows per visit across all tables
            sqlite_curs.execute(f'SELECT max(tally) FROM (SELECT count(*) tally FROM {table_name} GROUP BY visitid)')
            max_rows = sqlite_curs.fetchone()
            table_names[table_name]['multirow'] = max_rows[0] and max_rows[0] > 1
        except Exception as e:
            log.error(f'Error determining max rows for table {table_name}: {e}')
            table_names[table_name]['multirow'] = False

    # Get all the visits that have a topo project but still require aux measurements
    # The optional parameters will filter to just specific visits/watersheds/years for debugging
    pg_curs.execute('''
        SELECT v.visit_id, s.name, w.name, v.visit_year, p.guid
        FROM visits v
            inner join sites s on v.site_id = s.site_id
            inner join watersheds w on s.watershed_id = w.watershed_id
                inner join projects p on v.visit_id = p.visit_id
        WHERE (program_id = 1)
        AND (project_type_id = 1)
        AND (guid IS NOT NULL)
        AND (aux_uploaded IS NULL)
        AND (%s IS NULL OR v.visit_id = %s)
        AND (%s IS NULL OR w.name = %s)
        AND (%s IS NULL OR v.visit_year = %s)
        ORDER BY w.name, s.name, v.visit_year
    ''', [visit_id, visit_id, watershed, watershed, year, year])

    visits = {
        row[0]: {
            'site_name': row[1],
            'watershed_name': row[2],
            'visit_year': row[3],
            'project_guid': row[4]
        } for row in pg_curs.fetchall()
    }
    log.info(f'Found {len(visits)} CHaMP visits with topo projects that require aux measurement upload.')

    processed = 0
    errors = 0
    progbar = ProgressBar(len(visits), 50, 'CHaMP Aux', byte_format=True)

    for visit_id, visit_data in visits.items():
        try:
            site_name = visit_data['site_name']
            watershed_name = visit_data['watershed_name']
            visit_year = visit_data['visit_year']
            project_guid = visit_data['project_guid']
            log.info(f'Processing visit ID {visit_id} ({watershed_name} - {site_name} - {visit_year})')

            # Create a dedicated visit directory inside the download dir, with an aux directory inside it.
            visit_dir = os.path.join(download_dir, f'visit_{visit_id}_{project_guid}')
            aux_dir = os.path.join(visit_dir, 'aux_measurements')

            # Download the project.rs.xml file into the visit dir
            api.download_files(project_guid, visit_dir, ['project\\.rs\\.xml$'], force=True)
            api.download_files(project_guid, visit_dir, ['aux_measurements.*\\.json$'], force=True)
            project_xml_path = os.path.join(visit_dir, 'project.rs.xml')

            if not os.path.exists(project_xml_path):
                log.error(f'project.rs.xml not found for visit ID {visit_id} project ID {project_guid}')
                continue

            visit_aux_files = {}
            for table in tables:
                # This postgres table name will be lower case and have no spaces
                table_name = table['table_name']
                if table_name in TABLES_TO_SKIP:
                    continue

                sqlite_curs.execute(f'SELECT COUNT(*) FROM {table_name} WHERE visitid = ?', (visit_id,))
                row_count = sqlite_curs.fetchone()[0]
                if row_count == 0:
                    continue

                clean_table_name = table_name

                # Access tables misspelled "Invertebrate"
                if 'Inverterbrate' in clean_table_name:
                    clean_table_name = clean_table_name.replace('Inverterbrate', 'Invertebrate')

                final_file_name = None
                # These measurment names have spaces and capitalization
                for measurement_name in MEASUREMENT_NAMES:
                    measurement_name_no_spaces = measurement_name.replace(' ', '').replace('_', '')
                    if clean_table_name == measurement_name_no_spaces:
                        # The final table name will have underscores instead of spaces, and be lower case
                        final_file_name = measurement_name.replace(' ', '_')
                        break

                if final_file_name is None:
                    raise Exception(f"Table name {clean_table_name} not found in measurement names.")

                record_data = []
                sqlite_curs.execute(f'SELECT * FROM {table_name} WHERE visitid = ?', (visit_id,))
                for row in sqlite_curs.fetchall():
                    row_data = dict(row)
                    # for col in COLUMNS_TO_SKIP:
                    #     row_data.pop(col, None)

                    clean_data = {"note": "",
                                  "MeasurementType": measurement_name,
                                  "qaDecision": "None",
                                  "value": row_data,
                                  "objectType": "Measurement",
                                  }

                    record_data.append(clean_data)

                if len(record_data) > 0:

                    # This is meant to simulate the structure of the data returned from the old API
                    record_data = {"value": record_data}

                    aux_file_path = os.path.join(aux_dir, f'{final_file_name}.json')
                    if aux_file_path in visit_aux_files:
                        log.error(f'Duplicate aux measurement file for {measurement_name} at {aux_file_path}, overwriting.')
                        continue

                    os.makedirs(os.path.dirname(aux_file_path), exist_ok=True)
                    with open(os.path.join(aux_file_path), 'w', encoding='utf-8') as json_file:
                        json.dump(record_data, json_file, default=json_serial, indent=4)

                visit_aux_files[aux_file_path] = (measurement_name, os.path.splitext(os.path.basename(aux_file_path))[0])

            if len(visit_aux_files) == 0:
                log.info(f'No aux measurement files found for visit ID {visit_id}, skipping upload.')
                continue

            proj = api.get_project_full(project_guid)
            tags = proj.tags

            if 'CHaMP' in tags:
                tags.remove('CHaMP')

            if 'CHaMP_Watershed_South Fork Salmon' in tags:
                tags.remove('CHAMP_Watershed_South Fork Salmon')

            watershed_tag = f'CHAMP_Watershed_{watershed_name.replace(" ", "_").replace("(", "").replace(")", "")}'
            if watershed_tag not in tags:
                tags.append(watershed_tag)

            site_tag = f'CHAMP_Site_{site_name.replace(" ", "_")}'
            if site_tag not in tags:
                tags.append(site_tag)

            if f'CHAMP_Year_{visit_year}' not in tags:
                tags.append(f'CHAMP_Year_{visit_year}')

            visit_tag1 = f'CHAMP_Visit_{visit_id}'
            if visit_tag1 not in tags:
                tags.append(visit_tag1)

            visit_tag2 = f'CHAMP_Visit_{str(visit_id).zfill(4)}'
            if visit_tag2 != visit_tag1 and visit_tag2 not in tags:
                tags.append(visit_tag2)

            log.info(f'Prepared {len(visit_aux_files)} aux measurement files for visit ID {visit_id}')

            # Load the project XML and update it to reference the new aux measurement JSON files
            project = Project.load_project(project_xml_path)
            datasets = project.common_datasets
            for aux_file, (measurement_name, clean_name) in visit_aux_files.items():
                xml_id = f'CHAMP_Aux_{clean_name}'.upper()

                ds_exists = False
                for ds in datasets:
                    if ds.xml_id == xml_id:
                        log.info(f'Dataset with XML ID {xml_id} already exists in project, skipping addition.')
                        ds.path = os.path.relpath(aux_file, visit_dir)
                        ds_exists = True
                        break

                if ds_exists is False:
                    datasets.append(Dataset(
                        xml_id=xml_id,
                        name=measurement_name,
                        path=os.path.relpath(aux_file, visit_dir),
                        ds_type='File',
                        meta_data=MetaData([Meta('measurementType', measurement_name)])
                    ))
            project.write()

            # Upload the project found in this folder. This will include the new aux measurement JSON files.
            upload_project(api, project_xml_path, project_guid, project_owner, 'PUBLIC', tags, no_wait=True)
            log.info(f'Uploaded aux measurements for visit ID {visit_id}')

            # Track progress by updating the aux_uploaded flag in the Postgres database
            pg_curs.execute('UPDATE visits SET aux_uploaded = %s WHERE visit_id = %s', (datetime.datetime.now(), visit_id))
            pg_curs.connection.commit()

            # Optionally delete the downloaded files to save space
            if delete_files is True:
                try:
                    shutil.rmtree(visit_dir)
                except Exception as e:
                    log.error(f'Error deleting files for visit ID {visit_id}: {e}')

                processed += 1
                progbar.update(processed)
        except Exception as e:
            log.error(f'Error processing visit ID {visit_id}: {e}')
            errors += 1

    progbar.finish()
    print(f'Process complete. {processed} visits processed. {errors} errors encountered.')


def json_serial(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    """
    Main function to parse arguments and initiate processing.
    """
    parser = argparse.ArgumentParser(description="Export all measurements from the database to JSON files.")
    parser.add_argument('stage', type=str, help='RiverscapesAPI stage to connect to (e.g., DEV, TEST, PROD).')
    parser.add_argument('db_service', type=str, help='Name of postgres service in .pg_service.conf to connect to the database.')
    parser.add_argument('sqlite_db', type=str, help='Path to the all measurements SQLite database file.')
    parser.add_argument('download_dir', help='Path to the download directory to temporarily store visit files', type=str)
    parser.add_argument('delete_files', help='Whether to delete downloaded files after upload', type=bool)
    parser.add_argument('project_owner', help='RDx organization Owner GUID for CHaMP projects', type=str)
    parser.add_argument('--visit_id', type=int, help='Optional Visit ID to just debug a single visit', default=None)
    parser.add_argument('--watershed', type=str, help='Optional watershed name to filter visits', default=None)
    parser.add_argument('--year', type=int, help='Optional year to filter visits', default=None)
    parser.add_argument('--verbose', help='(optional) a little extra logging ', action='store_true', default=False)
    args = dotenv.parse_args_env(parser)

    log = Logger('CHaMP_Aux_Measurements_Postgres')
    log.setup(log_path=os.path.join(args.download_dir, "champ_aux_measurements.log"), log_level=args.verbose)
    log.info('Starting CHaMP Aux Measurements Postgres processing.')

    if args.visit_id is not None and (args.watershed is not None or args.year is not None):
        raise Exception('Visit ID filter cannot be used with watershed or year filters.')

    if args.visit_id:
        log.info(f'Processing only visit ID {args.visit_id} for debugging')

    if args.watershed:
        log.info(f'Filtering to watershed {args.watershed}')

    if args.year:
        log.info(f'Filtering to year {args.year}')

    pg_conn = psycopg2.connect(service=args.db_service)
    pg_curs = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    with sqlite3.connect(args.sqlite_db) as sqlite_conn:
        # include a row factory to return dicts
        sqlite_conn.row_factory = sqlite3.Row
        sqlite_curs = sqlite_conn.cursor()
        with RiverscapesAPI(stage=args.stage) as api:
            process_champ_visits(api, sqlite_curs, pg_curs, args.download_dir, args.delete_files, args.project_owner, visit_id=args.visit_id, watershed=args.watershed, year=args.year)


if __name__ == "__main__":
    main()
