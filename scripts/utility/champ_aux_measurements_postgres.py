import argparse
import os
import json
import psycopg2
import psycopg2.extras
import shutil
import datetime
from rsxml.util import safe_makedirs
from rsxml import ProgressBar, dotenv, Logger
from psycopg2.extensions import cursor as Cursor
from new_project_upload import upload_project
from pydex import RiverscapesAPI
from rsxml.project_xml import Project, Dataset, Meta, MetaData

TABLES_TO_SKIP = [
    'channel_unit_metrics',
    'channel_unit_tiers',
    'crew',
    'metric_plots',
    'visits',
    'sites',
    'statues',

    'projects',
    'programs',
    'project_types',
    'statuses',
    'visit_metrics',
    'watersheds',
    'vw_visits',
    'vw_projects',
    'metric_definitions',

    'livestock',
    'electropasstransectfish',
    'slopeandbearing',
    'slopeandbearingsetup'
]

COLUMNS_TO_SKIP = [
    'programsiteid',
    'sitename',
    'watershedid',
    'watershedname',
    'sampledate',
    'hitchname',
    'crewname',
    'visityear',
    'iterationid',
    'categoryname',
    'panelname',
    'visitdate',
    'protocolid',
    'programid',
    'aem',
    'bug validation',
    'champ 10% revisit',
    'champ core',
    'champ-pibo comparison',
    'effectiveness',
    'has fish data',
    'imw',
    'remove',
    'velocity validation',
    'primary visit',
    'qc visit',
    'error',
    'no',
    'yes',
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
    'Cross section',
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
    'Large Woody Piece',
    'Mid Channel Bottom of Site',
    'Monthly SolarPathfinder Result Measurement',
    'Monument',
    'Pebble',
    'Pebble Cross Section',
    'Pool Tail Fines',
    'Riparian Structure',
    'Sample Biomasses',
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
    'Taxon By Size Class Counts',
    'Topo Tool Log Entries',
    'Topo Tool Messages',
    'Topographic Info Corrected',
    'Topographic Info Original',
    'Transect',
    'Transect Photos',
    'Undercut Banks',
    'Visit Information',
    'Water Chemistry',
    'Woody Debris Jam'
]


def process_champ_visits(api: RiverscapesAPI, curs: psycopg2.extensions.cursor, download_dir: str, delete_files: bool, project_owner: str) -> None:

    log = Logger('CHaMP_Aux_Measurements')

    # Get a list of all the tables in the public schema
    curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = curs.fetchall()

    # Remove tables that we want to skip
    table_names = {table['table_name']: {'multirow': None, 'columns': []} for table in tables if table['table_name'] not in TABLES_TO_SKIP}
    log.info(f'Found {len(table_names)} measurement tables to process.')

    for table_name in table_names.keys():
        # Get the columns for each table
        curs.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,))
        columns = curs.fetchall()
        table_names[table_name]['columns'] = [column['column_name'] for column in columns if column['column_name'] not in COLUMNS_TO_SKIP]

        try:
            # Get the maximum number of rows per visit across all tables
            curs.execute(f"""
                select max(tally)
                from (SELECT visitid, count(*) tally
                FROM {table_name}
                group by visitid)""")
            max_rows = curs.fetchone()
            table_names[table_name]['multirow'] = max_rows[0] and max_rows[0] > 1
        except Exception as e:
            log.error(f'Error determining max rows for table {table_name}: {e}')
            table_names[table_name]['multirow'] = False

    # Get all the visits that still require aux
    # Retrieve CHaMP visit IDs that have topo projects, but are missing aux measurements
    curs.execute('''
        SELECT v.visit_id, s.name, w.name, v.visit_year, p.guid
        FROM visits v
            inner join sites s on v.site_id = s.site_id
            inner join watersheds w on s.watershed_id = w.watershed_id
                inner join projects p on v.visit_id = p.visit_id
        WHERE (program_id = 1)
        AND (project_type_id = 1)
        AND (guid IS NOT NULL)
        AND (aux_uploaded IS NULL)
        ORDER BY w.name, s.name, v.visit_year
    ''')

    visits = {
        row[0]: {
            'site_name': row[1],
            'watershed_name': row[2],
            'visit_year': row[3],
            'project_guid': row[4]
        } for row in curs.fetchall()
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

            visit_aux_files = {}
            for table in tables:
                # This postgres table name will be lower case and have no spaces
                table_name = table['table_name']
                if table_name in TABLES_TO_SKIP:
                    continue

                curs.execute(f'SELECT COUNT(*) FROM {table_name} WHERE visitid = %s', (visit_id,))
                row_count = curs.fetchone()[0]
                if row_count == 0:
                    continue

                clean_table_name = table_name
                if table_name == 'driftinverterbratesample':
                    clean_table_name = 'driftinvertebratesample'
                elif table_name == 'undercutbank':
                    clean_table_name = 'undercutbanks'
                elif table_name == 'samplebiomass':
                    clean_table_name = 'samplebiomasses'
                elif table_name == 'taxonbysizeclasscount':
                    clean_table_name = 'taxonbysizeclasscounts'
                elif table_name == 'transectphoto':
                    clean_table_name = 'transectphotos'

                final_file_name = None
                # These measurment names have spaces and capitalization
                for measurement_name in MEASUREMENT_NAMES:
                    measurement_name_no_spaces = measurement_name.replace(' ', '').replace('_', '').lower()
                    if clean_table_name.lower() == measurement_name_no_spaces.lower():
                        # The final table name will have underscores instead of spaces, and be lower case
                        final_file_name = measurement_name.replace(' ', '_').lower()
                        break

                if final_file_name is None:
                    raise Exception(f"Table name {clean_table_name} not found in measurement names.")

                record_data = []
                curs.execute(f'SELECT * FROM {table_name} WHERE visitid = %s', (visit_id,))
                for row in curs.fetchall():
                    row_data = dict(row)
                    for col in COLUMNS_TO_SKIP:
                        row_data.pop(col, None)

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

            # Download the project.rs.xml file into the visit dir
            api.download_files(project_guid, visit_dir, ['project\\.rs\\.xml$'], force=True)
            project_xml_path = os.path.join(visit_dir, 'project.rs.xml')

            if not os.path.exists(project_xml_path):
                log.error(f'project.rs.xml not found for visit ID {visit_id} project ID {project_guid}')
                continue

            log.info(f'Prepared {len(visit_aux_files)} aux measurement files for visit ID {visit_id}')

            # Load the project XML and update it to reference the new aux measurement JSON files
            project = Project.load_project(project_xml_path)
            datasets = project.common_datasets
            for aux_file, (measurement_name, clean_name) in visit_aux_files.items():
                datasets.append(Dataset(
                    xml_id=f'CHAMP_Aux_{clean_name}'.upper(),
                    name=measurement_name,
                    path=os.path.relpath(aux_file, visit_dir),
                    ds_type='File',
                    meta_data=MetaData([Meta('measurementType', measurement_name)])
                ))
            project.write()

            # Upload the project found in this folder. This will include the new aux measurement JSON files.
            upload_project(api, project_xml_path, project_guid, project_owner, 'PUBLIC')
            log.info(f'Uploaded aux measurements for visit ID {visit_id}')

            # Track progress by updating the aux_uploaded flag in the Postgres database
            curs.execute('UPDATE visits SET aux_uploaded = %s WHERE visit_id = %s', (datetime.datetime.now(), visit_id))
            curs.connection.commit()

            # Optionally delete the downloaded files to save space
            if delete_files is True:
                try:
                    shutil.rmtree(visit_dir)
                except Exception as e:
                    log.error(f'Error deleting files for visit ID {visit_id}: {e}')

                processed += 1
                progbar.update(processed)
        except Exception as e:
            print(f'Error processing visit ID {visit_id}: {e}')
            errors += 1

    progbar.finish()
    print(f'Process complete. {processed} visits processed. {errors} errors encountered.')


def json_serial(obj):
    """JSON serializer for objects not serializable by default"""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def main():
    parser = argparse.ArgumentParser(description="Export all measurements from the database to JSON files.")
    parser.add_argument('stage', type=str, help='RiverscapesAPI stage to connect to (e.g., DEV, TEST, PROD).')
    parser.add_argument('db_service', type=str, help='Name of postgres service in .pg_service.conf to connect to the database.')
    parser.add_argument('download_dir', help='Path to the download directory to temporarily store visit files', type=str)
    parser.add_argument('delete_files', help='Whether to delete downloaded files after upload', type=bool)
    parser.add_argument('project_owner', help='RDx organization Owner GUID for CHaMP projects', type=str)
    args = dotenv.parse_args_env(parser)

    conn = psycopg2.connect(service=args.db_service)
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    with RiverscapesAPI(stage=args.stage) as api:
        process_champ_visits(api, curs, args.download_dir, args.delete_files, args.project_owner)


if __name__ == "__main__":
    main()
